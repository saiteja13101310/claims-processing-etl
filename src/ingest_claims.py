"""
Claims Processing ETL - Bronze ingestion job (PySpark, serverless).

Pipeline steps
--------------
1. Read every CSV file matching --landing-path in the landing UC volume.
2. Validate the input contract (required columns present).
3. Classify each row: valid vs invalid (null/empty claim_id, unparseable
   claim_date, null/non-positive claim_amount) - invalid rows are
   quarantined with a reason, never silently dropped.
4. Deduplicate by claim_id, keeping the latest claim_date.
5. Transform: trim/normalize strings, cast types, derive claim_year.
6. Idempotent Delta MERGE into <catalog>.<schema>.<table> on claim_id,
   and into the quarantine table on quarantine_key.
7. Log a full processing summary; exit 1 on failure, 0 on success
   (including "no files landed yet", which is not an error for a
   scheduled job).

The script is pure PySpark (no pandas, no UDFs) and every step is a
small function so unit tests in tests/test_claims.py can exercise them
without touching Unity Catalog.
"""

import argparse
import logging
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

LOG = logging.getLogger("claims_ingestion")

# The contract every landing file must satisfy.
REQUIRED_COLUMNS = [
    "claim_id",
    "policy_number",
    "claim_date",
    "claim_amount",
    "status",
    "claimant_name",
    "region",
]
DATE_FORMAT = "yyyy-MM-dd"


class NoFilesLanded(Exception):
    """Raised when the landing path matches no files (not a job failure)."""


def get_spark() -> SparkSession:
    """One Spark session for the whole job (serverless: no cluster config)."""
    return SparkSession.builder.appName("claims_ingestion").getOrCreate()


def read_landing(spark: SparkSession, landing_path: str) -> DataFrame:
    """Read all landing CSVs as raw strings + capture the source file.

    Reading everything as STRING is deliberate: we never trust landing
    data, and casting happens in validate/transform where we can tell
    a *bad value* from a *missing column*.
    """
    df = (
        spark.read.option("header", True)
        .option("mode", "PERMISSIVE")
        .csv(landing_path)
    )
    if df.isEmpty():
        raise NoFilesLanded(landing_path)
    return df.withColumn("_source_file", F.input_file_name())


def validate_schema(df: DataFrame) -> None:
    """Fail fast if the landing contract is broken (missing columns)."""
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(
            f"Landing contract violated: missing required columns {missing}. "
            f"Found columns: {df.columns}"
        )


def _valid_flags(df: DataFrame):
    """Per-row boolean flags for each validation rule (native expressions)."""
    claim_id_ok = F.col("claim_id").isNotNull() & (F.trim(F.col("claim_id")) != "")
    date_ok = F.to_date(F.col("claim_date"), DATE_FORMAT).isNotNull()
    amount_ok = (
        F.col("claim_amount").isNotNull()
        & (F.col("claim_amount").cast("DECIMAL(12,2)").isNotNull())
        & (F.col("claim_amount").cast("DECIMAL(12,2)") > 0)
    )
    return claim_id_ok, date_ok, amount_ok


def split_valid_invalid(df: DataFrame):
    """Split raw rows into (valid, invalid). Invalid rows keep a reason.

    Null status is NOT treated as invalid - it is normalized to UNKNOWN
    during transform. Only keys, dates and amounts can quarantine a row.
    """
    claim_id_ok, date_ok, amount_ok = _valid_flags(df)
    is_valid = claim_id_ok & date_ok & amount_ok

    invalid_reason = F.concat_ws(
        "; ",
        F.when(~claim_id_ok, F.lit("missing_or_empty_claim_id")),
        F.when(~date_ok, F.lit("invalid_claim_date")),
        F.when(~amount_ok, F.lit("missing_or_invalid_claim_amount")),
    )

    invalid = (
        df.filter(~is_valid)
        .withColumn("invalid_reason", invalid_reason)
        .withColumn(
            "quarantine_key",
            F.coalesce(
                F.when(claim_id_ok, F.trim(F.col("claim_id"))),
                # No usable claim_id -> hash the whole row so identical bad
                # rows collapse to one quarantine record across re-runs.
                F.md5(
                    F.concat_ws(
                        "|",
                        F.col("policy_number"),
                        F.col("claim_date"),
                        F.col("claim_amount"),
                        F.col("status"),
                        F.col("claimant_name"),
                        F.col("region"),
                    )
                ),
            ),
        )
    )
    valid = df.filter(is_valid)
    return valid, invalid


def deduplicate(df: DataFrame) -> DataFrame:
    """Keep one row per claim_id - the one with the latest claim_date.

    monotonically_increasing_id() is captured as a deterministic tie-breaker
    for rows sharing claim_id AND claim_date.
    """
    ranked = (
        df.withColumn("_order", F.monotonically_increasing_id())
        .withColumn(
            "_rank",
            F.row_number().over(
                Window.partitionBy("claim_id").orderBy(
                    F.to_date(F.col("claim_date"), DATE_FORMAT).desc(),
                    F.col("_order").desc(),
                )
            ),
        )
    )
    return ranked.filter(F.col("_rank") == 1).drop("_rank", "_order")


def transform(df: DataFrame) -> DataFrame:
    """Map validated raw rows to the bronze claims_raw schema."""
    claim_date = F.to_date(F.col("claim_date"), DATE_FORMAT)
    return df.select(
        F.trim(F.col("claim_id")).alias("claim_id"),
        F.trim(F.col("policy_number")).alias("policy_number"),
        claim_date.alias("claim_date"),
        F.col("claim_amount").cast("DECIMAL(12,2)").alias("claim_amount"),
        F.upper(F.trim(F.coalesce(F.col("status"), F.lit("UNKNOWN")))).alias("status"),
        F.trim(F.col("claimant_name")).alias("claimant_name"),
        F.upper(F.trim(F.col("region"))).alias("region"),
        F.year(claim_date).alias("claim_year"),
        F.current_timestamp().alias("_ingested_at"),
        F.coalesce(F.col("_source_file"), F.lit("unknown")).alias("_source_file"),
    )


def _to_quarantine_schema(df: DataFrame) -> DataFrame:
    """Map invalid raw rows to the bronze claims_quarantine schema."""
    return df.select(
        F.col("quarantine_key"),
        F.trim(F.col("claim_id")).alias("claim_id"),
        F.col("policy_number"),
        F.col("claim_date").alias("claim_date"),      # raw string, unparsed
        F.col("claim_amount").alias("claim_amount"),  # raw string, unparsed
        F.col("status"),
        F.col("claimant_name"),
        F.col("region"),
        F.col("invalid_reason"),
        F.current_timestamp().alias("_quarantined_at"),
    )


def merge_upsert(spark: SparkSession, df: DataFrame, full_table: str, key: str) -> dict:
    """Idempotent Delta MERGE into a Unity Catalog table.

    The delta import is lazy so unit tests of the pure transform functions
    run on plain local Spark without delta-spark installed.
    Returns the Delta operation metrics for logging.
    """
    from delta.tables import DeltaTable  # imported lazily on purpose

    target = DeltaTable.forName(spark, full_table)
    (
        target.alias("t")
        .merge(df.alias("s"), f"s.{key} = t.{key}")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    history = target.history(1).select("operationMetrics").collect()[0][0]
    return dict(history) if history else {}


def run(args: argparse.Namespace, spark: SparkSession = None) -> int:
    spark = spark or get_spark()
    target_table = f"{args.catalog}.{args.schema}.{args.table}"
    quarantine_table = f"{args.catalog}.{args.schema}.{args.quarantine_table}"

    try:
        raw = read_landing(spark, args.landing_path)
    except NoFilesLanded:
        LOG.warning("No CSV files found at %s - nothing to ingest.", args.landing_path)
        return 0

    validate_schema(raw)

    valid_raw, invalid_raw = split_valid_invalid(raw)
    deduped = deduplicate(valid_raw)
    final_df = transform(deduped)
    quarantine_df = _to_quarantine_schema(invalid_raw)

    # Small landing volumes make these count() actions cheap. For large
    # volumes, drop them and rely on the MERGE metrics logged below.
    total_n = raw.count()
    valid_n = valid_raw.count()
    deduped_n = deduped.count()
    invalid_n = invalid_raw.count()

    LOG.info("Landing summary for %s", args.landing_path)
    LOG.info("  rows read           : %d", total_n)
    LOG.info("  valid rows          : %d", deduped_n)
    LOG.info("  duplicates dropped  : %d (latest claim_date kept)", valid_n - deduped_n)
    LOG.info("  invalid rows        : %d (quarantined)", invalid_n)

    metrics = merge_upsert(spark, final_df, target_table, "claim_id")
    LOG.info(
        "MERGE into %s -> inserted=%s updated=%s",
        target_table,
        metrics.get("numTargetRowsInserted", "?"),
        metrics.get("numTargetRowsUpdated", "?"),
    )

    if invalid_n > 0:
        q_metrics = merge_upsert(spark, quarantine_df, quarantine_table, "quarantine_key")
        LOG.info(
            "MERGE into %s -> inserted=%s updated=%s",
            quarantine_table,
            q_metrics.get("numTargetRowsInserted", "?"),
            q_metrics.get("numTargetRowsUpdated", "?"),
        )
    else:
        LOG.info("No invalid records - quarantine table untouched.")

    LOG.info("Claims ingestion completed successfully.")
    return 0


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Claims bronze ingestion ETL")
    parser.add_argument("--catalog", required=True, help="Unity Catalog catalog")
    parser.add_argument("--schema", required=True, help="Schema inside the catalog")
    parser.add_argument("--table", required=True, help="Target Delta table")
    parser.add_argument("--quarantine-table", required=True,
                        help="Table for records failing validation")
    parser.add_argument("--landing-path", required=True,
                        help="UC volume glob, e.g. /Volumes/c/s/v/*.csv")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    try:
        return run(parse_args(argv))
    except Exception:
        LOG.exception("Claims ingestion FAILED")
        return 1


if __name__ == "__main__":
    sys.exit(main())
