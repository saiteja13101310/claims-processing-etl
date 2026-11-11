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
import os
import sys
import uuid
from datetime import datetime, timezone

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

# try_to_date is a Databricks SQL extension: on Databricks (ANSI mode on)
# plain to_date RAISES on unparseable input, and bad rows must be
# quarantined, not crash the job. OSS Spark 3.5 (local unit tests) has no
# try_to_date, but runs with ANSI off, where to_date already returns NULL
# for unparseable input - the same effective semantics. Detect the engine
# once and parse dates accordingly.
_ON_DATABRICKS = bool(
    os.environ.get("DATABRICKS_RUNTIME_VERSION") or os.environ.get("SPARK_REMOTE")
)


def _parse_date(date_col: str):
    """Parse DATE_FORMAT to a date: NULL - never an error - on bad input."""
    if _ON_DATABRICKS:
        return F.expr(f"try_to_date({date_col}, '{DATE_FORMAT}')")
    return F.to_date(date_col, DATE_FORMAT)


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
    try:
        df = (
            spark.read.option("header", True)
            .option("mode", "PERMISSIVE")
            .csv(landing_path)
        )
    except Exception as exc:
        # A glob that matches nothing (empty landing volume - the normal
        # state of a fresh prod environment) raises PATH_NOT_FOUND before
        # any DataFrame exists. That is the same "no files landed yet"
        # condition, not a failure. Match both the Databricks error class
        # name and the OSS Spark message for engine portability.
        if "PATH_NOT_FOUND" in str(exc) or "Path does not exist" in str(exc):
            raise NoFilesLanded(landing_path) from exc
        raise
    if df.isEmpty():
        raise NoFilesLanded(landing_path)
    # input_file_name() is not supported on Unity Catalog / serverless
    # (Spark Connect) - the supported replacement is the _metadata column.
    return df.withColumn("_source_file", F.col("_metadata.file_path"))


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
    # try_to_date (not to_date): on serverless ANSI mode, to_date RAISES on
    # unparseable input instead of returning NULL - and these rows are meant
    # to be quarantined, not crash the job.
    date_ok = _parse_date("claim_date").isNotNull()
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
                    _parse_date("claim_date").desc(),
                    F.col("_order").desc(),
                )
            ),
        )
    )
    return ranked.filter(F.col("_rank") == 1).drop("_rank", "_order")


def transform(df: DataFrame) -> DataFrame:
    """Map validated raw rows to the bronze claims_raw schema."""
    claim_date = _parse_date("claim_date")
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


def _utc_now():
    """Timezone-aware UTC now (used in the per-run audit trail)."""
    return datetime.now(timezone.utc)


def write_audit_row(spark: SparkSession, audit_table: str, **values) -> None:
    """Append one per-run summary row to the claims_run_audit table.

    Best-effort by design: when this runs the data MERGEs have already
    committed, so a problem writing the audit row is logged but never
    fails the pipeline.
    """
    from pyspark.sql.types import (
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    audit_spec = [
        ("run_id", StringType()),
        ("started_at", TimestampType()),
        ("finished_at", TimestampType()),
        ("landing_path", StringType()),
        ("rows_read", LongType()),
        ("valid_rows", LongType()),
        ("duplicates_dropped", LongType()),
        ("invalid_rows", LongType()),
        ("raw_rows_inserted", LongType()),
        ("raw_rows_updated", LongType()),
        ("quarantine_rows_inserted", LongType()),
        ("status", StringType()),
        ("error_message", StringType()),
    ]
    try:
        # NOTE: audit_spec holds type INSTANCES - pass them to StructField
        # directly, do not call them.
        schema = StructType(
            [StructField(name, dtype, nullable=True) for name, dtype in audit_spec]
        )
        row = tuple(values.get(name) for name, _ in audit_spec)
        spark.createDataFrame([row], schema).write.mode("append").saveAsTable(
            audit_table
        )
        LOG.info(
            "Run audit row written to %s (status=%s)",
            audit_table,
            values.get("status"),
        )
    except Exception:
        LOG.warning(
            "Could not write run audit row to %s - continuing.",
            audit_table,
            exc_info=True,
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


def run(args: argparse.Namespace, spark: SparkSession = None, started_at=None) -> int:
    spark = spark or get_spark()
    target_table = f"{args.catalog}.{args.schema}.{args.table}"
    quarantine_table = f"{args.catalog}.{args.schema}.{args.quarantine_table}"
    # Derived from catalog/schema so no extra job parameter is needed.
    audit_table = f"{args.catalog}.{args.schema}.claims_run_audit"
    started_at = started_at or _utc_now()
    run_id = uuid.uuid4().hex

    try:
        raw = read_landing(spark, args.landing_path)
    except NoFilesLanded:
        LOG.warning("No CSV files found at %s - nothing to ingest.", args.landing_path)
        write_audit_row(
            spark,
            audit_table,
            run_id=run_id,
            started_at=started_at,
            finished_at=_utc_now(),
            landing_path=args.landing_path,
            rows_read=0,
            valid_rows=0,
            duplicates_dropped=0,
            invalid_rows=0,
            raw_rows_inserted=0,
            raw_rows_updated=0,
            quarantine_rows_inserted=0,
            status="SUCCESS",
            error_message="no files found at landing path",
        )
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

    q_metrics = {}
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

    write_audit_row(
        spark,
        audit_table,
        run_id=run_id,
        started_at=started_at,
        finished_at=_utc_now(),
        landing_path=args.landing_path,
        rows_read=total_n,
        valid_rows=deduped_n,
        duplicates_dropped=valid_n - deduped_n,
        invalid_rows=invalid_n,
        raw_rows_inserted=int(metrics.get("numTargetRowsInserted") or 0),
        raw_rows_updated=int(metrics.get("numTargetRowsUpdated") or 0),
        quarantine_rows_inserted=int(q_metrics.get("numTargetRowsInserted") or 0),
        status="SUCCESS",
        error_message=None,
    )

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
    args = None
    started_at = _utc_now()
    try:
        args = parse_args(argv)
        return run(args, started_at=started_at)
    except Exception as exc:
        LOG.exception("Claims ingestion FAILED")
        # Best-effort failure audit so the run history shows WHY it failed;
        # guarded so it can never mask the original error.
        if args is not None:
            write_audit_row(
                get_spark(),
                f"{args.catalog}.{args.schema}.claims_run_audit",
                run_id=uuid.uuid4().hex,
                started_at=started_at,
                finished_at=_utc_now(),
                landing_path=args.landing_path,
                rows_read=None,
                valid_rows=None,
                duplicates_dropped=None,
                invalid_rows=None,
                raw_rows_inserted=None,
                raw_rows_updated=None,
                quarantine_rows_inserted=None,
                status="FAILED",
                error_message=f"{type(exc).__name__}: {exc}"[:2000],
            )
        return 1


if __name__ == "__main__":
    # NOTE: never call sys.exit(0) on the success path. The serverless
    # (Spark Connect) python-file runner executes scripts under IPython,
    # where ANY SystemExit - even exit 0 - marks the workload FAILED.
    # A clean end-of-script is the success signal; exit nonzero on failure.
    if main() != 0:
        sys.exit(1)
