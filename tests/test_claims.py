"""Unit tests for the claims ingestion pipeline (tests/test_claims.py).

Run locally or in CI with:   pytest tests -v

These tests exercise the pure transform functions (schema validation,
valid/invalid split, deduplication, transformation) against a local Spark
session. They deliberately do NOT touch Unity Catalog or Delta tables -
that is what the dev-target smoke run in CI is for.
"""

import sys
from pathlib import Path

import pytest

# Make src/ingest_claims.py importable no matter where pytest is invoked.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pyspark.sql import SparkSession, functions as F, types as T  # noqa: E402

import ingest_claims as pipeline  # noqa: E402


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    return (
        SparkSession.builder.master("local[*]")
        .appName("claims_unit_tests")
        .getOrCreate()
    )


def make_raw(spark, rows):
    """Build a DataFrame that mimics a landing CSV: every column a string."""
    schema = T.StructType(
        [T.StructField(c, T.StringType(), True) for c in pipeline.REQUIRED_COLUMNS]
    )
    return spark.createDataFrame(rows, schema)


GOOD = "CLM-1", "POL-1", "2026-01-15", "2500.00", "Submitted", "Alice Johnson", "North"


# --------------------------------------------------------------------------

def test_missing_required_column_raises(spark):
    """The landing contract is enforced: missing columns fail the run."""
    df = make_raw(spark, [("CLM-1", "POL-1", "2026-01-15", "1.00", "P", "A", "N")])
    broken = df.drop("claim_amount")
    with pytest.raises(ValueError, match="missing required columns"):
        pipeline.validate_schema(broken)


def test_valid_contract_passes(spark):
    df = make_raw(spark, [GOOD])
    pipeline.validate_schema(df)  # must not raise


def test_invalid_rows_are_quarantined_with_reason(spark):
    rows = [
        GOOD,                                              # valid
        ("CLM-2", "POL-2", "not-a-date", "10.00", "P", "B", "S"),   # bad date
        ("CLM-3", "POL-3", "2026-01-16", None, "P", "C", "E"),        # null amount
        ("", "POL-4", "2026-01-17", "10.00", "P", "D", "W"),           # empty id
    ]
    valid, invalid = pipeline.split_valid_invalid(make_raw(spark, rows))
    assert valid.count() == 1
    assert invalid.count() == 3
    reasons = {r["invalid_reason"] for r in invalid.select("invalid_reason").collect()}
    assert reasons == {
        "invalid_claim_date",
        "missing_or_invalid_claim_amount",
        "missing_or_empty_claim_id",
    }


def test_quarantine_key_falls_back_to_row_hash(spark):
    """Rows without a usable claim_id get a stable hash key (idempotency)."""
    rows = [("", "POL-4", "2026-01-17", "10.00", "P", "D", "W")]
    _, invalid = pipeline.split_valid_invalid(make_raw(spark, rows))
    key = invalid.select("quarantine_key").first()["quarantine_key"]
    assert key is not None and len(key) == 32  # md5 hex digest


def test_dedup_keeps_latest_claim_date(spark):
    rows = [
        ("CLM-1", "POL-1", "2026-01-10", "100.00", "Submitted", "A", "N"),
        ("CLM-1", "POL-1", "2026-01-25", "125.00", "Submitted", "A", "N"),
        ("CLM-2", "POL-2", "2026-01-15", "50.00", "Submitted", "B", "S"),
    ]
    df = pipeline.deduplicate(make_raw(spark, rows))
    assert df.count() == 2
    latest = df.filter(F.col("claim_id") == "CLM-1").collect()[0]
    assert latest["claim_date"] == "2026-01-25"
    assert latest["claim_amount"] == "125.00"


def test_transform_normalizes_and_casts(spark):
    df = make_raw(spark, [("  CLM-1 ", "POL-1", "2026-01-15", "2500.00",
                           "  approved ", "Alice Johnson", "north")])
    df = df.withColumn("_source_file", F.lit("file:///claims.csv"))
    out = pipeline.transform(df).collect()[0]
    assert out["claim_id"] == "CLM-1"                      # trimmed
    assert str(out["claim_date"]) == "2026-01-15"          # cast to date
    assert out["claim_amount"] == 2500.00                    # cast to decimal
    assert out["status"] == "APPROVED"                       # upper + trimmed
    assert out["region"] == "NORTH"
    assert out["claim_year"] == 2026                          # derived
    assert out["_source_file"].endswith("claims.csv")


def test_transform_defaults_null_status(spark):
    row = ("CLM-1", "POL-1", "2026-01-15", "2500.00", None, "A", "N")
    df = make_raw(spark, [row]).withColumn("_source_file", F.lit("f"))
    out = pipeline.transform(df).collect()[0]
    assert out["status"] == "UNKNOWN"
