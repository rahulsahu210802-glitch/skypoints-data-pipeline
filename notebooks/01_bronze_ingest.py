# Databricks notebook source
# Idempotent Bronze ingestion for SkyPoints member profiles and redemption JSON.
#
# Supported member inputs:
#   - Pipe-delimited text files (.txt, .dat, .psv)
#   - CSV files (.csv)
#   - Excel workbooks (.xlsx) for small assessment/sample files only
#
# Production-scale recommendation:
#   Convert Excel samples to the agreed source feed format before ingestion.
#   Do not use driver-side pandas/openpyxl for billions of records. Use
#   distributed text/CSV ingestion or a managed ingestion connector instead.
#
# Redemption JSON is stored as raw JSON text in Bronze before flattening.
#
# Required Bronze tables:
#   bronze_member_files
#   bronze_redemption_files
#
# These tables are created by sql/01_create_tables.sql (to be updated).

from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F


# ---------------------------------------------------------------------------
# Parameters
# ---------------------------------------------------------------------------

dbutils.widgets.text("catalog", "main", "Unity Catalog")
dbutils.widgets.text("schema", "skypoints", "Schema")
dbutils.widgets.text("member_file_path", "", "Member file or folder path")
dbutils.widgets.text("source_country", "", "Source country (IND/USA/AUS)")
dbutils.widgets.text("redemption_file_path", "", "Redemption JSON file or folder path")
dbutils.widgets.text("batch_id", "", "Batch ID")

catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
member_file_path = dbutils.widgets.get("member_file_path").strip()
source_country = dbutils.widgets.get("source_country").strip().upper()
redemption_file_path = dbutils.widgets.get("redemption_file_path").strip()
batch_id = dbutils.widgets.get("batch_id").strip()

if not catalog or not schema:
    raise ValueError("Both catalog and schema parameters must be provided.")

if not member_file_path and not redemption_file_path:
    raise ValueError("Provide at least one member_file_path or redemption_file_path.")

if member_file_path and source_country not in {"IND", "USA", "AUS"}:
    raise ValueError("source_country must be IND, USA, or AUS when ingesting member files.")

if not batch_id:
    batch_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")

schema_name = f"`{catalog}`.`{schema}`"
member_bronze_table = f"{catalog}.{schema}.bronze_member_files"
redemption_bronze_table = f"{catalog}.{schema}.bronze_redemption_files"

ingestion_timestamp = datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _file_extension(path: str) -> str:
    """Return the lowercase extension from a file path."""
    return Path(path.rstrip("/")).suffix.lower()


def _read_member_file(path: str, country: str):
    """Read one member file and return raw source records plus metadata."""
    extension = _file_extension(path)

    if extension in {".txt", ".dat", ".psv"}:
        # Keep each original pipe-delimited line intact in Bronze.
        source_df = (
            spark.read.text(path)
            .where(F.trim(F.col("value")) != "")
            .select(F.col("value").alias("raw_record"))
        )

    elif extension == ".csv":
        # CSV input is retained as a raw line. Header rows are retained too;
        # Silver is responsible for isolating detail records.
        source_df = (
            spark.read.text(path)
            .where(F.trim(F.col("value")) != "")
            .select(F.col("value").alias("raw_record"))
        )

    elif extension == ".xlsx":
        # Assessment-only path: the supplied Excel workbooks are small.
        # pandas reads on the driver, so this is NOT suitable for large feeds.
        try:
            import pandas as pd
        except ImportError as exc:
            raise ImportError(
                "Reading .xlsx samples requires pandas and openpyxl on the "
                "Databricks driver. For production, convert the source to "
                "CSV/pipe-delimited files or use a distributed Excel connector."
            ) from exc

        pdf = pd.read_excel(path, dtype=str).fillna("")
        # Preserve a deterministic pipe-delimited representation for the
        # common sample columns. Source-specific mappings are handled later.
        raw_columns = [str(column).strip() for column in pdf.columns]
        rows = []
        for values in pdf.astype(str).itertuples(index=False, name=None):
            row = dict(zip(raw_columns, values))
            # Retain the source row as JSON so no workbook columns are lost.
            import json
            rows.append((json.dumps(row, ensure_ascii=False, default=str),))
        source_df = spark.createDataFrame(rows, ["raw_record"])

    else:
        raise ValueError(
            f"Unsupported member file extension '{extension}' for {path}. "
            "Supported: .txt, .dat, .psv, .csv, .xlsx"
        )

    return source_df.select(
        F.lit(batch_id).alias("batch_id"),
        F.lit(path).alias("source_file"),
        F.lit(country).alias("source_country"),
        F.lit(ingestion_timestamp).cast("timestamp").alias("ingested_at"),
        F.col("raw_record").cast("string").alias("raw_record"),
    )


def _read_redemption_json(path: str):
    """Read JSON documents as raw text so the original payload is preserved."""
    return (
        spark.read.text(path)
        .where(F.trim(F.col("value")) != "")
        .select(
            F.lit(batch_id).alias("batch_id"),
            F.lit(path).alias("source_file"),
            F.lit(ingestion_timestamp).cast("timestamp").alias("ingested_at"),
            F.col("value").cast("string").alias("raw_json"),
        )
    )


def _write_idempotently(df, table_name: str, write_key: str):
    """Append using Delta transaction identifiers to protect batch retries."""
    # Delta ignores a repeated appId + txnVersion pair. The caller must use a
    # stable batch_id for retries of the same logical batch.
    (
        df.write
        .format("delta")
        .mode("append")
        .option("txnAppId", f"skypoints-{table_name}-{write_key}")
        .option("txnVersion", 0)
        .saveAsTable(table_name)
    )


# ---------------------------------------------------------------------------
# Member profile ingestion
# ---------------------------------------------------------------------------

if member_file_path:
    extension = _file_extension(member_file_path)

    if extension == "":
        # Folder ingestion is supported for distributed text/CSV formats.
        # Excel workbooks should be supplied one at a time.
        member_files = [
            item.path
            for item in dbutils.fs.ls(member_file_path)
            if not item.isDir()
        ]
        if not member_files:
            raise ValueError(f"No files found in member folder: {member_file_path}")
    else:
        member_files = [member_file_path]

    member_frames = [
        _read_member_file(path, source_country)
        for path in member_files
    ]

    member_raw = member_frames[0]
    for frame in member_frames[1:]:
        member_raw = member_raw.unionByName(frame)

    _write_idempotently(
        member_raw,
        member_bronze_table,
        f"{batch_id}-members"
    )

    print(
        f"Member Bronze ingestion completed: {len(member_files)} file(s), "
        f"batch_id={batch_id}"
    )


# ---------------------------------------------------------------------------
# Redemption JSON ingestion
# ---------------------------------------------------------------------------

if redemption_file_path:
    redemption_raw = _read_redemption_json(redemption_file_path)

    _write_idempotently(
        redemption_raw,
        redemption_bronze_table,
        f"{batch_id}-redemptions"
    )

    print(f"Redemption Bronze ingestion completed: batch_id={batch_id}")


# ---------------------------------------------------------------------------
# Batch summary
# ---------------------------------------------------------------------------

summary_rows = []
if member_file_path:
    summary_rows.append(("member_profile", batch_id, member_bronze_table))
if redemption_file_path:
    summary_rows.append(("redemption_json", batch_id, redemption_bronze_table))

display(
    spark.createDataFrame(
        summary_rows,
        ["feed_type", "batch_id", "bronze_table"]
    )
)
