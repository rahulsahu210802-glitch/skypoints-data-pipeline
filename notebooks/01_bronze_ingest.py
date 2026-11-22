# Databricks notebook source
# Preserve source files in Bronze. Excel/CSV support is for the small assessment
# samples only; production feeds should use distributed text ingestion.

from datetime import datetime, timezone
from pathlib import Path
import json

from pyspark.sql import functions as F


dbutils.widgets.text("catalog", "main", "Unity Catalog")
dbutils.widgets.text("schema", "skypoints", "Schema")
dbutils.widgets.text("profile_file_path", "", "Member file or folder path")
dbutils.widgets.text("source_country", "", "Source country (IND/USA/AUS)")
dbutils.widgets.text("redemption_file_path", "", "Redemption JSON file or folder path")
dbutils.widgets.text("batch_id", "", "Batch ID")

catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
profile_file_path = dbutils.widgets.get("profile_file_path").strip()
source_country = dbutils.widgets.get("source_country").strip().upper()
redemption_file_path = dbutils.widgets.get("redemption_file_path").strip()
batch_id = dbutils.widgets.get("batch_id").strip() or datetime.now(
    timezone.utc
).strftime("%Y%m%d%H%M%S%f")

if not catalog or not schema:
    raise ValueError("Both catalog and schema parameters must be provided.")
if not profile_file_path and not redemption_file_path:
    raise ValueError("Provide a member file path, a redemption path, or both.")
if profile_file_path and source_country not in {"IND", "USA", "AUS"}:
    raise ValueError("source_country must be IND, USA, or AUS.")

schema_name = f"`{catalog}`.`{schema}`"
member_table = f"{schema_name}.`bronze_member_files`"
redemption_table = f"{schema_name}.`bronze_redemption_files`"


def _sample_rows_as_json(path: str, extension: str):
    """Read a small assessment workbook/CSV on the driver, preserving columns."""
    try:
        import pandas as pd
    except ImportError as exc:
        raise ImportError(
            "Assessment CSV/XLSX ingestion requires pandas (and openpyxl for "
            "XLSX). Production feeds should use distributed text ingestion."
        ) from exc

    frame = (
        pd.read_excel(path, dtype=str)
        if extension == ".xlsx"
        else pd.read_csv(path, dtype=str)
    ).fillna("")
    rows = [
        (path, json.dumps(row, ensure_ascii=False))
        for row in frame.to_dict(orient="records")
    ]
    return spark.createDataFrame(rows, ["source_file", "raw_record"])


def _member_rows(path: str):
    extension = Path(path).suffix.lower()
    if extension in {".xlsx", ".csv"}:
        return _sample_rows_as_json(path, extension)
    if extension not in {"", ".txt", ".dat", ".psv"}:
        raise ValueError(
            f"Unsupported member input '{path}'. Use a pipe-delimited text "
            "file, CSV, or XLSX assessment sample."
        )

    source = (
        spark.read.text(path)
        .where(F.trim(F.col("value")) != "")
        .select(F.input_file_name().alias("source_file"), F.col("value").alias("raw_record"))
    )
    return source


def _write_once(df, table_name: str, app_id: str):
    # Reuse the same batch_id on retries of a logical batch.
    (
        df.write.format("delta")
        .mode("append")
        .option("txnAppId", app_id)
        .option("txnVersion", 0)
        .saveAsTable(table_name.replace("`", ""))
    )


if profile_file_path:
    members = (
        _member_rows(profile_file_path)
        .select(
            F.lit(batch_id).alias("batch_id"),
            F.col("source_file"),
            F.lit(source_country).alias("source_country"),
            F.current_timestamp().alias("ingested_at"),
            F.col("raw_record").cast("string").alias("raw_record"),
        )
    )
    _write_once(
        members,
        member_table,
        f"skypoints-members-{source_country}-{batch_id}",
    )

if redemption_file_path:
    # wholetext keeps each pretty-printed JSON document together as one row.
    redemptions = (
        spark.read.option("wholetext", "true").text(redemption_file_path)
        .where(F.trim(F.col("value")) != "")
        .select(
            F.lit(batch_id).alias("batch_id"),
            F.input_file_name().alias("source_file"),
            F.current_timestamp().alias("ingested_at"),
            F.col("value").cast("string").alias("raw_json"),
        )
    )
    _write_once(redemptions, redemption_table, f"skypoints-redemptions-{batch_id}")

print(f"Bronze ingestion completed for batch_id={batch_id}")
