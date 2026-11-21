# Databricks notebook source
# Ingest a daily pipe-delimited member-profile feed without changing its contents.

from pyspark.sql import functions as F
from datetime import datetime, timezone

dbutils.widgets.text("profile_file_path", "/Volumes/main/skypoints/landing/members/", "Profile file path")
dbutils.widgets.text("source_country", "USA", "Source country")
dbutils.widgets.text("batch_id", "", "Batch ID")

profile_file_path = dbutils.widgets.get("profile_file_path")
source_country = dbutils.widgets.get("source_country").upper()
batch_id = dbutils.widgets.get("batch_id") or datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

raw_lines = (
    spark.read.text(profile_file_path)
    .where(F.trim("value") != "")
    .select(
        F.lit(batch_id).alias("batch_id"),
        F.input_file_name().alias("source_file"),
        F.lit(source_country).alias("source_country"),
        F.current_timestamp().alias("ingested_at"),
        F.col("value").alias("raw_record"),
    )
)

raw_lines.write.format("delta").mode("append").saveAsTable("skypoints.bronze_member_files")
