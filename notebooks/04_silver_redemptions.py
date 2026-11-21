# Databricks notebook source
# Flatten nested redemption JSON to one transaction per row.

from pyspark.sql import functions as F
from datetime import datetime, timezone

dbutils.widgets.text("redemption_file_path", "/Volumes/main/skypoints/landing/redemptions/", "Redemption JSON path")
dbutils.widgets.text("batch_id", "", "Batch ID")

redemption_file_path = dbutils.widgets.get("redemption_file_path")
batch_id = dbutils.widgets.get("batch_id") or datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

raw_json = spark.read.json(redemption_file_path)
redemptions = (
    raw_json.select(
        F.col("member_id").cast("string").alias("source_member_id"),
        F.to_date("feed_date", "yyyyMMdd").alias("feed_date"),
        F.explode("redemptions").alias("redemption"),
    )
    .select(
        F.col("redemption.txn_id").alias("transaction_id"),
        "source_member_id",
        F.to_date(F.col("redemption.txn_date"), "yyyyMMdd").alias("transaction_date"),
        F.col("redemption.partner").alias("partner"),
        F.col("redemption.miles_redeemed").cast("bigint").alias("miles_redeemed"),
        F.col("redemption.status").alias("transaction_status"),
        "feed_date",
        F.lit(batch_id).alias("batch_id"),
        F.current_timestamp().alias("ingested_at"),
    )
)

redemptions.write.format("delta").mode("append").saveAsTable("skypoints.silver_redemptions")
