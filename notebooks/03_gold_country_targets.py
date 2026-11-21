# Databricks notebook source
# Keep the newest valid record for each member and publish country-specific targets.

from pyspark.sql import Window
from pyspark.sql import functions as F

members = spark.table("skypoints.silver_members")
latest_record = Window.partitionBy("member_business_key").orderBy(
    F.col("source_updated_at").desc_nulls_last(),
    F.col("ingested_at").desc(),
)

current_members = (
    members.withColumn("record_rank", F.row_number().over(latest_record))
    .where(F.col("record_rank") == 1)
    .drop("record_rank")
)

for country_code, table_name in {
    "IND": "members_india",
    "USA": "members_usa",
    "AUS": "members_australia",
}.items():
    (
        current_members.where(F.col("country_code") == country_code)
        .write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"skypoints.{table_name}")
    )
