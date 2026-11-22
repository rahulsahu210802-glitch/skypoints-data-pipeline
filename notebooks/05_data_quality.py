# Databricks notebook source
# Report failed-row counts for core member and redemption quality checks.

from pyspark.sql import functions as F


dbutils.widgets.text("catalog", "main", "Unity Catalog")
dbutils.widgets.text("schema", "skypoints", "Schema")
catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()

if not catalog or not schema:
    raise ValueError("Both catalog and schema parameters must be provided.")

prefix = f"`{catalog}`.`{schema}`."
members = spark.table(f"{prefix}`silver_members`")
redemptions = spark.table(f"{prefix}`silver_redemptions`")

country_tables = [
    spark.table(f"{prefix}`members_india`")
    .select("source_member_id", "country_code")
    .withColumn("_expected_country", F.lit("IND")),
    spark.table(f"{prefix}`members_usa`")
    .select("source_member_id", "country_code")
    .withColumn("_expected_country", F.lit("USA")),
    spark.table(f"{prefix}`members_australia`")
    .select("source_member_id", "country_code")
    .withColumn("_expected_country", F.lit("AUS")),
]
gold_members = country_tables[0].unionByName(country_tables[1]).unionByName(country_tables[2])

checks = [
    ("duplicate_silver_member_ids", members.groupBy("source_member_id").count().where("count > 1").count()),
    ("missing_member_ids", members.where(F.col("source_member_id").isNull() | (F.col("source_member_id") == "")).count()),
    ("missing_member_names", members.where(F.col("member_name").isNull() | (F.col("member_name") == "")).count()),
    ("unsupported_member_countries", members.where(
        F.col("country_code").isNull()
        | (~F.col("country_code").isin("IND", "USA", "AUS"))
    ).count()),
    ("duplicate_transaction_ids", redemptions.groupBy("transaction_id").count().where("count > 1").count()),
    ("redemptions_without_member", redemptions.join(
        members.select("source_member_id").distinct(),
        "source_member_id",
        "left_anti",
    ).count()),
    ("duplicate_gold_member_ids", gold_members.groupBy("source_member_id").count().where("count > 1").count()),
    ("gold_country_mismatch", gold_members.where(
        F.col("country_code").isNull()
        | (F.col("country_code") != F.col("_expected_country"))
    ).count()),
]

results = spark.createDataFrame(checks, ["check_name", "failed_row_count"])
display(results.orderBy("check_name"))
