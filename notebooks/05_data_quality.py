# Databricks notebook source
# Report failed-row counts for core member and redemption quality checks.

from delta.tables import DeltaTable
from pyspark.sql import functions as F


dbutils.widgets.text("catalog", "main", "Unity Catalog")
dbutils.widgets.text("schema", "skypoints", "Schema")
dbutils.widgets.text("batch_id", "", "Batch ID")
catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
batch_id = dbutils.widgets.get("batch_id").strip()

if not catalog or not schema or not batch_id:
    raise ValueError("catalog, schema, and batch_id parameters must be provided.")

prefix = f"`{catalog}`.`{schema}`."
members = spark.table(f"{prefix}`silver_members`")
redemptions = spark.table(f"{prefix}`silver_redemptions`")
results_table = f"{prefix}`quality_check_results`"

country_tables = [
    spark.table(f"{prefix}`members_india`")
    .select("member_business_key", "source_member_id", "country_code")
    .withColumn("_expected_country", F.lit("IND")),
    spark.table(f"{prefix}`members_usa`")
    .select("member_business_key", "source_member_id", "country_code")
    .withColumn("_expected_country", F.lit("USA")),
    spark.table(f"{prefix}`members_australia`")
    .select("member_business_key", "source_member_id", "country_code")
    .withColumn("_expected_country", F.lit("AUS")),
]
gold_members = country_tables[0].unionByName(country_tables[1]).unionByName(country_tables[2])

checks = [
    ("duplicate_silver_member_keys", members.groupBy("member_business_key").count().where("count > 1").count()),
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
    ("duplicate_gold_member_keys", gold_members.groupBy("member_business_key").count().where("count > 1").count()),
    ("gold_country_mismatch", gold_members.where(
        F.col("country_code").isNull()
        | (F.col("country_code") != F.col("_expected_country"))
    ).count()),
]

results = (
    spark.createDataFrame(checks, ["check_name", "failed_row_count"])
    .withColumn("batch_id", F.lit(batch_id))
    .withColumn("checked_at", F.current_timestamp())
    .select("batch_id", "check_name", "failed_row_count", "checked_at")
)

(
    DeltaTable.forName(spark, results_table.replace("`", ""))
    .alias("target")
    .merge(
        results.alias("source"),
        "target.batch_id = source.batch_id "
        "AND target.check_name = source.check_name",
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

display(results.orderBy("check_name"))
