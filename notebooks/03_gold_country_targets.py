# Databricks notebook source
# Publish members changed by this batch to configured country targets.

import re

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

schema_name = f"`{catalog}`.`{schema}`"
members_table = f"{schema_name}.`silver_members`"
config_table = f"{schema_name}.`country_target_config`"
members = spark.table(members_table)

targets = (
    spark.table(config_table)
    .where(F.col("is_active"))
    .select("country_code", "target_table")
    .collect()
)
if not targets:
    raise ValueError("No active country publication targets are configured.")

for row in targets:
    if not re.fullmatch(r"[A-Z]{2,5}", row["country_code"]):
        raise ValueError(f"Unsafe country code in configuration: {row['country_code']}")
    if not re.fullmatch(r"[a-z][a-z0-9_]*", row["target_table"]):
        raise ValueError(f"Unsafe target table in configuration: {row['target_table']}")

changed_members = members.where(F.col("batch_id") == batch_id)
changed_keys = changed_members.select("member_business_key").distinct()

for row in targets:
    country_code = row["country_code"]
    table_name = row["target_table"]
    target_name = f"{catalog}.{schema}.{table_name}"
    target = DeltaTable.forName(spark, target_name)

    # Remove changed keys from every target first so a country move cannot
    # leave the previous country copy behind.
    (
        target.alias("target")
        .merge(
            changed_keys.alias("changed"),
            "target.member_business_key = changed.member_business_key",
        )
        .whenMatchedDelete()
        .execute()
    )

    country_changes = changed_members.where(
        F.col("country_code") == country_code
    )
    (
        target.alias("target")
        .merge(
            country_changes.alias("source"),
            "target.member_business_key = source.member_business_key",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

display(
    changed_members.groupBy("country_code")
    .agg(F.count("*").alias("published_member_count"))
    .orderBy("country_code")
)
