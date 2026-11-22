# Databricks notebook source
# Rebuild the country targets from the globally current Silver member records.

from pyspark.sql import Window
from pyspark.sql import functions as F


dbutils.widgets.text("catalog", "main", "Unity Catalog")
dbutils.widgets.text("schema", "skypoints", "Schema")
catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()

if not catalog or not schema:
    raise ValueError("Both catalog and schema parameters must be provided.")

schema_name = f"`{catalog}`.`{schema}`"
members_table = f"{schema_name}.`silver_members`"
members = spark.table(members_table)

latest_record = Window.partitionBy("member_business_key").orderBy(
    F.col("source_updated_at").desc_nulls_last(),
    F.col("ingested_at").desc_nulls_last(),
    F.col("batch_id").desc_nulls_last(),
    F.col("source_file").desc_nulls_last(),
)
current_members = (
    members.where(F.col("member_business_key").isNotNull())
    .withColumn("_record_rank", F.row_number().over(latest_record))
    .where(F.col("_record_rank") == 1)
    .drop("_record_rank")
)

country_targets = {
    "IND": "members_india",
    "USA": "members_usa",
    "AUS": "members_australia",
}

unsupported = current_members.where(
    ~F.col("country_code").isin(*country_targets.keys())
)
if unsupported.limit(1).count():
    display(unsupported.select("source_member_id", "country_code"))
    raise ValueError("Current members contain unsupported country codes.")

# Full replacement also removes records from an old country after a member moves.
for country_code, table_name in country_targets.items():
    (
        current_members.where(F.col("country_code") == country_code)
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{catalog}.{schema}.{table_name}")
    )

display(
    current_members.groupBy("country_code")
    .agg(F.count("*").alias("current_member_count"))
    .orderBy("country_code")
)
