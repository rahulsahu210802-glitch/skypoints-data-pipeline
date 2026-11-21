# Databricks notebook source
# Publish the latest member record to exactly one country-specific Gold table.
#
# Business assumption:
#   source_member_id is globally unique across all countries.
#   The source should provide source_updated_at (or a monotonically increasing
#   source sequence) to identify the true latest business record. Until that
#   field is populated, ingested_at is used as a fallback ordering column.
#
# This notebook uses a MERGE against the full latest-member source so that:
#   1. New members are inserted into their current country table.
#   2. Existing members are updated in their current country table.
#   3. Members who moved countries are deleted from their previous country
#      table and inserted/updated in their new country table.
#   4. Re-running the same input is idempotent.

from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql import functions as F


# ---------------------------------------------------------------------------
# Parameters
# ---------------------------------------------------------------------------

dbutils.widgets.text("catalog", "main", "Unity Catalog")
dbutils.widgets.text("schema", "skypoints", "Schema")
dbutils.widgets.text("as_of_date", "", "As-of date (YYYY-MM-DD)")

catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
as_of_date_text = dbutils.widgets.get("as_of_date").strip()

if not catalog or not schema:
    raise ValueError("Both catalog and schema parameters must be provided.")

schema_name = f"`{catalog}`.`{schema}`"
members_table = f"{schema_name}.`silver_members`"

# Keep this list aligned with the country codes accepted by the Silver layer.
country_targets = {
    "IND": f"{schema_name}.`members_india`",
    "USA": f"{schema_name}.`members_usa`",
    "AUS": f"{schema_name}.`members_australia`",
}


# ---------------------------------------------------------------------------
# Read and select the latest record globally per member
# ---------------------------------------------------------------------------

members = spark.table(members_table)

required_columns = {
    "source_member_id",
    "member_name",
    "country_code",
    "ingested_at",
    "source_updated_at",
}
missing_columns = required_columns.difference(members.columns)
if missing_columns:
    raise ValueError(
        "silver_members is missing required columns: "
        + ", ".join(sorted(missing_columns))
    )

# Do NOT include country_code in this partition key. A country move must
# replace the previous country assignment for the same global member ID.
latest_window = Window.partitionBy("source_member_id").orderBy(
    F.col("source_updated_at").desc_nulls_last(),
    F.col("ingested_at").desc_nulls_last(),
    F.col("batch_id").desc_nulls_last()
    if "batch_id" in members.columns
    else F.col("source_member_id").asc(),
    F.col("source_file").desc_nulls_last()
    if "source_file" in members.columns
    else F.col("source_member_id").asc(),
)

latest_members = (
    members
    .where(F.col("source_member_id").isNotNull())
    .withColumn("_record_rank", F.row_number().over(latest_window))
    .where(F.col("_record_rank") == 1)
    .drop("_record_rank")
)

# Only publish supported country targets. Unknown country codes should be
# handled by Silver data-quality/rejection logic rather than silently routed.
unsupported_countries = (
    latest_members
    .where(~F.col("country_code").isin(*country_targets.keys()))
    .select("source_member_id", "country_code")
)

if unsupported_countries.limit(1).count() > 0:
    display(unsupported_countries)
    raise ValueError(
        "Latest member records contain unsupported country codes. "
        "Resolve or quarantine them before publishing Gold targets."
    )

# A final guard: the global latest source must contain no duplicate member IDs.
duplicate_latest_ids = (
    latest_members
    .groupBy("source_member_id")
    .count()
    .where(F.col("count") > 1)
)
if duplicate_latest_ids.limit(1).count() > 0:
    raise ValueError("Duplicate source_member_id values remain after latest-record selection.")


# ---------------------------------------------------------------------------
# Create empty Gold targets if they do not exist
# ---------------------------------------------------------------------------

for target_table in country_targets.values():
    if not spark.catalog.tableExists(target_table.replace("`", "")):
        (
            latest_members.limit(0)
            .write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(target_table.replace("`", ""))
        )


# ---------------------------------------------------------------------------
# MERGE the global latest-member set into each country table
# ---------------------------------------------------------------------------

# Important: every target receives the GLOBAL latest-member set, not only
# records for that target country. This allows WHEN MATCHED DELETE to remove
# members who have moved out of that country.
for country_code, target_table in country_targets.items():
    target = DeltaTable.forName(spark, target_table.replace("`", ""))

    (
        target.alias("t")
        .merge(
            latest_members.alias("s"),
            "t.source_member_id = s.source_member_id"
        )
        .whenMatchedDelete(
            condition=f"s.country_code <> '{country_code}'"
        )
        .whenMatchedUpdateAll(
            condition=f"s.country_code = '{country_code}'"
        )
        .whenNotMatchedInsertAll(
            condition=f"s.country_code = '{country_code}'"
        )
        .execute()
    )


# ---------------------------------------------------------------------------
# Publish a small execution summary
# ---------------------------------------------------------------------------

summary = (
    latest_members
    .groupBy("country_code")
    .agg(F.count("*").alias("latest_member_count"))
    .orderBy("country_code")
)

display(summary)

print(
    "Gold country targets successfully synchronized. "
    "The latest global record for each source_member_id is present in "
    "exactly one supported country target."
)
