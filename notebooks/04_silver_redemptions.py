# Databricks notebook source
# Parse the raw Bronze redemption JSON feed into one Silver row per transaction.
#
# Expected JSON document:
# {
#   "member_id": "223457",
#   "feed_date": "20240115",
#   "redemptions": [
#     {
#       "txn_id": "RX10091",
#       "txn_date": "20240110",
#       "partner": "AeroLink",
#       "miles_redeemed": 12000,
#       "status": "COMPLETED"
#     }
#   ]
# }
#
# Bronze stores the original JSON document in raw_json. The Bronze ingestion
# reader must use whole-file text ingestion for pretty-printed JSON documents,
# or the source must deliver newline-delimited JSON (NDJSON).
#
# Required tables:
#   bronze_redemption_files
#   silver_redemptions
#   redemption_rejections

from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    LongType,
    StringType,
    StructField,
    StructType,
)


# ---------------------------------------------------------------------------
# Parameters
# ---------------------------------------------------------------------------

dbutils.widgets.text("catalog", "main", "Unity Catalog")
dbutils.widgets.text("schema", "skypoints", "Schema")
dbutils.widgets.text("batch_id", "", "Batch ID")

catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
batch_id = dbutils.widgets.get("batch_id").strip()

if not catalog or not schema:
    raise ValueError("Both catalog and schema parameters must be provided.")

schema_name = f"`{catalog}`.`{schema}`"
bronze_table = f"{catalog}.{schema}.bronze_redemption_files"
silver_table = f"{catalog}.{schema}.silver_redemptions"
rejections_table = f"{catalog}.{schema}.redemption_rejections"


# ---------------------------------------------------------------------------
# JSON schema
# ---------------------------------------------------------------------------

redemption_item_schema = StructType([
    StructField("txn_id", StringType(), True),
    StructField("txn_date", StringType(), True),
    StructField("partner", StringType(), True),
    StructField("miles_redeemed", LongType(), True),
    StructField("status", StringType(), True),
])

feed_schema = StructType([
    StructField("member_id", StringType(), True),
    StructField("feed_date", StringType(), True),
    StructField("redemptions", ArrayType(redemption_item_schema), True),
])


# ---------------------------------------------------------------------------
# Read Bronze and parse each raw JSON document
# ---------------------------------------------------------------------------

bronze = spark.table(bronze_table)

if batch_id:
    bronze = bronze.where(F.col("batch_id") == batch_id)

parsed_documents = bronze.withColumn(
    "_feed",
    F.from_json(F.col("raw_json"), feed_schema)
)

# Keep document-level parse failures for quarantine.
document_errors = (
    parsed_documents
    .where(F.col("_feed").isNull())
    .select(
        "batch_id",
        "source_file",
        F.col("raw_json").alias("raw_record"),
        F.lit("Malformed JSON document or schema mismatch").alias("rejection_reason"),
        F.current_timestamp().alias("rejected_at"),
    )
)

# Explode the nested redemptions array into one row per transaction.
flattened = (
    parsed_documents
    .where(F.col("_feed").isNotNull())
    .select(
        "batch_id",
        "source_file",
        "ingested_at",
        F.col("_feed.member_id").cast("string").alias("source_member_id"),
        F.to_date(F.col("_feed.feed_date"), "yyyyMMdd").alias("feed_date"),
        F.explode_outer(F.col("_feed.redemptions")).alias("redemption"),
        F.col("raw_json"),
    )
    .select(
        "batch_id",
        "source_file",
        "ingested_at",
        "source_member_id",
        "feed_date",
        F.col("redemption.txn_id").cast("string").alias("transaction_id"),
        F.to_date(F.col("redemption.txn_date"), "yyyyMMdd").alias("transaction_date"),
        F.trim(F.col("redemption.partner")).alias("partner"),
        F.col("redemption.miles_redeemed").cast("long").alias("miles_redeemed"),
        F.upper(F.trim(F.col("redemption.status"))).alias("transaction_status"),
        "raw_json",
    )
)


# ---------------------------------------------------------------------------
# Transaction-level validation
# ---------------------------------------------------------------------------

validated = flattened.withColumn(
    "rejection_reason",
    F.when(
        F.col("source_member_id").isNull() | (F.col("source_member_id") == ""),
        "Missing member ID"
    )
    .when(F.col("feed_date").isNull(), "Invalid or missing feed date")
    .when(
        F.col("transaction_id").isNull() | (F.col("transaction_id") == ""),
        "Missing transaction ID"
    )
    .when(F.col("transaction_date").isNull(), "Invalid or missing transaction date")
    .when(
        F.col("feed_date").isNotNull()
        & F.col("transaction_date").isNotNull()
        & (F.col("transaction_date") > F.col("feed_date")),
        "Transaction date is after feed date"
    )
    .when(F.col("partner").isNull() | (F.col("partner") == ""), "Missing partner")
    .when(F.col("miles_redeemed").isNull(), "Missing or invalid miles redeemed")
    .when(F.col("miles_redeemed") < 0, "Miles redeemed cannot be negative")
    .when(
        ~F.col("transaction_status").isin("COMPLETED", "PENDING", "FAILED", "CANCELLED"),
        "Invalid transaction status"
    )
)

rejections = (
    document_errors
    .unionByName(
        validated
        .where(F.col("rejection_reason").isNotNull())
        .select(
            "batch_id",
            "source_file",
            F.col("raw_json").alias("raw_record"),
            "rejection_reason",
            F.current_timestamp().alias("rejected_at"),
        )
    )
)

# The DDL update will create this dedicated table. Keeping redemption rejects
# separate from member rejects makes operational triage easier.
if rejections.limit(1).count() > 0:
    rejections.write.format("delta").mode("append").saveAsTable(rejections_table)


# ---------------------------------------------------------------------------
# Deduplicate and MERGE valid transactions
# ---------------------------------------------------------------------------

valid_transactions = validated.where(F.col("rejection_reason").isNull()).select(
    "transaction_id",
    "source_member_id",
    "transaction_date",
    "partner",
    "miles_redeemed",
    "transaction_status",
    "feed_date",
    "batch_id",
    "ingested_at",
)

# If the same transaction appears more than once in the current batch, retain
# the most recently ingested copy. transaction_id is assumed globally unique.
from pyspark.sql import Window

transaction_window = Window.partitionBy("transaction_id").orderBy(
    F.col("ingested_at").desc_nulls_last(),
    F.col("batch_id").desc_nulls_last()
)

deduplicated = (
    valid_transactions
    .withColumn("_row_num", F.row_number().over(transaction_window))
    .where(F.col("_row_num") == 1)
    .drop("_row_num")
)

if not spark.catalog.tableExists(silver_table):
    raise ValueError(
        f"Target table {silver_table} does not exist. "
        "Run sql/01_create_tables.sql after adding the redemption DDL."
    )

from delta.tables import DeltaTable

target = DeltaTable.forName(spark, silver_table)

(
    target.alias("t")
    .merge(
        deduplicated.alias("s"),
        "t.transaction_id = s.transaction_id"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)


# ---------------------------------------------------------------------------
# Execution metrics
# ---------------------------------------------------------------------------

metrics = [
    ("bronze_documents", parsed_documents.count()),
    ("flattened_transaction_rows", flattened.count()),
    ("valid_unique_transactions", deduplicated.count()),
    ("rejected_rows", rejections.count()),
]

display(spark.createDataFrame(metrics, ["metric_name", "row_count"]))
