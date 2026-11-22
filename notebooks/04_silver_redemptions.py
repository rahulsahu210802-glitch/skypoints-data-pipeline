# Databricks notebook source
# Parse raw Bronze JSON documents, quarantine invalid transactions, and upsert
# valid transactions by their globally unique transaction ID.

from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, LongType, StringType, StructField, StructType


dbutils.widgets.text("catalog", "main", "Unity Catalog")
dbutils.widgets.text("schema", "skypoints", "Schema")
dbutils.widgets.text("batch_id", "", "Batch ID")

catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
batch_id = dbutils.widgets.get("batch_id").strip()

if not catalog or not schema:
    raise ValueError("Both catalog and schema parameters must be provided.")

schema_name = f"`{catalog}`.`{schema}`"
bronze_table = f"{schema_name}.`bronze_redemption_files`"
silver_table = f"{schema_name}.`silver_redemptions`"
rejections_table = f"{schema_name}.`redemption_rejections`"

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


def _date(value):
    return F.try_to_timestamp(value, F.lit("yyyyMMdd")).cast("date")


bronze = spark.table(bronze_table)
if batch_id:
    bronze = bronze.where(F.col("batch_id") == batch_id)

parsed_documents = bronze.withColumn(
    "_feed", F.from_json(F.col("raw_json"), feed_schema)
).withColumn(
    "_document_rejection_reason",
    F.when(F.col("_feed").isNull(), "Malformed JSON document or schema mismatch")
    .when(
        F.col("_feed.member_id").isNull() | (F.col("_feed.member_id") == ""),
        "Missing member ID",
    )
    .when(F.col("_feed.redemptions").isNull(), "Missing redemptions array"),
)
malformed_documents = parsed_documents.where(
    F.col("_document_rejection_reason").isNotNull()
).select(
    "batch_id",
    "source_file",
    F.col("raw_json").alias("raw_record"),
    F.col("_document_rejection_reason").alias("rejection_reason"),
    F.current_timestamp().alias("rejected_at"),
)

flattened = (
    parsed_documents.where(F.col("_document_rejection_reason").isNull())
    .select(
        "batch_id", "source_file", "ingested_at", "raw_json",
        F.col("_feed.member_id").cast("string").alias("source_member_id"),
        _date(F.col("_feed.feed_date")).alias("feed_date"),
        F.explode(F.col("_feed.redemptions")).alias("_redemption"),
    )
    .select(
        "batch_id", "source_file", "ingested_at", "raw_json",
        "source_member_id", "feed_date",
        F.col("_redemption.txn_id").cast("string").alias("transaction_id"),
        _date(F.col("_redemption.txn_date")).alias("transaction_date"),
        F.trim(F.col("_redemption.partner")).alias("partner"),
        F.col("_redemption.miles_redeemed").cast("long").alias("miles_redeemed"),
        F.upper(F.trim(F.col("_redemption.status"))).alias("transaction_status"),
    )
)

validated = flattened.withColumn(
    "rejection_reason",
    F.when(
        F.col("source_member_id").isNull() | (F.col("source_member_id") == ""),
        "Missing member ID",
    )
    .when(F.col("feed_date").isNull(), "Invalid or missing feed date")
    .when(
        F.col("transaction_id").isNull() | (F.col("transaction_id") == ""),
        "Missing transaction ID",
    )
    .when(F.col("transaction_date").isNull(), "Invalid or missing transaction date")
    .when(
        F.col("transaction_date") > F.col("feed_date"),
        "Transaction date is after feed date",
    )
    .when(F.col("partner").isNull() | (F.col("partner") == ""), "Missing partner")
    .when(F.col("miles_redeemed").isNull(), "Missing or invalid miles redeemed")
    .when(F.col("miles_redeemed") < 0, "Miles redeemed cannot be negative")
    .when(
        F.col("transaction_status").isNull()
        | (F.col("transaction_status") == ""),
        "Missing transaction status",
    )
)

rejections = malformed_documents.unionByName(
    validated.where(F.col("rejection_reason").isNotNull()).select(
        "batch_id",
        "source_file",
        F.col("raw_json").alias("raw_record"),
        "rejection_reason",
        F.current_timestamp().alias("rejected_at"),
    )
).dropDuplicates(["batch_id", "source_file", "raw_record", "rejection_reason"])

if rejections.limit(1).count():
    (
        DeltaTable.forName(spark, rejections_table.replace("`", ""))
        .alias("target")
        .merge(
            rejections.alias("source"),
            "target.batch_id = source.batch_id "
            "AND target.source_file = source.source_file "
            "AND target.raw_record = source.raw_record "
            "AND target.rejection_reason = source.rejection_reason",
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

valid_transactions = validated.where(F.col("rejection_reason").isNull()).select(
    "transaction_id",
    "source_member_id",
    "source_file",
    "transaction_date",
    "partner",
    "miles_redeemed",
    "transaction_status",
    "feed_date",
    "batch_id",
    "ingested_at",
)

latest_transaction = Window.partitionBy("transaction_id").orderBy(
    F.col("ingested_at").desc_nulls_last(),
    F.col("batch_id").desc(),
    F.col("source_file").desc(),
    F.col("transaction_date").desc(),
)
valid_transactions = (
    valid_transactions.withColumn("_row_number", F.row_number().over(latest_transaction))
    .where(F.col("_row_number") == 1)
    .drop("_row_number")
)

(
    DeltaTable.forName(spark, silver_table.replace("`", ""))
    .alias("target")
    .merge(
        valid_transactions.alias("source"),
        "target.transaction_id = source.transaction_id",
    )
    .whenMatchedUpdateAll(condition="source.ingested_at >= target.ingested_at")
    .whenNotMatchedInsertAll()
    .execute()
)

spark.sql(f"""
    CREATE OR REPLACE VIEW {schema_name}.`member_redemption_view` AS
    SELECT
        r.transaction_id,
        r.transaction_date,
        r.partner,
        r.miles_redeemed,
        r.transaction_status,
        r.source_member_id,
        m.member_name,
        m.tier_code,
        m.country_code
    FROM {silver_table} r
    LEFT JOIN {schema_name}.`silver_members` m
      ON r.source_member_id = m.source_member_id
     AND m.member_business_key = m.source_member_id
""")

metrics = [
    ("bronze_documents", parsed_documents.count()),
    ("flattened_transaction_rows", flattened.count()),
    ("valid_unique_transactions", valid_transactions.count()),
    ("rejected_rows", rejections.count()),
]
display(spark.createDataFrame(metrics, ["metric_name", "row_count"]))
