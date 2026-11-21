# Databricks notebook source
# Parse and standardize pipe-delimited member records from the Bronze layer.
#
# Assessment sample layout:
# D|Member_Name|Member_Id|Enrollment_Date|Last_Flight_Date|Tier_Code|
# Agent_Name|State|Country|DOB|Is_Active
#
# The PDF field specification mentions Post Code, but the sample header and
# detail rows omit it. This notebook follows the sample layout. Confirm the
# production feed layout before adding Post Code.
#
# Assumption: source_member_id is globally unique across countries.

from pyspark.sql import functions as F


# ---------------------------------------------------------------------------
# Parameters
# ---------------------------------------------------------------------------

dbutils.widgets.text("catalog", "main", "Unity Catalog")
dbutils.widgets.text("schema", "skypoints", "Schema")
dbutils.widgets.text("batch_id", "", "Batch ID")
dbutils.widgets.text("as_of_date", "", "As-of date (YYYY-MM-DD)")

catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
batch_id = dbutils.widgets.get("batch_id").strip()
as_of_date_text = dbutils.widgets.get("as_of_date").strip()

if not catalog or not schema:
    raise ValueError("Both catalog and schema parameters must be provided.")

schema_name = f"`{catalog}`.`{schema}`"
bronze_table = f"{schema_name}.`bronze_member_files`"
silver_table = f"{schema_name}.`silver_members`"
rejections_table = f"{schema_name}.`member_rejections`"

as_of_date = (
    F.to_date(F.lit(as_of_date_text), "yyyy-MM-dd")
    if as_of_date_text
    else F.current_date()
)


# ---------------------------------------------------------------------------
# Read Bronze records and isolate detail rows
# ---------------------------------------------------------------------------

bronze = spark.table(bronze_table)

if batch_id:
    bronze = bronze.where(F.col("batch_id") == batch_id)

detail = bronze.where(
    F.element_at(F.split(F.col("raw_record"), r"\|"), 1) == "D"
)
fields = F.split(F.col("raw_record"), r"\|")

parsed = detail.select(
    "batch_id",
    "source_file",
    "source_country",
    "ingested_at",
    "raw_record",
    F.trim(F.element_at(fields, 2)).alias("member_name"),
    F.trim(F.element_at(fields, 3)).alias("source_member_id"),
    F.to_date(F.trim(F.element_at(fields, 4)), "yyyyMMdd").alias("enrollment_date"),
    F.to_date(F.trim(F.element_at(fields, 5)), "yyyyMMdd").alias("last_flight_date"),
    F.upper(F.trim(F.element_at(fields, 6))).alias("tier_code"),
    F.trim(F.element_at(fields, 7)).alias("agent_name"),
    F.upper(F.trim(F.element_at(fields, 8))).alias("state"),
    F.upper(F.trim(F.element_at(fields, 9))).alias("country_code"),
    F.to_date(F.trim(F.element_at(fields, 10)), "MMddyyyy").alias("date_of_birth"),
    F.upper(F.trim(F.element_at(fields, 11))).alias("active_flag"),
    F.size(fields).alias("_field_count"),
)


# ---------------------------------------------------------------------------
# Data-quality validation
# ---------------------------------------------------------------------------

validated = parsed.withColumn(
    "rejection_reason",
    F.when(F.col("_field_count") < 11, "Malformed detail record: expected 11 fields")
    .when(F.col("member_name").isNull() | (F.col("member_name") == ""), "Missing member name")
    .when(F.length(F.col("member_name")) > 255, "Member name exceeds 255 characters")
    .when(F.col("source_member_id").isNull() | (F.col("source_member_id") == ""), "Missing member ID")
    .when(F.length(F.col("source_member_id")) > 18, "Member ID exceeds 18 characters")
    .when(F.col("enrollment_date").isNull(), "Invalid or missing enrollment date")
    .when(F.col("enrollment_date") > as_of_date, "Enrollment date is in the future")
    .when(
        F.col("date_of_birth").isNotNull() & (F.col("date_of_birth") > as_of_date),
        "Date of birth is in the future"
    )
    .when(
        F.col("last_flight_date").isNotNull()
        & (F.col("last_flight_date") < F.col("enrollment_date")),
        "Last flight date is before enrollment date"
    )
    .when(
        F.col("tier_code").isNotNull()
        & (F.col("tier_code") != "")
        & (~F.col("tier_code").isin("GLD", "SLV", "PLT")),
        "Invalid tier code"
    )
    .when(
        F.col("country_code").isNull() | (F.col("country_code") == ""),
        "Missing country code"
    )
    .when(~F.col("country_code").isin("IND", "USA", "AUS"), "Unsupported country code")
    .when(
        F.col("active_flag").isNotNull()
        & (F.col("active_flag") != "")
        & (~F.col("active_flag").isin("A", "I")),
        "Invalid active-member flag"
    )
)

rejections = (
    validated
    .where(F.col("rejection_reason").isNotNull())
    .select(
        "batch_id",
        "source_file",
        "source_country",
        "raw_record",
        "rejection_reason",
        F.current_timestamp().alias("rejected_at"),
    )
)

# Append-only rejection history. A later enhancement can MERGE using a stable
# source record ID to make rejection writes idempotent on batch retries.
rejections.write.format("delta").mode("append").saveAsTable(
    rejections_table.replace("`", "")
)


# ---------------------------------------------------------------------------
# Build canonical Silver member records
# ---------------------------------------------------------------------------

valid_members = (
    validated
    .where(F.col("rejection_reason").isNull())
    .select(
        # Country is intentionally excluded from the global business key.
        F.col("source_member_id").alias("member_business_key"),
        "source_member_id",
        "member_name",
        "tier_code",
        "date_of_birth",
        "enrollment_date",
        "last_flight_date",
        "country_code",
        # The supplied sample feed has no member_type field.
        F.lit(None).cast("string").alias("member_type"),
        F.when(
            F.col("date_of_birth").isNotNull(),
            F.floor(F.months_between(as_of_date, F.col("date_of_birth")) / 12)
        ).cast("int").alias("age"),
        # Strictly greater than 90 days. Unknown flight dates remain NULL.
        F.when(
            F.col("last_flight_date").isNull(),
            F.lit(None).cast("boolean")
        ).otherwise(
            F.datediff(as_of_date, F.col("last_flight_date")) > 90
        ).alias("stale_member"),
        # No source update timestamp exists in the assessment sample.
        # Gold therefore falls back to ingested_at until source metadata exists.
        F.lit(None).cast("timestamp").alias("source_updated_at"),
        "batch_id",
        "source_file",
        "ingested_at",
    )
)

# Keep the output aligned with the current silver_members DDL. agent_name,
# state and active_flag are parsed, but need DDL expansion before persistence.
valid_members.write.format("delta").mode("append").saveAsTable(
    silver_table.replace("`", "")
)


# ---------------------------------------------------------------------------
# Execution metrics
# ---------------------------------------------------------------------------

metrics = [
    ("parsed_detail_rows", parsed.count()),
    ("valid_member_rows", valid_members.count()),
    ("rejected_member_rows", rejections.count()),
]
display(spark.createDataFrame(metrics, ["metric_name", "row_count"]))
