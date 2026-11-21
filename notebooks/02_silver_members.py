# Databricks notebook source
# Standardize valid detail records and quarantine records that cannot be trusted.

from pyspark.sql import functions as F

dbutils.widgets.text("batch_id", "", "Batch ID")
dbutils.widgets.text("as_of_date", "", "As-of date (YYYY-MM-DD)")

batch_id = dbutils.widgets.get("batch_id")
as_of_date_text = dbutils.widgets.get("as_of_date")
as_of_date = F.to_date(F.lit(as_of_date_text)) if as_of_date_text else F.current_date()

bronze = spark.table("skypoints.bronze_member_files")
if batch_id:
    bronze = bronze.where(F.col("batch_id") == batch_id)

# Source format: D|Member_Name|Member_Id|Enrollment_Date|Last_Flight_Date|Tier_Code|Agent_Name|State|Country|DOB|Is_Active
detail = bronze.where(F.element_at(F.split("raw_record", r"\|"), 1) == "D")
fields = F.split("raw_record", r"\|")

parsed = detail.select(
    "batch_id", "source_file", "source_country", "ingested_at", "raw_record",
    F.trim(F.element_at(fields, 2)).alias("member_name"),
    F.trim(F.element_at(fields, 3)).alias("source_member_id"),
    F.to_date(F.trim(F.element_at(fields, 4)), "yyyyMMdd").alias("enrollment_date"),
    F.to_date(F.trim(F.element_at(fields, 5)), "yyyyMMdd").alias("last_flight_date"),
    F.upper(F.trim(F.element_at(fields, 6))).alias("tier_code"),
    F.upper(F.trim(F.element_at(fields, 9))).alias("country_code"),
    F.to_date(F.trim(F.element_at(fields, 10)), "yyyyMMdd").alias("date_of_birth"),
)

validated = parsed.withColumn(
    "rejection_reason",
    F.when(F.col("source_member_id").isNull() | (F.col("source_member_id") == ""), "Missing member ID")
     .when(F.col("member_name").isNull() | (F.col("member_name") == ""), "Missing member name")
     .when(F.col("enrollment_date").isNull(), "Invalid or missing enrollment date")
     .when(~F.col("tier_code").isin("GLD", "SLV", "PLT"), "Invalid tier code")
     .when(F.col("country_code").isNull() | (F.col("country_code") == ""), "Missing country code")
)

rejections = validated.where(F.col("rejection_reason").isNotNull()).select(
    "batch_id", "source_file", "source_country", "raw_record", "rejection_reason",
    F.current_timestamp().alias("rejected_at"),
)
rejections.write.format("delta").mode("append").saveAsTable("skypoints.member_rejections")

valid_members = (
    validated.where(F.col("rejection_reason").isNull())
    .select(
        F.concat_ws(":", F.col("country_code"), F.col("source_member_id")).alias("member_business_key"),
        "source_member_id", "member_name", "tier_code", "date_of_birth", "enrollment_date",
        "last_flight_date", "country_code",
        F.lit(None).cast("string").alias("member_type"),
        F.when(F.col("date_of_birth").isNotNull(), F.floor(F.months_between(as_of_date, F.col("date_of_birth")) / 12)).cast("int").alias("age"),
        F.when(F.col("last_flight_date").isNull(), F.lit(None).cast("boolean"))
         .otherwise(F.col("last_flight_date") < F.date_sub(as_of_date, 90)).alias("stale_member"),
        F.lit(None).cast("timestamp").alias("source_updated_at"),
        "batch_id", "source_file", "ingested_at",
    )
)

valid_members.write.format("delta").mode("append").saveAsTable("skypoints.silver_members")
