# Databricks notebook source
# Normalize pipe-delimited production records and the assessment CSV/XLSX samples.

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType
from delta.tables import DeltaTable


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
country_config_table = f"{schema_name}.`country_target_config`"

as_of_date = (
    F.to_date(F.lit(as_of_date_text), "yyyy-MM-dd")
    if as_of_date_text
    else F.current_date()
)

# Excel/CSV sample rows are retained as JSON in Bronze. The pipe feed remains
# the production format and is parsed by its documented positional layout.
sample_schema = StructType([
    StructField(name, StringType(), True)
    for name in (
        "Unique ID", "Member Name", "Tier Type", "Date of Birth",
        "Date of Enrollment", "Date of Flight", "ID", "Name", "DOB",
        "TierCode", "EnrollmentDate", "Individual or Corporate",
        "Flight Date", "FlightDate",
    )
])

bronze = spark.table(bronze_table)
if batch_id:
    bronze = bronze.where(F.col("batch_id") == batch_id)

bronze = bronze.withColumn(
    "_is_sample_json",
    F.trim(F.col("raw_record")).startswith("{"),
).withColumn(
    "_sample",
    F.from_json(
        F.col("raw_record"),
        sample_schema,
        {"primitivesAsString": "true"},
    ),
)

pipe_record = F.regexp_replace(F.trim(F.col("raw_record")), r"^\|", "")
fields = F.split(pipe_record, r"\|")


def _pipe_field(position):
    return F.trim(F.try_element_at(fields, F.lit(position)))


def _sample_field(name):
    return F.col("_sample").getField(name)


def _date(value):
    text = F.trim(value.cast("string"))
    compact = F.lpad(text, 8, "0")
    formats = (
        (text, "yyyyMMdd"),
        (compact, "MMddyyyy"),
        (text, "yyyy-MM-dd"),
        (text, "yyyy-MM-dd HH:mm:ss"),
        (text, "M/d/yyyy"),
        (text, "MM/dd/yyyy"),
    )
    return F.coalesce(*[
        F.try_to_timestamp(candidate, F.lit(pattern)).cast("date")
        for candidate, pattern in formats
    ])


parsed = bronze.select(
    "batch_id", "source_file", "source_country", "ingested_at", "raw_record",
    "_is_sample_json",
    F.size(fields).alias("_field_count"),
    _pipe_field(2).alias("_pipe_member_name"),
    _pipe_field(3).alias("_pipe_member_id"),
    _pipe_field(4).alias("_pipe_enrollment_date"),
    _pipe_field(5).alias("_pipe_last_flight_date"),
    _pipe_field(6).alias("_pipe_tier"),
    _pipe_field(7).alias("_pipe_agent"),
    _pipe_field(8).alias("_pipe_state"),
    F.when(F.size(fields) >= 12, _pipe_field(10))
     .otherwise(F.lit(None).cast("string")).alias("_pipe_post_code"),
    _pipe_field(9).alias("_pipe_country"),
    F.when(F.size(fields) >= 12, _pipe_field(11))
     .otherwise(_pipe_field(10)).alias("_pipe_dob"),
    F.when(F.size(fields) >= 12, _pipe_field(12))
     .otherwise(_pipe_field(11)).alias("_pipe_active"),
    F.coalesce(
        _sample_field("Unique ID"), _sample_field("ID")
    ).alias("_sample_member_id"),
    F.coalesce(
        _sample_field("Member Name"), _sample_field("Name")
    ).alias("_sample_member_name"),
    F.coalesce(
        _sample_field("Tier Type"), _sample_field("TierCode")
    ).alias("_sample_tier"),
    F.coalesce(
        _sample_field("Date of Birth"), _sample_field("DOB")
    ).alias("_sample_dob"),
    F.coalesce(
        _sample_field("Date of Enrollment"), _sample_field("EnrollmentDate")
    ).alias("_sample_enrollment_date"),
    F.coalesce(
        _sample_field("Date of Flight"),
        _sample_field("Flight Date"),
        _sample_field("FlightDate"),
    ).alias("_sample_last_flight_date"),
    _sample_field("Individual or Corporate").alias("_sample_member_type"),
)

normalized = parsed.select(
    "batch_id", "source_file", "source_country", "ingested_at", "raw_record",
    "_is_sample_json", "_field_count",
    F.when(F.col("_is_sample_json"), F.trim(F.col("_sample_member_id")))
     .otherwise(F.col("_pipe_member_id")).alias("source_member_id"),
    F.when(F.col("_is_sample_json"), F.trim(F.col("_sample_member_name")))
     .otherwise(F.col("_pipe_member_name")).alias("member_name"),
    F.when(F.col("_is_sample_json"), F.upper(F.trim(F.col("_sample_tier"))))
     .otherwise(F.upper(F.col("_pipe_tier"))).alias("tier_code"),
    F.when(F.col("_is_sample_json"), _date(F.col("_sample_dob")))
     .otherwise(_date(F.col("_pipe_dob"))).alias("date_of_birth"),
    F.when(F.col("_is_sample_json"), _date(F.col("_sample_enrollment_date")))
     .otherwise(_date(F.col("_pipe_enrollment_date"))).alias("enrollment_date"),
    F.when(F.col("_is_sample_json"), _date(F.col("_sample_last_flight_date")))
     .otherwise(_date(F.col("_pipe_last_flight_date"))).alias("last_flight_date"),
    F.when(
        F.col("_is_sample_json"),
        F.when(F.upper(F.col("source_country")) == "AUS", "AU")
         .otherwise(F.upper(F.col("source_country"))),
    ).otherwise(
        F.when(F.upper(F.col("_pipe_country")) == "AUS", "AU")
         .otherwise(F.upper(F.col("_pipe_country")))
    ).alias("country_code"),
    F.when(F.col("_is_sample_json"), F.upper(F.trim(F.col("_sample_member_type"))))
     .otherwise(F.lit(None).cast("string")).alias("member_type"),
    F.when(F.col("_is_sample_json"), F.lit(None).cast("string"))
     .otherwise(F.trim(F.col("_pipe_agent"))).alias("agent_name"),
    F.when(F.col("_is_sample_json"), F.lit(None).cast("string"))
     .otherwise(F.upper(F.trim(F.col("_pipe_state")))).alias("state"),
    F.when(F.col("_is_sample_json"), F.lit(None).cast("string"))
     .otherwise(F.trim(F.col("_pipe_post_code"))).alias("post_code"),
    F.when(F.col("_is_sample_json"), F.lit(None).cast("string"))
     .otherwise(F.upper(F.trim(F.col("_pipe_active")))).alias("active_flag"),
)

detail = normalized.where(
    F.col("_is_sample_json")
    | (_pipe_field(1) == "D")
)

supported_country_codes = [
    row["country_code"]
    for row in (
        spark.table(country_config_table)
        .where(F.col("is_active"))
        .select("country_code")
        .collect()
    )
]
if not supported_country_codes:
    raise ValueError("No active countries exist in country_target_config.")

validated = detail.withColumn(
    "rejection_reason",
    F.when(
        (~F.col("_is_sample_json")) & (~F.col("_field_count").isin(11, 12)),
        "Malformed detail record: expected 11 or 12 fields",
    )
    .when(F.col("source_member_id").isNull() | (F.col("source_member_id") == ""), "Missing member ID")
    .when(F.length(F.col("source_member_id")) > 18, "Member ID exceeds 18 characters")
    .when(F.col("member_name").isNull() | (F.col("member_name") == ""), "Missing member name")
    .when(F.length(F.col("member_name")) > 255, "Member name exceeds 255 characters")
    .when(F.col("enrollment_date").isNull(), "Invalid or missing enrollment date")
    .when(F.col("enrollment_date") > as_of_date, "Enrollment date is in the future")
    .when(
        F.col("date_of_birth").isNotNull() & (F.col("date_of_birth") > as_of_date),
        "Date of birth is in the future",
    )
    .when(
        F.col("last_flight_date").isNotNull()
        & (F.col("last_flight_date") < F.col("enrollment_date")),
        "Last flight date is before enrollment date",
    )
    .when(
        F.col("tier_code").isNull()
        | (~F.col("tier_code").isin("GLD", "SLV", "PLT")),
        "Invalid tier code",
    )
    .when(
        F.col("country_code").isNull()
        | (F.col("country_code") == "")
        | (~F.col("country_code").isin(*supported_country_codes)),
        "Missing or unsupported country code",
    )
    .when(F.length(F.col("state")) > 5, "State exceeds 5 characters")
    .when(F.length(F.col("post_code")) > 5, "Post code exceeds 5 characters")
    .when(F.length(F.col("agent_name")) > 255, "Agent name exceeds 255 characters")
    .when(
        F.col("active_flag").isNotNull()
        & (~F.col("active_flag").isin("A", "I")),
        "Invalid active member flag",
    )
)

rejections = (
    validated.where(F.col("rejection_reason").isNotNull())
    .select(
        "batch_id", "source_file", "source_country", "raw_record",
        "rejection_reason", F.current_timestamp().alias("rejected_at"),
    )
    .dropDuplicates(["batch_id", "source_file", "raw_record", "rejection_reason"])
)

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

valid_members = (
    validated.where(F.col("rejection_reason").isNull())
    .select(
        F.when(
            F.col("_is_sample_json"),
            F.concat_ws(":", F.col("country_code"), F.col("source_member_id")),
        ).otherwise(F.col("source_member_id")).alias("member_business_key"),
        "source_member_id", "member_name", "tier_code", "date_of_birth",
        "enrollment_date", "last_flight_date", "country_code", "member_type",
        "agent_name", "state", "post_code", "active_flag",
        F.when(
            F.col("date_of_birth").isNotNull(),
            F.floor(F.months_between(as_of_date, F.col("date_of_birth")) / 12),
        ).cast("int").alias("age"),
        F.when(F.col("last_flight_date").isNull(), F.lit(None).cast("boolean"))
         .otherwise(F.datediff(as_of_date, F.col("last_flight_date")) > 90)
         .alias("stale_member"),
        F.lit(None).cast("timestamp").alias("source_updated_at"),
        "batch_id", "source_file", "ingested_at",
    )
)

latest_in_batch = Window.partitionBy("member_business_key").orderBy(
    F.col("ingested_at").desc_nulls_last(),
    F.col("batch_id").desc(),
    F.col("source_file").desc(),
)
valid_members = (
    valid_members.withColumn("_row_number", F.row_number().over(latest_in_batch))
    .where(F.col("_row_number") == 1)
    .drop("_row_number")
)

member_value_changed = "NOT (" + " AND ".join([
    f"target.{column} <=> source.{column}"
    for column in (
        "source_member_id", "member_name", "tier_code", "date_of_birth",
        "enrollment_date", "last_flight_date", "country_code", "member_type",
        "agent_name", "state", "post_code", "active_flag", "age",
        "stale_member",
    )
]) + ")"
source_is_latest = (
    "("
    "source.source_updated_at IS NOT NULL AND ("
    "target.source_updated_at IS NULL "
    "OR source.source_updated_at > target.source_updated_at "
    "OR (source.source_updated_at = target.source_updated_at "
    "AND source.ingested_at >= target.ingested_at)"
    ")) OR (source.source_updated_at IS NULL "
    "AND target.source_updated_at IS NULL "
    "AND source.ingested_at >= target.ingested_at)"
)

(
    DeltaTable.forName(spark, silver_table.replace("`", ""))
    .alias("target")
    .merge(
        valid_members.alias("source"),
        "target.member_business_key = source.member_business_key",
    )
    .whenMatchedUpdateAll(
        condition=f"({source_is_latest}) AND ({member_value_changed})"
    )
    .whenNotMatchedInsertAll()
    .execute()
)

metrics = [
    ("parsed_detail_rows", detail.count()),
    ("valid_member_rows", valid_members.count()),
    ("rejected_member_rows", rejections.count()),
]
display(spark.createDataFrame(metrics, ["metric_name", "row_count"]))
