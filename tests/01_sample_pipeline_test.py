# Databricks notebook source
# Fail the assessment workflow when the supplied sample produces unexpected results.

from pyspark.sql import functions as F


dbutils.widgets.text("catalog", "workspace", "Unity Catalog")
dbutils.widgets.text("schema", "skypoints", "Schema")
dbutils.widgets.text("batch_id", "", "Batch ID")

catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
batch_id = dbutils.widgets.get("batch_id").strip()

if not catalog or not schema or not batch_id:
    raise ValueError("catalog, schema, and batch_id parameters must be provided.")

prefix = f"`{catalog}`.`{schema}`."
bronze_members = spark.table(f"{prefix}`bronze_member_files`")
bronze_redemptions = spark.table(f"{prefix}`bronze_redemption_files`")
members = spark.table(f"{prefix}`silver_members`")
member_rejections = spark.table(f"{prefix}`member_rejections`")
redemptions = spark.table(f"{prefix}`silver_redemptions`")
redemption_rejections = spark.table(f"{prefix}`redemption_rejections`")
redemption_view = spark.table(f"{prefix}`member_redemption_view`")
quality_results = spark.table(f"{prefix}`quality_check_results`")
gold_india = spark.table(f"{prefix}`members_india`")
gold_usa = spark.table(f"{prefix}`members_usa`")
gold_philippines = spark.table(f"{prefix}`members_philippines`")
gold_canada = spark.table(f"{prefix}`members_canada`")
gold_australia = spark.table(f"{prefix}`members_australia`")

scenarios = {
    "20240115": {
        "bronze_member_rows": 151,
        "member_rejections": 0,
        "rejection_reason_count": 0,
        "stale_members": 150,
        "non_stale_members": 0,
        "redemption_rejections": 0,
        "redemption_rejection_reason_count": 0,
        "gold_india": 30,
        "gold_australia": 30,
        "moved_member_india": 0,
        "moved_member_australia": 1,
    },
    "20240116": {
        "bronze_member_rows": 156,
        "member_rejections": 5,
        "rejection_reason_count": 1,
        "stale_members": 150,
        "non_stale_members": 0,
        "redemption_rejections": 3,
        "redemption_rejection_reason_count": 1,
        "gold_india": 30,
        "gold_australia": 30,
        "moved_member_india": 0,
        "moved_member_australia": 1,
    },
    "20240117": {
        "bronze_member_rows": 151,
        "member_rejections": 0,
        "rejection_reason_count": 0,
        "stale_members": 140,
        "non_stale_members": 10,
        "redemption_rejections": 0,
        "redemption_rejection_reason_count": 0,
        "gold_india": 31,
        "gold_australia": 29,
        "moved_member_india": 1,
        "moved_member_australia": 0,
    },
}
expected = scenarios.get(batch_id)
if expected is None:
    raise ValueError(f"No sample-validation scenario exists for batch_id={batch_id}.")


def _count(frame):
    return frame.count()


def _quality_count(check_name):
    row = quality_results.where(
        (F.col("batch_id") == batch_id)
        & (F.col("check_name") == check_name)
    ).select("failed_row_count").first()
    if row is None:
        raise AssertionError(
            f"Missing persisted quality result for check '{check_name}'."
        )
    return row["failed_row_count"]


checks = [
    ("bronze_member_rows_for_batch", expected["bronze_member_rows"], _count(
        bronze_members.where(F.col("batch_id") == batch_id)
    )),
    ("bronze_global_rows_for_batch", expected["bronze_member_rows"], _count(
        bronze_members.where(
            (F.col("batch_id") == batch_id)
            & (F.col("source_country") == "GLOBAL")
        )
    )),
    ("bronze_redemption_documents_for_batch", 1, _count(
        bronze_redemptions.where(F.col("batch_id") == batch_id)
    )),
    ("valid_current_members", 150, _count(members)),
    ("distinct_member_business_keys", 150, members.select(
        "member_business_key"
    ).distinct().count()),
    ("member_rejections_for_batch", expected["member_rejections"], _count(
        member_rejections.where(F.col("batch_id") == batch_id)
    )),
    ("missing_member_name_rejections", expected["rejection_reason_count"], _count(
        member_rejections.where(
            (F.col("batch_id") == batch_id)
            & (F.col("rejection_reason") == "Missing member name")
        )
    )),
    ("missing_member_id_rejections", expected["rejection_reason_count"], _count(
        member_rejections.where(
            (F.col("batch_id") == batch_id)
            & (F.col("rejection_reason") == "Missing member ID")
        )
    )),
    ("invalid_enrollment_rejections", expected["rejection_reason_count"], _count(
        member_rejections.where(
            (F.col("batch_id") == batch_id)
            & (F.col("rejection_reason") == "Invalid or missing enrollment date")
        )
    )),
    ("invalid_tier_rejections", expected["rejection_reason_count"], _count(
        member_rejections.where(
            (F.col("batch_id") == batch_id)
            & (F.col("rejection_reason") == "Invalid tier code")
        )
    )),
    ("unsupported_country_rejections", expected["rejection_reason_count"], _count(
        member_rejections.where(
            (F.col("batch_id") == batch_id)
            & (F.col("rejection_reason") == "Missing or unsupported country code")
        )
    )),
    ("members_with_unknown_age", 0, _count(
        members.where(F.col("age").isNull())
    )),
    ("stale_members", expected["stale_members"], _count(
        members.where(F.col("stale_member"))
    )),
    ("non_stale_members", expected["non_stale_members"], _count(
        members.where(~F.col("stale_member"))
    )),
    ("moved_member_in_silver_india", expected["moved_member_india"], _count(
        members.where(
            (F.col("source_member_id") == "2256")
            & (F.col("country_code") == "IND")
        )
    )),
    ("silver_redemption_transactions", 9, _count(redemptions)),
    ("redemption_rejections_for_batch", expected["redemption_rejections"], _count(
        redemption_rejections.where(F.col("batch_id") == batch_id)
    )),
    ("negative_miles_rejections", expected["redemption_rejection_reason_count"], _count(
        redemption_rejections.where(
            (F.col("batch_id") == batch_id)
            & (F.col("rejection_reason") == "Miles redeemed cannot be negative")
        )
    )),
    ("future_transaction_rejections", expected["redemption_rejection_reason_count"], _count(
        redemption_rejections.where(
            (F.col("batch_id") == batch_id)
            & (F.col("rejection_reason") == "Transaction date is after feed date")
        )
    )),
    ("missing_transaction_id_rejections", expected["redemption_rejection_reason_count"], _count(
        redemption_rejections.where(
            (F.col("batch_id") == batch_id)
            & (F.col("rejection_reason") == "Missing transaction ID")
        )
    )),
    ("joined_sample_redemptions", 9, _count(
        redemption_view.where(F.col("member_name").isNotNull())
    )),
    ("orphan_sample_redemptions", 0, _count(
        redemption_view.where(F.col("member_name").isNull())
    )),
    ("quality_checks_for_batch", 10, _count(
        quality_results.where(F.col("batch_id") == batch_id)
    )),
    ("nonzero_quality_checks_for_batch", 0, _count(
        quality_results.where(
            (F.col("batch_id") == batch_id)
            & (F.col("failed_row_count") > 0)
        )
    )),
    ("reported_orphan_redemptions", 0, _quality_count(
        "redemptions_without_member"
    )),
    ("gold_ind_members", expected["gold_india"], _count(gold_india)),
    ("gold_usa_members", 30, _count(gold_usa)),
    ("gold_philippines_members", 30, _count(gold_philippines)),
    ("gold_canada_members", 30, _count(gold_canada)),
    ("gold_australia_members", expected["gold_australia"], _count(gold_australia)),
    ("moved_member_gold_india", expected["moved_member_india"], _count(
        gold_india.where(F.col("source_member_id") == "2256")
    )),
    ("moved_member_gold_australia", expected["moved_member_australia"], _count(
        gold_australia.where(F.col("source_member_id") == "2256")
    )),
]

results = spark.createDataFrame(
    [
        (check_name, expected, actual, expected == actual)
        for check_name, expected, actual in checks
    ],
    ["check_name", "expected_count", "actual_count", "passed"],
)
display(results.orderBy("check_name"))

failures = results.where(~F.col("passed")).collect()
if failures:
    details = "; ".join(
        f"{row['check_name']}: expected {row['expected_count']}, "
        f"got {row['actual_count']}"
        for row in failures
    )
    raise AssertionError(f"Sample pipeline validation failed: {details}")
