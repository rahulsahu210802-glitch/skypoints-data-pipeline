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
redemption_view = spark.table(f"{prefix}`member_redemption_view`")
quality_results = spark.table(f"{prefix}`quality_check_results`")


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
    ("bronze_member_rows_for_batch", 9, _count(
        bronze_members.where(F.col("batch_id") == batch_id)
    )),
    ("bronze_ind_rows_for_batch", 3, _count(
        bronze_members.where(
            (F.col("batch_id") == batch_id) & (F.col("source_country") == "IND")
        )
    )),
    ("bronze_usa_rows_for_batch", 3, _count(
        bronze_members.where(
            (F.col("batch_id") == batch_id) & (F.col("source_country") == "USA")
        )
    )),
    ("bronze_aus_rows_for_batch", 3, _count(
        bronze_members.where(
            (F.col("batch_id") == batch_id) & (F.col("source_country") == "AUS")
        )
    )),
    ("bronze_redemption_documents_for_batch", 1, _count(
        bronze_redemptions.where(F.col("batch_id") == batch_id)
    )),
    ("valid_current_members", 8, _count(members)),
    ("distinct_member_business_keys", 8, members.select(
        "member_business_key"
    ).distinct().count()),
    ("member_rejections_for_batch", 1, _count(
        member_rejections.where(F.col("batch_id") == batch_id)
    )),
    ("invalid_enrollment_rejections_for_batch", 1, _count(
        member_rejections.where(
            (F.col("batch_id") == batch_id)
            & (F.col("rejection_reason") == "Invalid or missing enrollment date")
        )
    )),
    ("members_with_unknown_age", 1, _count(
        members.where(F.col("age").isNull())
    )),
    ("stale_members", 8, _count(
        members.where(F.col("stale_member"))
    )),
    ("silver_redemption_transactions", 2, _count(redemptions)),
    ("orphan_sample_redemptions", 2, _count(
        redemption_view.where(F.col("member_name").isNull())
    )),
    ("quality_checks_for_batch", 8, _count(
        quality_results.where(F.col("batch_id") == batch_id)
    )),
    ("nonzero_quality_checks_for_batch", 1, _count(
        quality_results.where(
            (F.col("batch_id") == batch_id)
            & (F.col("failed_row_count") > 0)
        )
    )),
    ("reported_orphan_redemptions", 2, _quality_count(
        "redemptions_without_member"
    )),
    ("gold_ind_members", 3, _count(
        spark.table(f"{prefix}`members_india`")
    )),
    ("gold_usa_members", 3, _count(
        spark.table(f"{prefix}`members_usa`")
    )),
    ("gold_aus_members", 2, _count(
        spark.table(f"{prefix}`members_australia`")
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
