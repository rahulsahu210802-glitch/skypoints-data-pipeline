# Databricks notebook source
# Simple, visible checks for the live demonstration.

from pyspark.sql import functions as F

checks = [
    ("member_rows", spark.table("skypoints.silver_members").count()),
    ("rejected_member_rows", spark.table("skypoints.member_rejections").count()),
    ("duplicate_member_keys", spark.table("skypoints.silver_members").groupBy("member_business_key").count().where("count > 1").count()),
    ("duplicate_transaction_ids", spark.table("skypoints.silver_redemptions").groupBy("transaction_id").count().where("count > 1").count()),
    ("redemptions_without_member", spark.sql("""
        SELECT r.transaction_id
        FROM skypoints.silver_redemptions r
        LEFT ANTI JOIN skypoints.silver_members m
          ON r.source_member_id = m.source_member_id
    """).count()),
]

display(spark.createDataFrame(checks, ["check_name", "failed_row_count"]))
