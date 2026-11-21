-- Run in a Databricks SQL warehouse or notebook before the pipeline.
CREATE SCHEMA IF NOT EXISTS skypoints;

CREATE TABLE IF NOT EXISTS skypoints.bronze_member_files (
  batch_id STRING,
  source_file STRING,
  source_country STRING,
  ingested_at TIMESTAMP,
  raw_record STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS skypoints.silver_members (
  member_business_key STRING NOT NULL,
  source_member_id STRING NOT NULL,
  member_name STRING NOT NULL,
  tier_code STRING,
  date_of_birth DATE,
  enrollment_date DATE NOT NULL,
  last_flight_date DATE,
  country_code STRING NOT NULL,
  member_type STRING,
  age INT,
  stale_member BOOLEAN,
  source_updated_at TIMESTAMP,
  batch_id STRING NOT NULL,
  source_file STRING NOT NULL,
  ingested_at TIMESTAMP NOT NULL
) USING DELTA;

CREATE TABLE IF NOT EXISTS skypoints.member_rejections (
  batch_id STRING NOT NULL,
  source_file STRING NOT NULL,
  source_country STRING,
  raw_record STRING,
  rejection_reason STRING NOT NULL,
  rejected_at TIMESTAMP NOT NULL
) USING DELTA;

CREATE TABLE IF NOT EXISTS skypoints.silver_redemptions (
  transaction_id STRING NOT NULL,
  source_member_id STRING NOT NULL,
  transaction_date DATE NOT NULL,
  partner STRING,
  miles_redeemed BIGINT,
  transaction_status STRING,
  feed_date DATE,
  batch_id STRING NOT NULL,
  ingested_at TIMESTAMP NOT NULL
) USING DELTA;
