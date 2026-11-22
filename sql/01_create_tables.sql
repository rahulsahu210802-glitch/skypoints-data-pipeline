-- Run in a Databricks SQL warehouse or notebook before the pipeline.
CREATE SCHEMA IF NOT EXISTS skypoints;

CREATE TABLE IF NOT EXISTS skypoints.bronze_member_files (
  batch_id STRING,
  source_file STRING,
  source_country STRING,
  ingested_at TIMESTAMP,
  raw_record STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS skypoints.bronze_redemption_files (
  batch_id STRING NOT NULL,
  source_file STRING NOT NULL,
  ingested_at TIMESTAMP NOT NULL,
  raw_json STRING NOT NULL
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
  agent_name STRING,
  state STRING,
  post_code STRING,
  active_flag STRING,
  age INT,
  stale_member BOOLEAN,
  source_updated_at TIMESTAMP,
  batch_id STRING NOT NULL,
  source_file STRING NOT NULL,
  ingested_at TIMESTAMP NOT NULL
) USING DELTA;

CREATE TABLE IF NOT EXISTS skypoints.country_target_config (
  country_code STRING NOT NULL,
  target_table STRING NOT NULL,
  is_active BOOLEAN NOT NULL
) USING DELTA;

MERGE INTO skypoints.country_target_config AS target
USING (
  SELECT * FROM VALUES
    ('USA', 'members_usa', TRUE),
    ('IND', 'members_india', TRUE),
    ('PHIL', 'members_philippines', TRUE),
    ('CAN', 'members_canada', TRUE),
    ('AU', 'members_australia', TRUE)
  AS configuration(country_code, target_table, is_active)
) AS source
ON target.country_code = source.country_code
WHEN MATCHED THEN UPDATE SET
  target.target_table = source.target_table,
  target.is_active = source.is_active
WHEN NOT MATCHED THEN INSERT (country_code, target_table, is_active)
VALUES (source.country_code, source.target_table, source.is_active);

CREATE TABLE IF NOT EXISTS skypoints.members_india
LIKE skypoints.silver_members;

CREATE TABLE IF NOT EXISTS skypoints.members_usa
LIKE skypoints.silver_members;

CREATE TABLE IF NOT EXISTS skypoints.members_philippines
LIKE skypoints.silver_members;

CREATE TABLE IF NOT EXISTS skypoints.members_canada
LIKE skypoints.silver_members;

CREATE TABLE IF NOT EXISTS skypoints.members_australia
LIKE skypoints.silver_members;

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
  source_file STRING NOT NULL,
  transaction_date DATE NOT NULL,
  partner STRING,
  miles_redeemed BIGINT,
  transaction_status STRING,
  feed_date DATE,
  batch_id STRING NOT NULL,
  ingested_at TIMESTAMP NOT NULL
) USING DELTA;

CREATE TABLE IF NOT EXISTS skypoints.redemption_rejections (
  batch_id STRING NOT NULL,
  source_file STRING NOT NULL,
  raw_record STRING,
  rejection_reason STRING NOT NULL,
  rejected_at TIMESTAMP NOT NULL
) USING DELTA;

CREATE TABLE IF NOT EXISTS skypoints.quality_check_results (
  batch_id STRING NOT NULL,
  check_name STRING NOT NULL,
  failed_row_count BIGINT NOT NULL,
  checked_at TIMESTAMP NOT NULL
) USING DELTA;
