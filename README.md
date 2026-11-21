# SkyPoints member data pipeline

Databricks implementation for the Incubyte Data Engineer assessment.

## Pipeline layers

1. **Bronze** - immutable copies of incoming member-profile and redemption data, with ingestion metadata.
2. **Silver** - canonical member and redemption records, validated and standardized.
3. **Gold** - current member tables for each country and a member-redemption analytical view.

## Workspace home

- **Git:** this folder tracks all solution code and documentation. The original assessment files are deliberately excluded.
- **Databricks:** use the `skypoints` schema in your Unity Catalog catalog. It will hold the Bronze, Silver, Gold, rejection, and quality-metric tables.
- **Deployment:** `databricks.yml` is the Databricks Asset Bundle configuration. It will deploy notebooks and the workflow after they are added.

## Proposed Databricks workflow

Run notebooks in this order:

1. `notebooks/01_bronze_ingest.py`
2. `notebooks/02_silver_members.py`
3. `notebooks/03_gold_country_targets.py`
4. `notebooks/04_silver_redemptions.py`
5. `notebooks/05_data_quality.py`

The supplied spreadsheets are reference samples. The production pipeline expects daily profile files and a redemption JSON feed in cloud storage or a Databricks Volume.

## Design decisions to confirm

- The source must provide a reliable `source_updated_at` or sequence number for the "latest record wins" rule.
- `member_id` must be globally unique. Until confirmed, the pipeline uses `source_country + source_member_id` as the business key.
- `as_of_date` is the processing date used for age and stale-member calculations.
