# SkyPoints member data pipeline

Databricks implementation for the Incubyte Data Engineer assessment.

## Pipeline

- **Bronze:** retain raw member records and complete redemption JSON documents with batch and file metadata.
- **Silver:** normalize and validate member/transaction records; upsert the current record by member ID or transaction ID; quarantine rejected input.
- **Gold:** publish current member records to country tables and expose a member-redemption view.
- **Quality:** report duplicate, missing-key, unsupported-country, orphan-transaction, and Gold consistency checks.

The supplied country workbooks/CSVs are small assessment samples. The Bronze notebook can read them through pandas and stores each row as JSON. This driver-side path is sample-only; production feeds should use distributed pipe-delimited text ingestion. Redemption JSON is read as whole text so pretty-printed documents remain intact.

## Setup and execution

1. Create the `skypoints` schema and tables using [sql/01_create_tables.sql](sql/01_create_tables.sql).
2. Upload each member sample to the matching Unity Catalog Volume path and the redemption JSON file(s) to the redemption landing path. The default paths are in `databricks.yml`.
3. Validate and deploy the bundle. The workflow uses Free Edition-compatible serverless compute and installs its Python dependencies from `pyproject.toml`:

   ```bash
   databricks bundle validate --target dev
   databricks bundle deploy --target dev
   ```

4. Run the `skypoints-daily-pipeline` job. It ingests the three country feeds and redemption JSON, then processes Silver, Gold, and quality checks.

For manual execution, run member ingestion once per country, redemption ingestion once, then run `02_silver_members.py` and `04_silver_redemptions.py` before `03_gold_country_targets.py` and `05_data_quality.py`. Reuse the same `batch_id` when retrying a batch.

## Important assumptions

- Production member IDs and transaction IDs are globally unique. Pipe-feed members therefore use `source_member_id` as the business key so a country change replaces the current record and removes it from the old Gold table. The supplied workbooks reuse IDs `1` through `3` for different people across countries, so sample workbook rows use `country_code:source_member_id` to avoid collapsing unrelated members.
- A redemption feed can join automatically only to globally keyed production members. The country-scoped workbook samples need a global member identifier, or country in the redemption feed, before that relationship is unambiguous.
- The sample feed has no reliable source-update timestamp. Until one is supplied, Silver uses ingestion time as the latest-record ordering fallback. Late-arriving old records therefore cannot be reliably ordered by business update time.
- `as_of_date` defaults to the processing date and can be overridden for reproducible age and stale-member calculations. A member is stale only when the last flight was more than 90 days before that date.
- Silver supports both observed pipe layouts (with or without the Post Code field) and the supplied CSV/XLSX sample column names; confirm the production layout before deployment.
- The notebooks use Spark's `try_to_timestamp`. The serverless project environment is pinned in `pyproject.toml`. The bundled job is manually triggered; configure a schedule only after agreeing on delivery timing and landing-path conventions.
