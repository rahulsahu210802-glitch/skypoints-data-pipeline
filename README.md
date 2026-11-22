# SkyPoints member data pipeline

Databricks implementation for the Incubyte Data Engineer assessment.

## Pipeline

- **Bronze:** retain the global member flat-file records and complete redemption JSON documents with batch and file metadata.
- **Silver:** normalize and validate member/transaction records; upsert the current record by member ID or transaction ID; quarantine rejected input.
- **Gold:** incrementally publish changed members to configured country tables and expose a member-redemption view.
- **Quality:** report duplicate, missing-key, unsupported-country, orphan-transaction, and Gold consistency checks.
- **Sample validation:** assert the synthetic assignment fixtures produce the expected Bronze, Silver, rejection, redemption, and Gold counts.

The primary assessment fixture is one global pipe-delimited file containing 150
members across `USA`, `IND`, `PHIL`, `CAN`, and `AU`. It follows the upstream
Source System contract in the PDF. The separate country workbooks are retained
only as an optional schema-harmonization reference; they are not tasks in the
primary workflow. Redemption JSON uses Spark's binary-file reader so each
formatted file is retained as one Bronze document on serverless compute.

See [docs/architecture.md](docs/architecture.md) for the production-scale ingestion, incremental processing, storage-layout, and quality-control design.
See [docs/excel-adapter.md](docs/excel-adapter.md) for the optional workbook compatibility path.

## Setup and execution

1. Create the `skypoints` schema and tables using [sql/01_create_tables.sql](sql/01_create_tables.sql).
2. Upload `samples/members_20240115.psv` and
   `samples/redemptions_20240115.json` to the exact Unity Catalog Volume paths
   configured in `databricks.yml`.
3. Validate and deploy the bundle. The workflow uses Free Edition-compatible serverless compute and installs its Python dependencies from `pyproject.toml`:

   ```bash
   databricks bundle validate --target dev
   databricks bundle deploy --target dev
   ```

4. Run the `skypoints-daily-pipeline` job. It ingests the global member feed and
   redemption JSON, processes Silver and Gold, runs quality checks, and fails if
   the assessment outputs differ from the expected results. A 02:00 UTC daily
   schedule is included in a paused state so deployment cannot trigger
   unexpected runs.

For manual execution, run member ingestion once for the global file and
redemption ingestion once, then run `02_silver_members.py` and
`04_silver_redemptions.py` before `03_gold_country_targets.py` and
`05_data_quality.py`. Pass one batch ID through every task. Daily landing files
must have immutable, date-specific paths; rerunning the same path is idempotent.

## Important assumptions

- Production member IDs and transaction IDs are globally unique. Global
  pipe-feed members therefore use `source_member_id` as the business key so a
  country change replaces the current record and removes it from the old Gold
  table.
- A redemption feed can join automatically only to globally keyed production members. The country-scoped workbook samples need a global member identifier, or country in the redemption feed, before that relationship is unambiguous.
- The sample feed has no reliable source-update timestamp. Until one is supplied, Silver uses ingestion time as the latest-record ordering fallback. Late-arriving old records therefore cannot be reliably ordered by business update time.
- `as_of_date` defaults to the processing date and can be overridden for reproducible age and stale-member calculations. A member is stale only when the last flight was more than 90 days before that date.
- Silver supports both PDF-described pipe layouts, with or without `Post_Code`.
  Country publication is controlled by `country_target_config`; the initial
  entries cover all five PDF countries.
- The notebooks use Spark's `try_to_timestamp`. The serverless project
  environment is pinned in `pyproject.toml`. The bundled daily schedule remains
  paused until delivery timing and production landing paths are approved.
