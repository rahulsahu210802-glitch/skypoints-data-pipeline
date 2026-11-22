# One-time setup

## 1. Git

This folder is a local Git repository on the `main` branch. Before the first commit, configure your Git name and email if they are not already configured.

Create an empty repository in GitHub, then connect this folder to it:

```bash
git remote add origin <your-github-repository-url>
git add README.md docs sql databricks.yml .gitignore
git commit -m "chore: initialize SkyPoints pipeline"
git push -u origin main
```

The provided assessment files remain excluded from Git.

## 2. Databricks database home

Use one schema for this assessment in the catalog selected in your SQL editor:

```sql
CREATE SCHEMA IF NOT EXISTS skypoints;
```

Run `sql/01_create_tables.sql` after selecting the catalog in your SQL editor. The Free Edition bundle defaults to the `workspace` catalog; change it in `databricks.yml` for another workspace.

## 3. Generate and upload sample inputs

The committed fixtures can be regenerated deterministically:

```bash
python tools/generate_sample_data.py
```

Upload the baseline files:

- `samples/members_20240115.psv` to
  `/Volumes/workspace/skypoints/landing/members/members_20240115.psv`
- `samples/redemptions_20240115.json` to
  `/Volumes/workspace/skypoints/landing/redemptions/redemptions_20240115.json`

For the rejection demonstration, also upload:

- `samples/members_20240116.psv` to
  `/Volumes/workspace/skypoints/landing/members/members_20240116.psv`
- `samples/redemptions_20240116.json` to
  `/Volumes/workspace/skypoints/landing/redemptions/redemptions_20240116.json`

The exact defaults are configured by `profile_file_path`,
`redemption_file_path`, and the shared logical `batch_id` in `databricks.yml`.
They currently select the `20240116` rejection-demonstration delivery.

The production landing contract uses a new immutable, date-specific path for
each delivery. Do not replace a file after it has been processed.

## 4. Databricks project deployment

Install and authenticate the Databricks CLI, then validate and deploy the serverless bundle from this folder:

```bash
databricks bundle validate --target dev
databricks bundle deploy --target dev
```

The bundle creates the `skypoints-daily-pipeline` job on serverless compute. The
job ingests one global member feed and one redemption feed, processes Silver
and Gold, persists quality results, and runs the sample assertions. Its daily
02:00 UTC schedule is deployed as `PAUSED`.

For the first global-feed demonstration, use a clean assessment schema so data
from the earlier workbook experiment cannot affect expected counts. Then rerun
`sql/01_create_tables.sql`, deploy the bundle, and start one complete job run.

For each real daily delivery, update the two bundle input paths and `batch_id`
to the new delivery date before triggering the run. Reuse the same paths and
batch ID when retrying that delivery; Bronze prevents duplicate appends while
downstream tasks safely reprocess the same logical batch.
