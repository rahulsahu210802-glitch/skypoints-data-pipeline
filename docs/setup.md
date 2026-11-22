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

## 3. Upload sample inputs

Upload the member workbook/CSV files to the paths configured by `profile_ind_path`, `profile_usa_path`, and `profile_aus_path` in `databricks.yml`. Upload redemption JSON files to `redemption_file_path`. For a pipe-delimited production feed, set the corresponding profile path to a file or directory of text files.

The pandas-based workbook/CSV reader is only intended for the small assessment samples. Do not use it to ingest production-sized data.
XLSX sample ingestion requires `pandas` and `openpyxl` on the Databricks cluster; CSV sample ingestion requires `pandas`.

## 4. Databricks project deployment

Install and authenticate the Databricks CLI, then validate and deploy the serverless bundle from this folder:

```bash
databricks bundle validate --target dev
databricks bundle deploy --target dev
```

The bundle creates the `skypoints-daily-pipeline` job on serverless compute. Its project environment installs the pinned Excel dependency from `pyproject.toml`. The job ingests the three member feeds and redemption feed, processes Silver and Gold, and runs the quality checks. Use a batch-specific landing path or a new batch ID for each daily delivery; reuse a batch ID only when retrying that same delivery.
