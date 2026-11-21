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

Use one schema for this assessment:

```sql
CREATE SCHEMA IF NOT EXISTS main.skypoints;
```

If your Databricks workspace uses another Unity Catalog catalog, replace `main` with that catalog name. Run `sql/01_create_tables.sql` in a Databricks SQL editor after selecting the same catalog and schema.

## 3. Databricks project deployment

Install and authenticate the Databricks CLI, then validate the bundle from this folder:

```bash
databricks bundle validate --target dev
```

We will add notebooks and the daily Databricks Workflow as the pipeline is implemented.
