# Architecture

```text
Upstream Source System
  ├─> one dated global member flat file ──┐
  └─> one dated redemption JSON feed ─────┴─> Bronze ─> Silver ─> configured country tables
                                                               │
                                                               └─> rejections / quality results
```

## Layer responsibilities

| Layer | Purpose | Examples |
| --- | --- | --- |
| Bronze | Preserve profile rows and complete JSON documents with batch/file metadata. | `bronze_member_files`, `bronze_redemption_files` |
| Silver | Standardize and validate member/transaction data; upsert the current record by global ID. | `silver_members`, `silver_redemptions` |
| Gold | Publish each changed current member exactly once to its configured country table and provide the member-redemption view. | `members_india`, `members_usa`, `members_philippines`, `members_canada`, `members_australia`, `member_redemption_view` |

Bad records never disappear. They are written to `member_rejections` with the original row and a reason.

Malformed or invalid redemption records are written to `redemption_rejections`.
Bronze uses Spark's binary-file reader so each formatted multi-line JSON file
becomes exactly one raw document instead of one row per line.

## Ordering and identity

Production pipe-feed members use `source_member_id` across countries so a
country move replaces the prior country assignment. Gold deletes keys changed
by the current batch from every configured target before inserting their latest
country record. This prevents a moved member from remaining in the old table.

The source does not currently provide a business update timestamp; ingestion time is therefore only a fallback ordering signal. A source update timestamp or sequence is required to correctly order late-arriving records.

The Silver member parser accepts the sample detail row with 11 fields and the PDF-described variant with a Post Code field (12 fields).

The primary workflow uses the global pipe feed. Optional Excel/CSV compatibility
is isolated from the workflow and documented in `docs/excel-adapter.md`.

## Production-scale design

The assessment workbooks exercise parsing and validation but are not the billion-row transport format. A production rollout should use the following controls:

- Deliver member records as splittable pipe-delimited text and redemptions as newline-delimited JSON or many bounded JSON files. Do not use pandas, Excel, or one giant whole-text JSON document.
- Use Auto Loader or an equivalent managed file-ingestion service for incremental discovery, schema enforcement, checkpoints, and back-pressure rather than rescanning an entire landing directory.
- Keep Bronze append-only with `batch_id`, source file, ingestion timestamp, and the unchanged source record. Reuse a batch ID only when retrying the same logical delivery.
- Require a source update timestamp or monotonically increasing sequence. Ingestion time cannot correctly order a late-arriving older member record.
- Process only the current batch into Silver, deduplicate inside that batch, and use Delta `MERGE` on the stable business key or transaction ID.
- Do not update an unchanged Silver member merely because it appeared in the
  next full delivery. Compare canonical and derived values during `MERGE` so
  only new or changed members receive the new batch ID and flow to Gold.
- Partition large Bronze tables by a low-cardinality ingestion date when file volume justifies it. Prefer liquid clustering on frequently filtered or merged keys such as `member_business_key` and `transaction_id`; never partition by those high-cardinality identifiers.
- Control output-file size and schedule compaction based on observed file counts and query performance. Apply retention and `VACUUM` policies only after recovery requirements are agreed.
- Publish only member keys changed by the current batch rather than rewriting
  every country table. `country_target_config` maps source country codes to
  physical target tables, so a country can be added without changing notebook
  code.
- Persist quality metrics by batch and alert or stop publication when agreed thresholds are exceeded. The assessment sample intentionally reports two orphan redemptions because its JSON member ID is absent from the supplied workbooks.
- Limit concurrent tasks to workspace quotas and use serverless autoscaling.
  The member and redemption ingestion tasks can run concurrently.
