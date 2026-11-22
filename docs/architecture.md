# Architecture

```text
Member profile files ──┐
                      ├─> Bronze (raw Delta tables) ─> Silver (validated canonical tables) ─> Gold country tables
Redemption JSON ───────┘                                      │
                                                              └─> Rejections / quality metrics
```

## Layer responsibilities

| Layer | Purpose | Examples |
| --- | --- | --- |
| Bronze | Preserve profile rows and complete JSON documents with batch/file metadata. | `bronze_member_files`, `bronze_redemption_files` |
| Silver | Standardize and validate member/transaction data; upsert the current record by global ID. | `silver_members`, `silver_redemptions` |
| Gold | Publish the current member exactly once to the country table and provide the member-redemption view. | `members_india`, `members_usa`, `members_australia`, `member_redemption_view` |

Bad records never disappear. They are written to `member_rejections` with the original row and a reason.

Malformed or invalid redemption records are written to `redemption_rejections`. Bronze reads JSON with whole-file mode so formatted multi-line documents are not split into unrelated text lines.

## Ordering and identity

Production pipe-feed members use `source_member_id` across countries so a country move replaces the prior country assignment. The supplied workbooks reuse IDs `1` through `3` for different people, so workbook rows use `country_code:source_member_id` as their assessment-only business key. Those workbook rows cannot be joined unambiguously to redemption data unless the source provides a global member identifier or includes country in the redemption feed.

The source does not currently provide a business update timestamp; ingestion time is therefore only a fallback ordering signal. A source update timestamp or sequence is required to correctly order late-arriving records.

The Silver member parser accepts the sample detail row with 11 fields and the PDF-described variant with a Post Code field (12 fields).

Excel/CSV support is intentionally limited to the small assessment samples and uses driver-side pandas. Production-sized inputs must use distributed text ingestion or a managed ingestion service.

## Production-scale design

The assessment workbooks exercise parsing and validation but are not the billion-row transport format. A production rollout should use the following controls:

- Deliver member records as splittable pipe-delimited text and redemptions as newline-delimited JSON or many bounded JSON files. Do not use pandas, Excel, or one giant whole-text JSON document.
- Use Auto Loader or an equivalent managed file-ingestion service for incremental discovery, schema enforcement, checkpoints, and back-pressure rather than rescanning an entire landing directory.
- Keep Bronze append-only with `batch_id`, source file, ingestion timestamp, and the unchanged source record. Reuse a batch ID only when retrying the same logical delivery.
- Require a source update timestamp or monotonically increasing sequence. Ingestion time cannot correctly order a late-arriving older member record.
- Process only the current batch into Silver, deduplicate inside that batch, and use Delta `MERGE` on the stable business key or transaction ID.
- Partition large Bronze tables by a low-cardinality ingestion date when file volume justifies it. Prefer liquid clustering on frequently filtered or merged keys such as `member_business_key` and `transaction_id`; never partition by those high-cardinality identifiers.
- Control output-file size and schedule compaction based on observed file counts and query performance. Apply retention and `VACUUM` policies only after recovery requirements are agreed.
- Publish country tables from the current Silver state. For many countries, replace the three explicit targets with metadata-driven publication while retaining country-level access controls.
- Persist quality metrics by batch and alert or stop publication when agreed thresholds are exceeded. The assessment sample intentionally reports two orphan redemptions because its JSON member ID is absent from the supplied workbooks.
- Limit concurrent tasks to workspace quotas and use serverless autoscaling. The current first stage runs four ingestion tasks concurrently, within the Free Edition limit of five.
