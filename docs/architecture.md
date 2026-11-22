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

The member key is `source_member_id` across countries so that a country move replaces the prior country assignment. The source does not currently provide a business update timestamp; ingestion time is therefore only a fallback ordering signal. A source update timestamp or sequence is required to correctly order late-arriving records.

The Silver member parser accepts the sample detail row with 11 fields and the PDF-described variant with a Post Code field (12 fields).

Excel/CSV support is intentionally limited to the small assessment samples and uses driver-side pandas. Production-sized inputs must use distributed text ingestion or a managed ingestion service.
