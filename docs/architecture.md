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
| Bronze | Preserve exactly what arrived and record batch metadata. | Raw profile rows, raw JSON records |
| Silver | Standardize fields and reject records that cannot be trusted. | Parsed dates, normalized tier, derived age/stale flag |
| Gold | Present current, business-ready data. | `members_india`, `members_usa`, `members_australia` |

Bad records never disappear. They are written to `member_rejections` with the original row and a reason.
