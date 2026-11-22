# Optional workbook compatibility

The production contract begins with one global pipe-delimited member file.
Therefore, workbook ingestion is not part of the deployed workflow.

The Bronze notebook still contains an optional compatibility path for the
original `IND.xlsx`, `USA.xlsx`, and `AUS.xlsx` experiments. It reads a small
workbook or CSV with pandas, retains each row as JSON, and records the supplied
`source_country`. Silver maps the observed aliases to its canonical columns:

| Canonical field | Observed aliases |
| --- | --- |
| `source_member_id` | `Unique ID`, `ID` |
| `member_name` | `Member Name`, `Name` |
| `tier_code` | `Tier Type`, `TierCode` |
| `date_of_birth` | `Date of Birth`, `DOB` |
| `enrollment_date` | `Date of Enrollment`, `EnrollmentDate` |
| `last_flight_date` | `Date of Flight`, `Flight Date`, `FlightDate` |

This path demonstrates schema harmonization when upstream sources do not share
a contract. It is intentionally driver-based and must not be used for
production-scale Excel ingestion.

To run it manually, call `notebooks/01_bronze_ingest.py` with an XLSX or CSV
`profile_file_path`, a `source_country`, and a batch ID, then call
`notebooks/02_silver_members.py` with the same batch ID. It is excluded from the
daily bundle tasks.
