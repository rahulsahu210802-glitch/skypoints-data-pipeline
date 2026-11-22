from datetime import date, timedelta
import json
from pathlib import Path
from typing import TypedDict


ROOT = Path(__file__).resolve().parents[1]
SAMPLES = ROOT / "samples"
MEMBER_FILE = SAMPLES / "members_20240115.psv"
REDEMPTION_FILE = SAMPLES / "redemptions_20240115.json"
NEGATIVE_MEMBER_FILE = SAMPLES / "members_20240116.psv"
NEGATIVE_REDEMPTION_FILE = SAMPLES / "redemptions_20240116.json"

COUNTRIES = (
    ("USA", "CA", "90210"),
    ("IND", "TN", "60001"),
    ("PHIL", "NCR", "1000"),
    ("CAN", "ONT", "K1A0B"),
    ("AU", "VIC", "3000"),
)
TIERS = ("GLD", "SLV", "PLT")


class Redemption(TypedDict):
    txn_id: str
    txn_date: str
    partner: str
    miles_redeemed: int
    status: str


class RedemptionFeed(TypedDict):
    member_id: str
    feed_date: str
    redemptions: list[Redemption]


def _detail(
    name: str,
    member_id: str,
    enrollment_date: str,
    last_flight_date: str,
    tier_code: str,
    agent_name: str,
    state: str,
    country: str,
    post_code: str,
    dob: str,
    active_flag: str,
) -> str:
    return "|".join(
        (
            "",
            "D",
            name,
            member_id,
            enrollment_date,
            last_flight_date,
            tier_code,
            agent_name,
            state,
            country,
            post_code,
            dob,
            active_flag,
        )
    )


def build_members(*, include_invalid: bool = False) -> list[str]:
    rows = [
        "|H|Member_Name|Member_Id|Enrollment_Date|Last_Flight_Date|"
        "Tier_Code|Agent_Name|State|Country|Post_Code|DOB|Is_Active",
        _detail(
            "Elena", "223457", "20101012", "20121013", "GLD", "Sam",
            "CA", "USA", "90210", "03051985", "A",
        ),
        _detail(
            "Ravi", "223458", "20101012", "20121013", "SLV", "Sam",
            "TN", "IND", "60001", "03051985", "A",
        ),
        _detail(
            "Mateo", "223459", "20101012", "20121013", "GLD", "Sam",
            "NCR", "PHIL", "1000", "03051985", "A",
        ),
        _detail(
            "Nora", "22345", "20101012", "20121013", "PLT", "Sam",
            "ONT", "CAN", "K1A0B", "03051985", "A",
        ),
        _detail(
            "Jacob", "2256", "20101012", "20121013", "SLV", "Sam",
            "VIC", "AU", "3000", "03051985", "A",
        ),
    ]

    member_number = 1
    base_dob = date(1970, 1, 15)
    base_enrollment = date(2015, 1, 1)
    base_flight = date(2023, 1, 1)
    for country, state, post_code in COUNTRIES:
        for country_member_number in range(1, 30):
            dob = base_dob + timedelta(days=member_number * 97)
            enrollment = base_enrollment + timedelta(days=member_number * 11)
            flight = base_flight + timedelta(days=member_number)
            rows.append(
                _detail(
                    name=f"{country}_Member_{country_member_number:02d}",
                    member_id=f"M{member_number:06d}",
                    enrollment_date=enrollment.strftime("%Y%m%d"),
                    last_flight_date=flight.strftime("%Y%m%d"),
                    tier_code=TIERS[member_number % len(TIERS)],
                    agent_name=f"Agent_{(member_number % 7) + 1}",
                    state=state,
                    country=country,
                    post_code=post_code,
                    dob=dob.strftime("%Y%m%d"),
                    active_flag="A" if member_number % 10 else "I",
                )
            )
            member_number += 1

    if include_invalid:
        rows.extend([
            _detail(
                "", "BAD0001", "20200101", "20230101", "GLD", "Agent_1",
                "CA", "USA", "90210", "19900101", "A",
            ),
            _detail(
                "Missing_ID", "", "20200101", "20230101", "SLV", "Agent_2",
                "TN", "IND", "60001", "19900101", "A",
            ),
            _detail(
                "Invalid_Enrollment", "BAD0003", "20241340", "20230101", "PLT",
                "Agent_3", "NCR", "PHIL", "1000", "19900101", "A",
            ),
            _detail(
                "Invalid_Tier", "BAD0004", "20200101", "20230101", "BRZ",
                "Agent_4", "ONT", "CAN", "K1A0B", "19900101", "A",
            ),
            _detail(
                "Unsupported_Country", "BAD0005", "20200101", "20230101", "GLD",
                "Agent_5", "NA", "XYZ", "00000", "19900101", "A",
            ),
        ])

    expected_rows = 156 if include_invalid else 151
    if len(rows) != expected_rows:
        raise AssertionError(f"Expected {expected_rows} rows, got {len(rows)} rows.")
    return rows


def build_redemptions(
    feed_date: str,
    *,
    include_invalid: bool = False,
) -> list[RedemptionFeed]:
    feeds: list[RedemptionFeed] = [
                {
                    "member_id": "223457",
                    "feed_date": feed_date,
                    "redemptions": [
                        {
                            "txn_id": "RX10091",
                            "txn_date": "20240110",
                            "partner": "AeroLink",
                            "miles_redeemed": 12000,
                            "status": "COMPLETED",
                        },
                        {
                            "txn_id": "RX10092",
                            "txn_date": "20240113",
                            "partner": "SkyPoints",
                            "miles_redeemed": 5000,
                            "status": "PENDING",
                        },
                    ],
                },
                {
                    "member_id": "223458",
                    "feed_date": feed_date,
                    "redemptions": [
                        {
                            "txn_id": "RX10093",
                            "txn_date": "20240108",
                            "partner": "AeroLink",
                            "miles_redeemed": 8000,
                            "status": "COMPLETED",
                        },
                        {
                            "txn_id": "RX10094",
                            "txn_date": "20240111",
                            "partner": "CloudMiles",
                            "miles_redeemed": 3500,
                            "status": "COMPLETED",
                        },
                        {
                            "txn_id": "RX10095",
                            "txn_date": "20240114",
                            "partner": "SkyPoints",
                            "miles_redeemed": 2000,
                            "status": "PENDING",
                        },
                    ],
                },
                {
                    "member_id": "223459",
                    "feed_date": feed_date,
                    "redemptions": [
                        {
                            "txn_id": "RX10096",
                            "txn_date": "20240109",
                            "partner": "PacificAir",
                            "miles_redeemed": 15000,
                            "status": "COMPLETED",
                        },
                        {
                            "txn_id": "RX10097",
                            "txn_date": "20240112",
                            "partner": "SkyPoints",
                            "miles_redeemed": 4500,
                            "status": "CANCELLED",
                        },
                    ],
                },
                {
                    "member_id": "22345",
                    "feed_date": feed_date,
                    "redemptions": [
                        {
                            "txn_id": "RX10098",
                            "txn_date": "20240107",
                            "partner": "NorthStar",
                            "miles_redeemed": 7000,
                            "status": "COMPLETED",
                        },
                        {
                            "txn_id": "RX10099",
                            "txn_date": "20240115",
                            "partner": "SkyPoints",
                            "miles_redeemed": 1000,
                            "status": "PENDING",
                        },
                    ],
                },
    ]

    if include_invalid:
        feeds[0]["redemptions"].append({
            "txn_id": "RXBAD01",
            "txn_date": "20240112",
            "partner": "AeroLink",
            "miles_redeemed": -100,
            "status": "COMPLETED",
        })
        feeds[1]["redemptions"].append({
            "txn_id": "RXBAD02",
            "txn_date": "20240117",
            "partner": "CloudMiles",
            "miles_redeemed": 2500,
            "status": "COMPLETED",
        })
        feeds[2]["redemptions"].append({
            "txn_id": "",
            "txn_date": "20240112",
            "partner": "PacificAir",
            "miles_redeemed": 3000,
            "status": "COMPLETED",
        })

    return feeds


def _write_json(path: Path, feeds: list[RedemptionFeed]) -> None:
    path.write_text(json.dumps(feeds, indent=2) + "\n", encoding="utf-8")


def main() -> None:
    SAMPLES.mkdir(exist_ok=True)
    MEMBER_FILE.write_text(
        "\n".join(build_members()) + "\n",
        encoding="utf-8",
    )
    NEGATIVE_MEMBER_FILE.write_text(
        "\n".join(build_members(include_invalid=True)) + "\n",
        encoding="utf-8",
    )
    _write_json(REDEMPTION_FILE, build_redemptions("20240115"))
    _write_json(
        NEGATIVE_REDEMPTION_FILE,
        build_redemptions("20240116", include_invalid=True),
    )
    for path in (
        MEMBER_FILE,
        REDEMPTION_FILE,
        NEGATIVE_MEMBER_FILE,
        NEGATIVE_REDEMPTION_FILE,
    ):
        print(f"Wrote {path}")


if __name__ == "__main__":
    main()
