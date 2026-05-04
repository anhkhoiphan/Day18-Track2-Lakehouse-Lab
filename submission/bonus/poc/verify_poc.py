"""Quick verification that PoC logic works end-to-end."""
import hashlib
import hmac
import json
import shutil
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pandas as pd
from deltalake import DeltaTable, write_deltalake

APP_SECRET = b"vinuni-ai20k-secret-v2"
SILVER_PATH = "/tmp/lakehouse_poc_verify/silver/rides"

shutil.rmtree("/tmp/lakehouse_poc_verify", ignore_errors=True)
Path(SILVER_PATH).mkdir(parents=True, exist_ok=True)


def tokenize_pii(value: str, secret: bytes = APP_SECRET) -> str:
    return "v2:" + hmac.new(secret, value.encode(), hashlib.sha256).hexdigest()[:32]


# Test 1: tokenization is deterministic
t1 = tokenize_pii("0912345678")
t2 = tokenize_pii("0912345678")
assert t1 == t2, "tokenize not deterministic"
assert tokenize_pii("0912345678") != tokenize_pii("0987654321"), "different inputs must differ"
print(f"PASS 1: PII tokenization deterministic and unique — token={t1[:22]}...")

# Test 2: write Silver
BASE = datetime(2026, 5, 4, 8, 0, 0, tzinfo=timezone.utc)
rows = [
    {
        "ride_id": f"RIDE-{1000 + i:04d}",
        "status": "COMPLETED",
        "event_ts": (BASE + timedelta(minutes=i * 3)).isoformat(),
        "driver_token": tokenize_pii(f"09{i:08d}"),
        "passenger_token": tokenize_pii(f"08{i:08d}"),
        "city_code": "HCM",
        "fare_vnd": 50000 + i * 5000,
        "ride_date": "2026-05-04",
        "bronze_event_id": str(uuid.uuid4()),
        "silver_run_id": "run_init",
    }
    for i in range(30)
]
df = pd.DataFrame(rows)
write_deltalake(SILVER_PATH, df, mode="overwrite")
v0 = DeltaTable(SILVER_PATH).version()
print(f"PASS 2: Silver initial write — {len(df)} rows, version={v0}")

original_fare = int(df[df["ride_id"] == "RIDE-1000"]["fare_vnd"].iloc[0])

# Test 3: late-event MERGE guard
LATE = datetime(2026, 5, 3, 23, 0, 0, tzinfo=timezone.utc)
late_rows = [
    {
        "ride_id": f"RIDE-{1000 + i:04d}",
        "status": "COMPLETED",
        "event_ts": (LATE + timedelta(minutes=i)).isoformat(),  # OLDER timestamp
        "driver_token": "LATE_TOKEN",
        "passenger_token": "LATE_TOKEN",
        "city_code": "HGI",
        "fare_vnd": 999999999,  # Wrong fare — must be rejected
        "ride_date": "2026-05-03",
        "bronze_event_id": str(uuid.uuid4()),
        "silver_run_id": "run_late",
    }
    for i in range(5)
]
df_late = pd.DataFrame(late_rows)

dt = DeltaTable(SILVER_PATH)
(
    dt.merge(
        source=df_late,
        predicate="target.ride_id = source.ride_id",
        source_alias="source",
        target_alias="target",
    )
    .when_matched_update(
        predicate="source.event_ts > target.event_ts",
        updates={
            "fare_vnd": "source.fare_vnd",
            "event_ts": "source.event_ts",
            "silver_run_id": "source.silver_run_id",
        },
    )
    .when_not_matched_insert_all()
    .execute()
)

df_after = DeltaTable(SILVER_PATH).to_pandas()
after_fare = int(df_after[df_after["ride_id"] == "RIDE-1000"]["fare_vnd"].iloc[0])
assert after_fare == original_fare, f"MERGE GUARD FAILED: {after_fare} != {original_fare}"
print(f"PASS 3: Late-event MERGE guard — stale event rejected, fare={original_fare:,} VND (unchanged)")

# Test 4: time travel
v_now = DeltaTable(SILVER_PATH).version()
v0_fare = int(DeltaTable(SILVER_PATH, version=0).to_pandas()[
    lambda d: d["ride_id"] == "RIDE-1000"
]["fare_vnd"].iloc[0])
assert v0_fare == original_fare, f"Time travel wrong: {v0_fare}"
print(f"PASS 4: Time travel — v0 fare={v0_fare:,} VND correct (current version={v_now})")

# Test 5: DuckDB Gold query
con = duckdb.connect()
con.register("silver", df_after)
result = con.execute(
    "SELECT city_code, COUNT(*) AS rides, SUM(fare_vnd) AS revenue "
    "FROM silver WHERE status='COMPLETED' GROUP BY city_code ORDER BY rides DESC"
).df()
assert len(result) > 0, "DuckDB query returned no rows"
print(f"PASS 5: DuckDB Gold query — {len(result)} city groups, top city={result['city_code'].iloc[0]}")

print()
print("ALL 5 TESTS PASSED — PoC ready for submission")
