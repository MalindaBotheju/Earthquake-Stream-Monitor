"""
process_lambda
===============
Triggered by SQS, one batch at a time. For every earthquake message:
  1. Upsert the record into `earthquakes` (by id -- USGS revises quakes,
     so this is a real update, not just a first-write).
  2. Update today's rollup for this quake's region in `region_rollups`.
  3. Run clustering (is this part of an existing aftershock sequence?).
  4. Run anomaly detection (is this region unusually active today?).

One failed record in a batch shouldn't take down the rest -- SQS's own
batch-item-failure reporting handles that: we report exactly which
message IDs failed, and only those get retried / eventually DLQ'd.
"""

import json
import os
import sys
import boto3
import decimal

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "shared"))
from utils import region_bucket, ttl_epoch_seconds, day_bucket  # noqa: E402
from clustering import assign_sequence  # noqa: E402
from anomaly import check_and_alert  # noqa: E402

EARTHQUAKES_TABLE = os.environ["EARTHQUAKES_TABLE"]
ROLLUPS_TABLE = os.environ["ROLLUPS_TABLE"]

dynamodb = boto3.resource("dynamodb")
earthquakes_table = dynamodb.Table(EARTHQUAKES_TABLE)
rollups_table = dynamodb.Table(ROLLUPS_TABLE)


def _upsert_earthquake(q: dict, region: str):
    earthquakes_table.put_item(Item={
        "id": q["id"],
        "feed": "global",              # constant partition key for the recent-index GSI
        "mag": q["mag"],
        "place": q.get("place"),
        "region": region,
        "time": q["time"],
        "updated": q["updated"],
        "depth_km": q.get("depth_km"),
        "lat": q["lat"],
        "lon": q["lon"],
        "tsunami": q.get("tsunami", 0),
        "sig": q.get("sig"),
        "ttl": ttl_epoch_seconds(q["time"]),
    })


def _update_rollup(region: str, event_time_ms: int, mag: float) -> dict:
    """
    Read-modify-write on today's rollup row for this region. At this
    project's write volume (roughly single-digit quakes per region per
    hour) the odds of two Lambda invocations racing on the exact same
    region+day are low enough that a simple read-then-write is fine --
    a stricter design would use DynamoDB's ADD/atomic-counter updates,
    which is a reasonable V2 hardening step, not a V1 requirement.
    """
    date = day_bucket(event_time_ms)
    resp = rollups_table.get_item(Key={"region": region, "date": date})
    existing = resp.get("Item")

    if existing:
        count = int(existing["count"]) + 1
        max_mag = max(existing["max_mag"], mag)
        sum_mag = existing["sum_mag"] + mag
    else:
        count, max_mag, sum_mag = 1, mag, mag

    rollups_table.put_item(Item={
        "region": region,
        "date": date,
        "count": count,
        "max_mag": max_mag,
        "sum_mag": sum_mag,
        "ttl": ttl_epoch_seconds(event_time_ms, retention_seconds=90 * 24 * 60 * 60 * 3),
        # rollups are tiny (a handful of numbers per region per day), so we
        # keep them well beyond the 6-month raw-event window -- see README
    })
    return {"count": count, "max_mag": max_mag}


def lambda_handler(event, context):
    failures = []

    for record in event.get("Records", []):
        message_id = record["messageId"]
        try:
            q = json.loads(record["body"], parse_float=decimal.Decimal)
            region = region_bucket(q["lat"], q["lon"])

            _upsert_earthquake(q, region)
            rollup = _update_rollup(region, q["time"], q["mag"])

            sequence_id = assign_sequence({
                "id": q["id"], "mag": q["mag"], "time": q["time"],
                "lat": q["lat"], "lon": q["lon"], "region": region,
            })
            earthquakes_table.update_item(
                Key={"id": q["id"]},
                UpdateExpression="SET sequence_id = :sid",
                ExpressionAttributeValues={":sid": sequence_id},
            )

            check_and_alert(region, rollup["count"], rollup["max_mag"], q["time"])

        except Exception as exc:  # noqa: BLE001 -- deliberately broad: one bad
            # message must not crash the whole batch. Report it as a
            # partial failure so only this message gets retried/DLQ'd.
            print(f"Failed to process message {message_id}: {exc}")
            failures.append({"itemIdentifier": message_id})

    return {"batchItemFailures": failures}
