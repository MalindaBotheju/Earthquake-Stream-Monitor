"""
ingest_lambda
=============
Woken hourly by EventBridge. Job: ask USGS what's new, and only pass on
records that are actually new or have been revised since we last saw them.
Does no analysis itself -- that's process_lambda's job. Keeping this
function dumb and fast is deliberate: if USGS is briefly slow or the
feed hiccups, this function fails cheaply and the next hourly run just
tries again.
"""

import json
import os
import sys
import urllib.request
import boto3

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "shared"))
from utils import epoch_ms_now  # noqa: E402

# USGS's own "all earthquakes, past day" feed. Magnitude isn't filtered
# server-side here -- process_lambda decides what's significant. This
# feed updates every minute on USGS's end; we just happen to only look
# at it once an hour.
USGS_FEED_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"

QUEUE_URL = os.environ["QUEUE_URL"]
EARTHQUAKES_TABLE = os.environ["EARTHQUAKES_TABLE"]

sqs = boto3.client("sqs")
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(EARTHQUAKES_TABLE)


def fetch_usgs_feed() -> dict:
    with urllib.request.urlopen(USGS_FEED_URL, timeout=20) as resp:
        return json.loads(resp.read().decode("utf-8"))


def already_current(quake_id: str, updated_ms: int) -> bool:
    """
    True if we've already stored this exact `updated` timestamp for this
    quake id -- i.e. nothing has changed since last time, skip it.
    USGS revises magnitude/location as more seismic stations report in,
    so a quake can legitimately show up again later with a newer
    `updated` value; that's not a duplicate, it's a real update.
    """
    resp = table.get_item(Key={"id": quake_id}, ProjectionExpression="updated")
    item = resp.get("Item")
    return item is not None and int(item.get("updated", 0)) == updated_ms


def lambda_handler(event, context):
    feed = fetch_usgs_feed()
    features = feed.get("features", [])

    sent, skipped = 0, 0
    for feature in features:
        props = feature.get("properties", {})
        geom = feature.get("geometry", {})
        coords = geom.get("coordinates") or [None, None, None]

        quake_id = feature.get("id")
        updated_ms = props.get("updated")
        if not quake_id or updated_ms is None or coords[0] is None:
            skipped += 1
            continue

        if already_current(quake_id, updated_ms):
            skipped += 1
            continue

        message = {
            "id": quake_id,
            "mag": props.get("mag"),
            "place": props.get("place"),
            "time": props.get("time"),
            "updated": updated_ms,
            "tsunami": props.get("tsunami", 0),
            "sig": props.get("sig"),
            "status": props.get("status"),
            "lon": coords[0],
            "lat": coords[1],
            "depth_km": coords[2],
        }
        sqs.send_message(QueueUrl=QUEUE_URL, MessageBody=json.dumps(message))
        sent += 1

    result = {
        "checked": len(features),
        "sent_to_queue": sent,
        "skipped_unchanged": skipped,
        "run_at": epoch_ms_now(),
    }
    print(json.dumps(result))
    return result
