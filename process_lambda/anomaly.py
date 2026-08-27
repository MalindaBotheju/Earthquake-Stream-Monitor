"""
anomaly.py
==========
Compares a region's activity today against that same region's own
historical baseline. A rate that's alarming in a quiet region can be
totally normal in the Pacific Ring of Fire, so baselines are always
computed per-region, never globally.

Deliberately simple statistics (mean + standard deviation -> z-score)
rather than anything fancier -- easy to reason about, easy to explain,
and a documented starting point for a more sophisticated model later
(see the roadmap in the README).
"""

import os
from decimal import Decimal
import statistics
import time
import boto3
from boto3.dynamodb.conditions import Key

from utils import ttl_epoch_seconds, day_bucket

ROLLUPS_TABLE = os.environ["ROLLUPS_TABLE"]
ALERTS_TABLE = os.environ["ALERTS_TABLE"]

BASELINE_WINDOW_DAYS = 90
Z_SCORE_THRESHOLD = 2.5
MIN_QUAKES_TO_FLAG = 3   # ignore tiny absolute counts even if z-score is high

dynamodb = boto3.resource("dynamodb")
rollups_table = dynamodb.Table(ROLLUPS_TABLE)
alerts_table = dynamodb.Table(ALERTS_TABLE)


def _recent_daily_counts(region: str, exclude_date: str) -> list:
    """Pull up to BASELINE_WINDOW_DAYS of daily counts for this region,
    excluding today (today is the thing we're testing, not part of the
    baseline it's tested against)."""
    resp = rollups_table.query(
        KeyConditionExpression=Key("region").eq(region),
        ScanIndexForward=False,   # newest first
        Limit=BASELINE_WINDOW_DAYS + 1,
    )
    items = resp.get("Items", [])
    return [int(i["count"]) for i in items if i["date"] != exclude_date]


def check_and_alert(region: str, today_count: int, today_max_mag: float, event_time_ms: int):
    today = day_bucket(event_time_ms)
    history = _recent_daily_counts(region, exclude_date=today)

    # Need a reasonable amount of history before a baseline means anything.
    if len(history) < 7:
        return None

    mean = statistics.mean(history)
    stdev = statistics.pstdev(history) or 1.0  # avoid divide-by-zero on a flat history
    z_score = (today_count - mean) / stdev

    if z_score >= Z_SCORE_THRESHOLD and today_count >= MIN_QUAKES_TO_FLAG:
        alerts_table.put_item(Item={
            "region": region,
            "timestamp": event_time_ms,
            "alert_type": "activity_spike",
            "details": f"{today_count} quakes today vs baseline avg {mean:.1f} (z={z_score:.1f})",
            "today_count": today_count,
            "baseline_mean": Decimal(str(round(mean, 2))),
            "z_score": Decimal(str(round(z_score, 2))),
            "max_magnitude_today": today_max_mag,
            "ttl": ttl_epoch_seconds(event_time_ms),
        })
        return {"region": region, "z_score": round(z_score, 2)}

    return None
