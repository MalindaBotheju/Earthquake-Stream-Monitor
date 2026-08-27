"""
api_lambda
==========
Read-only HTTP endpoints behind API Gateway. This function never writes
to any table -- it's purely how the Streamlit dashboard sees the data.
Splitting endpoints between "Live" (recent, individual records) and
"History" (6-month, pre-aggregated) keeps each response small and keeps
the dashboard from ever having to sort/aggregate thousands of raw points
itself.
"""

import json
import os
import sys
import time
import decimal
import boto3
from boto3.dynamodb.conditions import Key

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "shared"))

EARTHQUAKES_TABLE = os.environ["EARTHQUAKES_TABLE"]
ROLLUPS_TABLE = os.environ["ROLLUPS_TABLE"]
SEQUENCES_TABLE = os.environ["SEQUENCES_TABLE"]
ALERTS_TABLE = os.environ["ALERTS_TABLE"]

dynamodb = boto3.resource("dynamodb")
earthquakes_table = dynamodb.Table(EARTHQUAKES_TABLE)
rollups_table = dynamodb.Table(ROLLUPS_TABLE)
sequences_table = dynamodb.Table(SEQUENCES_TABLE)
alerts_table = dynamodb.Table(ALERTS_TABLE)

DAY_MS = 24 * 60 * 60 * 1000


class DecimalEncoder(json.JSONEncoder):
    """DynamoDB returns Decimal for all numbers; JSON doesn't know that type."""
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        return super().default(o)


def _response(body, status=200):
    return {
        "statusCode": status,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",  # dashboard is a separate origin
        },
        "body": json.dumps(body, cls=DecimalEncoder),
    }


def _recent(days: int = 7, limit: int = 300):
    cutoff = int(time.time() * 1000) - days * DAY_MS
    resp = earthquakes_table.query(
        IndexName="recent-index",
        KeyConditionExpression=Key("feed").eq("global") & Key("time").gte(cutoff),
        ScanIndexForward=False,
        Limit=limit,
    )
    return _response({"quakes": resp.get("Items", [])})


def _clusters():
    resp = sequences_table.scan()
    active = [s for s in resp.get("Items", []) if int(s.get("quake_count", 1)) >= 2]
    active.sort(key=lambda s: int(s["last_time"]), reverse=True)
    return _response({"sequences": active})


def _alerts(limit: int = 50):
    resp = alerts_table.scan()
    items = resp.get("Items", [])
    items.sort(key=lambda a: int(a["timestamp"]), reverse=True)
    return _response({"alerts": items[:limit]})


def _history_trend(days: int = 180):
    """Sum quake counts per day across all regions -- a simple global trend line."""
    resp = rollups_table.scan()
    by_date = {}
    for item in resp.get("Items", []):
        by_date[item["date"]] = by_date.get(item["date"], 0) + int(item["count"])
    trend = [{"date": d, "count": c} for d, c in sorted(by_date.items())]
    return _response({"trend": trend})


def _history_heatmap():
    """Total activity per region over the retained window -- feeds the heatmap."""
    resp = rollups_table.scan()
    by_region = {}
    for item in resp.get("Items", []):
        r = item["region"]
        agg = by_region.setdefault(r, {"region": r, "total_count": 0, "max_mag": 0})
        agg["total_count"] += int(item["count"])
        agg["max_mag"] = max(agg["max_mag"], float(item["max_mag"]))
    return _response({"regions": list(by_region.values())})


def _history_top(limit: int = 10):
    cutoff = int(time.time() * 1000) - 180 * DAY_MS
    resp = earthquakes_table.query(
        IndexName="recent-index",
        KeyConditionExpression=Key("feed").eq("global") & Key("time").gte(cutoff),
        ScanIndexForward=False,
    )
    items = resp.get("Items", [])
    items.sort(key=lambda q: float(q.get("mag") or 0), reverse=True)
    return _response({"top_quakes": items[:limit]})


ROUTES = {
    "/recent": _recent,
    "/clusters": _clusters,
    "/alerts": _alerts,
    "/history/trend": _history_trend,
    "/history/heatmap": _history_heatmap,
    "/history/top-quakes": _history_top,
}


def lambda_handler(event, context):
    path = event.get("path") or event.get("resource") or "/"
    handler = ROUTES.get(path)
    if handler is None:
        return _response({"error": f"unknown route {path}"}, status=404)
    try:
        return handler()
    except Exception as exc:  # noqa: BLE001
        print(f"API error on {path}: {exc}")
        return _response({"error": "internal error"}, status=500)
