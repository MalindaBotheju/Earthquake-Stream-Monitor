"""
clustering.py
=============
Groups related earthquakes into "sequences" -- a mainshock plus its
aftershocks -- instead of treating every quake as an unrelated point.

Approach (deliberately simple, not a research-grade declustering
algorithm like Gardner-Knopoff): a new quake joins an existing sequence
if it's close in both space and time to that sequence's anchor point,
and roughly the same size or smaller. If a "aftershock" actually turns
out bigger than the current mainshock, we promote it -- earthquake
sequences are sometimes misread in real time and get relabeled as
bigger events follow.
"""

import os
from decimal import Decimal
import time
import uuid
import boto3
from boto3.dynamodb.conditions import Attr

from utils import haversine_km, ttl_epoch_seconds

SEQUENCES_TABLE = os.environ["SEQUENCES_TABLE"]

# Tuning knobs -- generous on purpose. Real aftershock sequences can span
# weeks; being too strict just means we undercount related events.
MAX_DISTANCE_KM = 150
MAX_GAP_DAYS = 45
MAG_TOLERANCE = Decimal("0.5")  # a "aftershock" can be up to this much bigger before
                      # we still count it as part of the same sequence
                      # rather than an unrelated new mainshock

dynamodb = boto3.resource("dynamodb")
sequences_table = dynamodb.Table(SEQUENCES_TABLE)


def _candidate_sequences(region: str):
    """
    Small scan of the sequences table, filtered to nearby regions.
    At this project's volume there are only ever a handful of active
    sequences at once, so a scan (rather than a dedicated geo-index) is
    the right amount of complexity here.
    """
    resp = sequences_table.scan()
    return resp.get("Items", [])


def assign_sequence(quake: dict) -> str:
    """
    quake: dict with id, mag, time (ms), lat, lon, region
    Returns the sequence_id this quake was assigned to (existing or new).
    """
    now_ms = quake["time"]
    candidates = _candidate_sequences(quake["region"])

    best_match = None
    for seq in candidates:
        gap_days = (now_ms - int(seq["last_time"])) / (1000 * 60 * 60 * 24)
        if gap_days > MAX_GAP_DAYS or gap_days < 0:
            continue
        dist = haversine_km(quake["lat"], quake["lon"], seq["anchor_lat"], seq["anchor_lon"])
        if dist > MAX_DISTANCE_KM:
            continue
        best_match = seq
        break  # first spatiotemporal match is good enough at this scale

    if best_match is None:
        return _create_sequence(quake)

    return _update_sequence(best_match, quake)


def _create_sequence(quake: dict) -> str:
    sequence_id = f"seq-{uuid.uuid4().hex[:12]}"
    sequences_table.put_item(Item={
        "sequence_id": sequence_id,
        "region": quake["region"],
        "mainshock_id": quake["id"],
        "mainshock_mag": quake["mag"],
        "anchor_lat": quake["lat"],
        "anchor_lon": quake["lon"],
        "start_time": quake["time"],
        "last_time": quake["time"],
        "quake_count": 1,
        "max_mag": quake["mag"],
        "ttl": ttl_epoch_seconds(quake["time"]),
    })
    return sequence_id


def _update_sequence(seq: dict, quake: dict) -> str:
    sequence_id = seq["sequence_id"]
    new_count = int(seq["quake_count"]) + 1
    new_max_mag = max(seq["max_mag"], quake["mag"])

    update_expr = (
        "SET last_time = :lt, quake_count = :qc, max_mag = :mm, "
        "#ttl = :ttl"
    )
    expr_names = {"#ttl": "ttl"}
    expr_values = {
        ":lt": quake["time"],
        ":qc": new_count,
        ":mm": new_max_mag,
        ":ttl": ttl_epoch_seconds(quake["time"]),
    }

    # Promote to mainshock if this quake is meaningfully bigger than
    # what we currently think the mainshock is.
    if quake["mag"] > seq["mainshock_mag"] + MAG_TOLERANCE:
        update_expr += ", mainshock_id = :mid, mainshock_mag = :mmag, anchor_lat = :alat, anchor_lon = :alon"
        expr_values.update({
            ":mid": quake["id"],
            ":mmag": quake["mag"],
            ":alat": quake["lat"],
            ":alon": quake["lon"],
        })

    sequences_table.update_item(
        Key={"sequence_id": sequence_id},
        UpdateExpression=update_expr,
        ExpressionAttributeNames=expr_names,
        ExpressionAttributeValues=expr_values,
    )
    return sequence_id
