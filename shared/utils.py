"""
Shared helpers used by ingest_lambda, process_lambda, and api_lambda.

Kept dependency-free (standard library only) so it can be zipped straight
into any Lambda without a build step for this file specifically.
"""

import math
import time
from datetime import datetime, timezone

# How long we keep data before DynamoDB's TTL sweeps it away, in seconds.
RETENTION_SECONDS = 6 * 30 * 24 * 60 * 60  # ~6 months

# Grid size (in degrees) used to bucket earthquakes into coarse "regions".
# 5 degrees is roughly 550km at the equator -- coarse enough that a
# mainshock and its aftershocks almost always land in the same bucket,
# fine enough that "region" still means something on a world map.
REGION_GRID_DEGREES = 5


def region_bucket(lat: float, lon: float) -> str:
    """
    Turn a lat/lon into a coarse region key, e.g. "35_140" for Japan.

    We don't have (and don't need) a real geocoder here -- USGS's `place`
    field is free text ("32km SSW of Sola, Vanuatu") which is great for
    display but useless as a stable grouping key. Snapping to a grid cell
    gives us a cheap, deterministic region id that's stable enough for
    rollups and anomaly baselines.
    """
    grid_lat = round(lat / REGION_GRID_DEGREES) * REGION_GRID_DEGREES
    grid_lon = round(lon / REGION_GRID_DEGREES) * REGION_GRID_DEGREES
    return f"{grid_lat}_{grid_lon}"


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance between two points, in kilometers."""
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlambda / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def epoch_ms_now() -> int:
    return int(time.time() * 1000)


def ttl_epoch_seconds(event_time_ms: int, retention_seconds: int = RETENTION_SECONDS) -> int:
    """
    DynamoDB TTL wants a plain epoch-seconds number on an attribute
    (we use `ttl`). Deliberately keyed off the earthquake's own event
    time, not when we happened to ingest it -- an old, late-arriving
    record shouldn't get a fresh 6 months just because we saw it late.
    """
    return int(event_time_ms / 1000) + retention_seconds


def day_bucket(event_time_ms: int) -> str:
    """YYYY-MM-DD string for an event time, used as the rollup table's sort key."""
    return datetime.fromtimestamp(event_time_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
