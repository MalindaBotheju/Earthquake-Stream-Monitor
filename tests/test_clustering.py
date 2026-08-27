"""
Basic tests for the aftershock clustering logic, using moto to fake
DynamoDB so this runs with no real AWS account needed.

Run with:  pip install moto pytest && pytest tests/
"""

import os
import sys
import boto3
import pytest
from moto import mock_aws

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "process_lambda"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "shared"))

os.environ.setdefault("SEQUENCES_TABLE", "sequences-test")

DAY_MS = 24 * 60 * 60 * 1000


@pytest.fixture
def sequences_table():
    with mock_aws():
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        table = dynamodb.create_table(
            TableName="sequences-test",
            KeySchema=[{"AttributeName": "sequence_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "sequence_id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        table.wait_until_exists()
        yield table


def _quake(id_, mag, time_ms, lat, lon):
    return {"id": id_, "mag": mag, "time": time_ms, "lat": lat, "lon": lon, "region": "35_140"}


def test_first_quake_creates_new_sequence(sequences_table):
    from clustering import assign_sequence
    seq_id = assign_sequence(_quake("q1", 5.0, 1_000_000, 35.0, 140.0))
    assert seq_id.startswith("seq-")
    item = sequences_table.get_item(Key={"sequence_id": seq_id})["Item"]
    assert item["quake_count"] == 1
    assert item["mainshock_id"] == "q1"


def test_nearby_soon_after_quake_joins_same_sequence(sequences_table):
    from clustering import assign_sequence
    seq1 = assign_sequence(_quake("q1", 6.0, 1_000_000, 35.0, 140.0))
    seq2 = assign_sequence(_quake("q2", 4.5, 1_000_000 + 3600_000, 35.2, 140.1))  # 1hr later, ~20km away
    assert seq1 == seq2
    item = sequences_table.get_item(Key={"sequence_id": seq1})["Item"]
    assert item["quake_count"] == 2
    assert item["mainshock_id"] == "q1"  # q1 was bigger, stays mainshock


def test_bigger_aftershock_gets_promoted_to_mainshock(sequences_table):
    from clustering import assign_sequence
    seq1 = assign_sequence(_quake("q1", 4.0, 1_000_000, 35.0, 140.0))
    seq2 = assign_sequence(_quake("q2", 6.5, 1_000_000 + 3600_000, 35.1, 140.0))
    assert seq1 == seq2
    item = sequences_table.get_item(Key={"sequence_id": seq1})["Item"]
    assert item["mainshock_id"] == "q2"
    assert float(item["mainshock_mag"]) == 6.5


def test_far_away_quake_starts_new_sequence(sequences_table):
    from clustering import assign_sequence
    seq1 = assign_sequence(_quake("q1", 5.0, 1_000_000, 35.0, 140.0))       # Japan
    seq2 = assign_sequence(_quake("q2", 5.0, 1_000_000, -33.0, -70.0))       # Chile, far away
    assert seq1 != seq2


def test_quake_long_after_starts_new_sequence(sequences_table):
    from clustering import assign_sequence
    seq1 = assign_sequence(_quake("q1", 5.0, 1_000_000, 35.0, 140.0))
    far_future = 1_000_000 + (60 * DAY_MS)  # well beyond MAX_GAP_DAYS
    seq2 = assign_sequence(_quake("q2", 4.5, far_future, 35.0, 140.0))
    assert seq1 != seq2
