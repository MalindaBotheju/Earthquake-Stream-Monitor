# Global Earthquake Stream Monitor — Serverless Real-Time Ingestion & Anomaly Detection

A fully serverless, event-driven pipeline that polls the USGS real-time
earthquake feed, buffers and processes events through a queue, maintains
rolling per-region statistics in a NoSQL store, detects aftershock clusters
and anomalous activity spikes, and serves the result through a public
dashboard — built entirely on AWS's always-free tier, no persistent server,
no ongoing cost.

**Live dashboard:** _TBD — add Streamlit Cloud URL after deploy_

```
USGS GeoJSON feed → EventBridge (hourly) → Lambda (fetch/dedupe) → SQS
                  → Lambda (process/cluster) → DynamoDB (6-month rolling)
                  → API Gateway → Streamlit dashboard (Live + History views)
```

The core engineering idea: USGS only exposes the *current* snapshot of
recent seismic activity. This pipeline turns that into a live, stateful
picture — tracking not just individual quakes but *sequences* (mainshock +
aftershocks) and *anomalies* (regions whose activity has spiked above their
own historical baseline) — using a fully event-driven, decoupled AWS stack
instead of a single scheduled script, and keeps a genuine 6-month rolling
history rather than just the last few days.

## Architecture

| Stage | Tool | Responsibility |
|---|---|---|
| Trigger | EventBridge Scheduled Rule | Fire the ingest Lambda every hour |
| Extract | `ingest_lambda/` | Pull latest GeoJSON feed from USGS, dedupe/upsert by quake `id` |
| Buffer | SQS queue | Decouple ingest from processing; absorbs bursts |
| Transform + Load | `process_lambda/` | Validate, normalize, write raw event to DynamoDB |
| Analytics | `process_lambda/` (same invocation) | Update rolling per-region stats, run cluster/anomaly checks |
| Storage | DynamoDB | `earthquakes` (individual events, 6-month TTL), `region_rollups` (daily per-region stats, kept longer since it's tiny), `sequences` (active aftershock groupings), `alerts` (anomaly flags) — no S3 archiving needed at this volume |
| API | API Gateway + `api_lambda/` | Read-only REST endpoints for both live and historical queries |
| Presentation | `dashboard.py` (Streamlit) | **Live view** (recent individual quakes, map + feed) and **History view** (6-month summaries: heatmap, trend chart, top quakes, sequences) |

## Project layout

```
earthquake-stream-monitor/
├── infra/
│   └── template.yaml            # AWS SAM template — all resources, IAM, schedules
├── ingest_lambda/
│   ├── handler.py                # Fetch USGS feed, dedupe, push to SQS
│   └── requirements.txt
├── process_lambda/
│   ├── handler.py                # SQS-triggered: write to DynamoDB, update rollups
│   ├── clustering.py             # Spatiotemporal aftershock grouping
│   ├── anomaly.py                # Rolling baseline + spike detection per region
│   └── requirements.txt
├── api_lambda/
│   ├── handler.py                # Read-only endpoints for dashboard
│   └── requirements.txt
├── dashboard/
│   ├── dashboard.py               # Streamlit presentation layer (read-only)
│   └── requirements.txt
├── shared/
│   └── utils.py                   # Region bucketing, distance calc, TTL/date helpers
├── notebooks/
│   └── 01_explore_usgs_feed.ipynb # exploration only — not part of the pipeline
├── tests/
│   └── test_clustering.py
├── samconfig.toml
├── .gitignore
└── .env.example
```

## How it works end to end

1. **Every hour**, an EventBridge rule invokes the ingest Lambda.
2. `ingest_lambda` requests the current USGS GeoJSON feed (magnitude ≥ 1.0,
   past-day window), filters out quake `id`s already seen with no changes
   (checked against DynamoDB), and pushes new/updated records onto the SQS
   queue as individual messages.
3. SQS triggers `process_lambda` in batches. For each message it:
   - Upserts the normalized event (magnitude, depth, coordinates, place,
     timestamp, tsunami flag) into the `earthquakes` table, by `id` — USGS
     revises magnitude/location as more stations report in, so this is an
     update-if-exists, not a blind insert.
   - Updates the `region_rollups` table — per-region counts and max
     magnitude, kept at daily granularity so 6 months of it stays small.
   - Runs `clustering.py`: checks whether the new quake falls within the
     time/distance/magnitude window of an existing sequence; if so, tags
     it with that sequence's ID, otherwise starts a new one.
   - Runs `anomaly.py`: compares the region's current rate against its own
     6-month baseline; if it crosses a threshold, writes an alert row.
4. `api_lambda`, behind API Gateway, exposes read-only endpoints split by
   purpose — `/recent`, `/clusters`, `/alerts` for the Live view, and
   `/history/trend`, `/history/heatmap`, `/history/top-quakes` for the
   History view — all reading from DynamoDB, never writing.
5. `dashboard.py`, deployed separately on Streamlit Community Cloud, has
   two tabs:
   - **Live** — a map and feed of individual quakes from the last few
     days, plus any active alerts.
   - **History** — 6-month *summaries*, not raw points: a regional
     activity heatmap, a weekly/monthly count trend line, a top-10
     biggest-quakes list, and grouped aftershock sequences. Raw 6-month
     data is never dumped onto the map directly — it's pre-aggregated by
     the API so the page stays fast and readable.

The ingestion stack and the dashboard are two separate deployments that
only share the API — either can be redeployed or restarted independently.

## Setting this up yourself

### 1. AWS account

Use a fresh or existing AWS account. Set a **CloudWatch billing alarm**
(free) before deploying anything, as a safety net — every service used here
is free-tier-forever at this data volume, but it's good practice regardless.

### 2. Deploy the backend (AWS SAM)

```bash
sam build
sam deploy --guided
```

This provisions: EventBridge rule, both Lambdas, the SQS queue, both
DynamoDB tables (with TTL enabled), API Gateway, and the IAM roles/policies
connecting them. `sam deploy --guided` will prompt for and save your stack
config to `samconfig.toml`.

### 3. Local development (optional, for testing changes)

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r process_lambda/requirements.txt
sam local invoke ProcessLambda -e tests/events/sample_sqs_event.json
```

### 4. Dashboard (Streamlit Community Cloud)

1. Go to [share.streamlit.io](https://share.streamlit.io) → **Create app**
   → **Deploy a public app from GitHub**.
2. Point it at this repo, branch `main`, main file `dashboard/dashboard.py`.
3. Under **Advanced settings → Secrets**, add:
   ```toml
   API_BASE_URL = "https://<your-api-id>.execute-api.<region>.amazonaws.com/prod"
   ```
4. Deploy. The dashboard updates as new quakes flow in — no redeploy
   needed for new data, only for code changes.

## Design notes

- **Identity:** USGS's own `id` field (e.g. `us7000abcd`) is used as the
  primary key throughout — it's stable across updates to the same event
  (USGS revises magnitude/location as more seismic stations report in).
- **Updates, not just inserts:** unlike a pure append-only log, a quake
  record can legitimately change (magnitude gets revised) — `process_lambda`
  upserts by `id` rather than always inserting, using USGS's `updated`
  timestamp to avoid overwriting newer data with stale data.
- **Retention:** DynamoDB TTL is set to **6 months** on both tables, keyed
  off event time, not ingestion time. At this data volume (roughly
  7,000–10,000+ meaningful quakes over 6 months, each record ~1 KB) total
  storage is on the order of tens of MB — nowhere near DynamoDB's 25 GB
  free-forever limit, so no S3 archiving is needed for V1. S3 stays a
  reasonable idea for a V2 if retention ever grows to a year-plus.
- **Live vs. History are genuinely different queries, not the same data
  reused:** Live reads individual recent records; History reads
  pre-aggregated `region_rollups`, never the raw 6-month table directly —
  this keeps the dashboard fast and avoids dumping thousands of overlapping
  points on one map.
- **Clustering, not just plotting:** the interesting output isn't "here are
  200 dots" — it's "here are 3 active sequences, and here's the mainshock
  for each," computed via spatiotemporal grouping in `clustering.py`.
- **Anomaly detection is per-region, not global:** a background rate that's
  normal for a highly active region (e.g. the Pacific Ring of Fire) would
  be a major anomaly somewhere seismically quiet — baselines are computed
  per region bucket, not against a single global average.
- **Decoupled by design:** ingest, processing, and the read API are
  separate Lambdas connected only by SQS and DynamoDB — any one can fail,
  redeploy, or be replaced without touching the others.
- **Failure handling:** a failed USGS fetch or a processing error for a
  single SQS message doesn't take down the pipeline — SQS retries the
  message per its redrive policy, and a persistent failure lands in a
  dead-letter queue for inspection rather than blocking the queue.

## What was deliberately left out of V1

- No population/city-proximity dataset joined in yet — magnitude and depth
  alone drive the current "significance" signal.
- No SMS/email alerting — anomalies are surfaced on the dashboard only.
- No historical backfill beyond what USGS's feed windows provide — this
  pipeline only accumulates forward from whenever it's first deployed.
- No cross-region correlation (e.g. detecting that activity in two
  different fault systems might be related) — out of scope for V1.

## Roadmap (not built, on purpose — ideas for a V2+)

- Join a static city/population dataset to compute a felt-risk score
  combining magnitude, depth, and proximity to population centers.
- Add SNS-based alerting (email/SMS) for high-significance events, still
  within the free tier at low volume.
- Add a rollup job that compresses expired raw events into daily/regional
  aggregates before TTL removes them, preserving long-term trend data.
- Explore a lightweight ML baseline (e.g. isolation forest) for anomaly
  detection instead of the simple rolling z-score threshold.

V1's goal is deliberately narrow: a reliable, serverless, permanently-free
pipeline with real spatiotemporal analysis behind it — not a maximal
feature list.
