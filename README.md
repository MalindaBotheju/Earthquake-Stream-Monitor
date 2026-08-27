# Global Earthquake Stream Monitor — Serverless Real-Time Ingestion & Anomaly Detection

A fully serverless, event-driven pipeline that polls the USGS real-time
earthquake feed, buffers and processes events through a queue, maintains
rolling per-region statistics in a NoSQL store, detects aftershock clusters
and anomalous activity spikes, and serves the result through a public
dashboard — built entirely on AWS's free tier, no persistent server, no
ongoing cost.

**Repo:** https://github.com/MalindaBotheju/Earthquake-Stream-Monitor
**API base URL:** `https://osn9ah51hi.execute-api.us-east-1.amazonaws.com/Prod`
**Live dashboard:** _add the Streamlit Community Cloud URL here once deployed_

```
USGS GeoJSON feed → EventBridge (hourly) → Lambda (fetch/dedupe) → SQS
                  → Lambda (process/cluster/alert) → DynamoDB (6-month rolling)
                  → API Gateway → Streamlit dashboard (Live + History tabs)
```

The core engineering idea: USGS only exposes the *current* snapshot of
recent seismic activity. This pipeline turns that into a live, stateful
picture — tracking not just individual quakes but *sequences* (mainshock +
aftershocks) and *anomalies* (regions whose activity has spiked above their
own historical baseline) — using a fully event-driven, decoupled AWS stack
instead of a single scheduled script, and accumulates a genuine 6-month
rolling history rather than just showing the last few days.

**Status:** deployed and running. Every hour, the pipeline pulls the latest
USGS feed, processes new/updated quakes end-to-end, and the dashboard reads
live from it. History depth grows day by day from first deployment — see
[Known limitations](#known-limitations-honest-list) below.

## Architecture

| Stage | Tool | Responsibility |
|---|---|---|
| Trigger | EventBridge Scheduled Rule | Fire the ingest Lambda every hour |
| Extract | `ingest_lambda/` | Pull latest GeoJSON feed from USGS, skip quakes whose `updated` timestamp hasn't changed |
| Buffer | SQS queue + dead-letter queue | Decouple ingest from processing; absorbs bursts; isolates poison messages after 3 failed attempts |
| Transform + Load | `process_lambda/` | Validate, convert to `Decimal`, upsert into DynamoDB |
| Analytics | `process_lambda/` (same invocation) | Update rolling per-region stats, run clustering + anomaly checks |
| Storage | DynamoDB (on-demand billing) | `earthquakes` (individual events, 6-month TTL), `region_rollups` (daily per-region stats, kept longer since it's tiny), `sequences` (active aftershock groupings), `alerts` (anomaly flags) |
| API | API Gateway + `api_lambda/` | Read-only REST endpoints for both live and historical queries |
| IaC | AWS SAM (`infra/template.yaml`) | Defines all 9 resources + IAM wiring in one deploy |
| Presentation | `dashboard/dashboard.py` (Streamlit) | **Live tab** (recent individual quakes, map + feed + alerts) and **History tab** (6-month summaries: heatmap, trend, top quakes, sequences) |

## Project layout

```
earthquake-stream-monitor/
├── infra/
│   └── template.yaml            # AWS SAM template — all resources, IAM, schedule
├── ingest_lambda/
│   ├── handler.py                # Fetch USGS feed, dedupe, push to SQS
│   ├── utils.py                  # Own copy of shared helpers (see note below)
│   └── requirements.txt
├── process_lambda/
│   ├── handler.py                # SQS-triggered: upsert to DynamoDB, update rollups
│   ├── clustering.py             # Spatiotemporal aftershock grouping
│   ├── anomaly.py                # Rolling baseline + spike detection per region
│   ├── utils.py                  # Own copy of shared helpers
│   └── requirements.txt
├── api_lambda/
│   ├── handler.py                # Read-only endpoints for dashboard
│   ├── utils.py                  # Own copy of shared helpers
│   └── requirements.txt
├── dashboard/
│   ├── dashboard.py               # Streamlit presentation layer (read-only)
│   └── requirements.txt
├── shared/
│   └── utils.py                   # Canonical source — copy into each Lambda folder before building
├── notebooks/
│   └── 01_explore_usgs_feed.ipynb # exploration only — not part of the pipeline
├── tests/
│   └── test_clustering.py         # uses moto to mock DynamoDB; not yet run in CI
├── samconfig.toml
├── .gitignore
└── .env.example                   # unused placeholder — see note below
```

## How it works end to end

1. **Every hour**, an EventBridge rule invokes the ingest Lambda.
2. `ingest_lambda` requests the current USGS GeoJSON feed (`all_day.geojson`
   — all magnitudes, past 24h window), skips any quake `id` whose `updated`
   timestamp we've already stored (nothing changed), and pushes
   new/revised records onto the SQS queue as individual messages.
3. SQS triggers `process_lambda` in batches of up to 10. For each message it:
   - Parses the JSON with `parse_float=Decimal` — DynamoDB rejects native
     Python floats outright, so every number is converted to `Decimal` at
     the JSON-parsing boundary, before it touches any table.
   - Upserts the normalized event into the `earthquakes` table, by `id` —
     USGS revises magnitude/location as more stations report in, so this
     is a real update, not a blind insert.
   - Updates the `region_rollups` table — per-region daily counts and max
     magnitude.
   - Runs `clustering.py`: checks whether the new quake falls within the
     time/distance/magnitude window of an existing sequence; if so, tags
     it with that sequence's ID (promoting the mainshock if the new quake
     is meaningfully bigger); otherwise starts a new sequence.
   - Runs `anomaly.py`: once a region has 7+ days of rollup history,
     compares today's count against that region's own baseline (z-score);
     writes an alert row if it crosses the threshold.
   - Reports per-message failures back to SQS (`batchItemFailures`) so one
     bad record doesn't block or fail the rest of the batch.
4. `api_lambda`, behind API Gateway, exposes six read-only routes —
   `/recent`, `/clusters`, `/alerts` for the Live tab, and
   `/history/trend`, `/history/heatmap`, `/history/top-quakes` for the
   History tab — all reading from DynamoDB, never writing.
5. `dashboard/dashboard.py`, deployed separately on Streamlit Community
   Cloud, polls that API and renders two tabs:
   - **Live** — a scatter-geo map of the last 7 days, a magnitude
     histogram, active alerts, and a sortable feed table.
   - **History** — 6-month *summaries*, not raw points: a density heatmap
     by region, a quake-count trend line, a top-10 biggest-quakes table,
     and active multi-quake aftershock sequences.

The ingestion stack and the dashboard are two separate deployments that
only share the API — either can be redeployed or restarted independently.

## Setting this up yourself

### 1. Prerequisites

- Python 3.12+ (the Lambdas target `python3.12` — if your local Python is
  newer, use `sam build --use-container` so the build happens inside a
  matching container instead of failing on a version mismatch)
- Docker (running, for `sam build --use-container`)
- AWS CLI v2 and AWS SAM CLI, both configured with credentials for an IAM
  user (avoid using the AWS root account directly)

### 2. AWS account setup

Create an IAM user with `AdministratorAccess` (fine for a personal
project), generate an access key, and run `aws configure` with it. **Set a
CloudWatch billing alarm** before deploying anything — this project should
cost $0 at this data volume (DynamoDB on-demand, Lambda, SQS, EventBridge
are all effectively free here; API Gateway is free for 12 months on a new
account, pennies/month after that) but a $1 alarm is cheap insurance.

### 3. Deploy the backend

```bash
sam build --use-container
sam deploy --guided
```

`--use-container` matters if your local Python version doesn't match the
Lambda runtime — it builds inside a Docker image with the correct version
instead of using your system Python. The guided deploy will ask about IAM
role creation (say yes) and about each API route having no authentication
(say yes — this API is deliberately public/read-only). It saves your
answers to `samconfig.toml` so future deploys are just `sam deploy`.

Note the `ApiUrl` from the Outputs at the end — you'll need it for the
dashboard.

### 4. Local development

Each Lambda folder needs its **own copy** of `shared/utils.py` — SAM's
`CodeUri` only packages the folder itself, not sibling directories, so a
`sys.path.append("../shared")` import works when running locally but fails
after deployment with `No module named 'utils'`. If you edit
`shared/utils.py`, re-copy it into `ingest_lambda/`, `process_lambda/`, and
`api_lambda/` before rebuilding. (A Lambda Layer would remove this
duplication — see [Roadmap](#roadmap).)

To run the clustering tests locally (uses `moto` to fake DynamoDB, no real
AWS account needed):
```bash
pip install moto pytest boto3
pytest tests/
```

### 5. Dashboard

Local:
```bash
conda create -n dashboard-env python=3.12 -y && conda activate dashboard-env
pip install -r dashboard/requirements.txt
mkdir -p .streamlit
echo 'API_BASE_URL = "https://<your-api-id>.execute-api.<region>.amazonaws.com/Prod"' > .streamlit/secrets.toml
streamlit run dashboard/dashboard.py
```

Deployed (Streamlit Community Cloud):
1. [share.streamlit.io](https://share.streamlit.io) → **Create app** →
   deploy from this GitHub repo, branch `main`, main file
   `dashboard/dashboard.py`.
2. Under **Advanced settings → Secrets**, add the same `API_BASE_URL` line
   as above.
3. Deploy. The dashboard updates as new quakes flow in hourly — no
   redeploy needed for new data, only for code changes.

## Design notes

- **Identity:** USGS's own `id` field (e.g. `us7000abcd`) is the primary
  key throughout — stable across revisions to the same event.
- **Updates, not just inserts:** `process_lambda` upserts by `id`, using
  USGS's `updated` timestamp in `ingest_lambda` to skip anything that
  hasn't actually changed since we last saw it.
- **DynamoDB requires `Decimal`, not `float`:** every number from the
  USGS feed is parsed as `Decimal` at the JSON boundary
  (`json.loads(body, parse_float=Decimal)`), and every downstream
  calculation that gets written back (rollup sums, z-scores, magnitude
  tolerances) stays in `Decimal` rather than being cast back to `float`.
- **`ttl` is a DynamoDB reserved word:** any `UpdateExpression` that
  touches the TTL attribute needs an `ExpressionAttributeNames` alias
  (`#ttl`) — using the raw word directly raises a `ValidationException`.
  `put_item` calls aren't affected, only `update_item`.
- **Region bucketing:** `region_bucket()` snaps lat/lon to a 5°×5° grid
  (e.g. `"40_-120"`) as a cheap, deterministic grouping key for rollups —
  not a real place name. Readable-name mapping is a nice-to-have, not yet
  built.
- **Retention:** DynamoDB TTL is set to ~6 months on the raw `earthquakes`
  table, keyed off event time, not ingestion time. AWS's own TTL sweep
  isn't instant — expired items are typically removed within ~48 hours of
  expiry, which is irrelevant at this data volume. Total 6-month storage
  is tens of MB, nowhere near DynamoDB's free-tier limits, so no S3
  archiving was needed.
- **Live vs. History are genuinely different queries:** Live reads
  individual recent records; History reads pre-aggregated
  `region_rollups`, never the raw table directly — keeps the dashboard
  fast and avoids dumping thousands of overlapping points on one map.
- **Clustering is intentionally simple, not research-grade:** a new quake
  joins an existing sequence if it's within ~150km and 45 days of that
  sequence's anchor point, with a bigger later quake able to "promote"
  itself to mainshock. It does **not** currently distinguish a real
  mainshock/aftershock relationship from routine small-quake background
  chatter (e.g. California's constant low-magnitude seismicity) — see
  [Known limitations](#known-limitations-honest-list).
- **Anomaly detection is per-region, not global:** a background rate
  that's normal for the Pacific Ring of Fire would be a major anomaly
  somewhere seismically quiet. Needs 7+ days of rollup history before it
  will flag anything, by design — no false alerts in week one from an
  empty baseline.
- **Architecture is x86_64, not ARM/Graviton:** the original template
  targeted `arm64` for a small cost saving, but building on an x86_64
  laptop without `--use-container` needing full cross-architecture
  emulation made local builds slow/flaky. Switched to `x86_64` to match
  the dev machine; revisit if deploying from Graviton-based infra.
- **Billing mode is on-demand (`PAY_PER_REQUEST`), not provisioned:** at
  this write volume the cost difference is a fraction of a cent/month
  either way; on-demand avoids needing to size read/write capacity units.
- **Failure handling:** a failed USGS fetch or a single bad SQS message
  doesn't take down the pipeline — SQS retries per its redrive policy
  (max 3 attempts) before routing to the dead-letter queue.

## Known limitations (honest list)

- **No historical backfill.** The pipeline only accumulates forward from
  whenever it was first deployed (Aug 27, 2026) — it has no memory of
  earthquakes before that, including major ones (e.g. the M7.8 Philippines
  quake in June 2026). "History (6 months)" means "however much history
  this pipeline has personally witnessed," growing day by day, capped at
  6 months.
- **Region heatmaps are biased by sensor density, not danger.** USGS
  logs far more small quakes in densely-instrumented regions (California,
  Alaska) than in less-monitored but genuinely more seismically active
  places. The raw-count heatmap reflects monitoring coverage as much as
  actual risk — the "Biggest quakes" table (by magnitude) is a better
  danger signal than the heatmap (by count).
- **Aftershock sequences currently include background noise.** Any 2+
  small quakes within the distance/time window get grouped as a
  "sequence," even if neither is a meaningful mainshock. A minimum
  magnitude threshold for sequence anchors would fix this.
- **`tests/test_clustering.py` was written but not run in this
  environment** (no network access to install `moto`/`pytest` during
  development) — verify it passes locally before relying on it.
- No population/city-proximity dataset joined in — magnitude and depth
  alone drive significance.
- No SMS/email alerting — anomalies are dashboard-only.
- No cross-region correlation between separate fault systems.

## Roadmap (not built, on purpose — ideas for a V2+)

- Backfill script against USGS's historical query API (different endpoint,
  supports date ranges) to populate 6 months of real data on day one
  instead of waiting for it to accumulate.
- A Lambda Layer for `shared/utils.py` instead of copy-pasting it into
  three Lambda folders.
- Minimum-magnitude threshold on sequence anchors, to separate real
  aftershock sequences from background chatter.
- Join a static city/population dataset for a felt-risk score combining
  magnitude, depth, and proximity to population centers.
- SNS-based email/SMS alerting for high-significance events.
- A rollup job that compresses expired raw events into long-term daily
  aggregates before TTL removes them, preserving trend data beyond 6 months.
- Human-readable region names instead of `"40_-120"`-style grid buckets.
- CI pipeline that actually runs `tests/test_clustering.py` on push.

V1's goal was deliberately narrow: a reliable, serverless, free pipeline
with real spatiotemporal analysis behind it, actually deployed and
producing live data — not a maximal feature list.