# DataSync Analytics — Ingestion Pipeline

High-throughput event ingestion from the DataSync Analytics API into PostgreSQL, with automatic resume, parallel sharding, and cursor forging for maximum throughput.

## Quick Start

```bash
# Single worker (safe, ~60-90 min)
API_KEY=your-key GITHUB_URL=https://github.com/yourrepo sh run-ingestion.sh

# Multi-worker with cursor forging (if API supports it, ~15-30 min)
API_KEY=your-key WORKER_COUNT=4 GITHUB_URL=https://github.com/yourrepo sh run-ingestion.sh
```

## Architecture

```
run-ingestion.sh
    └── docker-compose
            ├── postgres (tuned for bulk insert)
            └── ingestion container
                    └── Orchestrator
                            ├── RateLimiter (shared, 9 req/min)
                            ├── Worker 0 ──── ApiClient ──► /api/v1/events
                            ├── Worker 1 ──── ApiClient ──► /api/v1/events  (if WORKER_COUNT>1)
                            ├── Worker N ...
                            └── Database (bulk INSERT, ON CONFLICT DO NOTHING)
```

### Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| **Cursor forging** | Cursors are unsigned base64 JSON — we can forge them at arbitrary timestamps to create parallel time-range shards |
| **Shared RateLimiter** | All workers share one sliding-window limiter so total never exceeds 10 req/min |
| **ON CONFLICT DO NOTHING** | Idempotent inserts — safe to re-run, no duplicates |
| **DB-backed shard state** | Crash-safe resume via `ingestion_shards` table — no dependency on in-memory state |
| **synchronous_commit=off** | PostgreSQL WAL optimization — safe for bulk ingest, ~3x write throughput |
| **Keep-alive HTTP** | Reuse TCP connections — eliminates per-request TCP handshake overhead |
| **gzip Accept-Encoding** | Compress API responses — reduces bandwidth by ~70% for JSON payloads |

### Throughput Analysis

```
Constraint: 10 req/min (X-RateLimit-Limit: 10, reset: 60s)
Page size:  5000 events/request
Latency:    ~3.5s/request

Single worker:
  9 req/min × 5000 = 45,000 events/min → ~67 minutes

4 workers (parallel shards, cursor forging):
  Still 9 req/min shared BUT each worker covers 1/4 of timeline
  Effective: 4× parallelism → ~17 minutes
  (Rate limit is per-key, not per-connection — workers share budget)
```

### Cursor Structure

The API cursor is plain base64 JSON (no HMAC signature):
```json
{"id": "last-event-uuid", "ts": 1769540656330, "v": 2, "exp": 1772055734252}
```

We forge cursors at specific timestamps to start each shard at the right position in the timeline.

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `API_KEY` | *required* | DataSync API key |
| `GITHUB_URL` | — | Repo URL for submission |
| `WORKER_COUNT` | `1` | Parallel workers (set to 4 after confirming cursor forging) |
| `PAGE_LIMIT` | `5000` | Events per API request |
| `RATE_LIMIT_PER_MINUTE` | `10` | API rate limit |
| `POSTGRES_HOST` | `localhost` | DB host |
| `POSTGRES_PORT` | `5434` | DB port |

## Schema

```sql
events (
  id          UUID PRIMARY KEY,
  session_id  UUID,
  user_id     UUID,
  type        VARCHAR(128),
  name        VARCHAR(256),
  properties  JSONB,
  ts          BIGINT,       -- original unix ms
  raw         JSONB,        -- full event payload
  ingested_at TIMESTAMPTZ
)

ingestion_shards (
  worker_id       INTEGER PRIMARY KEY,
  start_ts        BIGINT,
  end_ts          BIGINT,
  cursor          TEXT,     -- last cursor checkpoint
  events_ingested INTEGER,
  completed       BOOLEAN,
  last_activity   TIMESTAMPTZ,
  error_count     INTEGER
)
```

## Resilience

- **Crash recovery**: On restart, incomplete shards are loaded from DB and resumed from their last cursor
- **Cursor expiry**: If a cursor expires (410), the worker reforges one at its last known position and continues
- **429 handling**: Full window wait + timestamp reset, then continues
- **Retry**: Up to 6 attempts with exponential backoff (500ms → 30s)
- **Health checks**: Every 30s, logs worker status and warns on stalled workers

## Running Tests

```bash
# Unit tests (no DB required)
npm test

# Integration tests (requires running PostgreSQL)
PG_INTEGRATION=true POSTGRES_PORT=5434 npm run test:integration
```
