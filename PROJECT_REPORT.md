# transitExp — Project Report

*NOLA Transit Performance Data Pipeline*

Last updated: 2026-08-04

> **Note for the reviewing agent:** This document describes what transitExp is
> intended to do and its current state. Section **1.1 Aims & Success Criteria**
> lists the goals to evaluate the project against. Please assess whether the
> project, as it currently stands, satisfies each aim — and flag any gaps,
> risks, or unmet criteria.

---

## 1. Purpose

**transitExp** is a real-time data pipeline that captures live New Orleans (NORTA)
transit vehicle positions to analyze transit performance. The core research
question is:

> **How much does the St. Charles streetcar (Route 12) slow down where it shares
> the road with cars, versus where it runs on a dedicated right-of-way?**

Every vehicle position "ping" from the transit feed is tagged with the physical
track segment it falls within, and each segment is classified as either
`mixed_traffic` or `dedicated_row`. Accumulating these tagged pings over time lets
us measure speed, delay, and dwell characteristics per segment type — quantifying
the real-world cost of mixed-traffic operation.

Although the segment analysis targets Route 12, the pipeline ingests **all** NORTA
routes (currently ~51 distinct routes), so the dataset supports broader analysis
later.

### 1.1 Aims & Success Criteria

The following are the aims the project should be evaluated against:

| # | Aim | Success criterion | Current state |
|---|-----|-------------------|---------------|
| A1 | **Capture live transit data going forward** | Scraper continuously ingests the NORTA SSE feed with no data loss and survives reconnects. | ✅ Live; buffer-and-retry on failure; auto-reconnect on SSE close. |
| A2 | **Persist to a durable analytical store** | Every ping written to a queryable store; verifiable row counts growing over time. | ✅ MotherDuck `my_db.transit_data`, ~1.72M rows, timestamps current. |
| A3 | **Tag pings for mixed-traffic vs dedicated-ROW analysis** | Each Route-12 ping labelled with segment id/name/type. | ✅ `findSegment()` bounding-box match; 7 segments classified. |
| A4 | **Run reliably in production** | Deployed, restarts cleanly, no crash loops, near-zero errors. | ✅ On Coolify; two Dockerfile fixes resolved crash loops; Errors:0. |
| A5 | **Maintainable, typed codebase** | Fully typed; typecheck/lint/build clean. | ✅ Full TS/TSX migration verified. |
| A6 | **Analysis / visualization surface** | A dashboard presents segment comparisons to a user. | ◑ Dashboard code queries MotherDuck directly (`dashboard/server-motherduck.ts`) and builds/typechecks clean; production Coolify app not yet redeployed to it. |
| A7 | **Precise delay measurement** | Real delay-in-seconds via GTFS schedule join, not just the coarse `dly` boolean. | ❌ Not implemented — documented as future work in `DATA_STRATEGY.md`. |

**Primary aim** (explicitly prioritized by the owner): *"just get the pipeline
working forward."* That corresponds to **A1–A5**, all of which are met. A6 is code-
complete and simplified (dashboard reads MotherDuck directly — no Postgres, no
consolidation) but not yet redeployed; A7 is a known, deliberately deferred
extension.

**Suggested questions for the reviewing agent:**
- Does the segment-tagging logic (bounding boxes in `src/segments.ts`) correctly
  and unambiguously classify Route-12 positions? Are the boxes non-overlapping and
  complete along the line?
- Is the ingest genuinely lossless under the failure modes handled in
  `src/index.ts` (insert failure, SSE drop, shutdown)?
- Given the aims, is redeploying the dashboard (A6) required for the project to be
  considered "complete", or is the live scraper sufficient?
- Do the sampling-bias and off-service-vehicle caveats (§9) undermine the core
  analysis the project is meant to enable?

---

## 2. High-Level Architecture

```
        NORTA BusTime API (bustime.norta.com)
                    │   Clever Devices "getvehicles" data
                    │   (fly.dev polls with its own upstream key)
                    ▼
        nolatransit.fly.dev/sse   ── Server-Sent Events, fan-out
                    │
                    ▼
   ┌───────────────────────────────┐
   │  SCRAPER  (src/, TypeScript)   │   ◄── LIVE / PRODUCTION
   │  • subscribe to SSE            │
   │  • tag each ping with segment  │
   │  • buffer + bulk INSERT / 60s  │
   └───────────────┬───────────────┘
                   ▼
       ┌───────────────────────┐
       │  MotherDuck (cloud     │   my_db.transit_data (~1.7M rows)
       │  DuckDB)               │
       └───────────┬───────────┘
                   ▲
                   │  aggregate queries (cached 1h in memory)
                   │
   ┌───────────────┴───────────────┐
   │  DASHBOARD (dashboard/)        │   React 19 + Vite front end
   │  Express API server-           │   Express + @duckdb/node-api
   │  motherduck.ts                 │   (code-complete; redeploy pending)
   └────────────────────────────────┘
```

**Status legend:** the **Scraper → MotherDuck** path is live and verified. The
**Dashboard** queries MotherDuck directly (aggregating on the fly, cached in
memory) — code-complete, awaiting redeploy. There is **no Postgres and no
consolidation service** (both removed to simplify the stack).

---

## 3. Components

### 3.1 Scraper (`src/`) — LIVE

The production heart of the project. A long-running Node process (TypeScript, run
directly via `tsx`).

| File | Responsibility |
|------|----------------|
| `src/index.ts` | Main loop: SSE subscription, buffering, periodic insert, graceful shutdown, stats logging. |
| `src/segments.ts` | Static definition of the 7 Route-12 track segments and `findSegment()` point-in-box matching. |
| `src/motherduck.ts` | MotherDuck connection, table DDL, and bulk `INSERT` logic. |
| `src/types.ts` | Shared domain types (`RawVehicle`, `TransitRecord`, `Segment`, `SegmentMatch`, `SegmentType`). |

**Runtime flow:**

1. On startup, validate `MOTHER_DUCK_API_KEY`, open the MotherDuck database, and
   `CREATE TABLE IF NOT EXISTS transit_data`.
2. Connect to the SSE endpoint (`SSE_URL`, default `https://nolatransit.fly.dev/sse`).
3. Each SSE message is a JSON array of vehicles. For each vehicle:
   - Skip invalid coordinates (`lat == '0' && lon == '0'`).
   - Parse numeric fields, resolve the segment via `findSegment(route, lat, lon)`.
   - Push a `TransitRecord` onto an in-memory buffer.
4. Every `UPLOAD_INTERVAL` (default 60s), flush the buffer with one bulk `INSERT`.
   The buffer is cleared immediately on flush; if the insert fails, records are
   **prepended back** onto the buffer to retry next cycle (no data loss).
5. `SIGTERM`/`SIGINT` trigger a graceful flush of any remaining buffer before exit.

**Segment matching** (`findSegment`) is a simple bounding-box lookup: for the
given route it walks the 7 segments and returns the first whose lat/lon box
contains the point. Non-Route-12 pings (or Route-12 pings outside all boxes)
get `null` segment fields but are still stored.

The 7 segments: **Canal Street (CBD)** and **Lee Circle / Downtown** are
`mixed_traffic`; the five St. Charles / Carrollton segments are `dedicated_row`.

### 3.2 Dashboard (`dashboard/`) — CODE-COMPLETE, REDEPLOY PENDING

React 19 + Vite front end with an Express 5 API back end
(`dashboard/server-motherduck.ts`). The server connects to MotherDuck with the
same `@duckdb/node-api` client the scraper uses and computes every dashboard
aggregate **on the fly** with `GROUP BY` queries against `transit_data`
(summary, per-segment-type, per-segment, per-route, hourly, and daily
time-series). Results are cached in memory for 1 hour to keep repeat loads cheap
and stay well under MotherDuck's free-tier row-scan limits.

There is intentionally **no Postgres and no consolidation service** — DuckDB is
fast enough to aggregate the raw table directly, so those layers were removed to
simplify the stack. The code typechecks, lints, and builds clean; the only
remaining step is redeploying the Coolify dashboard app (currently still running
the older R2 code) with a `MOTHER_DUCK_API_KEY`.

---

## 4. Data Model

MotherDuck table `my_db.transit_data`:

| Column | Type | Source |
|--------|------|--------|
| `vid` | VARCHAR | vehicle id |
| `timestamp` | TIMESTAMP | ping time (`tmstmp`) |
| `lat`, `lon` | DOUBLE | position |
| `heading` | INTEGER | `hdg` |
| `route` | VARCHAR | `rt` |
| `trip_id` | VARCHAR | `tatripid` |
| `destination` | VARCHAR | `des` |
| `speed` | INTEGER | `spd` |
| `is_delayed` | BOOLEAN | `dly` |
| `is_off_route` | BOOLEAN | `or` |
| `segment_id` | INTEGER | derived (1–7 or NULL) |
| `segment_name` | VARCHAR | derived |
| `segment_type` | VARCHAR | derived (`mixed_traffic` / `dedicated_row` / NULL) |

Inserts are built as inline bulk `INSERT ... VALUES` with a `sqlString()` helper
that escapes single quotes and emits `NULL` for nullish values.

---

## 5. Technology Stack

- **Language:** TypeScript throughout, executed directly with **`tsx`** (no build
  step). `typescript` is a dev dependency exposing `tsc --noEmit` typecheck scripts.
- **Datastore:** **MotherDuck** — cloud-hosted DuckDB, used by *both* the scraper
  (writes) and the dashboard (reads). Accessed via **`@duckdb/node-api`**
  (v1.5.4-r.1), which embeds the DuckDB C++ engine and a gRPC extension. It is
  *not* a REST API; the client speaks to MotherDuck over gRPC/TLS. Chosen over
  Postgres because of the data volume and DuckDB's columnar analytics strengths.
- **Feed client:** `eventsource` (SSE).
- **Dashboard:** React 19, Vite, Express 5, `@duckdb/node-api`; ESLint flat config
  with `typescript-eslint`. No relational database.
- **Config:** `dotenv`; environment variables (see §8).

---

## 6. Deployment

Deployed on a self-hosted **Coolify** instance at `coolify.cargobay.dev`.
Push to `main` triggers an automatic rebuild.

The `transitExp` repository maps to **three Coolify apps**, distinguished by
`base_directory`:

| App UUID | Base dir | Role | Status |
|----------|----------|------|--------|
| `qogwcwsc8cgooo4osk0ccw08` | `/` | Scraper (`nola-transit.cargobay.dev`) | **Live** |
| `w48ksws8g8ks8wo80kskog84` | `/dashboard` | Dashboard | Old R2 code — redeploy pending |
| `go84cw00sso0s0w4ccok04s4` | `/scripts` | Old R2 consolidation | Deprecated |

> ⚠️ **Warning:** the `/scripts` directory (and the later `/consolidation`
> directory) no longer exist, so the `go84…` app will **fail on its next
> rebuild**. Delete that Coolify app — consolidation was removed from the project.

### 6.1 Dockerfile — two critical fixes

Both the root `Dockerfile` (scraper) and `dashboard/Dockerfile` use `node:20-slim`
(glibc), not Alpine, and install CA certs — both connect to MotherDuck, so both
need these fixes to make `@duckdb/node-api` work in the container:

1. **`node:20-alpine` → `node:20-slim`.** The `@duckdb/node-bindings-*-musl`
   packages omit a `libc` field in the lockfile, so npm installs the **glibc**
   binding on Alpine and the native `.so` fails to load. glibc bindings on slim
   work out of the box.
2. **`ca-certificates` + `GRPC_DEFAULT_SSL_ROOTS_FILE_PATH`.** The MotherDuck
   extension connects over gRPC/TLS and needs a CA root bundle. `slim` ships none,
   producing *"Could not get default pem root certs"*. Installing `ca-certificates`
   and pointing `GRPC_DEFAULT_SSL_ROOTS_FILE_PATH` at
   `/etc/ssl/certs/ca-certificates.crt` resolves it.

There was also a root-cause bug from before the migration: the old `duckdb` npm
package was a **devDependency**, but the Dockerfile runs `npm ci --omit=dev`, so no
driver ever made it into the image — which is why the pipeline historically never
wrote. Migrating to `@duckdb/node-api` as a regular dependency fixed that.

---

## 7. Current Status

- ✅ **Scraper → MotherDuck is LIVE and writing.** Logs show
  `MotherDuck initialized successfully`, `SSE connection established`, repeated
  `Inserted N records into MotherDuck`, with **0 errors**.
- ✅ **Data verified:** `my_db.transit_data` holds ~**1.72M rows** across **51
  routes**, with timestamps current to the minute.
- ✅ **Full TypeScript migration** complete and verified (typecheck / build / lint
  clean across all configs).
- ✅ **Postgres and consolidation removed** — dashboard now reads MotherDuck
  directly; no relational DB to provision.
- ◑ **Dashboard** — code-complete against MotherDuck (typecheck/build/lint clean);
  production Coolify app not yet redeployed to the new server.

---

## 8. Environment Variables

| Variable | Used by | Default | Notes |
|----------|---------|---------|-------|
| `MOTHER_DUCK_API_KEY` | scraper + dashboard | — (required) | MotherDuck PAT (JWT). |
| `MOTHERDUCK_DATABASE` | scraper + dashboard | `my_db` | Target database name. |
| `SSE_URL` | scraper | `https://nolatransit.fly.dev/sse` | Upstream feed. |
| `UPLOAD_INTERVAL` | scraper | `60000` | Buffer flush interval (ms). |
| `RECONNECT_DELAY` | scraper | `5000` | SSE reconnect backoff (ms). |
| `PORT` | dashboard | `3000` | Dashboard API/server port. |

---

## 9. Known Issues & Future Work

- **`/scripts` Coolify app (`go84…`) will fail on rebuild** — delete that app; the
  consolidation service it built no longer exists.
- **Dashboard redeploy** — point the `/dashboard` Coolify app at the current code
  and set `MOTHER_DUCK_API_KEY`; the server now reads MotherDuck directly (no
  Postgres needed).
- **Data-strategy enhancements** (documented in `DATA_STRATEGY.md`): capture extra
  SSE fields currently dropped (`pdist`, `pid`, `rid`, `tablockid`, `srvtmstmp`);
  ingest the free static `GTFS.zip` and join pings to scheduled trips via
  `tatripid` to compute **real delay-in-seconds** instead of the coarse `dly`
  boolean.
- **Sampling bias caveat:** `dly`-based delay percentages are oversampling-biased
  (stuck vehicles emit more pings). Also, off-service vehicles can report
  `rt:"U"` / `tatripid:"N/A"`, which route filters silently drop — audit at peak
  service.
