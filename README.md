# NOLA Transit Observations

Independent observations of New Orleans RTA buses and streetcars, using the RTA
relay SSE feed and Le Pass. The dashboard studies roadway travel time and
candidate signal waits, alongside independently calculated schedule-based OTP.

Current services, recovery and freshness checks are documented in
[operations](OPERATIONS.md). The [focused study plan](STUDY_PLAN.md) defines the
next Route 12 collection period and the manual validation still to be performed.

## How it works

The collector preserves source identities, raw responses, provider sample times
and exact receipt times in a durable local journal. A separate worker ingests
local DuckDB, analyses observations and publishes a saved dashboard summary.
Le Pass predictions remain separate from measured positions. Missing speed
remains unknown, and the two feeds are analysed separately because they may
share an upstream source.

MotherDuck is an optional, guarded cloud archive. The public dashboard serves
cached local summaries and makes no MotherDuck queries. R2 is retired from the
new pipeline; its unique historical files have been preserved and verified.

For the existing small server, `COLLECTOR_MODE=cloud` preserves its SSE ingestion
into MotherDuck and disables cloud analysis. A workstation collects both feeds,
calculates studies, and publishes the latest summary to the server. This staged
deployment keeps only the summary on server disk; daily job handoff and moving
Le Pass collection to the server remain separate steps. See
[deployment](DEPLOYMENT.md) and [summary publication](SUMMARY_PUBLICATION.md).

## Research questions

- **Roadway time:** compare complete 200 m passages on reviewed shared and
  reserved streetcar sections, matching service date, direction, time band and
  mapped stop/signal exposure. Unreviewed sections and historical periods remain
  unknown. This is an observational comparison, with remaining confounders.
- **Signals:** examine complete directional encounters for all mapped RTA bus
  and streetcar routes. Detected stationary time, mixed boarding waits, no
  detected wait and insufficient sampling remain distinct. Hypothetical
  25/50/75% recovery scenarios do not claim measured signal-priority effects.
- **OTP:** match our observed scheduled timepoint events to archived GTFS,
  classify early/on-time/late, and report coverage and matching provenance.
  RTA's realtime delay flag is not OTP. See [OTP.md](OTP.md).

Headlines require 30 eligible observations across seven dates. Sampling bounds
and service-day bootstrap confidence intervals are shown separately. Independent
traffic/congestion data for buses is deferred to a later version.

## Run locally

For continuous server capture with automatic daily processing on either a
desktop or Mac, see [DAILY_PROCESSING.md](DAILY_PROCESSING.md). That opt-in mode
uses verified transfers, separate workstation databases and one server lease;
the combined local mode below remains supported.

Use Node.js 20.3 or later. Copy `.env.example` to `.env`, set a random server-only
`TRANSIT_SUMMARY_TOKEN`, and choose a persistent `TRANSIT_DATA_DIR`. MotherDuck and
Le Pass can be disabled while SSE collection continues.

```bash
npm ci
npm start
```

Le Pass requires the verified API credential, a separate encryption key and an
existing encrypted guest session. The checked-in query catalog covers all 33
passenger routes. Automatic guest creation defaults off. See
[LOCAL_DATA_PIPELINE.md](LOCAL_DATA_PIPELINE.md) for configuration and catalog
provenance, and [LIVE_FEED_COMPARISON.md](LIVE_FEED_COMPARISON.md) for the live audit.

Run the dashboard from its directory using its own server-only summary endpoint
and token configuration; see [dashboard/README.md](dashboard/README.md).

## Import server history

Use immutable local Parquet exports of `transit_data` and `streetcar_snapshots`.
Restart the collector with `LOCAL_ANALYSIS_ENABLED=false` and wait for its old
database worker to exit. The collector can keep journaling while the database
is offline. Take a database backup with no writer running, then import:

```bash
npm run history:import -- \
  --data-dir /path/to/persistent/data \
  --transit-data /path/to/exports/transit_data.parquet \
  --streetcar-snapshots /path/to/exports/streetcar_snapshots.parquet
```

All three paths are required. The command opens an existing local database and
performs no network access, bootstrap or process management. It preserves local
rows and exact duplicate multiplicity, rejects conflicting snapshot IDs, and
verifies row counts, source containment and file hashes before committing.
Reimporting the same files inserts zero rows. Each successful run writes a unique,
atomic audit under the chosen data directory's `imports/`, including hashes,
insert/replay counts and affected study dates. Imported raw-data dates are queued
for OTP recalculation using only retained schedules that cover each date; the
audit lists queued dates and dates without a covering schedule. It never assumes
that today's schedule applies to an earlier date.

Keep the collector in journal-only mode while the finite backfill runs:

```bash
TRANSIT_DATA_DIR=/path/to/persistent/data MOTHERDUCK_BOOTSTRAP=false MOTHERDUCK_CLOUD_WRITES=false npm run study:backfill
```

For larger local runs, set `LOCAL_DB_MEMORY` to the DuckDB memory allowance and
`NODE_OPTIONS=--max-old-space-size=3072` to allow a 3 GiB JavaScript heap. These are
separate limits: leave room for both processes and the operating system. The
September 12 catchup completed with a 2 GB DuckDB allowance after the 1 GB default
ran out of memory; this workstation's normal launcher now uses 4 GB for DuckDB.

Wait for a successful exit, then restart the collector with
`LOCAL_ANALYSIS_ENABLED=true` to resume its normal database worker. Never run the
importer, backfill and normal worker concurrently against the same database.

## Storage and deployment

The operating ceilings are 80% local filesystem use with at least 10 GB free,
and 8 GB / 8 CU-hours for the optional MotherDuck archive. Cloud writes default
off and require a fresh verified free-tier usage checkpoint. The application
never upgrades a service plan. These guards cannot guarantee a provider invoice.

Daily Parquet archives are verified before hot detail is removed. Full research
events and daily results remain local; the browser receives a smaller filtered
summary. Public studies cover up to 90 calendar days within a 60 MiB snapshot
limit. When necessary, publication removes whole oldest study dates and explains
the shortened window; stored research results remain intact. Local archives
still need protection against loss of the server.

See [DEPLOYMENT.md](DEPLOYMENT.md) for the two persistent Coolify services and
[LOCAL_DATA_PIPELINE.md](LOCAL_DATA_PIPELINE.md) for restart, backfill and retention
requirements. Legacy streetcar diagnostics remain documented in
[STREETCAR_ANALYSIS.md](STREETCAR_ANALYSIS.md) and
[STREETCAR_PRIORITY.md](STREETCAR_PRIORITY.md).

## Validation

```bash
npm test
npm run typecheck
```

The dashboard has its own tests, typecheck, lint and production build commands.

## License

MIT
