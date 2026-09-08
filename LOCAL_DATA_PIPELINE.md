# Two-source collection and local analysis

The collector retains every received RTA bus/streetcar observation in a durable
local journal. A separate process ingests the journal into local DuckDB, runs OTP
and the ROW/signal studies, and creates the dashboard snapshot. MotherDuck is the
cloud archive; R2 is not part of this pipeline.

## Data and clocks

SSE frames include every validated GTFS bus/streetcar route, including Riverfront.
LePass queries use independently validated provider stop/line/pattern mappings.
Vehicle IDs, sample clocks, receipt clocks and mapping confidence remain
source-qualified. Predictions are stored separately and never become observed
arrivals. Missing speed remains null. Both feeds may share upstream GPS data.

The journal uses immutable gzip files, file fsync, atomic rename and directory
fsync. A frame is acknowledged only after local DuckDB commits it. Batch and
observation IDs make retries safe after an ambiguous commit. Repeated payloads
share a content-addressed row while every receipt remains separate. Changing
coordinates under the same provider minute are never treated as duplicates.

Local analysis is isolated from the collector's event loop. It reconstructs
complete 200-metre passages, directional signal encounters, and conservative
stationary episodes. Historical provider-only points support passage analysis;
they cannot supply missing historical receipt evidence for signal waits.

The reviewed catalog records dated ROW evidence and leaves unsupported locations
and historical periods unknown. ROW headlines require 30 passages per class on
seven common dates; signal headlines require 30 evaluable encounters on seven
dates. Service-day bootstrap intervals are separate from timestamp bounds.
Signal-priority scenarios remove 25/50/75 percent of eligible isolated waiting;
they are hypotheses, not measured intervention effects.

## Storage and spending

Cloud uploads default **off**. Collection, analysis and the public dashboard keep
working without a MotherDuck connection. No paid service upgrade is performed.
The operating ceilings are 8 GB of total accounted storage and 8 CU-hours/month.
Historical, clone-retained and failsafe bytes count too. Deleting a cloud row does
not immediately reclaim its retained storage.

Before enabling `MOTHERDUCK_CLOUD_WRITES=true`, verify the actual Lite plan and
month-to-date usage in the MotherDuck billing interface. Put only the following
non-secret billing checkpoint in `MOTHERDUCK_USAGE_FILE`:

```json
{"plan":"lite","billing_mode":"free","month":"2026-09","measured_at":"2026-09-08T00:00:00Z","compute_cu_hours":0.0}
```

Use actual measured values. The checkpoint expires after six hours. Missing,
stale or over-budget usage pauses cloud uploads; it never enables paid overage.
These application ceilings are conservative operational controls, not a provider
billing guarantee. MotherDuck's own current billing configuration remains the
source of truth.

Uploads combine normalized payloads, exact receipts and batch metadata in bounded
Parquet batches. Closed daily study results and catalog versions are retained in
a separate transient results database. Public HTTP requests never query
MotherDuck, and large mutable dashboard snapshots remain on the existing server.

Completed source days are exported as compressed Parquet with SHA256 manifests
and readback verification. Local receipt detail older than seven days can be
removed from hot DuckDB only after exact archive comparison; archive-backed
reanalysis and pending cloud retries can rehydrate it. Study events and daily
results remain available. The 12 legacy R2 archives (2025-12-30 through 2026-01-10) were copied
to `runtime-data/r2-archive`, preserving 9,422,603 rows in 482,512,591 bytes.
SHA256, file sizes and Parquet readback are recorded in
`data/archive-audit/r2-local-manifest.json`. The legacy job has been stopped;
its existing cloud objects remain intact. Those older archives lack trip IDs
and receipt provenance and are kept as source history, without fabricated OTP
or signal-wait backfill.

The server pauses collection at 80% filesystem usage or less than 10 GB free.
It does not silently delete observations or expand paid storage. Local cold
archives alone do not protect against losing the server.

## Configuration and deployment

Mount persistent storage at `/app/data` for the collector. Its private endpoint
is `GET /internal/summary` on port 3100 and requires
`Authorization: Bearer $TRANSIT_SUMMARY_TOKEN`. The dashboard needs
`TRANSIT_SUMMARY_URL` pointing to that endpoint, the same token, and its own
persistent snapshot-cache directory. Use a stable Docker network alias for the
collector; Coolify's default generated container name changes on deployment.
Enable Coolify's **Consistent Container Name** setting for the collector so the
old container stops before its replacement starts. The persistent DuckDB file
and rotating LePass session each need a single writer. For the installed Coolify
4.0.0-beta.455, persistent storage, network aliases and this setting are supported
in the application UI, but are absent from its application PATCH API. Newer API
documentation must not be assumed to match that version.

Keep all tokens in server-only environment variables. LePass token state uses
AES-256-GCM with the encryption key supplied separately. The API/user credential
and rotating token pair remain out of logs and public snapshots. Refresh uses
90% of token lifetime, coalesces concurrent requests and retries an authentication
failure once. The normal guest's nominal refresh expiry is not a promise that
the service will never revoke it. A working guest bootstrap must be live-tested;
the production collector will not loop creating accounts.

`src/data/lepass-queries.json` records provider mappings and their confidence.
Polling respects the advertised interval, at least 20 seconds per query, a single
in-flight request and a global one-request-per-second ceiling. Requests batch
up to eight queries. The current catalog covers all 33 passenger routes using
70 queries; live validation measured roughly 20–22 seconds per query. Actual
coverage and response cadence remain visible in private source health.

On first startup, historical MotherDuck tables are copied to verified local
Parquet and imported without remote changes. The import is resumable; incomplete
cloud access must not interrupt new journal ingestion. Existing OTP schedule
archives, mappings and backfill requests migrate with the raw observations.

To process all eligible historical dates on a stopped collector's local database:

```bash
TRANSIT_DATA_DIR=/path/to/persistent/data MOTHERDUCK_BOOTSTRAP=false npm run study:backfill
```

Only one local DuckDB writer process may open that database. In the normal
collector, its supervised analysis worker performs bounded background backfill.
For offline maintenance, restart the collector with `LOCAL_ANALYSIS_ENABLED=false`
to keep both feeds writing their journal while its database worker is stopped.
Return that setting to true after maintenance; pending frames replay idempotently.
New study APIs are `/api/row-study`, `/api/signal-study` and `/api/source-quality`.
The public study snapshot covers up to 90 days with a 28-day default; older
observations and results remain in the local/cloud archives for research.

## Deployment acceptance

Before pushing this branch to the production `main` branch, provide enough free
space for the configured 80% filesystem ceiling and 10 GB reserve, including
the initial history transfer and build. The existing server was last observed
with about 6 GB free on a 38 GB filesystem; this does not satisfy that guard.
Shared Docker cleanup requires separate owner approval because unused images
can belong to other applications' rollback history.

The collector needs a persistent `/app/data` volume, a stable `transit-collector`
network alias, the single-writer deployment setting above, and port 3100. The
dashboard needs its own persistent `/app/data` cache, the same internal Docker
network, and `TRANSIT_SUMMARY_URL=http://transit-collector:3100/internal/summary`.
Supply matching server-only summary tokens. Import the preserved local history
and encrypted LePass state while their writer is stopped; copy a checkpointed
DuckDB file without a stale WAL from an earlier copy. Keep encryption keys in
server-only environment variables, separate from the mounted credential file.

Verify both sources in authenticated `/internal/health`, unauthorized summary
requests returning 401, successful `/api/source-quality` on the dashboard, and
snapshot recovery after a dashboard restart. MotherDuck cloud writes remain off
until a fresh free-tier billing checkpoint passes the configured guards. The
legacy production collector/dashboard are replaced only after these deployment
prerequisites are met; a local acceptance run is not a production deployment.
