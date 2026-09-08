# Transit pipeline deployment

Deploy the collector and dashboard from the same revision. The collector owns
local DuckDB and a durable journal; the dashboard serves saved summaries. R2 is
not required. The full storage and research contract is in
[LOCAL_DATA_PIPELINE.md](LOCAL_DATA_PIPELINE.md).

## Collector

Use the repository root Dockerfile, port 3100, and persistent storage mounted at
`/app/data` owned by the container's `node` user (UID 1000). Set a stable internal
network alias, `transit-collector`.

Required configuration:

```text
TRANSIT_DATA_DIR=/app/data
PORT=3100
TRANSIT_SUMMARY_TOKEN=<long server-only random value>
MOTHERDUCK_CLOUD_WRITES=false
LEPASS_ENABLED=true
LEPASS_API_KEY=<verified app credential>
LEPASS_ENCRYPTION_KEY=<separate encryption key>
LEPASS_ALLOW_GUEST_BOOTSTRAP=false
```

Copy the verified encrypted session to `/app/data/lepass/lepass-credentials.enc`.
Keep its encryption key separate in environment configuration. Use
`MOTHERDUCK_BOOTSTRAP=false` if the preserved history has already been transferred;
otherwise configure the source database and token for its read-only migration.
Cloud writes need a separate, fresh free-tier billing checkpoint and remain off
until the guard passes. Do not set an invented usage value to enable uploads.

In Coolify, enable **Consistent Container Name** so the old collector stops
before the new one starts. Concurrent containers must not write the same DuckDB
file or rotating session. The installed 4.0.0-beta.455 supports persistent
storage, aliases and this setting in its UI, although its application PATCH API
omits those fields.

## Dashboard

Use base directory `/dashboard`, its Dockerfile and port 3000. Put it on the
collector's internal Docker network and mount a separate persistent `/app/data`
cache. Configure:

```text
PORT=3000
TRANSIT_SUMMARY_URL=http://transit-collector:3100/internal/summary
TRANSIT_SUMMARY_TOKEN=<same server-only value as collector>
TRANSIT_SUMMARY_CACHE_FILE=/app/data/transit-summary.json
```

Remove the dashboard's old MotherDuck credentials. It no longer needs database
access. No API or encryption credentials belong in Vite/browser variables.

## Migration and acceptance

1. Ensure the filesystem satisfies the 80% usage ceiling and 10 GB free-space
   reserve after accounting for the history transfer and Docker build.
2. Preserve existing source history before retiring jobs. The old R2 files are
   inventoried and verified in `data/archive-audit`; the archive job is stopped
   and automatic redeployment is disabled. Existing R2 objects remain intact.
3. Transfer local DuckDB and archives while their writer is stopped. Checkpoint
   the database first and never pair it with a stale WAL from an earlier copy.
   Archive catalog paths must refer to the mounted destination.
4. Deploy the collector, confirm both feeds in authenticated `/internal/health`,
   and verify that `/internal/summary` returns 401 without its bearer token.
5. Deploy the dashboard and verify `/api/source-quality`, `/api/row-study`,
   `/api/signal-study` and `/api/otp`. Restart it to confirm cache recovery.
6. Keep collecting long enough for date and sample thresholds. A valid backfill
   can honestly report insufficient evidence; missing historical Le Pass data,
   receipt times or dated ROW classifications must not be invented.

Run the full local study backfill only when no collector worker holds its database:

```bash
TRANSIT_DATA_DIR=/app/data MOTHERDUCK_BOOTSTRAP=false npm run study:backfill
```

The normal supervised worker also performs bounded background backfill. The
public snapshot covers up to 90 days with a 28-day default; full local archives
and results remain available for further research.
