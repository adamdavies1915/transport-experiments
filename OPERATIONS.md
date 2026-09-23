# Current operating mode — September 22, 2026

The production server captures SSE into MotherDuck. This workstation journals
both SSE and Le Pass, imports the server's missing history daily, performs finite
analysis, and publishes only a verified completed result. Workstation capture
and database processing are separate systemd user services. No live database is
shared, no cloud plan is upgraded, and local archive cloud writes remain off.

The server had 7.2 GiB free at recovery, below the 10 GB reserve required for
durable server capture. The fully coordinated two-workstation mode in
`DAILY_PROCESSING.md` remains opt-in and is **not active**. More server storage
is required before moving Le Pass and durable capture there. No unrelated images
or rollback history were removed to create space.

## Active services

- Workstation `transit-capture.service`: continuously journals both feeds;
  restarts on failure. `scripts/capture-workstation.ts` forces analysis off and
  reads API, encryption and summary credentials from private `*_FILE` settings.
- Workstation `transit-cloud-history.timer`: checks hourly; one successful
  catchup per Chicago calendar date. Failed jobs retry next hour. Catchup runs
  at the next available attempt, not a guaranteed 06:00 deadline.
- Workstation `transit-health.timer`: checks public pipeline clocks and the
  authenticated local Le Pass collector every ten minutes.
- Server `transit-health-server.timer`: checks live collection, persistence,
  analysis age and summary delivery every ten minutes even while the workstation
  is offline. It runs the small monitoring script inside the existing collector.

The workstation user manager has lingering enabled. This survives logout and
starts when WSL boots; it cannot keep a sleeping Windows host or stopped WSL
instance collecting. Server SSE capture continues independently. Historical
Le Pass gaps cannot be reconstructed from predictions.

## Catchup and recovery

Use `deploy/processing/transit-cloud-history.service` and its timer as templates,
with absolute checkout, Node and private environment-file paths. Configure:

```dotenv
TRANSIT_DATA_DIR=/absolute/path/to/existing/transit-data
CLOUD_HISTORY_FROM=2026-09-12
MOTHERDUCK_DATABASE=my_db
MOTHER_DUCK_API_KEY_FILE=/private/existing-cloud-token
TRANSIT_SUMMARY_PUBLISH_URL=https://nola-transit.cargobay.dev/internal/summary
TRANSIT_SUMMARY_PUBLISH_TOKEN_FILE=/private/summary-publish-token
LOCAL_DB_MEMORY=4GB
LOCAL_DB_THREADS=4
NODE_OPTIONS=--max-old-space-size=3072
```

The existing cloud credential is used only for SELECT/COPY reads. It is not
claimed to be a provider-enforced read-only credential. Exported Parquet is kept
under the data directory's `imports/cloud-*` with hashes and counts. The importer
verifies exact content and replay multiplicity. OTP uses historically retained
schedules, not today's schedule retroactively. Before processing the queue, it
reconciles requested dates with the latest eligible retained schedule. This
prevents an import queued before refresh from overwriting a newer calculation
with the old schedule. A separate native worker lock
prevents overlapping catchup/daily jobs; DuckDB also prevents concurrent writers.
Do not run the combined collector worker against this database.

The successful cloud cutoff advances only after import, finite backfill,
job-specific completion verification and accepted publication. The next job
reexports from one Chicago day before that cutoff to cover overlap. Retries are
idempotent. `processing/cloud-history-completed.json` records the cutoff and
successful publication. Retained exports and journals consume disk; the normal
10 GB / 80% local disk guard remains active. This is not unlimited retention.

Run a manual retry through `systemctl --user start transit-cloud-history.service`.
To intentionally rerun on a date already completed, stop the timer, ensure the
service is inactive, and use the same environment with
`npm run process:cloud-history -- --force`; then restart the timer. Do not stop
capture for an ordinary catchup. A pre-recovery database/WAL/summary copy remains
under `runtime-data/recovery-2026-09-22`.

## Freshness and alerts

`/api/health` remains web-server liveness. The dashboard's `/api/readiness`
returns 503 when its saved analysis is stale. The prepared dashboard deployment
sets a 36-hour analysis window for daily processing; the running dashboard keeps
its previous window until that rollout is approved. The scheduled checks independently
require collection, persistence and successful summary transport within five
minutes. Repeatedly fetching an old summary never resets its analysis age.

Failures produce a failed systemd check and a JSON status in the server's
`/app/data/pipeline-health.json` or the workstation's private health file.
Inspect with `journalctl --user -u transit-health.service` or, on the server,
`journalctl -u transit-health-server.service`. The website also displays its
stale-analysis warning. No email, Slack or external notification recipient has
been configured; alerts currently exist in service status/logs and the dashboard.

If Le Pass reports `mapping_revision_changed`, preserve its raw receipts and
follow the read-only catalog discovery workflow in `LOCAL_DATA_PIPELINE.md`.
Review the new schedule hash, routes, ordered stop matches and candidate
patterns before activating the catalog. Never fix this by only editing the
revision number. The September 22 revalidation is recorded in
`data/feed-audit/2026-09-22-lepass-revalidation.json`.

## Research follow-up

`STUDY_PLAN.md` fixes the next Route 12 comparison period and field protocol.
`fieldwork/` contains empty collection and reconciliation templates. Existing
roadway evidence is preserved; proposed expanded windows are not labeled until
reviewed. New shape IDs alone do not authorize rewriting old geographic offsets.
