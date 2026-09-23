# Current operating mode — server cutover September 22–23, 2026

The active input path is **server → MotherDuck → this PC**. The server captures
both SSE and Le Pass, journals them durably, and uploads original compressed
bundles plus first-observed GTFS versions to MotherDuck. This PC downloads only
committed MotherDuck inputs, processes them in local DuckDB, and publishes
completed results to the public gateway. It does not record either feed.
The original SSE-to-MotherDuck collector remains running for compatibility.

MotherDuck tables `my_db.transit_capture_assets` and
`my_db.transit_capture_manifests` are the durable handoff. Assets contain the exact
compressed raw bytes, hashes, descriptors and sequence IDs; a manifest becomes
visible only after remote readback verification. These are raw archive bundles,
not a claim that each raw frame is already normalized into SQL observation rows.
Existing normalized observation/result archive tables remain available too.
Server bundle payloads older than the latest sealed bundle are released only
after verifying their committed MotherDuck copies; sidecars/GTFS are retained.
The latest bundle stays on the server for seal crash recovery. Quota/network
failure leaves uncommitted input queued; no processing runs on the server.

The real handoff was verified September 23: 48 server bundles (3,689 frames)
were committed to MotherDuck and imported into the PC without direct raw
server downloads. Daily analysis is scheduled at 06:00 Chicago; it refuses
to mark a date complete if the cloud input has not reached that date's end.

At the user's request, Hermes agent and web UI containers/images were removed;
their volumes/configuration were retained. OpenClaw is explicitly retained.
Server capture uses a 512 MiB temporary queue ceiling (including 64 MiB
packaging headroom) and a 2 GB free-space reserve, configured through
`CAPTURE_QUEUE_MAX_BYTES` and `CAPTURE_FREE_RESERVE_BYTES`. Verified older
bundles are released after upload. Quota/network failures retain queued data;
capture pauses at either ceiling and must be restarted after pressure clears.
This bounds the feed spool, not other applications, logs or GTFS storage.
The workstation's default 10 GB / 80% disk guard is unchanged.

The direct-server leased workflow in `DAILY_PROCESSING.md` is now inactive.
A second workstation has not been provisioned. The verified baseline is retained at
`runtime-data/server-capture-seed-2026-09-22`; final PC journal receipts were
also copied to `runtime-data/cutover-journal-2026-09-22` before ingestion.

MotherDuck is the requested destination for both feeds and closed daily results.
`transit-motherduck-archive.timer` attempts one guarded upload cycle hourly.
The account owner confirmed that no credit card is linked. The active uploader
uses `MOTHERDUCK_BILLING_MODE=free_no_card`: an 8 GB **live-data** guard and
MotherDuck's own quota enforcement, without requiring unavailable monthly CU
telemetry. Retained/historical/failsafe storage is still reported separately;
this is our explicit no-card operating policy, not a claim that MotherDuck
never counts retained bytes. Quota errors retain pending data for the next
hourly attempt. Never add payment details or upgrade billing automatically.
If billing becomes enabled, remove this mode and reverify the budget before
continuing. Inspect `processing/motherduck-archive-health.json` and the service
journal; a scheduled attempt is not proof of a successful upload. Cloud
retention is bounded; verified disk archives remain necessary.

The first normalized no-card cycle on September 23 at 05:29 UTC was verified remotely:
12,389 SSE observations, 2,248 Le Pass observations and 10 closed daily results.
843,237 locally queued observations remained after that bounded cycle. The
normalized/result archive uploader runs on this PC. Raw capture upload now
runs independently on the server, including while the PC is asleep.

## Active services

- Coolify service `hogc8wwcsk48c88kkcsgkog4`, container
  `capture-hogc8wwcsk48c88kkcsgkog4`: durable server capture, no database analysis.
  Its private port is `127.0.0.1:3102`; data/code/secrets are under
  `/data/transit-capture`. The compose template reuses the existing dependency
  image (unchanged package lock) with explicitly staged read-only source mounts.
  Source updates must preserve the server/worker analysis digest match.
- Workstation `transit-capture.service`, `transit-cloud-history.timer`, and
  `transit-server-history.timer`:
  disabled. Do not restart PC capture with the transferred Le Pass session.
- Workstation `transit-capture-tunnel.service`: port 13102, now for authenticated
  health checks only, not raw-data transfer or processing.
- Server `transit-capture-upload.timer`: uploads a bounded batch every fifteen
  minutes, exact readback verification, then releases verified older spool files.
- Workstation `transit-motherduck-processing.timer`: polls every fifteen minutes;
  the previous day's job is due at 06:00 Chicago. The first
  scheduled service date is September 22, due September 23 at 06:00.
  Failed analysis/publication leaves the day due for retry. Raw downloads are
  checksum-verified and imports are idempotent; each attempt has finite input.
- Workstation `transit-motherduck-archive.timer`: hourly bounded upload attempt;
  shares the native database-worker lock with daily processing.
- Workstation `transit-health.timer`: checks public pipeline clocks and the
  authenticated server Le Pass collector via SSH every ten minutes.
- Server `transit-health-server.timer`: checks live collection, persistence,
  analysis age and summary delivery every ten minutes even while the workstation
  is offline, including Le Pass. It runs inside the durable capture container.

The workstation user manager has lingering enabled. This survives logout and
starts when WSL boots; it cannot keep a sleeping Windows host or stopped WSL
instance processing. Both server feeds continue while this PC sleeps, subject
to disk headroom and provider availability. Historical Le Pass gaps cannot be
reconstructed from predictions.

## Previous cloud-history catchup and recovery (manual fallback)

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
returns 503 when its saved analysis is stale. The approved dashboard deployment
uses a 36-hour analysis window for daily processing. The scheduled checks independently
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
