# Scraper hardening — change summary

Covers the ingest-side work: capturing dropped SSE fields, scraper-level
deduplication, ingest robustness, and two read-only analysis scripts.
No changes to `dashboard/` or `consolidation/`. The existing
`my_db.transit_data` table is preserved; all new columns are nullable and
added idempotently on startup, so the running scraper and historical rows
remain compatible.

## Schema changes (`my_db.transit_data`)

Five new **nullable** columns, appended after `segment_type`:

| Column      | Type    | Source SSE field | Meaning                                   |
|-------------|---------|------------------|-------------------------------------------|
| `pdist`     | INTEGER | `pdist`          | Linear distance along the route pattern   |
| `pid`       | INTEGER | `pid`            | Pattern id                                |
| `rid`       | VARCHAR | `rid`            | Route id                                  |
| `tablockid` | VARCHAR | `tablockid`      | Block/run identifier                      |
| `srvtmstmp` | VARCHAR | `srvtmstmp`      | Service timestamp (stored as-is)          |

- Fresh tables get these via `CREATE TABLE IF NOT EXISTS`; existing tables
  are migrated on every startup with `ALTER TABLE ... ADD COLUMN IF NOT
  EXISTS` (no-op once present).
- The bulk `INSERT` now uses an **explicit column list** (`INSERT_COLUMNS`
  in `src/motherduck.ts`), so inserts stay correct regardless of the live
  table's physical column order.
- On startup the scraper logs one sample parsed record (`[Sample] ...`) so
  the new fields can be eyeballed in production logs.

## Behavior changes (`src/index.ts`)

- **Deduplication** — an in-memory `Map<vid, tmstmp>` skips any ping whose
  `tmstmp` has not advanced for that vehicle. The watermark only advances
  for pings that are kept. Deduped count shows in the `[Stats]` line.
  Historical duplicates are left untouched.
- **Retry-buffer cap** — the buffer is capped at `MAX_BUFFER_RECORDS`.
  When exceeded (on ingest or after a failed insert is re-queued) the
  **oldest** records are dropped and a loud `[DROP]` line is logged.
- **Feed freshness watchdog** — if no SSE message arrives within
  `STALE_FEED_THRESHOLD` ms, an `[STALE FEED]` error is logged and the SSE
  connection is force-reconnected.
- **Dry-run mode** — if `MOTHER_DUCK_API_KEY` is absent, the scraper starts
  anyway, skips MotherDuck init, and stubs inserts (`[DryRun] ...`). Useful
  for a local smoke test against the live feed without writing.

## New environment variables

| Var                    | Default  | Purpose                                            |
|------------------------|----------|----------------------------------------------------|
| `MAX_BUFFER_RECORDS`   | `500000` | Retry-buffer cap; oldest dropped past this         |
| `STALE_FEED_THRESHOLD` | `300000` | ms of silence before forcing an SSE reconnect      |

(Existing vars unchanged: `MOTHER_DUCK_API_KEY`, `MOTHERDUCK_DATABASE`,
`SSE_URL`, `UPLOAD_INTERVAL`, `RECONNECT_DELAY`.)

## Analysis scripts (read-only, tsx-runnable)

Both require `MOTHER_DUCK_API_KEY` (they query production data) and read
`.env` from the repo root. Run from the repo root:

```bash
# Duplicate rate over the last 7 days: total rows, distinct (vid, timestamp)
# pairs, and duplicate percentage.
npx tsx scripts/analyze-dupes.ts

# Route-12 segment-tagging validation over the last 7 days:
#  - ping counts per segment_id and per segment_type
#  - count / % of in-service pings with a NULL segment
#    (in-service = route '12', excluding rt 'U' and tatripid 'N/A')
#  - coarse boundary check: NULL-segment pings bucketed by rounded lat/lon
npx tsx scripts/validate-segments.ts
```

Neither script writes to the database.

## Verification

- `npm run typecheck` passes; the two scripts also typecheck cleanly.
- Dry-run smoke test against the live feed confirmed startup, sample-record
  logging with all five new fields populated, deduplication, and stubbed
  inserts.
