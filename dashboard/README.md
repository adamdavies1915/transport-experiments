# NOLA transit dashboard

The public server serves collector-produced JSON summaries from memory and a durable local snapshot. It has no MotherDuck connection, database token, or request-triggered query path. A background refresh requests the private collector endpoint once a minute; the collector computes summaries every 15 minutes. A snapshot older than 35 minutes, or a failed refresh, is marked stale without discarding usable data.

Configure only server-side environment variables:

| Variable | Purpose |
| --- | --- |
| `TRANSIT_SUMMARY_URL` | Private collector URL, e.g. `http://collector:3100/internal/summary` |
| `TRANSIT_SUMMARY_TOKEN` | Shared bearer secret; never use a `VITE_` prefix |
| `TRANSIT_SUMMARY_CACHE_FILE` | Durable last-successful snapshot; Docker default `/app/data/transit-summary.json` |
| `PORT` | Public HTTP port, default `3000` |

Mount a persistent volume at `/app/data` in Coolify. A container restart then restores the last successful snapshot even while the collector is unavailable. The private collector must be reachable on the internal network. Remove old MotherDuck credentials from the dashboard deployment. Credentials are not required in browser configuration.

For development, run `npm run dev:server` and `npm run dev` from this directory. Vite proxies `/api` to the local server on port 3000. `npm run build` creates the frontend; `npm start` serves it and the public API. The historical entry filename `server-motherduck.ts` is retained for deployment compatibility, but its implementation only starts the saved-summary server.

The new **Roadway time** and **Signals** views filter 28 days of additive summaries by source, service, route, direction, dates, day type and hour. SSE and Le Pass data are never pooled. ROW comparisons use only same-date matches with equal date weights, at least 30 passages per roadway class and seven common dates. Unreviewed roadway remains unknown. Signal encounters include insufficiently sampled encounters in the denominator, show evaluable coverage separately, require 30 evaluable encounters over seven dates for headline estimates, and withhold priority scenarios for passenger-stop overlaps or entirely unevaluable samples. Recovery percentages are illustrative assumptions, not measured effects.

Overview and OTP remain independent historical views. `buildLegacySummary(query, database)` is a pure helper for the collector's local DuckDB worker; it never opens a connection. Earlier streetcar priority scenarios use a fixed 28-day snapshot because saved quantiles cannot be truthfully re-filtered. The older speed diagnostics retain seven days of site bins to bound payload size. Their available date controls reflect that limit.

The private envelope is documented by `src/summary-data.ts`; the study event contracts live in `../src/transit-study-types.ts`. Public responses expose summary provenance and readable collecting states. Private fetches have a 10-second timeout, no redirects, a 64 MiB payload bound, and atomic snapshot replacement. A malformed, older or failed response keeps the last valid snapshot. Public requests do not trigger upstream fetches.

Verification: `npm run typecheck`, `npm run lint`, `npm test`, `npm run build`. Summary/API tests run entirely offline with synthetic responses and in-process HTTP requests; they need no ports, credentials or live collectors.

The collector's optional cloud archive requires a fresh usage record with `plan: "lite"` **and** `billing_mode: "free"`, within its storage and compute operating limits. Lite free-tier transient databases support zero historical snapshot retention and retain failsafe bytes for one day. Paid Lite requires one day of historical retention, so paid or unknown billing modes are rejected. This is verified against the [MotherDuck storage lifecycle documentation](https://motherduck.com/docs/concepts/storage-lifecycle/). The dashboard itself never enables cloud writes.
