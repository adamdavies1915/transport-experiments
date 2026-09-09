# NOLA Transit Observations

Independent observations of New Orleans RTA buses and streetcars, using the RTA
relay SSE feed and Le Pass. The dashboard studies roadway travel time and
candidate signal waits, alongside independently calculated schedule-based OTP.

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

Use Node.js 20 or later. Copy `.env.example` to `.env`, set a random server-only
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
