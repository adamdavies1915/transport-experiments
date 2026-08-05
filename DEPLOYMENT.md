# Transit Data System Deployment Guide

## Architecture Overview

Two services, both backed by MotherDuck. The dashboard queries the raw
`transit_data` table directly and aggregates on the fly (results cached in
memory for 1 hour), so there is no separate consolidation step or relational
database to operate.

```
┌─────────────┐      ┌──────────────┐      ┌──────────────┐
│   Scraper   │─────▶│  MotherDuck  │◀─────│  Dashboard   │
│  (Raw Data) │      │ transit_data │      │   (React)    │
└─────────────┘      └──────────────┘      └──────────────┘
```

### Components

1. **Scraper** — collects real-time transit data every minute → MotherDuck.
2. **MotherDuck** — cloud DuckDB storing raw transit readings (millions of rows).
3. **Dashboard** — React front end + Express API (`server-motherduck.ts`) that
   queries MotherDuck directly and caches aggregates in memory.

### Why this architecture?

- **MotherDuck** is a columnar analytics engine — the dashboard's `GROUP BY`
  aggregates over millions of rows run fast without pre-computation.
- **In-memory caching** (1 hour TTL) keeps repeated dashboard loads cheap and
  well under MotherDuck's free-tier row-scan limits.
- **No extra moving parts** — no cron job, no Postgres to provision, back up, or
  keep in sync. Raw data stays in one place for ad-hoc analysis.

## Services to Deploy

### 1. Scraper

**Location**: `/` (root)
**Dockerfile**: `Dockerfile` (root)
**Environment Variables**:
```bash
MOTHER_DUCK_API_KEY=your_key
MOTHERDUCK_DATABASE=my_db
UPLOAD_INTERVAL=60000
```

**Coolify Setup**:
- Base directory `/`.
- Set the environment variables above.

### 2. Dashboard

**Location**: `/dashboard`
**Dockerfile**: `dashboard/Dockerfile`
**Environment Variables**:
```bash
MOTHER_DUCK_API_KEY=your_key
MOTHERDUCK_DATABASE=my_db
PORT=3000
```

**Coolify Setup**:
1. Base directory `/dashboard`.
2. Set the environment variables above (same MotherDuck token as the scraper).
3. Deploy.

> **Note:** both images are `node:20-slim` with `ca-certificates` installed and
> `GRPC_DEFAULT_SSL_ROOTS_FILE_PATH` set — required because the MotherDuck
> client talks to the service over gRPC/TLS and `slim` ships no CA bundle.

## Testing

### Test the Dashboard locally

```bash
cd dashboard
npm install
# Terminal 1: API server (reads MotherDuck)
MOTHER_DUCK_API_KEY=your_key npm run dev:server
# Terminal 2: Vite dev server
npm run dev
```

Visit the Vite URL and verify data loads. Or build and run the production
server (serves the built front end + API on one port):

```bash
npm run build
MOTHER_DUCK_API_KEY=your_key npm start   # http://localhost:3000
```

### Test API Endpoints

```bash
curl http://localhost:3000/api/health
curl http://localhost:3000/api/summary
curl http://localhost:3000/api/segment-types
curl http://localhost:3000/api/daily
```

## Monitoring

### Dashboard / MotherDuck usage

- The dashboard caches every aggregate for 1 hour, so a busy day still results
  in only a handful of full-table scans per endpoint.
- Check the MotherDuck dashboard for row-scan and storage usage.

### Scraper

- Watch Coolify logs for `Inserted N records into MotherDuck` and a low error
  count in the periodic `[Stats]` line.

## Troubleshooting

### Dashboard shows no data / 503

1. Verify `MOTHER_DUCK_API_KEY` is set and valid.
2. Check the container logs for `MotherDuck connected - ready to query`.
3. Confirm the table has rows: query `my_db.transit_data` from the MotherDuck UI.
4. Hit `curl http://localhost:3000/api/summary` and read any `error` field.

### "Could not get default pem root certs"

The gRPC/TLS CA bundle is missing. Ensure the image installed `ca-certificates`
and set `GRPC_DEFAULT_SSL_ROOTS_FILE_PATH=/etc/ssl/certs/ca-certificates.crt`
(both Dockerfiles already do this).

### Scraper not uploading to MotherDuck

1. Verify `MOTHER_DUCK_API_KEY` is set.
2. Check scraper logs for insert errors.
3. Test the MotherDuck connection locally.

## Cost Analysis

### MotherDuck (Free Tier)
- Storage: 10GB limit.
- Row scans: 50M/month limit.
- The dashboard's 1-hour cache keeps scan volume low; the scraper only writes.

## Future Improvements

1. **Archive old raw data**: move data older than 1 year to cold storage.
2. **Precise delay metric**: join pings to the static GTFS schedule to compute
   real delay-in-seconds (see `DATA_STRATEGY.md`).
3. **Dashboard caching**: swap the in-memory cache for Redis if horizontally
   scaled.
