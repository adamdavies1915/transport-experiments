# Transit Data System Deployment Guide

## Architecture Overview

The system now uses a hybrid architecture to handle year-long data efficiently:

```
┌─────────────┐      ┌──────────────┐      ┌──────────────┐
│   Scraper   │─────▶│  MotherDuck  │─────▶│Consolidation │
│  (Raw Data) │      │  (Raw Data)  │      │   Service    │
└─────────────┘      └──────────────┘      └──────┬───────┘
                                                   │
                                                   ▼
                     ┌──────────────┐      ┌──────────────┐
                     │  Dashboard   │◀─────│   Postgres   │
                     │   (React)    │      │ (Aggregates) │
                     └──────────────┘      └──────────────┘
```

### Components:

1. **Scraper** - Collects real-time transit data every minute → MotherDuck
2. **MotherDuck** - Cloud DuckDB storing raw transit readings (millions of rows)
3. **Consolidation Service** - Runs twice daily (6 AM, 6 PM) to compute aggregates
4. **Postgres** - Stores pre-computed daily/hourly/route aggregates
5. **Dashboard** - Reads from Postgres for instant queries

### Why This Architecture?

- **MotherDuck**: Free tier has 50M row-scans/month limit
- **Problem**: Dashboard querying year of data = expensive compute
- **Solution**: Pre-compute aggregates in Postgres, dashboard reads aggregates (instant, no compute)
- **Benefit**: Raw data available in MotherDuck for ad-hoc analysis when needed

## Services to Deploy

### 1. Scraper (Existing)

**Location**: `/src`
**Dockerfile**: `Dockerfile` (root)
**Environment Variables**:
```bash
MOTHER_DUCK_API_KEY=your_key
MOTHERDUCK_DATABASE=my_db
UPLOAD_INTERVAL=60000
```

**Coolify Setup**:
- Uses existing scraper service
- Update environment variables to use MotherDuck

### 2. Consolidation Service (New)

**Location**: `/consolidation`
**Dockerfile**: `consolidation/Dockerfile`
**Schedule**: Runs twice daily (6 AM, 6 PM)
**Environment Variables**:
```bash
# MotherDuck
MOTHER_DUCK_API_KEY=your_key
MOTHERDUCK_DATABASE=my_db

# Postgres
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=transit
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password
POSTGRES_SSL=false
```

**Coolify Setup**:
1. Create new service: "Transit Consolidation"
2. Point to GitHub repo: `consolidation/` directory
3. Add all environment variables above
4. Link to Postgres database (create if doesn't exist)
5. Deploy

**Test Run**:
```bash
# Run once to test
npm run once

# Or run in scheduled mode
npm start
```

### 3. Dashboard (Updated)

**Location**: `/dashboard`
**Dockerfile**: `dashboard/Dockerfile`
**Environment Variables**:
```bash
# Postgres (reads from here)
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=transit
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password
POSTGRES_SSL=false
```

**Coolify Setup**:
1. Update existing dashboard service
2. Replace environment variables (remove MotherDuck, add Postgres)
3. Redeploy

### 4. Postgres Database (New)

**Coolify Setup**:
1. Add new Postgres database in Coolify
2. Name: `transit-postgres`
3. Database: `transit`
4. Note the connection details for other services

**Schema**: Automatically created by consolidation service on first run

## Deployment Steps

### Step 1: Create Postgres Database

1. In Coolify, go to "Databases"
2. Click "Add New Database" → PostgreSQL
3. Configure:
   - Name: `transit-postgres`
   - Database: `transit`
   - Username: `postgres`
   - Password: (generate secure password)
4. Deploy and note connection details

### Step 2: Deploy Consolidation Service

1. In Coolify, go to "Services"
2. Click "Add New Service"
3. Configure:
   - Name: `transit-consolidation`
   - Repository: Your GitHub repo
   - Build Pack: Dockerfile
   - Dockerfile Location: `consolidation/Dockerfile`
4. Add environment variables (see above)
5. Link to Postgres database
6. Deploy

### Step 3: Update Dashboard

1. In Coolify, find existing dashboard service
2. Update environment variables:
   - Remove: `MOTHER_DUCK_API_KEY`, `MOTHERDUCK_DATABASE`
   - Add: Postgres connection variables
3. Redeploy

### Step 4: Verify Scraper

1. Ensure scraper has MotherDuck environment variables
2. Redeploy if needed

## Testing

### Test Consolidation Service

```bash
cd consolidation
npm install
node index.js --once
```

Expected output:
```
Transit Data Consolidation Service
===================================
MotherDuck connected
✓ Postgres schema initialized

Running consolidation once...

[timestamp] Starting consolidation...
✓ Postgres connected
Data range: 2025-01-01 to 2025-01-10
Total records: 234,567

1. Computing daily summaries...
  ✓ Inserted 10 daily summaries

2. Computing daily route performance...
  ✓ Inserted 150 daily route records

... (more output)

✓ Consolidation complete
```

### Test Dashboard

```bash
cd dashboard
npm install
npm run dev:postgres
```

Visit `http://localhost:3000` and verify data loads.

### Test API Endpoints

```bash
# Health check
curl http://localhost:3000/api/health

# Summary
curl http://localhost:3000/api/summary

# Daily data
curl http://localhost:3000/api/daily
```

## Monitoring

### Consolidation Service Logs

Check Coolify logs for consolidation service:
- Should run twice daily (6 AM, 6 PM)
- Look for "✓ Consolidation complete" messages
- Watch for any errors

### Database Size

Monitor Postgres database size:
```sql
SELECT pg_size_pretty(pg_database_size('transit'));
```

Expected size: ~100MB per year of data with current aggregation level

### MotherDuck Usage

Check MotherDuck dashboard for:
- Row scan usage (should be low after consolidation)
- Storage usage (raw data)

## Troubleshooting

### Consolidation Service Not Running

1. Check environment variables are set correctly
2. Verify Postgres connection: `psql -h $POSTGRES_HOST -U $POSTGRES_USER -d $POSTGRES_DB`
3. Verify MotherDuck connection: Check API key is valid
4. Check logs for specific errors

### Dashboard Shows No Data

1. Verify consolidation service has run at least once
2. Check Postgres has data: `SELECT COUNT(*) FROM daily_summary;`
3. Check dashboard environment variables point to correct Postgres instance
4. Check API endpoints return data: `curl http://localhost:3000/api/summary`

### Scraper Not Uploading to MotherDuck

1. Verify `MOTHER_DUCK_API_KEY` is set
2. Check scraper logs for errors
3. Test MotherDuck connection locally

## Maintenance

### Manual Consolidation Run

If you need to run consolidation outside the schedule:

```bash
# SSH into Coolify host or use Coolify terminal
docker exec -it <consolidation-container> npm run once
```

### Backfill Historical Data

If you need to recompute aggregates:

1. Truncate Postgres tables (keeps schema):
```sql
TRUNCATE TABLE daily_summary, daily_route_performance, daily_segment_performance,
  hourly_segment_performance, segment_summary CASCADE;
```

2. Run consolidation:
```bash
npm run once
```

### Database Backup

Postgres backup:
```bash
pg_dump -h $POSTGRES_HOST -U $POSTGRES_USER $POSTGRES_DB > backup.sql
```

## Cost Analysis

### MotherDuck (Free Tier)
- Storage: 10GB limit
- Row scans: 50M/month limit
- Current usage: ~5M row scans/month with consolidation (90% reduction)
- Estimated run time: 1+ year before hitting limits

### Postgres
- Depends on hosting provider
- Estimated storage: ~1GB per year of aggregates
- Query cost: Minimal (reading pre-computed data)

## Future Improvements

1. **Archive old raw data**: Move data older than 1 year to cold storage
2. **Add more aggregate tables**: Hourly route performance, vehicle-level stats
3. **Optimize queries**: Add indexes based on usage patterns
4. **Alerts**: Set up monitoring for consolidation failures
5. **Dashboard caching**: Add Redis for frequently accessed data
