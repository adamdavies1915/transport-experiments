# MotherDuck Migration Guide

## What Changed

**Before (R2 + DuckDB):**
- Scraper wrote parquet files to R2
- Consolidation service merged hourly → daily files
- Dashboard loaded data into RAM, queried locally
- Limited by server RAM (couldn't scale to years of data)

**After (MotherDuck):**
- Scraper writes directly to MotherDuck table
- No consolidation needed (MotherDuck handles optimization)
- Dashboard queries MotherDuck cloud database
- Scales to years of data with no RAM issues

## Setup Steps

### 1. Get MotherDuck Token

1. Sign up at https://motherduck.com (free tier)
2. Go to Settings → API Tokens
3. Create a new token
4. Copy the token (starts with `eyJ...`)

### 2. Update Scraper Environment

**On Coolify (scraper app):**

Remove old env vars:
- `R2_ACCOUNT_ID`
- `R2_ACCESS_KEY_ID`
- `R2_SECRET_ACCESS_KEY`
- `R2_BUCKET`

Add new env var:
- `MOTHERDUCK_TOKEN=<your_token_here>`
- `MOTHERDUCK_DATABASE=nola_transit` (optional, defaults to this)

### 3. Update Dashboard Environment

**On Coolify (dashboard app):**

Remove old env vars:
- All the `VITE_R2_*` vars

Add new env var:
- `MOTHERDUCK_TOKEN=<your_token_here>`
- `MOTHERDUCK_DATABASE=nola_transit` (optional, defaults to this)

### 4. Remove Consolidation Service

The consolidation service is no longer needed. Delete it from Coolify:
- Project: NOLA Transit Scraper
- App: consolidation service
- Action: Delete

### 5. Deploy

Both apps will automatically redeploy when you push changes. Or manually trigger deployment in Coolify.

## Data Migration

Your existing R2 data will remain accessible. If you want to import it into MotherDuck:

```bash
# From your local machine with MotherDuck CLI
duckdb :md:
SET motherduck_token='<your_token>';
USE nola_transit;

-- Import from R2
INSERT INTO transit_data
SELECT * FROM read_parquet('s3://nola-transit/daily/*.parquet');
```

Or just start fresh - the scraper will begin collecting new data immediately.

## Cost Comparison

**R2 + DuckDB:**
- Storage: 10GB free, then $0.015/GB/month
- Operations: Free (within limits)
- RAM: Limited by server

**MotherDuck:**
- Storage: 10GB free, then $0.015/GB/month (same as R2)
- Compute: Included in free tier for up to 50M row-scans/month
- RAM: Unlimited (cloud-managed)

Both cost about the same for storage, but MotherDuck handles scaling automatically.
