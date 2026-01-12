# Transit Data Consolidation Service

This service periodically queries raw transit data from MotherDuck and computes pre-aggregated summaries that are stored in Postgres. This reduces query costs and provides instant dashboard performance.

## What It Does

Runs twice daily (6 AM and 6 PM) to compute:

1. **Daily Summary** - Overall system metrics per day
   - Total records, routes, vehicles
   - Average speed, delay percentages

2. **Daily Route Performance** - Per-route metrics per day
   - Readings, delays, on-time percentages
   - Average speed by route

3. **Daily Segment Performance** - ROW vs Mixed traffic per day
   - Performance comparison by segment type

4. **Hourly Segment Performance** - Time-of-day patterns
   - Aggregated across all days for hourly analysis

5. **Segment Summary** - Individual segment statistics
   - Performance of each named segment

## Running

### Development

```bash
# Install dependencies
npm install

# Run once (test mode)
npm run once

# Run in scheduled mode (production)
npm start
```

### Production (Docker)

```bash
# Build
docker build -t transit-consolidation .

# Run
docker run -d \
  -e MOTHER_DUCK_API_KEY=your_key \
  -e MOTHERDUCK_DATABASE=my_db \
  -e POSTGRES_HOST=postgres \
  -e POSTGRES_PORT=5432 \
  -e POSTGRES_DB=transit \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=your_password \
  transit-consolidation
```

## Environment Variables

See `.env.example` for required variables.

## Schedule

The service runs on a cron schedule: `0 6,18 * * *`
- 6:00 AM daily
- 6:00 PM daily

It also runs once immediately on startup.

## Output

Each run produces output like:

```
[2025-01-10T12:00:00Z] Starting consolidation...
✓ Postgres connected
Data range: 2025-01-01 to 2025-01-10
Total records: 234,567

1. Computing daily summaries...
  ✓ Inserted 10 daily summaries

2. Computing daily route performance...
  ✓ Inserted 150 daily route records

3. Computing daily segment performance...
  ✓ Inserted 20 daily segment records

4. Computing hourly segment performance...
  ✓ Inserted 48 hourly records

5. Computing segment type summaries...
  ✓ 2 segment type summaries computed

6. Computing individual segment summaries...
  ✓ Inserted 24 segment summaries

✓ Consolidation complete at 2025-01-10T12:01:23Z
```

## Database Schema

The service automatically creates Postgres tables on first run using `schema.sql`.

Tables created:
- `daily_summary`
- `daily_route_performance`
- `daily_segment_performance`
- `hourly_segment_performance`
- `segment_summary`

All inserts use `ON CONFLICT ... DO UPDATE` for idempotency (safe to re-run).

## Graceful Shutdown

The service handles `SIGTERM` and `SIGINT` signals to close database connections cleanly:

```bash
# Sends SIGTERM
docker stop <container>

# Or Ctrl+C in terminal (SIGINT)
```

## Monitoring

Check logs for:
- ✓ Success messages
- ✗ Error messages
- Row counts for each aggregation

## Troubleshooting

### "No data to consolidate yet"

This means MotherDuck has 0 records. Verify:
1. Scraper is running and uploading to MotherDuck
2. `MOTHERDUCK_DATABASE` matches scraper's database
3. Table name is `transit_data`

### "Consolidation error: ..."

Check:
1. MotherDuck API key is valid
2. Postgres credentials are correct
3. Network connectivity to both databases

### Long run times

Expected run time scales with data volume:
- 100K records: ~10 seconds
- 1M records: ~1 minute
- 10M records: ~5 minutes

If slower, check database performance and network latency.
