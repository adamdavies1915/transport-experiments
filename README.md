# NOLA Transit Scraper

Independent third-party collection of New Orleans RTA real-time transit data. Captures vehicle positions via SSE and stores as Parquet files in Cloudflare R2 for long-term analysis.

## Why?

To analyze streetcar delays in mixed traffic vs dedicated right-of-way segments, and provide independent data to verify or challenge RTA performance claims.

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│ nolatransit.fly │────▶│    Scraper      │────▶│  Cloudflare R2  │
│   (SSE feed)    │     │  (this app)     │     │ (parquet files) │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                                                        │
                                                        ▼
                                                ┌─────────────────┐
                                                │    DuckDB       │
                                                │ (query anywhere)│
                                                └─────────────────┘
```

## Data Collected

Each vehicle position includes:
- `vid` - Vehicle ID
- `timestamp` - Reading time
- `lat`, `lon` - GPS coordinates
- `route` - Route number (e.g., "12" for St. Charles streetcar)
- `speed` - Current speed
- `is_delayed` - RTA delay flag
- `segment_id`, `segment_name`, `segment_type` - Geographic classification

### Streetcar Segments (Route 12)

| Segment | Type |
|---------|------|
| Canal Street (CBD) | Mixed Traffic |
| Lee Circle / Downtown | Mixed Traffic |
| St. Charles - Lower Garden District | Dedicated ROW |
| St. Charles - Garden District | Dedicated ROW |
| St. Charles - Uptown | Dedicated ROW |
| Carrollton - Riverbend | Dedicated ROW |
| S. Carrollton Ave | Dedicated ROW |

## Setup

### Prerequisites

- Node.js 18+
- Cloudflare account with R2 enabled
- Docker (for deployment)

### 1. Create R2 Bucket

1. Go to [Cloudflare Dashboard](https://dash.cloudflare.com) → R2
2. Create a bucket named `nola-transit`
3. Create an API token with "Object Read & Write" permission

### 2. Configure Environment

Copy `.env.example` to `.env` and fill in:

```bash
R2_ACCOUNT_ID=your_cloudflare_account_id
R2_ACCESS_KEY_ID=your_r2_access_key
R2_SECRET_ACCESS_KEY=your_r2_secret_key
R2_BUCKET=nola-transit
```

### 3. Run Locally

```bash
npm install
npm start
```

### 4. Deploy with Docker

```bash
docker compose up -d
```

## Querying Data

Query the Parquet files directly from R2 using DuckDB:

```bash
npm run query
```

Or use DuckDB directly:

```sql
-- Mixed traffic vs dedicated ROW comparison
SELECT
  segment_type,
  COUNT(*) as readings,
  ROUND(100.0 * SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) / COUNT(*), 2) as delay_pct,
  ROUND(AVG(speed), 1) as avg_speed
FROM read_parquet('s3://nola-transit/**/*.parquet')
WHERE route = '12' AND segment_type IS NOT NULL
GROUP BY segment_type;
```

## Storage Costs

| Timeframe | Data Size | Cost |
|-----------|-----------|------|
| Year 1 | ~10 GB | Free |
| Year 2+ | +10 GB/year | ~$2/year |
| 5 years | ~50 GB | ~$8/year |

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `SSE_URL` | nolatransit.fly.dev/sse | Transit data source |
| `UPLOAD_INTERVAL` | 3600000 (1 hour) | How often to upload to R2 |
| `R2_BUCKET` | nola-transit | R2 bucket name |

## License

MIT
