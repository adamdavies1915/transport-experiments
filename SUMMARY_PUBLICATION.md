# Publishing workstation analysis

The collection server can serve the latest workstation analysis without running
the analysis itself or opening a DuckDB database. The workstation writes and
verifies its normal bounded `summary.json`, records that local publication, then
optionally uploads the public JSON. The server atomically replaces one saved
summary. This bridge transfers no raw observations, database files, or credentials
inside the summary.

Set these on the collection server:

```dotenv
TRANSIT_DATA_DIR=/app/data
TRANSIT_SUMMARY_TOKEN=<separate read token, at least 32 characters>
TRANSIT_SUMMARY_PUBLISH_TOKEN=<separate write token, at least 32 characters>
```

`TRANSIT_DATA_DIR` must be persistent. Set the dashboard's summary URL to the
collector's `/internal/summary` endpoint and give the dashboard only the read
token. On the workstation set:

```dotenv
TRANSIT_SUMMARY_PUBLISH_URL=https://your-collector.example/internal/summary
TRANSIT_SUMMARY_PUBLISH_TOKEN=<the collector's write token>
```

Both tokens accept 32–1024 non-whitespace characters and must differ. The upload
URL must use HTTPS, or HTTP on localhost for a local connection or SSH tunnel;
redirects, URL credentials, query strings and fragments are rejected. The worker
uploads after each successful local publication. An upload failure is logged
without secrets and does not stop analysis or remove the local summary. The next
publication retries with its latest results; there is no accumulating upload
queue. Server rejection preserves the last valid publication.

`GET /internal/summary` requires the read bearer token. `POST /internal/summary`
requires the separate write bearer token and JSON, optionally gzip compressed.
GET responses are gzip compressed when the dashboard requests gzip, reducing
repeated network transfer without changing the saved publication.
Authentication occurs before reading the request. Both compressed and decoded
requests have a 60 MiB ceiling, and only one upload is processed at a time. The
shared dashboard parser validates and selects the public envelope; unknown
top-level fields are discarded. Generated timestamps cannot move backwards.
The GET response preserves analysis and source timestamps from the saved
publication; serving a file does not make collection or analysis fresh.

This bridge does not activate daily workstation scheduling or transfer ownership
of the collection feeds. A worker with `PROCESSING_JOB_ID` never uses the bridge:
daily processing continues to publish through its leased coordinator so that a
worker with an expired lease cannot bypass fencing.
