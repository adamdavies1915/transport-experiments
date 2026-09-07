# Streetcar speeds, stops, and traffic signals

The dashboard's streetcar study covers St. Charles (12), Canal (47/48), and
Rampart–Loyola (46). It combines our vehicle observations with GTFS tracks and
passenger stops, OSM signal locations, and a separately recorded Mapillary check.
Route 49 is Riverfront and is outside this study.

## What is measured

The analysis measures complete passages through **fixed, non-overlapping 200 m
windows along each directional track**. Each window has a fixed geographic class:

- Road traffic signal only, with no mapped passenger stop in its exposure area.
- Passenger stop only, with no mapped road traffic signal in its exposure area.
- Both a passenger stop and a road signal; their contributions cannot be separated.
- Neither mapped feature; this is not automatically a free-flow baseline.

Exposure includes mapped features within 40 m of the entire track window, including
features crossed between GPS observations. Signal nodes representing nearby
approaches are clustered; directional stop applicability comes from GTFS stop lists.
The geographic class does not change when a streetcar travels faster or slower.

This matters because a one-minute GPS interval can encounter several intersections
when a car is moving, while a stationary car stays in one category. Simply comparing
speeds of such differently exposed GPS intervals creates a sampling bias. Those
interval classifications are retained only in core diagnostics, not headline metrics.

Positions are projected onto GTFS track geometry; distance follows the track rather
than a straight shortcut across a bend. Entry and exit times are bracketed by the
surrounding reported observations. The dashboard shows **mean travel-time bounds per
completed passage** and, secondarily, a speed estimate using linear interpolation.
Ranges include temporal sampling and the feed's minute timestamp precision, under
the assumption that provider times are truncated to their reported minute. They are
not statistical confidence intervals and do not cover GPS positional error or unknown
provider latency. Exact positions on a boundary still retain the preceding
observation's timing uncertainty. A first observation already on the entry boundary
cannot establish when the car entered and is treated as left-censored.

A window crossed entirely between two observations can have a zero lower duration
bound. Its upper speed is then unresolved; the UI does not manufacture a finite
upper bound. Mean speed is total window distance divided by total estimated travel
time, not an average of instantaneous speed readings. The historical `speed` field
is not used because missing provider speeds were formerly stored as zero.

No signal phases or signal-controller timing records are available. The study shows
travel-time associations, **not seconds caused by a red light**. Boarding, traffic,
track geometry, turning movements, and service patterns can differ between locations.
Use corridor, direction, local-hour, date, and weekday/weekend filters when comparing.
A missing isolated category means the data cannot separate that effect. Completed
passages can underrepresent very long waits or interrupted trips.

## Quality and provenance

The worker rejects coordinate collisions under the same vehicle/timestamp, off-route points,
route/trip changes, gaps over 90 seconds, impossible movement, ambiguous track/direction
matches, and terminal proximity within 60 m. Purely stationary and incomplete tracks
produce no complete passage. GPS interval counts and completed passage counts have
different denominators. Site summaries may overlap when one window contains multiple
sites; do not sum site counts to obtain corridor totals.

Railway signal nodes have unverified functions. They can represent streetcar traffic
controls or switch indications. They remain visible on the map, but affected windows
are excluded from the road-signal comparison pending better identification. Their
exclusion is a coverage limitation, not a claim that they are irrelevant to delays.
Intervals crossing the midnight boundary are omitted to avoid assigning one interval
to two dates. Dates and hours use America/Chicago.

The initial source catalog has eight passenger-service paths and 322 sites. It retains
103 road-signal sites with nearby Mapillary vehicle-light detections; three sites have
additional dated visual reviews. One disputed OSM cluster is locally excluded with
its evidence preserved. Current presence, signal phase, and streetcar applicability
are not established merely by historical imagery. All raw source snapshots, queries,
licenses, image references, and local overrides are described in
[STREETCAR_SIGNAL_SOURCES.md](STREETCAR_SIGNAL_SOURCES.md).

The relay polls every ten seconds but requests minute-resolution vehicle timestamps.
Positions can change while their reported timestamp remains the same; this is not
evidence that the vehicle only updates once a minute. See the relay's
[request and timestamp parser](https://github.com/codefornola/nola-transit-map/blob/main/main.go).
Older stored readings retain more of these timestamp collisions, which the matcher
excludes conservatively. Later collection deduplicates vehicle/timestamp pairs and
cannot recover their lost timing detail. Dates without complete passages show
unavailable; a zero measured speed is never substituted for missing data.

Stored passage bounds bracket entry and exit using the reported timestamps. The API
then widens each passage's duration range by up to 60 seconds in each direction,
clamping the lower total at zero. It applies this conservative padding to aggregated
totals using the passage count. Stored bounds remain unpadded so the adjustment is
applied exactly once. The method metadata records `timestamp_quantization_seconds: 60`.
Interpolated speeds remain approximate; broad ranges can prevent a useful comparison.
Improving future measurements requires verified second-resolution provider timestamps.
Receipt time must not be substituted for GPS observation time. The compatible
[BusTime API guide, page 8](https://www.transitchicago.com/assets/1/6/cta_Bus_Tracker_API_Developer_Guide_and_Documentation_2025-04-21.pdf)
documents `tmres=s`; availability must be verified against the RTA feed before use.

## Operation

The existing scraper starts a separate streetcar worker 90 seconds after startup,
then hourly. It recalculates yesterday and today and resumes at most two requested
historical dates with an outdated network/method version. It has its own connection
and process, so geometry calculations do not block raw ingestion. No additional
Coolify application or credentials are needed for routine calculations.

The worker creates additive derived tables only:

- `streetcar_networks`: public catalog and method metadata.
- `streetcar_bins`: per-date/route/direction/hour/category passage totals and bounds.
- `streetcar_site_bins`: corresponding site summaries.
- `streetcar_quality`: GPS coverage and exclusion counts.
- `streetcar_backfill_days`: explicit requests and completed versions.

Daily replacement is transactional and idempotent; raw observations are unchanged.
Catalog and method versions accompany every row. The current method is
`streetcar-fixed-window-v2`. The internal `intervals` metric in passage tables counts
completed windows; `candidate_intervals` and `accepted_intervals` in quality tables
count GPS pairs.

With MotherDuck environment variables configured:

```sh
npm run streetcars:backfill -- --from 2026-08-01 --to 2026-09-07
```

An optional `--network /path/to/catalog.json` selects another explicitly built catalog.
Refresh OSM and Mapillary using the source guide, inspect disagreements, rebuild the
catalog, and rerun the desired history. Snapshot dates and attribution are visible in
the dashboard; catalog refresh is explicit, not an unreviewed live overwrite.
Mapillary refresh uses `MAPILLARY_ACCESS_TOKEN` or a private credential file. Tokens
and signed image URLs must not be committed. The supplied public client access was
sufficient; no account OAuth callback was needed for the source reads.

The dashboard independently fetches `/api/streetcars`. The endpoint defaults to the
latest seven available dates for St. Charles, supports `corridor`, `from`, and `to`,
and limits one request to 93 days. It serves gzip when accepted and caches results
for one minute. Direction, weekday/weekend, and hour filters run on the returned
passage bins. Only rows matching the selected catalog and current method are served.

## Validation

Core tests cover track geometry, feature crossings, directional stops, conflicting
observations, physical plausibility, fixed-window classification independent of speed,
complete/incomplete and stationary tracks, boundary dwell, left-censoring, and timing
bounds. Database tests exercise actual persistence and API SQL, repeat runs, preserved
raw data, timestamp-precision padding, and date validation. UI tests cover weighted summaries, uncertainty, empty
selections, filters, independent loading, map/source labels, reviewed imagery links,
and unverified railway signals.

```sh
node --import tsx --test src/*.test.ts
npm run typecheck
cd dashboard
node --import tsx --test src/*.test.tsx
npm run typecheck
npm run lint
npm run build
```
