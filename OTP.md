# Independent schedule-based OTP

The dashboard computes its own OTP from the vehicle observations we collect.
RTA's public GTFS supplies the **schedule**, not measured performance. Published
RTA OTP figures are displayed separately for matching routes/months as a benchmark.
No official percentage, calibration multiplier, or `dly` flag enters our OTP.

## Definition

`OTP = 100 * on_time / (early + on_time + late)`

The unit is one scheduled **timepoint event** per service date, GTFS trip, and stop
sequence. Departures are measured except at the final stop, where arrival is used.
Early means before -60 seconds; late means after +300 seconds. Boundaries are inclusive
for on time. The timing window follows the historical RTA definition documented in
[RIDE's 2021 report](https://rideneworleans.org/wp/wp-content/uploads/2021/10/2021-RIDE-State-of-Transit-.pdf).
RTA's current internal event selection, tolerances, and adjustments have **not** been
verified as identical, so the dashboard identifies our result as an independent estimate.

Direct schedule trip IDs and conflict-free observed ID mappings enter reported OTP. Inferred block matches are
stored for diagnostics and **excluded** from the dashboard numerator and denominator.
Percentages are weighted by events, never averages of route/day percentages.
Zero observations produce `Unavailable`, not 0% or 100%.

## The ID bug

In a public-feed sample collected September 7, 2026, 87 of 94 assigned entries had
`tripid` values matching the published GTFS trip IDs and route IDs; **zero** matched
using `tatripid`. The remaining entries included other-agency routes and an invalid
trip ID. The existing collector saved `tatripid` as `trip_id` and discarded `tripid`.

The collector now preserves both:

- `trip_id`: legacy BusTime `tatripid`, retained for compatibility and diagnostics.
- `gtfs_trip_id`: BusTime `tripid`, the verified schedule join key.
- `observed_at`: timestamp with its UTC offset preserved in `TIMESTAMPTZ`.
- `is_delayed`: nullable; an absent flag stays unknown. It is never used for OTP.

Missing historical `gtfs_trip_id` values can be reconstructed from observed
`tatripid`/`tripid` pairs. `otp_trip_mappings` stores each pair under its schedule
SHA-256, route, and evidence dates. It retains contradictory pairs rather than
replacing them. Only a unique target is usable, and the historical route, active
service, and any available block/destination must agree. The event records
`match_method = 'crosswalk'` and `mapping_legacy_id`; raw readings are unchanged.
The dashboard reports how many events were reconstructed. A conflicting pair
causes affected events to be excluded on the next uncached API query.

Historical reconstruction assumes a pair stayed stable within the same GTFS
archive's effective period; evidence collected today alone cannot prove stability
on every older date. The archive hash prevents carrying a mapping into a changed
GTFS schedule. Unmatched IDs remain unmeasured. We never equate a legacy ID with
a GTFS ID merely because their strings coincide.

## Observation and matching rules

1. Use the schedule archive valid for the service day; apply weekday calendars,
   holiday additions/removals, and the agency timezone. GTFS times over 24:00 stay
   on their original service day. DST follows GTFS's local-noon-minus-12-hours rule.
2. Limit service to GTFS bus/streetcar routes. Exclude unassigned, pull-in/pull-out,
   off-route, invalid-coordinate, and missing-trip observations. Frequency-based
   trips are excluded because they do not identify a unique scheduled departure.
3. Deduplicate timestamps within each vehicle/trip run. Exact trip IDs must also
   match the route and a unique active service day, within a generous six-hour
   envelope around the trip. This envelope rejects wrong-day assignments; it is
   not an on-time threshold. More extreme deviations remain unmeasured.
4. Observe a visit within 35 metres of a scheduled stop. A departure is bracketed
   by the last in-radius reading and first out-of-radius reading; a terminal arrival
   uses the last outside and first inside. Require a gap of at most 120 seconds
   and plausible movement (at most 45 m/s). No arrival-time predictions are used.
5. Classify only when the **entire observation bracket** is early, on time, or late.
   Brackets crossing either timing boundary remain uncertain. Duplicate visits,
   geographically indistinguishable stops, non-monotonic visits, and conflicting
   vehicles/trip assignments remain unmeasured.
6. For historical diagnostics only, require a unique active route/block/destination
   match within 90 minutes of its schedule. Never select the nearest trip from
   multiple candidates. These inferred events cannot enter reported OTP.

GPS geofences approximate stop events; the radius introduces spatial uncertainty
beyond the timestamp bracket. Sparse GPS samples miss visits. Missing trips are
not automatically classified as late or on time. Scheduled timepoints with no
classified observation remain in the coverage denominator, not the OTP denominator.
This coverage and the uncertain-event count must accompany every comparison.
The estimate can still have sampling bias and is not proof of service delivery.

## Operation

The scraper starts a separate OTP worker process on startup and hourly thereafter.
This keeps schedule parsing and matching off the raw collector's event loop.
It downloads [RTA's GTFS archive](https://www.norta.com/RTA/media/GTFS/GTFS.zip)
daily, archives the original ZIP by SHA-256 in `otp_schedules`, and recalculates
the current and previous two service days when a valid saved archive is available.
It also learns new observed ID pairs from those days. After a requested backfill,
`otp_backfill_days` tracks the explicit historical dates and mapping revision.
The same hourly worker revisits at most two requested dates per run when mapping
pairs change, prioritizing the least recently calculated dates. No separate
scheduler or deployed backfill service is created.
Results are stored in `otp_events` and `otp_coverage`, atomically replaced per day.
Every row records the schedule hash and method version (`timepoint-departure-v2`).
The dashboard reads `/api/otp`; existing raw APIs call their old flag statistic
`not_flagged_pct` and no longer expose it as `on_time_pct`.

The new columns/tables are additive and created automatically. Deploy both the
scraper and dashboard changes. No additional secret is needed: the worker inherits
the scraper's `MOTHER_DUCK_API_KEY` and `MOTHERDUCK_DATABASE`. `GTFS_URL` optionally
overrides the public schedule download URL. Results update hourly; the dashboard
cache can add another hour. Reload the page to fetch updated results.

New downloads are automatically usable only from their collection date (and not
before the feed's start date). We do not silently apply today's schedule to old
observations. Failed refreshes use previously saved, still-valid archives; failure
does not stop collection. A first run without a valid schedule shows unavailable.
Current-day coverage includes only timepoints due by calculation time and is partial.

For an explicitly verified historical archive:

```bash
npm run otp:backfill -- --gtfs /path/to/archived-GTFS.zip --from 2026-08-01 --to 2026-09-07 --crosswalk src/data/trip-crosswalk-2026-09-07.json
```

The command checks declared feed validity, but the operator must verify that this
archive actually applied on those dates. It uses our stored vehicle observations.
Old observations with a validated observed ID mapping contribute reconstructed OTP; other observations remain unmeasured or block-based diagnostics.
The seed file contains 86 public-feed pairs observed on September 7; it covers only part of the historical service. New observations expand the mapping automatically.
Legacy timezone-naive timestamps are interpreted in the agency timezone; ambiguous
or nonexistent DST wall times are rejected. Backfills are idempotent.

## RTA benchmark and validation

The dashboard includes five published route benchmarks for March and April 2026,
from [RTA's report, pages 26–27](https://norta.legistar.com/View.ashx?GUID=91C6E35C-D2F7-4736-8CFC-3D154B5DFBE4&ID=1365378&M=PA).
They appear only for the matching month and route; missing benchmarks show a dash.
The difference is in percentage points. A partial collection month is not equivalent
to a complete official month; use coverage before interpreting that difference.

Tests: `npm test`, `npm run typecheck`, and in `dashboard`, `npm test`, `npm run typecheck`,
`npm run lint`, `npm run build`. The suite covers timing boundaries, weighting,
duplicates, ambiguity, holidays, midnight, DST, actual SQL aggregation, and
idempotent database writes. Synthetic fixtures test correctness, not RTA agreement.

A short September 7 public-feed smoke test also produced both early and late events
using exact IDs. It is too small to establish a representative route/month OTP or
validate agreement with RTA. Production deployment and a matching reporting-period
comparison remain necessary before claiming numerical equivalence.
