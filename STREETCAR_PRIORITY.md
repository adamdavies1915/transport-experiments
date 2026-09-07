# Signal delays and transit-priority scenarios

The dashboard opens on **Signal priority**, with separate Overview and On-time
performance pages. All three load independently. The priority view covers St.
Charles (12), Canal (47/48), and Rampart–Loyola (46), with one route and direction
selected at a time. It uses our observations for both measurements and benchmarks.

## What we can measure

A vehicle remaining near a light is a candidate signal wait. The collector now keeps
every streetcar entry in every SSE receipt, including stationary repeats and positions
that change within a provider minute. The source polls about every ten seconds but
requests minute-resolution vehicle times. The receipt instant and original provider
time are stored separately. More receipts do not create precise GPS fix timestamps.
See the [source request and parser](https://github.com/codefornola/nola-transit-map/blob/main/main.go).

Candidate waits require at least three positions spanning 20 seconds within a 12 m
cluster, all reporting zero speed, with received frames no more than 25 seconds apart. The provider time must
be present, carry a timezone, and be no more than 120 seconds old. Movement of at
least 25 m must bracket both ends within 45 seconds. Route/trip changes, off-route
points, invalid clocks, terminals, and unverified rail controls break or exclude
observations. Cluster diameter prevents a chain of moving points from being treated
as one stationary period. Positive and missing speeds exclude an episode. Only the
new receipt ledger preserves missing speed as null; older speed values cannot support
this check because missing source speeds were formerly stored as zero. A live audit
found unchanged coordinates paired with positive speeds, so the zero-speed
corroboration prevents those cached-position pauses from being counted as waits.

Each episode counts once, at its nearest mapped road signal or passenger stop within
40 m. If both are nearby, it remains a mixed case. The duration is the first-to-last
receipt span of stable positions; cached or smoothed provider positions can still
look stationary. This is an inferred wait near a feature, not confirmation of a red
phase. The summaries show recorded vehicle-minutes and mean time per detected wait,
not delay per trip. Their corridor-wide counts do not follow the selected direction.
Date/day/hour filters apply to events; snapshot coverage describes complete dates.

Historical rows lack the original receipt order and sub-minute timestamps, so these
new stationary episodes cannot be reconstructed reliably for the old history. They
start with retained snapshots; no fabricated receipt times are backfilled.

## Estimating the opportunity

The historical model preserves each complete passage through a fixed, non-overlapping
200 m track window, including its path, contiguous run, time, feature context, and
nominal timing bounds. It compares each window with its own faster passages in the
same direction, weekday/weekend group, and four-hour band. A stratum needs at least
20 passages on three different dates; sparse strata are omitted. The default display
uses the latest 28 dates so weekend groups can accumulate enough days.

The benchmark is the 20th percentile of passage time. Extra time is the nonnegative
difference between the mean and that benchmark, weighted by the number of eligible
passages in each time stratum. The API also returns 10th/30th-percentile sensitivity
values. This comparison retains typical running and stop time at the same location,
but variation in boarding, traffic, driving and GPS timing can still contribute to
extra time. The benchmark does not identify green phases or a causal signal effect.

Each route profile adds one traversal of every eligible window. Shared site markers
never add a window twice. The result covers the displayed fraction of one directional
route; missing windows, terminals and rail controls are not extrapolated. It is not
an observed complete end-to-end trip. Different windows may have different eligible
time bands; totals summarize those eligible samples, not every selected hour.
Directions and Canal branches remain separate.

Two explicit scenario inputs determine potential savings:

```
savings = extra time near selected signals
        × assumed share caused by signals
        × assumed share of that delay removed by priority
```

The initial 50%/50% settings are illustrative assumptions, not fitted parameters or
predicted effectiveness. Mixed stop-and-signal windows can be excluded. Changing
these assumptions does not change the measured passage times. Actual attributable
signal delay and the effect of deployed priority need signal phases, boarding evidence,
a controlled intervention, or another validated source of identification.

Transit priority can prolong an existing green or bring green forward, subject to
coordination and timing constraints; requests can be refused. A 100% recovery setting
is a hypothetical limit, not normal priority operation.
[FHWA priority mechanics](https://ops.fhwa.dot.gov/publications/fhwahop08024/chapter9.htm)
and [evaluation data needs](https://ops.fhwa.dot.gov/publications/fhwahop16037/ch3.htm)
explain why vehicle travel observations and signal-operation records are distinct
inputs. No published citywide percentage is used to calibrate these NOLA scenarios.

## Storage and operation

`streetcar_snapshots` is an additive receipt ledger. Stable snapshot IDs make database
retries idempotent; separate receipts remain separate even when positions repeat.
A bounded retry buffer defaults to 100,000 snapshots (`MAX_SNAPSHOT_BUFFER_RECORDS`).
Raw/OTP collection retains its existing provider-minute watermark. Both ingestion
buffers serialize writes and drain on shutdown.

The existing hourly worker maintains `streetcar_passages`, `streetcar_priority_days`,
`streetcar_waits`, and `streetcar_wait_quality`. It processes recent dates after startup
and resumes requested history missing the current passage version. No extra Coolify
application is created. Passage history is refreshed with:

```sh
npm run streetcars:backfill -- --from 2026-08-01 --to 2026-09-07
```

`/api/streetcar-priority` supports corridor, from/to, day_type, hour_from, and hour_to;
requests are capped at 93 dates and cached for one minute. The API sends compact
window summaries, not raw trajectory records. Stationary episodes are recalculated
for yesterday and today on hourly runs. Their source coverage is displayed separately
from historical passage coverage.

Tests cover same-location baselines, sparse contexts, branch/direction separation,
non-overlapping totals, scenario assumptions, receipt preservation, retry idempotence,
stationarity and stale-data rejection, persistence, API SQL, and independent page loads.
The underlying passage matching and source provenance remain documented in
[STREETCAR_ANALYSIS.md](STREETCAR_ANALYSIS.md) and
[STREETCAR_SIGNAL_SOURCES.md](STREETCAR_SIGNAL_SOURCES.md).
