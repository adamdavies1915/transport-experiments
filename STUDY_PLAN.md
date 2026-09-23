# Focused Route 12 study and field validation

Adopted September 22, 2026, after inspecting preliminary results. This is a
prospective follow-up plan, not a preregistration of the existing observations.

## Primary comparison

Collect September 23–October 20 inclusive (America/Chicago). Aim for 20 matched
weekdays; missing service dates remain missing. Report any shortfall rather than
quietly extending or changing the target after seeing results.

Keep Route 12, direction 1, weekdays 08:00–11:59 as the primary stratum, with
one mapped signal and two mapped passenger stops per 200 m window. This stratum
was chosen for its existing sample support, not its estimated effect. Keep the
one-signal/one-stop stratum and direction 0 as explicitly secondary comparisons;
do not combine their counts or choose whichever produces the desired sign.

Use the existing reviewed shared-carriageway sections and the Jackson and
Pleasant median sections listed in `src/data/transit-row-reviews.json`. New
sections must have geographic evidence, explicit applicability dates and matching
stop/signal exposure before being eligible. Do not extend a median label across
an intersection, transition or unreviewed stretch merely to increase sample size.

The original publication gate remains 30 passages in each class over seven
common dates. It is a minimum for a descriptive interval, not proof of causality
or sufficient statistical power. Preserve daily weighting, separate source
estimates, timing bounds and service-day bootstrap intervals. Report the number
of classified passages, all completed passages, matched dates and excluded dates
beside the effect. Zero-spanning timing bounds remain unresolved even if a
bootstrap interval excludes zero. A faster shared section is a valid observation.

The public snapshot may omit older whole dates to fit its size limit. Evaluate
the fixed study period from retained daily research results after the database
worker exits, rather than silently shortening it to the current browser window:

```bash
node --import tsx scripts/export-focused-study.ts \
  --data-dir /absolute/path/to/transit-data \
  --from 2026-09-23 --to 2026-10-20 --output /private/route12-followup.json
```

This exports source-specific coverage and comparisons using the same estimator,
with actual available dates and no filling of missing days.

## Geometry and review queue

The GTFS fetched September 22 changed Route 12 shape IDs. The retained study
catalog continues to describe fixed geographic corridors, not every current
service pattern. The audit in `data/feed-audit/2026-09-22-route12-geometry.json`
compares every reviewed section to the newly retained shapes at 10 m spacing.
Five sections fully match within 10 m; direction-1 Pleasant matches 19 of 21
points. This does not establish unchanged stop exposure or traffic restrictions.

Review direction-1 Pleasant first, then Jackson and the two shared-carriageway
sections in the field. Confirm physical track, longitudinal road traffic,
passenger stops, signal approaches, temporary restrictions and the endpoints of
each measured window. The script `scripts/review-route12-geometry.ts` provides
endpoint coordinates and a reproducible check for subsequent schedule changes.
Do not replace historical path-distance offsets with offsets from new shapes.

Candidate expansion is the adjacent 200 m section on each side of the Jackson
and Pleasant anchors, in both directions. These remain **unreviewed**, with no
new observations classified, until current imagery or a field visit verifies
the full section. Choose candidates before inspecting their speeds. Record an
explicit valid-from date; no historical labels are inferred from a current visit.

## Field sampling protocol

Use `fieldwork/observations.csv` for new rows; its empty header is a template,
not a collected dataset. Work from a safe public location with a view of the
approach; do not enter the track or roadway to measure an endpoint.

1. Synchronize the observer clock to an internet-synchronized clock before and
   after each session. Record timezone offsets and estimated clock error. Use
   ISO 8601 timestamps, such as `2026-09-23T09:15:20-05:00`.
2. Fix entry/exit landmarks to the exact study window before observing. Record
   every vehicle in the preselected session, including passes with no wait,
   obstructed views and events that cannot be matched to either feed.
3. Start with three distinct weekdays and two sessions per day (08:00–10:00 and
   16:00–18:00), alternating shared and reserved windows. Aim for at least 30
   complete passages per roadway class. Record session exposure and missing
   observations, not just successful timings. This is a measurement pilot,
   not a powered causal evaluation.
4. Record vehicle ID, route, actual travel direction, entry/exit time and every
   stationary episode. Give each pass one `observation_id` and one row in
   `observations.csv`; put each separate wait in `waits.csv` under that ID.
   A fully observed pass with no wait has `wait_event_count=0` and no wait rows;
   incomplete visibility remains unknown. Multiple waits must not duplicate the
   pass denominator. Separate boarding/alighting, visible red indication,
   traffic queue, mixed and unknown causes. Never infer a red indication from
   the presence of signal hardware alone. Record stops with no red indication.
5. At Rampart/St. Bernard and Rampart/Toulouse, record at least 30 consecutive
   encounters per selected direction over three dates, including no-wait cases.
   These sites were selected after preliminary analysis and are a validation
   pilot; they are not an unbiased ranking of citywide signal delay.
6. Keep original notes immutable. Match separately to SSE and Le Pass by route,
   vehicle, direction, window/site and overlapping time. Mark ambiguous matches;
   do not pick the feed observation closest to the manual duration. Record the
   original event IDs and source timing bounds in a separate reconciliation file.

Report unmatched counts, median signed error, median absolute error and the
fraction of manual durations inside source timing bounds. For waits, report
detected/undetected versus observed wait/no-wait as a confusion table, keeping
insufficient sampling separate. Stratify by source and observed cause. Do not
change detection thresholds on this same sample and then claim independent
validation; reserve a later session for confirmation if adjustments are needed.

No field observations have been collected by this change. An on-site observer
must perform the sessions before this validation or expanded classifications
can be completed.
