# Data Strategy: Measuring What Delays New Orleans Transit

Analysis of the current pipeline, the NORTA data source landscape, and the plan to
turn raw vehicle pings into **measured schedule adherence** — without needing an API key.

Last updated: 2026-07-19

---

## 1. Current pipeline

```
nolatransit.fly.dev/sse  →  scraper (SSE, RAM buffer, periodic flush)
   →  MotherDuck  my_db.transit_data  (raw vehicle pings)
   →  consolidation cron (6am/6pm): DuckDB aggregates  →  Postgres tables
   →  React dashboard
```

The design is sound. The gaps below are about **data quality and the delay metric**, not the plumbing.

---

## 2. Core problem: the delay signal is too weak to answer the question

The whole analysis rests on `is_delayed`, which is just the Clever Devices `dly` boolean
re-broadcast by the fly.dev app. That flag means roughly "vehicle isn't moving as expected."
It is **not** schedule adherence, and you cannot attribute *causes* of delay from a vendor boolean.

### Stacked statistical bias

A stuck vehicle emits many position pings while stationary. The current metric is:

```
delay_pct = delayed_pings / total_pings
```

So a bus delayed for 10 minutes contributes ~10x the rows of one sailing through — delayed
vehicles are **oversampled by construction**, and `delay_pct` is biased upward. `readings`
counts also aren't comparable across segments. The fix is a better signal (Section 6), not
more math around the same boolean.

---

## 3. Data source landscape (NORTA)

| Source | Format | Key needed? | Verdict |
|--------|--------|-------------|---------|
| `nolatransit.fly.dev/sse` (what we use) | JSON over SSE | No | Clever Devices `getvehicles` data, relayed. Good, but a middleman. |
| `bustime.norta.com` (direct) | Clever Devices BusTime API (JSON/XML) | **Yes** — email NORTA, free, ~10k req/day cap | The firsthand source. Adds `getpredictions`. |
| `gpsinfo.norta.com` (legacy) | Custom XML, deg-min coords | Yes — signed license agreement | Strictly worse. Skip. |
| `GTFS.zip` (static schedule) | Zipped CSVs | **No** | Free. The missing half — see Section 5. |

**Key finding:** the SSE feed's fields (`vid`, `tmstmp`, `dly`, `tatripid`, `des`, `rt`, `hdg`,
`spd`, `pdist`, `pid`) are exactly the Clever Devices BusTime `getvehicles` schema. So the data
we already collect is firsthand-quality — fly.dev just polls BusTime with its own key and
fans it out over SSE (which is why the middleman exists: one key, many subscribers).

**The 10k/day cap** on a direct BusTime key ≈ one call every 8.6s, which is why going direct
is a reliability upgrade, not a data upgrade. One `getvehicles` call (no route filter) returns
all vehicles, so polling every 10–15s covers the system under the cap.

---

## 4. SSE field inventory

Full field list emitted by the live feed:

```json
{"vid","tmstmp","srvtmstmp","lat","lon","hdg","rt","tatripid","tablockid",
 "zone","oid","rid","des","pdist","pid","spd","blk","tripid","dly","or"}
```

### Currently captured (11)
`vid`, `tmstmp`→timestamp, `lat`, `lon`, `hdg`→heading, `rt`→route, `tatripid`→trip_id,
`des`→destination, `spd`→speed, `dly`→is_delayed, `or`→is_off_route (+ derived segment fields).

### Worth adding (free, strictly better)

| Field | Why it matters |
|-------|----------------|
| `pdist` | **Distance traveled along the route pattern (feet).** Linearizes position — travel time between two `pdist` values = real segment run-time. Better than lat/lon geofencing. |
| `pid`   | **Pattern ID** — which path variant the vehicle runs. Join key to GTFS `shapes`. |
| `rid`   | GTFS `route_id` — clean join key to GTFS static. |
| `tablockid` | Scheduled block assignment — links vehicle to its full-day GTFS trip chain. |
| `srvtmstmp` | Server timestamp vs. vehicle timestamp — measure feed staleness, drop stale pings. |
| `blk` / `tripid` | Numeric block/trip ids; marginal (already have `tatripid`). |

`zone`, `oid` — empty/near-useless. Skip.

### Ceiling of this feed
Reading more fields does **not** yield delay-in-seconds. This is a `getvehicles` snapshot; the
strongest delay signal in it is the `dly` boolean. Predicted-vs-scheduled times live in BusTime
`getpredictions`, which fly.dev does not relay.

### ⚠️ Data-quality red flag (observed 2026-07-19 ~17:54)
Every vehicle in the live sample reported `"rt":"U"`, `"tatripid":"N/A"`, `"pid":-1`,
`"dly":false`. Route "U" = **unassigned / not in scheduled service** (vehicles broadcast GPS as
soon as cameras power on, before/after runs). Trip assignment only populates during active
service. This matters: the consolidation queries filter `WHERE route = '12'` and
`segment_type IS NOT NULL`, so **every unassigned ping is silently dropped**. Re-sample at peak
service (7–9am) to confirm the join keys populate then, and audit how much data is "U".

---

## 5. What GTFS.zip is (the missing half)

**GTFS** = General Transit Feed Specification — the standard format agencies publish schedules in.
NORTA's is a free, public zip at `norta.com/RTA/media/GTFS/GTFS.zip` (no key). Inside are CSV tables:

| File | Contents |
|------|----------|
| `routes.txt` | Every route — `route_id`, short name ("12"), long name, type |
| `trips.txt` | Every scheduled trip — `trip_id`, route, direction, `shape_id`, `service_id` |
| `stops.txt` | Every stop — `stop_id`, name, lat/lon |
| `stop_times.txt` | **The schedule itself** — scheduled arrival time at each stop, per trip, in order |
| `shapes.txt` | Geographic path (lat/lon points) each trip follows |
| `calendar.txt` | Which days each `service_id` runs |

The SSE feed says where a vehicle **is**; GTFS says where it was **supposed to be**. Delay is
the difference — and needs both. It's "static" because it changes only a few times a year:
download once, refresh occasionally.

---

## 6. The plan: reconstruct real delay from SSE + GTFS (no key)

The join keys are already in the ping: `tatripid` → `trips.txt`.`trip_id`, `rid` → `route_id`,
`pid` → pattern/`shape_id`.

```
ping.tatripid  →  trips.txt (trip_id)  →  stop_times.txt (scheduled arrivals for that trip)
                                       →  compare to observed position/time
                                       →  deviation in seconds = REAL schedule adherence
```

With `pdist` (distance along pattern) we can place the vehicle between scheduled stops and
interpolate the scheduled time it "should" be at that point — turning every ping into a measured
delay, not a boolean.

### Steps
1. **Capture the new fields.** Update `processVehicle` (`src/index.js`) and the MotherDuck schema
   (`src/motherduck.js`) to store `pdist`, `pid`, `rid`, `tablockid`, `srvtmstmp`.
2. **Ingest GTFS static.** Download `GTFS.zip`, load `routes/trips/stops/stop_times/shapes` into
   MotherDuck (or a DuckDB file). Version it (agencies republish on schedule changes).
3. **Join for delay.** Match pings to scheduled trips via `tatripid`; use `pdist`/`pid` to locate
   position along the pattern; compute deviation vs. `stop_times`.
4. **Re-base the metrics.** Replace `dly`-based `delay_pct` with measured seconds-of-deviation,
   aggregated per route / segment / hour. Fixes the oversampling bias in Section 2.
5. **Audit the "U" problem.** Quantify how many pings lack a trip assignment; decide how to handle.

### Value: ~90% of the direct-API benefit, zero API key.

---

## 7. Later / optional

- **Direct BusTime key** (email NORTA, free): removes the fly.dev single point of failure, and
  unlocks `getpredictions` for predicted-vs-scheduled on high-value routes (budget the 10k/day cap;
  reserve predictions for the St. Charles 12).
- **Ask NORTA if they expose GTFS-RT TripUpdates** — some Clever Devices deployments have a
  GTFS-RT generator. If yes, that's delay-in-seconds for free with no per-route budgeting — the
  ideal source.
- **Causal covariates** for "what delays transit": weather (NOAA KMSY), events (Mardi Gras
  parades, festivals, Saints games — likely the single largest NOLA delay driver), time-of-day,
  day-of-week, holidays.

---

## Sources
- bhelx/norta-data — realtime API docs: https://github.com/bhelx/norta-data/blob/master/docs/norta_realtime_api.md
- NORTA BusTime portal: https://bustime.norta.com/bustime/login.jsp
- Clever Devices BusTime Developer API Guide (v3): https://ride.smtd.org/bustime/apidoc/docs/DeveloperAPIGuide3_0.pdf
- Ride New Orleans GTFS open data: https://rideneworleans.org/opendata/gtfs/
- Transitland — NORTA feed: https://www.transit.land/feeds/f-9vrf-neworleansrta/
- GTFS spec: https://gtfs.org/
