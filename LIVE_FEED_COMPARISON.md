# Comparing Le Pass, Google Maps and our vehicle feed

Investigation completed 2026-09-07 America/Chicago. The official Le Pass app was
installed from Google Play, its HTTPS transit responses were inspected, and a
vehicle was matched against our SSE feed and RTA's public predictions. The app
separates arrival estimates, timetable entries and vehicle sample times. Matching
vehicle positions therefore need not produce identical arrival timelines.

This sample establishes how to compare the sources. It does not establish the
cause of every discrepancy reported during the user's trip.

## What has been verified

Our dashboard measures historical performance. Neither it nor the Fly map
calculates live stop-arrival predictions. The Fly map displays vehicle positions
from its SSE relay; an arrival countdown in another app is a separate prediction.

The official Android app is [Le Pass, `com.norta.lepass`](https://play.google.com/store/apps/details?id=com.norta.lepass).
RTA identifies Moovit as the provider of its planning, arrival and location
features, and Token Transit as its fare-payment provider in its
[app terms](https://www.norta.com/app-terms-and-conditions).
Moovit publishes [transit API documentation](https://api-docs.moovit.com/api-docs/5.1/MoovitPublicTransitAPIs.html),
but those APIs require issued credentials and request authentication. They are
not anonymous public feeds.

The official Play Store installation is version **5.197.0.1799**, requiring
Android API 32 or later and targeting API 36. The UI was inspected on the user's
API 36 emulator. The same unmodified APKs were then run in an isolated API 32
emulator with a locally trusted inspection certificate. The proxy successfully
read these actual requests and responses:

| Method | Observed endpoint | Purpose |
| --- | --- | --- |
| POST | `https://app5.moovitapp.com/services-app/services/V4/StopsArrivals` | Stop arrival board |
| POST | `https://app5.moovitapp.com/services-app/services/V4/LineArrivals` | Line arrivals and available vehicle locations |

Responses used unframed **Apache Thrift binary**, not JSON, and requested
**20-second polling** through `nextPollingIntervalSecs`. The running app sent
`API_KEY`, `USER_KEY` and `CLIENT_VERSION` headers. Header values were not saved
in the evidence, and captured credentials were not replayed outside the app.
The anonymous-access check below returned HTTP 401 for `StopsArrivals`.

The APK also contains `V4/StopMapItems`, but that path was not observed in this
capture. Configuration and static requests used `app4cdn.moovitapp.com` and
`static.moovitapp.com`. See the
[sanitized runtime metadata](data/feed-audit/2026-09-07-lepass-api-metadata.json).

## Can we call the Le Pass API without a token?

At **22:02 and 22:07 CDT on September 7**, two credential-free POST requests to
the observed `StopsArrivals` endpoint returned **HTTP 401 Unauthorized**, with
empty response bodies. The first used an empty Thrift struct. The second used
a 33-byte body constructed and round-trip checked against the installed app's
request model: stop `4539336` and all default arrival-configuration fields,
with optional time fields omitted. Neither supplied an app key, user key,
authorization header or cookies. This verifies rejection of these anonymous
requests; it does not isolate which credential is required or establish the
behavior of every Moovit endpoint.

Interactive Le Pass sign-in was not required for the app to obtain transit data.
During normal first launch, the app called `UserAuth/CreateUser` with an
`API_KEY` but no `USER_KEY`, receiving HTTP 200. Later arrival requests included
both headers. That sequence supports automatic guest credential setup. It
does not establish that `USER_KEY` is a short-lived login session token. The
credential-free check did not call `CreateUser` or reuse captured credentials.

The public RTA stop-prediction endpoint returned **HTTP 200 with stop and
prediction XML without credentials** during the same check. This late-evening
response contained a cancelled scheduled departure; earlier comparison samples
below include live countdowns. RTA's website endpoints can therefore support
our comparison without a Le Pass session. Moovit's separately documented
partner API requires issued credentials and HMAC request authentication.
See the [access-check evidence](data/feed-audit/2026-09-07-public-api-access.json)
and [official API documentation](https://api-docs.moovit.com/api-docs/5.1/MoovitPublicTransitAPIs.html).

### Credential lifetime

A fresh guest-creation response captured at **22:25:53 CDT on September 7**
provided explicit issue and expiry timestamps:

| Credential | Issued lifetime | Declared expiry, UTC |
| --- | --- | --- |
| Access token | **24 hours** (86,400 seconds) | 2026-09-09 03:25:53.443 |
| Refresh token | **About 100 years** (36,524.25 days) | 2126-09-08 09:25:53.443 |
| `USER_KEY` | **Still unknown** | No expiry field established |

The access and refresh tokens are separate from `MVCreateUser.userKey`, nested
under `authenticationInfo.validationTokens`. Each supplies `issueTime` and
`expirationTime`. Their common issue time was **2026-09-08 03:25:53.443 UTC**,
within the server's HTTP Date second and 158 milliseconds before receipt,
confirming Unix milliseconds for this response. The refresh duration equals
100 average Gregorian years; it is a nominal server-issued expiry, not a
guarantee against earlier invalidation. This one guest response does not prove
that every account or future issuance receives the same lifetimes.

The earlier 19 successful arrival requests carried `USER_KEY` without
`Access-Token`. The 24-hour access-token expiry therefore does not establish
that arrival access or the user key expires every 24 hours.

Only issue/expiry numbers and response timing/status were retained from this
fresh capture. The unmodified official app ran in a disposable guest emulator;
no token strings or user identifiers were saved in the capture. See the
[timestamp evidence and calculations](data/feed-audit/2026-09-07-lepass-token-lifetime.json).

### How token refresh works

Static inspection of the same official app version identifies automatic renewal
through **`POST https://app4.moovitapp.com/services-app/services/UserAuth/RefreshTokens`**.
The request is a Thrift structure containing the stored refresh-token string
in field 1. The response contains `validationTokens`, with both access and
refresh tokens and their issue/expiry timestamps. The app saves the complete
returned pair before reporting refresh success. It must therefore retain the
returned refresh token, even though this static trace does not prove that the
server changes its value on every renewal.

Two request implementations use this mechanism:

- **Legacy device-token manager:** when a request needs a token, it checks
  `now < expiry - (expiry - issue) / 10`. Once 90% of its lifetime has elapsed,
  it normally refreshes under a shared lock, after checking the cache again.
  For our 24-hour sample, that means **21 hours 36 minutes after issue**, leaving
  2 hours 24 minutes before expiry. This is a check on token acquisition, not
  evidence of a background timer. A cache of the same token also checks elapsed
  time since refresh, protecting against wall-clock changes.
- **Newer Ktor client:** handles HTTP 401, selects its device-token auth
  provider, obtains a refreshed pair and retries the request with the new
  `Access-Token`. Concurrent refresh attempts share one operation; token-version
  tracking lets another request reuse an already refreshed token. A provider is
  removed from the current attempt set, and retried requests carry an internal
  marker to prevent repeated authentication retry loops.

The legacy code also recognizes HTTP 401 with
`WWW-Authenticate: CLIENT_DEVICE_ACCESS_TOKEN_INVALID` and invokes refresh when
device authentication is configured. That handler propagates the original
request error; it does not immediately replay the failed request at that point.
Account-access and account-refresh errors take separate branches.

The refresh request sets an internal attribute that skips initial `Access-Token`
attachment, allowing renewal without depending on that short-lived token. This
does not establish that app/API/user headers are unnecessary. It also disables
request-body logging. Missing refresh credentials or caught refresh/storage
failures produce no replacement pair in the repository path.

This mechanism updates the access/refresh pair, with **no `USER_KEY` update in
the inspected path**. It is designed to renew a guest session automatically
without another interactive sign-in. The 24-hour lifetime therefore does not
mean the app needs a daily login.

The endpoint, request handling, timing rule and storage behavior are verified
from app code. **A live refresh exchange and actual token rotation have not yet
been observed.** See the
[refresh-mechanism evidence](data/feed-audit/2026-09-07-lepass-token-refresh.json).

## What the app response actually contains

| Field | Meaning established by the app's model and captured values |
| --- | --- |
| `staticEtdUTC` | Timetable departure, UTC Unix milliseconds |
| `rtEtdUTC` | Live departure estimate, UTC Unix milliseconds; can equal the timetable |
| `statisticalEtdUTC` | Separate statistical-estimate field in the model; absent in this sample |
| `vehicleLocation.vehicleSampleTimeUtc` | Optional subminute sample clock, UTC Unix milliseconds |
| `vehicleLocation.locationSource` | Distinguishes provider GPS from estimated locations |
| `status` | On time, delayed, cancelled or ahead; not a live/scheduled boolean |
| `arrivalCertainty` | Separate high/medium/low enum; absent in this sample |

At **21:39:50 CDT**, Le Pass's line response included vehicle
`460::1705629903`, labelled `PROVIDER_GPS`, at **29.953527, -90.070130**. Our SSE
receipt **8.138 seconds later** contained car **460** only **0.148 metres** away.
The app's sample timestamp was **21:39:33.563**, about 16.6 seconds before its
response. The relay's provider timestamp was **21:39:00**, at minute precision.
The composite app vehicle-ID format is only verified for this matched example.

Le Pass predicted departure at **21:45:37**, equal to its timetable entry. RTA's
nearby response said **5 MIN** for car 460, consistent at displayed precision.
This validates source agreement in this example, not eventual arrival accuracy.
See the [matched API/RTA/SSE sample](data/feed-audit/2026-09-07-lepass-api-rta-relay.json).

Three earlier line responses had the same live ETA but **no vehicle location**;
location returned in the fourth response. The UI showed that location was
temporarily unavailable during that interval. The
[saved response sequence](data/feed-audit/2026-09-07-lepass-api-arrival-sequence.json)
then shows sample clocks advancing by approximately 30 seconds while the ETA
stayed unchanged. Millisecond timestamp representation does **not** establish
millisecond GPS accuracy or fix frequency; whether this clock denotes physical
measurement or provider ingestion remains unverified.

The next **22:00** departure had only a timetable time. The **22:25** entry was
marked `CANCELLED`, corresponding to RTA's `CNCL` entry. Le Pass's
[arrival-details screen](data/feed-audit/2026-09-07-lepass-arrival-details.png)
explicitly labelled it "Canceled for this station". A timetable-only comparison
can miss that distinction.

Identifiers also differ between systems: this stop is GTFS **1067**, public
code **259**, and Moovit stop **4539336**. Route 12's inspected Moovit line was
**8697788**, with pattern **17096284**. These IDs must not be interchanged with
the RTA vehicle feed's route and pattern IDs.

## Other public comparison sources

RTA's public BusTime map does expose read-only responses without a key:

| Data | Public URL | Important fields or limitations |
| --- | --- | --- |
| Vehicles | [getBusesForRouteAll.jsp](https://bustime.norta.com/bustime/map/getBusesForRouteAll.jsp) | Vehicle ID, route, pattern, direction, coordinates; no vehicle observation timestamp |
| Stop predictions | [Route 12, stop 259](https://bustime.norta.com/bustime/map/getStopPredictions.jsp?stop=259&route=12) | Vehicle ID, destination, displayed countdown, `scheduled` flag |
| Vehicle predictions | [Example: vehicle 972, route 12](https://bustime.norta.com/bustime/map/getBusPredictions.jsp?bus=972&route=12) | Displayed predictions for upcoming stops; values change live |

These are website endpoints, not a documented replacement production API. A
successful read does not establish that Le Pass uses them or guarantee their
availability. RTA's [Open Transit Data page](https://www.norta.com/help-and-contacts/business-information/open-transit-data-%28otd%29)
describes a license and issued credentials for its supported real-time API.

## Simultaneous route 12 sample

[Sanitized evidence](data/feed-audit/2026-09-07-poydras-rta-relay.json) records seven
public-map/prediction rounds and twelve SSE frames during a nominal two-minute
capture on September 7, approximately 20:56–20:58 America/Chicago. It retains
vehicle/location/prediction fields and omits operator, account and authentication
data. Host wall-clock adjustment means wall-clock duration differs slightly from
the capture's monotonic schedule; these are not exact source-latency measurements.

The target was **St Charles Ave at Poydras St, route 12 outbound**. Its GTFS
`stop_id` is **1067**, while its public stop code is **259**. The BusTime website
expects **259**. Sending `1067` instead returns a different stop. This is a
cross-source mapping requirement; our existing GTFS-to-GTFS OTP join correctly
uses `stop_id`.

Each official-map response was paired with the latest preceding SSE receipt,
at most ten seconds old. One round had no qualifying receipt. Across 30
observations of five matching vehicle/route IDs:

- 22 pairs were within one metre; median difference was zero metres.
- The largest position difference was about 168 metres.
- All paired pattern IDs matched.
- The public-map response does not provide the time of the underlying vehicle
  fix, so a coordinate difference cannot establish which source was newer.

RTA's outbound Poydras prediction named **vehicle 972**, falling from **17 to
15 minutes**, with `scheduled=false`. Throughout this sample, both position feeds
showed that same vehicle on **inbound pattern 588**, heading toward Canal Street.
This is consistent with a prediction for a later trip after turning around
downtown. It does not prove that explanation, but it shows why the countdown
cannot simply be compared with current outbound movement. The sample contains no
observed arrival at Poydras and cannot establish ETA accuracy.

A separate vehicle-prediction check at 21:03 CDT still listed vehicle 972's
inbound stops on Carondelet, ending at Canal/Carondelet. The GTFS outbound
sequence starts there, then serves Common, Union and Poydras (259). Poydras 259
is absent from the inbound sequence. This supports the turnaround interpretation,
but the later response does not identify the earlier prediction's trip assignment.

## Why timelines can differ

The [public relay source](https://github.com/codefornola/nola-transit-map/blob/447065511a54290404f9f8aceb7b848a59f64ddd/main.go)
requests minute-resolution vehicle timestamps (`tmres=m`) while polling about
every ten seconds. Receipt frequency is not GPS fix precision. Its frontend's
connection indicator measures receipt of any SSE message, so a green connection
does not demonstrate that every vehicle position is fresh. Vehicle popups expose
their own timestamp age. No incorrect Chicago timezone conversion was found in
the reviewed source.

Moovit's [arrival-time guide](https://support.moovitapp.com/hc/en-us/articles/13145331853842-Ver-5-125-Arrival-Time-Accuracy-Guide)
distinguishes live predictions from non-live times, which can be timetable or
historical estimates. Tapping a time shows its basis. The installed Le Pass
version was also inspected directly: its arrival-details sheet distinguishes
green live arrivals, their published schedule and a black clock marked
"Scheduled time".

The [Le Pass Poydras capture](data/feed-audit/2026-09-07-poydras-lepass-board.json)
around 21:27 CDT showed live arrivals in **2 and 18 minutes**, followed by a
**33-minute scheduled** arrival. A nearby RTA read at 21:27:02 CDT returned the
same first two countdowns for cars 953 and 460, both `scheduled=false`. These
readings were close in time, not simultaneous, and did not reproduce a live ETA
discrepancy. Before this capture, the emulator clock was synchronized to the
host: its automatic time had been approximately one minute slow. Earlier
unsynchronized app screenshots are not used for ETA comparisons.

Google's [Transit Partners documentation](https://support.google.com/transitpartners/answer/10105040?hl=en)
says it can choose between supplied TripUpdate predictions and predictions it
calculates from VehiclePositions. That is a possible reason for differing ETAs
even with similar coordinates. We have not verified RTA's present Google feed
configuration.

At 21:07:53 CDT, the public [Google Maps board for stop 259](https://www.google.com/maps/place/?q=place_id:ChIJXfHEvHSmIIYRN4RPUyt-sck&hl=en)
showed outbound departures at 21:14, 21:29, 21:45, 22:00, 22:25 and 22:50, with no
explicit live/on-time/delay indicator. All six matched the static GTFS schedule
to the displayed minute. That suggests schedule-based presentation for this
board, without establishing the backend or behavior at other stops. At
21:09:29 CDT, a separate RTA read returned 5 minutes for car 972 and 20 minutes
for car 953, both `scheduled=false`: approximately the same first two arrival
times in this particular check. These reads were not simultaneous and do not
reproduce a Google-versus-RTA ETA discrepancy. See the
[timestamped board and follow-up evidence](data/feed-audit/2026-09-07-poydras-google-board.json).

## Repeating the comparison

Run the bounded foreground diagnostic:

```bash
python3 scripts/compare-live-feeds.py --route 12 --stop-code 259 \
  --duration 120 --output /tmp/rta-poydras-comparison.json
```

On this WSL machine, `--windows-curl` is available when Windows networking is
needed. The command enforces its duration with a monotonic deadline and allows
up to three seconds for cleanup. It creates no scheduled or production job and
writes no observations into the dashboard's performance tables. Its timeout and
partial-result handling were checked with simulated stalled requests and a live
smoke test.

For an app comparison, record the same stop, direction and time, plus the exact
displayed ETA, its live/scheduled indicator, vehicle ID when available, and the
arrival-details explanation. Distinguish the current trip from any later trip
after a turnaround. Only an observed stop arrival can validate the ETA error.

The inspection emulator and local proxy were stopped after capture. The user's
signed-in emulator retains Le Pass; its original proxy and automatic-time
settings were restored. No production data source or calculation was changed.

For our measurements, retain receipt time separately from the provider sample
clock, preserve uncertainty from minute-resolution timestamps, and distinguish
observations from predicted or scheduled events. The Moovit sample provides a
comparison reference; adopting its ETA as an observed arrival would invalidate
the independence of our performance calculation. A supported direct RTA feed
with finer timestamps would need its own issued access and validation.
