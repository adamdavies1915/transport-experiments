# Streetcar signal sources

The signal inventory is [src/data/streetcar-signals-osm.json](src/data/streetcar-signals-osm.json), fetched from OpenStreetMap through Overpass on September 7, 2026. It is a raw bounding-box extract for south=29.915, west=-90.145, north=29.995, east=-90.045. The file includes the exact query, fetch time, OSM replication timestamp, original node tags and revisions, and parent ways for road-name and pedestrian context. Refresh it with:

```sh
python3 scripts/fetch-streetcar-signals.py
```

Use attribution “© OpenStreetMap contributors” and link to [OpenStreetMap copyright and license](https://www.openstreetmap.org/copyright) when displaying or distributing this ODbL data.

## What the inventory establishes

OpenStreetMap maps traffic-control locations. It does **not** supply live red/green phases, signal cycle times, or traffic speeds in this extract. [OSM's traffic signal documentation](https://wiki.openstreetmap.org/wiki/Tag:highway%3Dtraffic_signals) distinguishes car, pedestrian, and bicycle signals and explains that several approach nodes may describe a single intersection. Multiple nearby nodes should not become multiple streetcar delays.

The extract has 1,596 signal-related nodes and 2,176 parent ways:

| Tag group | Raw nodes |
| --- | ---: |
| `highway=traffic_signals` | 497 |
| Signalized crossing without that highway tag | 1,067 |
| `railway=signal` | 32 |

All 32 railway signals lack signal-type details; one has a direction tag. Their tags do not establish whether they control a junction, a streetcar movement, a switch, or something else. Keep them distinct from road lights. [OSM's railway-signal documentation](https://wiki.openstreetmap.org/wiki/Tag:railway%3Dsignal) describes the further tags that would be needed.

The first corridor audit, using minimum perpendicular distance to all GTFS shape segments and a 50 m radius, found:

| Route | Road signal nodes | Railway signal nodes | Signalized crossing nodes |
| --- | ---: | ---: | ---: |
| 12 · St. Charles | 60 | 6 | 96 |
| 46 · Rampart–Loyola | 55 | 7 | 131 |
| 47 · Canal–Cemeteries | 63 | 22 | 146 |
| 48 · Canal–City Park | 76 | 22 | 167 |

These are **candidate nodes**, not independent intersections, confirmed streetcar controls, or verified delay causes. Routes overlap, and approaches duplicate intersections. Parent ways identify intersecting streets for many nodes. A close location alone does not establish which travel direction a light controls. Missing mapped nodes do not establish an unsignalized intersection. Nearby passenger stops must remain separate features; observations close to both need an overlapping/ambiguous classification.

## GTFS geometry and stops

The public NORTA archive inspected was feed version `S1000253`, valid May 17, 2026–January 24, 2027, SHA-256 `3b32471680f5736301fdcaa2ae98bf62ba6e108ea51589febb1d66114ee567f9`. The official [NORTA rider tools](https://www.norta.com/rider-tools/) agree that Rampart–Loyola is route **46**. Route 49 is Riverfront, a different route.

| Route | Unique scheduled stop IDs | Shape IDs |
| --- | ---: | --- |
| 12 | 107 | `shp-12-04`, `shp-12-05`, `shp-12-55` |
| 46 | 20 | `shp-46-01`, `shp-46-53` |
| 47 | 48 | `shp-47-01`, `shp-47-09`, `shp-47-56`, `shp-47-60`, `shp-47-61` |
| 48 | 48 | `shp-48-12`, `shp-48-13`, `shp-48-56` |

Use the GTFS trip's shape, direction, and ordered `stop_times`, retaining all stops rather than only schedule timepoints. The aggregate counts above include all service patterns in this archive, including shortened patterns; they are not all simultaneously active trips. The generated analysis catalog excludes the five shapes whose trips are all labelled `Not in Service`, leaving eight passenger-service paths. Stops come only from those passenger trips, and each path retains its own ordered raw GTFS stop IDs. Site identifiers are corridor-scoped, with `gtfs:stop:<id>` provenance.

Build that catalog offline with:

```sh
node --import tsx scripts/build-streetcar-network.ts --gtfs /tmp/norta-gtfs.zip --osm src/data/streetcar-signals-osm.json
```

Road signal nodes must have a road parent way; pedestrian-only crossing nodes cannot establish road signals. Candidate road nodes within 50 m of a passenger-service path are grouped with a maximum pairwise diameter of 30 m. This bounds merging, but can still leave several groups at a large intersection. Railway signal nodes remain separate sites. Geometry retains the original shape coordinates, removing only consecutive identical coordinates.

## Mapillary crosscheck

The independent Mapillary crosscheck is now saved in [src/data/streetcar-mapillary.json](src/data/streetcar-mapillary.json). All 73 corridor tile queries succeeded on September 7, 2026. They returned 6,694 unique traffic-light feature IDs, of which 3,010 were within 60 m of passenger-service paths. Of those, 2,394 have general vehicle-light classes. Pedestrian, bicycle, and other classes remain separate. Several detected features can represent the same physical hardware, so these totals are not intersection counts.

**103 of the original 104 OSM road-signal sites have a general vehicle-light detection within 45 m.** This corroborates nearby hardware, not the signal direction, streetcar movement controlled, current phase, or cause of an observed delay. Mapillary feature `first_seen_at`/`last_seen_at` values must not be substituted for an image's `captured_at` when describing imagery age.

Three representative sites have actual visual reviews with public image IDs, capture dates, and observations recorded in [src/data/streetcar-mapillary-reviews.json](src/data/streetcar-mapillary-reviews.json):

| Intersection | Image date | Visible evidence |
| --- | --- | --- |
| St. Charles / Jackson | February 27, 2016 | Vehicle lights and streetcar tracks |
| Canal / Carrollton | October 9, 2022 | Overhead vehicle lights, street-name sign, tracks, passenger platform |
| Rampart / Toulouse | March 25, 2022 | Vehicle signals, separate pedestrian lights, street signs and tracks |

The only unmatched OSM cluster, **St. Charles / Pleasant / Toledano**, was inspected in three October 6, 2022 images covering the crossing and both directions along St. Charles. The views show the crosswalks, tracks, signs and streetscape without traffic-light hardware at that crossing; distant signals belong to another junction. This OSM candidate is contradicted by the available 2022 imagery; **current presence remains unverified**. A documented local override excludes this one candidate from signal exposure, leaving **103 retained sites, all spatially corroborated**. It preserves the raw OSM extract and applies only while the four source nodes retain their recorded OSM revisions. No external OSM edits were made.

The raw crosscheck retains all 104 original site checks and the disputed review, so the exclusion is reviewable. Rebuilding the network incorporates the three positive visual reviews and the single local exclusion. Updating OSM to a new revision requires revisiting the override; rebuilding a materially different geometry catalog marks incompatible old spatial crosschecks as stale.

Refresh the spatial crosscheck and rebuild locally with a supplied client token in the server environment:

```sh
python3 scripts/crosscheck-streetcar-mapillary.py
node --import tsx scripts/build-streetcar-network.ts
```

Alternatively, the fetch script accepts `--credentials /path/to/private/client.json` with an `access_token` field. It writes only allowlisted public fields, never credentials, pagination URLs, or signed thumbnail URLs. Manual image reviews are preserved in their separate file. No image pixels are published in these data files.

The public [Mapillary SDK authentication documentation](https://mapillary.github.io/mapillary-python-sdk/docs/mapillary.models/mapillary.models.client/) explains client/user tokens. The [official entity documentation](https://mapillary.github.io/mapillary-python-sdk/docs/mapillary.config.api/mapillary.config.api.entities/) documents feature metadata, `/{map_feature_id}/detections`, and image capture metadata. Public client reads worked; no OAuth callback flow was needed for these data reads.

[Mapillary's map-feature documentation](https://help.mapillary.com/hc/en-us/articles/115002332165-Map-features) explains that object coordinates are reconstructed from multiple images and depend on image GPS accuracy; point features require at least three image detections. Therefore a missing detection alone cannot disprove an OSM signal. Hardware coordinates also differ from OSM road stop-line or intersection coordinates. Imagery establishes presence at capture time, not live operation or present-day phases.

The published Mapillary-derived metadata is attributed to Mapillary and imagery contributors and carries **ODbL-1.0**, following [Mapillary's OpenStreetMap compatibility terms](https://help.mapillary.com/hc/en-us/articles/115001777705-OpenStreetMap-compatibility). Those terms allow publicly accessible derived metadata under ODbL; image pixels have their separate [CC-BY-SA terms](https://help.mapillary.com/hc/en-us/articles/115001770409-CC-BY-SA-license-for-open-data).

A bounded additional visual review covered railway signal nodes `4756265624` near Canal/Elk Place (2024 imagery) and `4431089497` near Canal/Rampart (2023 imagery). Vehicle heads, tracks and junction trackwork are visible, but the views do not securely associate a head with either rail node or establish whether it controls streetcar traffic or a switch. Both remain rail signals of unknown function. [OSM's tram-signal discussion](https://help.openstreetmap.org/questions/52716/traffic-signals-for-psv-light-rail-or-trams/) confirms that `railway=signal` can include tram traffic lights; the tag does not make a signal irrelevant to streetcar delay. These unresolved reviews are retained with the other imagery evidence.

In the generated UI catalog, `verification=mapillary` means the location is corroborated by Mapillary evidence. All 103 retained spatially matched road-signal sites carry a `mapillary/feature/<id>` reference to the nearest general vehicle-light detection. Only the three sites with actual positive image reviews also carry `mapillary/image/<id>`. Feature corroboration must not be labelled as human imagery review; `mapillary_status` retains the separate counts.
