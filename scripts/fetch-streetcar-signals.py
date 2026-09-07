#!/usr/bin/env python3
"""Refresh the public OSM signal inventory (no live signal phases are supplied).

Usage: python3 scripts/fetch-streetcar-signals.py [--endpoint URL] [--output FILE]
"""
import argparse
import collections
import datetime
import json
import pathlib
import urllib.parse
import urllib.request

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--endpoint', default='https://overpass-api.de/api/interpreter')
parser.add_argument('--output', default='src/data/streetcar-signals-osm.json')
args = parser.parse_args()
bounds = [29.915, -90.145, 29.995, -90.045]
bbox = ','.join(map(str, bounds))
query = f'''[out:json][timeout:90];
(
  node["highway"="traffic_signals"]({bbox});
  node["railway"="signal"]({bbox});
  node["crossing"="traffic_signals"]({bbox});
  node["crossing:signals"="yes"]({bbox});
)->.signals;
.signals out meta;
way(bn.signals);out body;'''
request = urllib.request.Request(args.endpoint, data=urllib.parse.urlencode({'data': query}).encode(), headers={'User-Agent': 'NOLA-transit-research/1.0'})
with urllib.request.urlopen(request, timeout=110) as response:
    raw = json.load(response)
if raw.get('remark'):
    raise RuntimeError(f"Overpass did not complete: {raw['remark']}")
elements = [element for element in raw['elements'] if element['type'] == 'node']
ways = [element for element in raw['elements'] if element['type'] == 'way']
# Preserve all signal node tags and OSM revision metadata. Parent ways allow
# callers to distinguish pedestrian-only signals and label adjacent roads.
result = {
    'schema_version': 1,
    'source': 'OpenStreetMap via Overpass API',
    'source_url': args.endpoint,
    'fetched_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
    'osm_timestamp': raw.get('osm3s', {}).get('timestamp_osm_base'),
    'bounds_south_west_north_east': bounds,
    'query': query,
    'license': 'ODbL-1.0',
    'attribution': '© OpenStreetMap contributors',
    'license_url': 'https://www.openstreetmap.org/copyright',
    'documentation_urls': [
        'https://wiki.openstreetmap.org/wiki/Tag:highway%3Dtraffic_signals',
        'https://wiki.openstreetmap.org/wiki/Tag:railway%3Dsignal',
        'https://wiki.openstreetmap.org/wiki/Key:crossing:signals',
    ],
    'notes': [
        'This is a raw bounding-box inventory. Filter to the route geometry before analysis.',
        'These mapped locations do not contain live traffic speeds, red/green phases, or signal cycle timings.',
        'An intersection can have several signal nodes; cluster nearby approaches before counting intersection encounters.',
        'Road signals, pedestrian signals, and railway operational signals are not interchangeable. Retain tags and parent-way context.',
        'OSM completeness and positions vary. Absence of a mapped signal is not evidence that an intersection is unsignalized.',
        'Mapillary imagery verification has not been performed for this inventory.',
    ],
    'elements': elements,
    'context_ways': ways,
}
path = pathlib.Path(args.output)
path.parent.mkdir(parents=True, exist_ok=True)
path.write_text(json.dumps(result, indent=2) + '\n')
counts = collections.Counter('railway_signal' if e.get('tags', {}).get('railway') == 'signal' else 'traffic_signal' if e.get('tags', {}).get('highway') == 'traffic_signals' else 'signalized_crossing' for e in elements)
print(json.dumps({'output': str(path), 'nodes': len(elements), 'context_ways': len(ways), 'types': counts, 'osm_timestamp': result['osm_timestamp']}))
