#!/usr/bin/env python3
"""Read public Mapillary light features and spatially crosscheck the OSM catalog.

Supply MAPILLARY_ACCESS_TOKEN or --credentials /path/to/private/client.json.
No credential, signed image URL, or API pagination URL is written to output.
"""
import argparse
import concurrent.futures
import datetime
import json
import math
import os
import pathlib
import urllib.error
import urllib.parse
import urllib.request

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--credentials')
parser.add_argument('--network', default='src/data/streetcar-network.json')
parser.add_argument('--output', default='src/data/streetcar-mapillary.json')
parser.add_argument('--reviews', default='src/data/streetcar-mapillary-reviews.json')
args = parser.parse_args()
token = os.environ.get('MAPILLARY_ACCESS_TOKEN')
if args.credentials:
    token = json.loads(pathlib.Path(args.credentials).read_text())['access_token']
if not token:
    raise SystemExit('Set MAPILLARY_ACCESS_TOKEN or provide --credentials')
network = json.loads(pathlib.Path(args.network).read_text())
endpoint = 'https://graph.mapillary.com/map_features'
fields = 'id,geometry,object_value,first_seen_at,last_seen_at'
cell = 0.005
cells = set()
# Cover all route geometry, including corners and a 100 m boundary margin, so
# Mapillary candidates absent from OSM can also be found.
for path in network['paths']:
    for point in path['points']:
        for lat in [point['lat'] - 0.001, point['lat'] + 0.001]:
            for lon in [point['lon'] - 0.0012, point['lon'] + 0.0012]:
                cells.add((math.floor(lon / cell), math.floor(lat / cell)))
boxes = [[round(x*cell, 6), round(y*cell, 6), round((x+1)*cell, 6), round((y+1)*cell, 6)] for x,y in sorted(cells)]

def get_json(url):
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme != 'https' or parsed.hostname != 'graph.mapillary.com':
        raise RuntimeError('Unexpected Mapillary pagination host')
    # Use the authorization header, never a token-bearing query parameter.
    params = [(k,v) for k,v in urllib.parse.parse_qsl(parsed.query) if k != 'access_token']
    clean = urllib.parse.urlunparse(parsed._replace(query=urllib.parse.urlencode(params)))
    request = urllib.request.Request(clean, headers={'Authorization': 'OAuth '+token})
    try:
        with urllib.request.urlopen(request, timeout=45) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        raise RuntimeError('Mapillary HTTP '+str(error.code)) from None

def fetch_box(box, depth=0):
    params = {'bbox': ','.join(map(str,box)), 'fields': fields, 'object_values': 'object--traffic-light--*', 'limit': 2000}
    url = endpoint+'?'+urllib.parse.urlencode(params)
    features = []
    try:
        while url:
            result = get_json(url)
            page = result.get('data', [])
            features.extend(page)
            url = result.get('paging', {}).get('next')
            # Do not silently accept a possibly truncated result.
            if len(page) >= 2000 and not url:
                raise RuntimeError('Mapillary result reached its page limit')
        return features, []
    except Exception as error:
        if depth < 2:
            west,south,east,north = box
            if east-west >= north-south:
                mid=(west+east)/2; halves=[[west,south,mid,north],[mid,south,east,north]]
            else:
                mid=(south+north)/2; halves=[[west,south,east,mid],[west,mid,east,north]]
            left,le=fetch_box(halves[0],depth+1);right,re=fetch_box(halves[1],depth+1)
            return left+right,le+re
        return [], [{'bbox':box,'error':str(error).replace(token,'[redacted]')}]

all_features = {};errors=[]
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
    for i,(features,failed) in enumerate(pool.map(fetch_box,boxes),1):
        for feature in features:
            # Explicit allowlist discards any credential-bearing paging or URLs.
            clean={key:feature[key] for key in ['id','geometry','object_value','first_seen_at','last_seen_at'] if key in feature}
            all_features[clean['id']]=clean
        errors.extend(failed)
        if i%10==0 or i==len(boxes):
            print(json.dumps({'boxes_completed':i,'boxes_total':len(boxes),'unique_features':len(all_features),'failed_boxes':len(errors)}),flush=True)

mx=111195*math.cos(math.radians(29.95));my=111195

def distance(a,b):
    return math.hypot((a[0]-b[0])*mx,(a[1]-b[1])*my)

def segment_distance(p,a,b):
    dx=(b[0]-a[0])*mx;dy=(b[1]-a[1])*my
    px=(p[0]-a[0])*mx;py=(p[1]-a[1])*my
    t=max(0,min(1,(px*dx+py*dy)/(dx*dx+dy*dy))) if dx*dx+dy*dy else 0
    return math.hypot(px-t*dx,py-t*dy)

segments={c['id']:[] for c in network['corridors']}
for path in network['paths']:
    points=[(p['lon'],p['lat']) for p in path['points']]
    segments[path['corridor']].extend(zip(points,points[1:]))
features=[]
for feature in all_features.values():
    p=feature.get('geometry',{}).get('coordinates',[])
    if len(p)<2:continue
    corridors=[c for c,segs in segments.items() if min(segment_distance(p,a,b) for a,b in segs)<=60]
    if corridors:
        feature['corridors']=corridors
        feature['vehicle_light_candidate']=feature.get('object_value','').startswith('object--traffic-light--general-')
        features.append(feature)
review_path=pathlib.Path(args.reviews)
review_data=json.loads(review_path.read_text()) if review_path.exists() else {}
reviews=review_data.get('reviews',[])
reviewed={r['site_id'] for r in reviews if r.get('outcome')=='signal_present_at_capture'}
checks=[]
for site in network['sites']:
    if site['kind']!='signal':continue
    candidates=[]
    for feature in features:
        d=distance([site['lon'],site['lat']],feature['geometry']['coordinates'])
        if feature['vehicle_light_candidate'] and d<=45:
            candidates.append({'feature_id':feature['id'],'distance_meters':round(d,1)})
    candidates.sort(key=lambda x:x['distance_meters'])
    checks.append({'site_id':site['id'],'corridor':site['corridor'],'match_radius_meters':45,
        'status':'nearby_vehicle_light_detections' if candidates else 'no_nearby_vehicle_light_detection',
        'matches':candidates,'imagery_verified':site['id'] in reviewed})
result={
    'schema_version':1,'source':'Mapillary map features','source_url':endpoint,
    'fetched_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),
    'network_version':network['version'],'query':{'fields':fields,'object_values':'object--traffic-light--*','bounding_boxes_west_south_east_north':boxes},
    'attribution':'Mapillary and imagery contributors',
    'license':'ODbL-1.0',
    'license_url':'https://help.mapillary.com/hc/en-us/articles/115001777705-OpenStreetMap-compatibility',
    'documentation_url':'https://help.mapillary.com/hc/en-us/articles/115002332165-Map-features',
    'status':'complete' if not errors else 'partial','failed_queries':errors,
    'notes':[
        'This is a spatial crosscheck of independently detected hardware, not manual imagery verification.',
        'Only general vehicle-light classes can corroborate road-signal candidates. Pedestrian, bicycle, and other classes remain separate.',
        'Several model features can represent the same physical hardware across imagery dates. Counts are not independent intersections.',
        'Mapillary positions can describe a pole while OSM describes a stop line or intersection node; matching uses a 45 m radius.',
        'Missing detections do not prove there is no signal. GPS, imagery coverage, age, and model errors affect the comparison.',
        'No live red/green phases or signal timings are provided.',
    ],
    'features':sorted(features,key=lambda f:f['id']), 'site_checks':checks,
    'imagery_checks':reviews, 'local_exclusions':review_data.get('exclusions',[]),
}
p=pathlib.Path(args.output);p.parent.mkdir(parents=True,exist_ok=True);p.write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps({'output':str(p),'features_within60m':len(features),'signal_sites':len(checks),'spatially_corroborated_sites':sum(bool(c['matches']) for c in checks),'failed_queries':len(errors)}),flush=True)
