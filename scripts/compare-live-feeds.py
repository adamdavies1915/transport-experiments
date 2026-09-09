#!/usr/bin/env python3
"""Bounded manual comparison of the public RTA map and the existing NOLA relay.

Example:
  python3 scripts/compare-live-feeds.py --route 12 --stop-code 259 --duration 120 \
    --output /tmp/rta-feed-comparison.json
Use --windows-curl explicitly if WSL networking requires Windows curl.exe.
Capture duration is a hard deadline, followed by at most 3 seconds of process
cleanup. Slow rounds are skipped; partial results are saved on interruption.
No app credentials, accounts, operator identifiers, or recurring jobs are used.
"""

import argparse
import concurrent.futures
import datetime
import json
import math
import pathlib
import subprocess
import sys
import threading
import time
import urllib.parse
import xml.etree.ElementTree as ET

RELAY = 'https://nolatransit.fly.dev/sse'
MAP = 'https://bustime.norta.com/bustime/map/getBusesForRouteAll.jsp'
PREDICTIONS = 'https://bustime.norta.com/bustime/map/getStopPredictions.jsp'
WINDOWS_CURL = '/mnt/c/Windows/System32/curl.exe'
CLEANUP_GRACE_SECONDS = 3

# Keep blocking DNS and HTTP reads outside the main process so that even a
# trickling response cannot outlive the capture deadline. No extra dependency.
HTTP_WORKER = '''
import sys, urllib.request
with urllib.request.urlopen(sys.argv[1], timeout=float(sys.argv[2])) as response:
    if sys.argv[3] == 'xml':
        date = response.headers.get('Date', '').encode('latin1')
        sys.stdout.buffer.write(b'HTTP/1.1 200 OK\\r\\nDate: ' + date + b'\\r\\n\\r\\n')
    while True:
        chunk = response.read1(65536)
        if not chunk:
            break
        sys.stdout.buffer.write(chunk)
'''


class CaptureDeadline(TimeoutError):
    pass


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def epoch(value):
    return datetime.datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp()


def distance(a, b):
    lat1, lon1 = float(a['lat']), float(a['lon'])
    lat2, lon2 = float(b['lat']), float(b['lon'])
    return 6371000 * math.hypot(math.radians(lat2 - lat1),
        math.radians(lon2 - lon1) * math.cos(math.radians((lat1 + lat2) / 2)))


def compare(data, maximum_receipt_gap=10):
    """Pair the most recent preceding receipt within the limit, never provider minutes."""
    pairs = []
    unpaired = 0
    frames = data['relay_frames']
    for sample in data['rounds']:
        official = sample.get('map')
        if not official or not frames:
            unpaired += 1
            continue
        monotonic = 'received_elapsed_seconds' in official and all('received_elapsed_seconds' in f for f in frames)
        receipt = (lambda value: value['received_elapsed_seconds']) if monotonic else (lambda value: epoch(value['received_at']))
        candidates = [(receipt(official) - receipt(frame), frame) for frame in frames]
        candidates = [(gap, frame) for gap, frame in candidates if 0 <= gap <= maximum_receipt_gap]
        if not candidates:
            unpaired += 1
            continue
        gap, frame = min(candidates, key=lambda item: item[0])
        relay = {str(v['vid']): v for v in frame['vehicles']}
        for vehicle in official['vehicles']:
            other = relay.get(str(vehicle['id']))
            if other is None or str(vehicle['rt']) != str(other['rt']):
                continue
            try:
                meters = distance(vehicle, other)
            except (TypeError, ValueError, KeyError):
                continue
            if not math.isfinite(meters):
                continue
            pairs.append({'round': sample['round'], 'vid': vehicle['id'], 'route': vehicle['rt'],
                'map_received_at': official['received_at'], 'relay_received_at': frame['received_at'],
                'receipt_gap_seconds': round(gap, 3), 'receipt_pairing_clock': 'monotonic' if monotonic else 'wall',
                'position_distance_meters': round(meters, 3),
                'same_pattern': (str(vehicle['pid']) == str(other['pid'])) if vehicle.get('pid') and other.get('pid') else None,
                'relay_provider_at': other.get('tmstmp')})
    distances = sorted(p['position_distance_meters'] for p in pairs)
    middle = len(distances) // 2
    median = (distances[middle] if len(distances) % 2 else sum(distances[middle - 1:middle + 1]) / 2) if distances else None
    return {'pairs': pairs, 'summary': {'map_rounds': len(data['rounds']), 'relay_frames': len(frames),
        'unpaired_map_rounds': unpaired, 'paired_vehicle_observations': len(pairs),
        'within_one_meter': sum(d <= 1 for d in distances),
        'median_position_difference_meters': median, 'maximum_position_difference_meters': max(distances, default=None),
        'pattern_mismatches': sum(p['same_pattern'] is False for p in pairs),
        'unknown_pattern_pairs': sum(p['same_pattern'] is None for p in pairs)}}


class Transport:
    def __init__(self, use_windows_curl, duration):
        self.started = time.monotonic()
        self.deadline = self.started + duration
        self.processes = set()
        self.process_lock = threading.Lock()
        self.closed = False
        self.curl = WINDOWS_CURL if use_windows_curl else None
        if self.curl and not pathlib.Path(self.curl).is_file():
            raise ValueError('Windows curl.exe is unavailable at ' + self.curl)

    def remaining(self):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise CaptureDeadline()
        return remaining

    def command(self, url, timeout, stream=False):
        if self.curl:
            return [self.curl, '--silent', '--show-error', '--fail', '--location',
                '--max-time', str(timeout), '--no-buffer' if stream else '--include', url]
        return [sys.executable, '-u', '-c', HTTP_WORKER, url, str(timeout), 'stream' if stream else 'xml']

    def spawn(self, url, timeout, stream=False):
        with self.process_lock:
            if self.closed:
                raise CaptureDeadline()
            timeout = min(timeout, self.remaining())
            process = subprocess.Popen(self.command(url, timeout, stream),
                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
            self.processes.add(process)
            return process

    def release(self, process):
        if process.poll() is not None:
            with self.process_lock:
                self.processes.discard(process)

    def xml(self, url):
        started_at, started = now(), time.monotonic()
        request_deadline = min(self.deadline, started + 25)
        process = self.spawn(url, request_deadline - started)
        try:
            try:
                body, _ = process.communicate(timeout=max(0, request_deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                try:
                    process.kill()
                except ProcessLookupError:
                    pass
                process.wait(timeout=CLEANUP_GRACE_SECONDS)
                if time.monotonic() >= self.deadline:
                    raise CaptureDeadline() from None
                raise TimeoutError() from None
            self.remaining()
            if process.returncode:
                raise subprocess.CalledProcessError(process.returncode, 'HTTP request')
        finally:
            self.release(process)
        headers = {}
        # Consume redirects / proxy CONNECT / interim headers before the body.
        while body.startswith(b'HTTP/'):
            header, separator, remaining = body.partition(b'\r\n\r\n')
            if not separator:
                header, separator, remaining = body.partition(b'\n\n')
            if not separator:
                raise ValueError('No HTTP header terminator in response')
            headers = dict(line.decode('latin1').split(':', 1) for line in header.splitlines()[1:] if b':' in line)
            body = remaining
        response_date = next((v.strip() for k, v in headers.items() if k.lower() == 'date'), None)
        root = ET.fromstring(body)
        self.remaining()
        metadata = {'started_at': started_at, 'received_at': now(), 'http_date': response_date,
            'received_elapsed_seconds': round(time.monotonic() - self.started, 6),
            'request_duration_seconds': round(time.monotonic() - started, 3)}
        return metadata, root

    def stream(self, stop, append):
        process = self.spawn(RELAY, self.remaining(), stream=True)
        try:
            payload = []
            with process.stdout:
                for line in process.stdout:
                    if stop.is_set() or time.monotonic() >= self.deadline:
                        return 'deadline' if time.monotonic() >= self.deadline else 'stopped'
                    if line.startswith(b'data:'):
                        payload.append(line[5:].strip())
                    elif not line.strip() and payload:
                        append(json.loads(b'\n'.join(payload)), now(), time.monotonic() - self.started)
                        payload = []
            if stop.is_set() or time.monotonic() >= self.deadline:
                return 'deadline' if time.monotonic() >= self.deadline else 'stopped'
            process.wait(timeout=self.remaining())
            if process.returncode:
                raise subprocess.CalledProcessError(process.returncode, 'SSE request')
            return 'disconnected'
        finally:
            self.release(process)

    def close(self, cleanup_deadline):
        """Kill all HTTP children, then reap them within one shared grace period."""
        with self.process_lock:
            self.closed = True
            processes = list(self.processes)
        for process in processes:
            if process.poll() is None:
                try:
                    process.kill()
                except ProcessLookupError:
                    pass
        unreaped = 0
        for process in processes:
            try:
                process.wait(timeout=max(0, cleanup_deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                unreaped += 1
            self.release(process)
        return unreaped


def capture(args):
    transport = Transport(args.windows_curl, args.duration)
    stop, lock = threading.Event(), threading.Lock()
    prediction_url = PREDICTIONS + '?' + urllib.parse.urlencode({'stop': args.stop_code, 'route': args.route})
    data = {'schema_version': 1, 'started_at': now(), 'sources': {'relay': RELAY, 'map': MAP, 'predictions': prediction_url},
        'selection': {'route': args.route, 'stop_code': args.stop_code},
        'limits': {'duration_seconds': args.duration, 'interval_seconds': args.interval,
            'cleanup_grace_seconds': CLEANUP_GRACE_SECONDS,
            'maximum_receipt_gap_seconds': args.maximum_receipt_gap},
        'notes': ['Receipt and HTTP Date describe retrieval, not the time of a vehicle fix.',
            'Relay provider timestamps have minute precision; their age is not an exact source latency.',
            'Pairs use the latest preceding relay receipt aged 0 to 10 seconds (or the configured smaller limit), using monotonic receipt clocks when available.',
            'Position agreement does not identify an arrival prediction algorithm or prove ETA accuracy.',
            'Requests and SSE collection stop at the monotonic capture deadline; missed polling rounds are skipped.',
            'Operator, run, block, trip, account and authentication fields are deliberately omitted.'],
        'rounds': [], 'relay_frames': [], 'errors': []}

    def append(payload, received, elapsed):
        vehicles = payload if isinstance(payload, list) else payload.get('vehicles', [])
        fields = ['vid', 'rt', 'lat', 'lon', 'hdg', 'tmstmp', 'srvtmstmp', 'pid', 'spd', 'des', 'or']
        with lock:
            if stop.is_set() or elapsed >= args.duration:
                return
            data['relay_frames'].append({'received_at': received, 'received_elapsed_seconds': round(elapsed, 6), 'vehicles': [
                {k: v.get(k) for k in fields} for v in vehicles if str(v.get('rt')) == args.route]})

    def stream():
        try:
            reason = transport.stream(stop, append)
        except Exception as error:
            if stop.is_set() or time.monotonic() >= transport.deadline:
                reason = 'deadline' if time.monotonic() >= transport.deadline else 'stopped'
            else:
                reason = 'error'
                with lock:
                    data['errors'].append({'source': 'relay', 'at': now(), 'error_type': type(error).__name__})
        with lock:
            data['relay_end_reason'] = reason

    def get_map():
        meta, root = transport.xml(MAP + '?' + urllib.parse.urlencode({'key': time.time_ns()}))
        fields = ['id', 'rt', 'lat', 'lon', 'dn', 'pid', 'pd', 'fs']
        meta['vehicles'] = [{k: b.findtext(k) for k in fields} for b in root.findall('.//bus') if b.findtext('rt') == args.route]
        return meta

    def get_predictions():
        meta, root = transport.xml(prediction_url + '&key=' + str(time.time_ns()))
        meta.update({'stop_code': root.findtext('id'), 'stop_name': root.findtext('nm'), 'direction': root.findtext('sri/d'),
            'no_prediction_message': root.findtext('noPredictionMessage'),
            'predictions': [{k: p.findtext(k) for k in ['pt', 'fd', 'v', 'scheduled', 'rn']} for p in root.findall('pre')]})
        return meta

    thread = threading.Thread(target=stream, daemon=True)
    thread.start()
    workers = [thread]

    def submit(function):
        # Daemon workers avoid ThreadPoolExecutor's unconditional exit-time join.
        # Network reads themselves are killable, tracked child processes.
        future = concurrent.futures.Future()

        def run():
            try:
                future.set_result(function())
            except BaseException as error:
                future.set_exception(error)

        worker = threading.Thread(target=run, daemon=True)
        workers.append(worker)
        worker.start()
        return future

    data['completion_reason'] = 'deadline'
    try:
        for index in range(math.ceil(args.duration / args.interval)):
            scheduled = transport.started + index * args.interval
            if time.monotonic() >= transport.deadline:
                break
            if index and scheduled < time.monotonic():
                continue
            stop.wait(max(0, scheduled - time.monotonic()))
            if time.monotonic() >= transport.deadline:
                break
            sample = {'round': index, 'at': now()}
            data['rounds'].append(sample)
            futures = {'map': submit(get_map), 'predictions': submit(get_predictions)}
            for source, future in futures.items():
                try:
                    sample[source] = future.result(timeout=max(0, transport.deadline - time.monotonic()))
                except Exception as error:
                    error_type = ('CaptureDeadline' if isinstance(error, TimeoutError)
                        and time.monotonic() >= transport.deadline else type(error).__name__)
                    data['errors'].append({'source': source, 'at': now(), 'error_type': error_type})
            print(json.dumps({'round': index, 'at': sample['at'],
                'predictions': sample.get('predictions', {}).get('predictions', [])}), flush=True)
        stop.wait(max(0, transport.deadline - time.monotonic()))
    except BaseException as error:
        data['completion_reason'] = 'interrupted' if isinstance(error, KeyboardInterrupt) else 'error'
        data['errors'].append({'source': 'capture', 'at': now(), 'error_type': type(error).__name__})
        raise
    finally:
        stop.set()
        data['capture_elapsed_seconds'] = round(time.monotonic() - transport.started, 3)
        cleanup_deadline = time.monotonic() + CLEANUP_GRACE_SECONDS
        unreaped = transport.close(cleanup_deadline)
        for worker in workers:
            worker.join(timeout=max(0, cleanup_deadline - time.monotonic()))
        with lock:
            if unreaped or any(worker.is_alive() for worker in workers):
                data['errors'].append({'source': 'cleanup', 'at': now(), 'error_type': 'CleanupGraceExceeded'})
            data.setdefault('relay_end_reason', 'cleanup_incomplete')
            data['finished_at'] = now()
            data['elapsed_seconds'] = round(time.monotonic() - transport.started, 3)
            data['wall_duration_seconds'] = round(epoch(data['finished_at']) - epoch(data['started_at']), 3)
            data['wall_clock_difference_seconds'] = round(data['wall_duration_seconds'] - data['elapsed_seconds'], 3)
            data.update(compare(data, args.maximum_receipt_gap))
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(data, indent=2) + '\n')
        print(json.dumps({'output': str(args.output), **data['summary'], 'errors': data['errors']}), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--route', default='12')
    parser.add_argument('--stop-code', default='259', help='Public stop code, not GTFS stop_id')
    parser.add_argument('--duration', type=int, default=120, help='Capture seconds, 20–900 (default 120)')
    parser.add_argument('--interval', type=int, default=20, help='Public map/prediction polling seconds, at least 10')
    parser.add_argument('--maximum-receipt-gap', type=float, default=10)
    parser.add_argument('--output', type=pathlib.Path, required=True)
    parser.add_argument('--windows-curl', action='store_true', help='Explicitly use Windows curl.exe from WSL')
    args = parser.parse_args()
    if not 20 <= args.duration <= 900 or not 10 <= args.interval <= args.duration:
        parser.error('Duration must be 20–900 seconds; interval must be 10 seconds or more and no longer than duration.')
    if not 0 < args.maximum_receipt_gap <= 10:
        parser.error('Maximum receipt gap must be greater than zero and at most 10 seconds.')
    if args.output.exists():
        parser.error('Output already exists; choose a new path to preserve the previous capture.')
    capture(args)


if __name__ == '__main__':
    main()
