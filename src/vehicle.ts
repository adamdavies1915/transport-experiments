import { findSegment } from './segments';
import type { RawVehicle, TransitRecord } from './types';

// Parse an integer-ish SSE field, preserving null when absent/unparseable so
// we never coerce a missing value to a misleading 0.
function parseIntOrNull(value: string | undefined): number | null {
  if (value == null || value === '') return null;
  const n = parseInt(value, 10);
  return Number.isNaN(n) ? null : n;
}

function nullIfEmpty(value: string | undefined): string | null {
  return value == null || value === '' ? null : value;
}

export function processVehicle(v: RawVehicle): TransitRecord | null {
  // Skip vehicles with invalid coordinates
  if (v.lat === '0' && v.lon === '0') return null;

  const lat = parseFloat(v.lat);
  const lon = parseFloat(v.lon);
  if (!Number.isFinite(lat) || !Number.isFinite(lon) || Math.abs(lat) > 90 || Math.abs(lon) > 180 ||
      !Number.isFinite(Date.parse(v.tmstmp))) return null;
  const segment = findSegment(v.rt, lat, lon);
  const gtfsId = v.tripid == null ? '' : String(v.tripid).trim();

  return {
    vid: v.vid,
    timestamp: v.tmstmp,
    lat,
    lon,
    heading: parseInt(v.hdg ?? '') || 0,
    route: v.rt,
    trip_id: v.tatripid ?? null,
    gtfs_trip_id: ['', '0', 'N/A', 'null'].includes(gtfsId) ? null : gtfsId,
    destination: v.des || null,
    speed: parseInt(v.spd ?? '') || 0,
    is_delayed: typeof v.dly === 'boolean' ? v.dly : null,
    is_off_route: v.or === true,
    pdist: parseIntOrNull(v.pdist),
    pid: parseIntOrNull(v.pid),
    rid: nullIfEmpty(v.rid),
    tablockid: nullIfEmpty(v.tablockid),
    srvtmstmp: nullIfEmpty(v.srvtmstmp),
    ...segment
  };
}
