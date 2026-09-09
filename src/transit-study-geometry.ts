import type { StudyPath, StudyPoint } from './transit-study-types';

export const METERS_PER_DEGREE = Math.PI * 6371000 / 180;
export const LONGITUDE_SCALE = METERS_PER_DEGREE * Math.cos(29.95 * Math.PI / 180);
export function distance(a: StudyPoint, b: StudyPoint): number {
  return Math.hypot((a.lat - b.lat) * METERS_PER_DEGREE, (a.lon - b.lon) * LONGITUDE_SCALE);
}
export function pathLength(points: StudyPoint[]): number {
  return points.slice(1).reduce((sum, p, i) => sum + distance(points[i], p), 0);
}
export function projectSegment(point: StudyPoint, a: StudyPoint, b: StudyPoint) {
  const dx = (b.lon - a.lon) * LONGITUDE_SCALE, dy = (b.lat - a.lat) * METERS_PER_DEGREE;
  const px = (point.lon - a.lon) * LONGITUDE_SCALE, py = (point.lat - a.lat) * METERS_PER_DEGREE;
  const length = Math.hypot(dx, dy);
  const t = length ? Math.max(0, Math.min(1, (px * dx + py * dy) / length ** 2)) : 0;
  return { distance: Math.hypot(px - t * dx, py - t * dy), along: length * t, length };
}
/** Reject repeated track/loop positions instead of choosing a convenient anchor. */
export function projectPath(path: StudyPath, point: StudyPoint, tolerance = 40): { position: number; distance: number } | null {
  let offset = 0;
  const options: Array<{ position: number; distance: number }> = [];
  for (let i = 1; i < path.points.length; i++) {
    const p = projectSegment(point, path.points[i - 1], path.points[i]);
    if (p.distance <= tolerance) options.push({ position: offset + p.along, distance: p.distance });
    offset += p.length;
  }
  options.sort((a, b) => a.distance - b.distance || a.position - b.position);
  const best = options[0];
  if (!best || options.some(p => p.distance <= best.distance + 8 && Math.abs(p.position - best.position) > 50)) return null;
  return best;
}
/** Intersections of the whole route polyline with the feature radius. */
export function featureRanges(path: StudyPath, point: StudyPoint, radius = 40): Array<[number, number]> {
  let offset = 0;
  const ranges: Array<[number, number]> = [];
  for (let i = 1; i < path.points.length; i++) {
    const a = path.points[i - 1], b = path.points[i];
    const dx = (b.lon - a.lon) * LONGITUDE_SCALE, dy = (b.lat - a.lat) * METERS_PER_DEGREE, length = Math.hypot(dx, dy);
    if (!length) continue;
    const px = (point.lon - a.lon) * LONGITUDE_SCALE, py = (point.lat - a.lat) * METERS_PER_DEGREE;
    const along = (px * dx + py * dy) / length, perpendicular = Math.max(0, px * px + py * py - along * along);
    if (perpendicular <= radius * radius) {
      const reach = Math.sqrt(radius * radius - perpendicular), from = Math.max(0, along - reach), to = Math.min(length, along + reach);
      if (from <= to) ranges.push([offset + from, offset + to]);
    }
    offset += length;
  }
  return ranges;
}
