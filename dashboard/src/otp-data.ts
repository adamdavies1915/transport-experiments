export interface OtpDay {
  date: string;
  route: string;
  scheduled: number;
  observed: number;
  classified: number;
  early: number;
  on_time: number;
  late: number;
  uncertain: number;
  block_events: number;
  crosswalk_events: number;
  observed_trips: number;
  matched_trips: number;
  block_matched_trips: number;
  updated_at: string;
}
export interface OtpData {
  status: 'ready' | 'not_ready';
  days: OtpDay[];
}
export function percentage(numerator: number, denominator: number): number | null {
  return denominator > 0 ? Math.round(10000 * numerator / denominator) / 100 : null;
}
export function totals(days: OtpDay[]) {
  const result = { scheduled: 0, observed: 0, classified: 0, early: 0, on_time: 0,
    late: 0, uncertain: 0, block_events: 0, crosswalk_events: 0, observed_trips: 0, matched_trips: 0, block_matched_trips: 0 };
  for (const day of days) for (const key of Object.keys(result) as Array<keyof typeof result>) result[key] += Number(day[key]);
  return { ...result, on_time_pct: percentage(result.on_time, result.classified),
    coverage_pct: percentage(result.classified, result.scheduled) };
}
