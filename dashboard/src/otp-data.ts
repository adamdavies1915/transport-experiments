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
  sequence_events: number;
  sequence_early: number;
  sequence_on_time: number;
  sequence_late: number;
  sequence_uncertain: number;
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
    late: 0, uncertain: 0, block_events: 0, crosswalk_events: 0, sequence_events: 0, sequence_early: 0, sequence_on_time: 0, sequence_late: 0, sequence_uncertain: 0, observed_trips: 0, matched_trips: 0, block_matched_trips: 0 };
  for (const day of days) for (const key of Object.keys(result) as Array<keyof typeof result>) result[key] += Number(day[key]);
  return { ...result, on_time_pct: percentage(result.on_time, result.classified),
    coverage_pct: percentage(result.classified, result.scheduled) };
}

export function observedIdsOnly(day: OtpDay): OtpDay {
  return { ...day, observed: day.observed - day.sequence_events,
    classified: day.classified - day.sequence_early - day.sequence_on_time - day.sequence_late,
    early: day.early - day.sequence_early, on_time: day.on_time - day.sequence_on_time,
    late: day.late - day.sequence_late, uncertain: day.uncertain - day.sequence_uncertain,
    sequence_events: 0, sequence_early: 0, sequence_on_time: 0, sequence_late: 0, sequence_uncertain: 0 };
}
