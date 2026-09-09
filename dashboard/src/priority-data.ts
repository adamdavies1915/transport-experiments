import type { ExposureCategory, StreetcarNetwork } from './streetcar-data';
export const PRIORITY_WAIT_METHOD = 'streetcar-receipt-waits-v2';

export interface PriorityWindow {
  id: string; path_id: string; route: string; direction: string;
  from_meters: number; to_meters: number; category: ExposureCategory;
  signal_ids: string[]; stop_ids: string[]; name: string;
  passages: number; days: number; eligible: boolean;
  observed_seconds: number; baseline_seconds: number | null;
  extra_seconds: number | null; extra_p10_seconds: number | null; extra_p30_seconds: number | null;
}
export interface PriorityProfile {
  path_id: string; route: string; direction: string; headsign: string;
  route_meters: number; covered_meters: number; coverage_pct: number;
  eligible_windows: number; total_windows_observed: number; passages: number;
  observed_seconds: number | null; baseline_seconds: number | null;
  signal_only_extra_seconds: number | null; mixed_extra_seconds: number | null;
  other_extra_seconds: number | null; windows: PriorityWindow[];
}
export interface PriorityWaitSite {
  site_id: string; name: string; context: 'signal_only' | 'stop_only' | 'both';
  events: number; total_seconds: number; mean_seconds: number;
}
export interface PriorityWaitData {
  status: 'collecting' | 'ready'; snapshots: number; from: string | null; to: string | null;
  events: number; signal_only_seconds: number; mixed_seconds: number; stop_only_seconds: number;
  sites: PriorityWaitSite[]; clock: 'collector_receipt';
}
export interface PriorityData {
  /** Historical profiles have a fixed range; their quantiles cannot be re-filtered. */
  snapshot_only?: boolean;
  status: 'ready' | 'not_ready'; network: StreetcarNetwork;
  profiles: PriorityProfile[]; waits: PriorityWaitData;
  available_from: string | null; available_to: string | null;
  selected_from: string | null; selected_to: string | null; updated_at: string | null;
  processed_days?: number; selected_days?: number;
  method: { name: string; baseline_percentile: number; min_passages: number; min_days: number;
    time_band_hours: number; timestamp_precision_seconds: number; limitations: string[] };
}
export const PRIORITY_METHOD: PriorityData['method'] = {
  name: 'streetcar-priority-same-window-v1', baseline_percentile: 20,
  min_passages: 20, min_days: 3, time_band_hours: 4, timestamp_precision_seconds: 60,
  limitations: [
    'Extra time compares each fixed track window with its own faster passages, in the same direction, weekday/weekend and four-hour band. The 20th percentile is a benchmark, not an observed green signal.',
    'Extra time can include boarding, queues, variable driving and GPS timing error. Its signal-related share is unknown. Priority savings are user-controlled scenarios, not a measured treatment effect.',
    'Priority can extend an existing green or bring green forward, subject to signal-operation constraints. Recovering 100% of signal delay is a hypothetical limit.',
    'Times describe one traversal of the covered track windows. Gaps, terminals and unverified rail controls are omitted; totals are not complete end-to-end journey times.',
    'Profile totals sum averages from eligible windows and time bands. They do not represent one recorded journey or every selected hour; sparse time bands are omitted.',
    'Current vehicle timestamps have minute precision. Keeping more SSE snapshots preserves position changes but does not create second-resolution GPS fix times.',
    'Stationary episodes require fresh repeated positions and reported zero speed, bracketed by movement. Positive or missing speeds are excluded. Durations use collector receipt time; a nearby light does not prove that it was red.',
  ],
};
export function priorityScenario(profile: PriorityProfile, signalSharePercent: number, recoveryPercent: number, includeStopOverlap: boolean) {
  const share=Number.isFinite(signalSharePercent)?Math.max(0,Math.min(100,signalSharePercent))/100:0;
  const recovery=Number.isFinite(recoveryPercent)?Math.max(0,Math.min(100,recoveryPercent))/100:0;
  if(profile.observed_seconds==null || profile.signal_only_extra_seconds==null || profile.mixed_extra_seconds==null)
    return {saved_seconds:null,after_seconds:null,reduction_pct:null};
  const candidate=profile.signal_only_extra_seconds+(includeStopOverlap?profile.mixed_extra_seconds:0);
  const saved=Math.min(profile.observed_seconds,candidate*share*recovery);
  return {saved_seconds:saved,after_seconds:profile.observed_seconds-saved,
    reduction_pct:profile.observed_seconds>0?100*saved/profile.observed_seconds:0};
}
