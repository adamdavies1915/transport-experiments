/** JSON contracts shared by the offline study worker and the dashboard. */
export type StudySource = 'sse' | 'lepass';
export type StudyMode = 'streetcar' | 'bus';
export type RowClass = 'reserved' | 'shared' | 'unknown';
export type StudyContext = 'signal_only' | 'stop_only' | 'both' | 'neither';
export interface StudyPoint { lat: number; lon: number }
export interface StudyPath {
  id: string; route_id: string; direction_id: string; mode: StudyMode;
  name: string; points: StudyPoint[]; stop_ids: string[];
}
export interface StudySite extends StudyPoint {
  id: string; kind: 'signal' | 'stop' | 'rail_signal'; name: string;
  route_ids: string[]; path_ids?: string[]; source_ids: string[];
  verification: 'gtfs' | 'osm_unverified' | 'mapillary' | 'reviewed_control';
}
export interface StudyRowSection {
  id: string; path_id: string; from_meters: number; to_meters: number;
  row_class: RowClass; reviewed_at: string | null; evidence_urls: string[];
  /** Explicit historical applicability; a review date alone does not prove history. */
  valid_from: string | null; valid_to: string | null; notes: string;
}
export interface StudyCatalog {
  version: string; generated_at: string; schedule_hash: string;
  paths: StudyPath[]; sites: StudySite[]; row_sections: StudyRowSection[];
  sources: Array<{ name: string; url: string; fetched_at: string; attribution: string }>;
  limitations: string[];
}
export interface StudyDimensions {
  date: string; source: StudySource; mode: StudyMode; route_id: string;
  direction_id: string; path_id: string; hour: number;
  day_type: 'weekday' | 'weekend'; time_band: number;
}
export interface StudyPassage extends StudyDimensions {
  id: string; run_id: string; vehicle_id: string; trip_id: string | null;
  window_id: string; from_meters: number; to_meters: number; distance_meters: number;
  entry_at: number; exit_at: number; duration_seconds: number;
  duration_lower_seconds: number; duration_upper_seconds: number;
  timestamp_precision_seconds: number; row_class: RowClass; row_section_ids: string[];
  context: StudyContext; signal_ids: string[]; stop_ids: string[];
  network_version: string; method: string;
}
export interface StudyEncounter extends StudyDimensions {
  id: string; passage_id: string; run_id: string; vehicle_id: string;
  site_id: string; approach_id: string; context: 'signal_only' | 'both';
  control_verified: boolean; entry_at: number; exit_at: number;
  wait_status: 'detected' | 'not_detected' | 'insufficient_sampling';
  wait_events: number; wait_seconds: number; wait_lower_seconds: number; wait_upper_seconds: number;
  wait_clock: 'collector_receipt' | 'provider_sample';
  network_version: string; method: string;
}
export interface StudyQuality {
  date: string; source: StudySource; raw_observations: number; usable_observations: number;
  completed_passages: number; complete_encounters: number; excluded: Record<string, number>;
}
export interface TransitStudyEvents {
  passages: StudyPassage[]; encounters: StudyEncounter[]; quality: StudyQuality[];
}
export interface StudyFilters {
  from?: string; to?: string; source?: StudySource; mode?: StudyMode; route_id?: string;
  direction_id?: string; day_type?: 'weekday' | 'weekend'; hour_from?: number; hour_to?: number;
}
export interface StudyRowCell extends StudyDimensions {
  row_class: RowClass; context: StudyContext; signal_count: number; stop_count: number;
  passages: number; distance_meters: number; duration_seconds: number;
  duration_lower_seconds: number; duration_upper_seconds: number;
  /** Detailed daily archives retain these; bounded public cells may omit them. */
  window_ids?: string[]; run_ids?: string[];
}
export interface StudyRowComparison {
  id: string; source: StudySource; mode: StudyMode; route_id: string; direction_id: string;
  day_type: 'weekday' | 'weekend'; time_band: number; context: StudyContext;
  signal_count: number; stop_count: number; status: 'ready' | 'insufficient_data';
  reserved_passages: number; shared_passages: number; matched_dates: number;
  reserved_seconds_per_km: number | null; shared_seconds_per_km: number | null;
  reserved_speed_mph: number | null; shared_speed_mph: number | null;
  shared_extra_seconds_per_km: number | null;
  shared_extra_lower_seconds_per_km: number | null; shared_extra_upper_seconds_per_km: number | null;
  /** 95% service-date cluster bootstrap interval, separate from GPS timing bounds. */
  shared_extra_ci_lower_seconds_per_km: number | null; shared_extra_ci_upper_seconds_per_km: number | null;
}
export interface StudySignalCell extends StudyDimensions {
  site_id: string; approach_id: string; context: 'signal_only' | 'both'; control_verified: boolean;
  encounters: number; evaluable_encounters: number; detected_wait_encounters: number; wait_events: number;
  wait_seconds: number; wait_lower_seconds: number; wait_upper_seconds: number;
  run_ids?: string[];
}
export interface StudySignalSummary {
  source: StudySource; mode: StudyMode; route_id: string; direction_id: string;
  site_id: string; context: 'signal_only' | 'both'; encounters: number; evaluable_encounters: number;
  detected_wait_encounters: number; wait_events: number; wait_seconds: number;
  status: 'ready' | 'insufficient_data'; observed_dates: number; evaluable_dates: number;
  detected_wait_probability: number | null; mean_detected_wait_seconds: number | null;
  /** Detected time / all complete encounters; not total true signal delay. */
  detected_wait_seconds_per_encounter: number | null;
  detected_wait_seconds_per_encounter_ci_lower: number | null;
  detected_wait_seconds_per_encounter_ci_upper: number | null;
  recovery_seconds_per_encounter: { percent: 25 | 50 | 75; seconds: number | null }[];
}
export interface StudyEnvelope {
  status: 'ready' | 'collecting'; from: string | null; to: string | null;
  network: StudyCatalog; quality: StudyQuality[]; method: string; limitations: string[];
}
export interface RowStudyData extends StudyEnvelope {
  cells: StudyRowCell[]; comparisons: StudyRowComparison[];
  /** Public unknown-ROW coverage, grouped without inventing path or exposure metadata. */
  coverage_cells?: StudyRowCoverageCell[];
  coverage: { passages: number; classified_passages: number; unknown_passages: number };
}
export interface StudyRowCoverageCell extends Omit<StudyDimensions, 'path_id'> { passages: number }
export interface SignalStudyData extends StudyEnvelope {
  cells: StudySignalCell[]; signals: StudySignalSummary[];
}
