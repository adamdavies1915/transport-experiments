export type CorridorId = 'st_charles' | 'canal' | 'rampart';
export type ExposureCategory = 'signal_only' | 'stop_only' | 'both' | 'neither';
export interface GeoPoint { lat: number; lon: number }
export interface StreetcarSite extends GeoPoint {
  id: string; corridor: CorridorId; kind: 'signal' | 'stop' | 'rail_signal'; name: string;
  routes: string[]; source_ids: string[]; verification: 'osm_unverified' | 'gtfs' | 'mapillary';
}
export interface StreetcarPath {
  id: string; corridor: CorridorId; route: string; direction: string; headsign: string;
  points: GeoPoint[]; stop_ids: string[];
}
export interface StreetcarNetwork {
  version: string; generated_at: string; schedule_hash: string;
  corridors: Array<{ id: CorridorId; name: string; routes: string[] }>;
  paths: StreetcarPath[]; sites: StreetcarSite[];
  sources: Array<{ name: string; url: string; fetched_at: string; attribution: string }>;
  mapillary_status: string;
}
export interface StreetcarBin {
  date: string; corridor: CorridorId; route: string; direction: string; hour: number;
  day_type: 'weekday' | 'weekend'; category: ExposureCategory;
  intervals: number; duration_seconds: number; distance_meters: number; slow_seconds: number;
  duration_lower_seconds?: number | null; duration_upper_seconds?: number | null;
  vehicle_ids: string[];
}
export interface StreetcarSiteBin extends StreetcarBin { site_id: string }
/** One completed spatial window, before aggregation. Duration bounds use the
 * reported provider clock; consumers apply timestamp uncertainty once. */
export interface StreetcarPassage {
  date: string; corridor: CorridorId; route: string; direction: string;
  path_id: string; window_id: string; from_meters: number; to_meters: number;
  run_id: string; vid: string; trip_id: string | null;
  entry_at: number; exit_at: number; hour: number; day_type: 'weekday' | 'weekend';
  category: ExposureCategory; signal_ids: string[]; stop_ids: string[];
  duration_seconds: number; duration_lower_seconds: number; duration_upper_seconds: number;
}
export interface StreetcarQuality {
  date: string; corridor: CorridorId; raw_points: number; candidate_intervals: number;
  accepted_intervals: number; excluded: Record<string, number>;
}
export interface StreetcarData {
  available_from?: string | null; available_to?: string | null;
  selected_from?: string | null; selected_to?: string | null;
  status: 'ready' | 'not_ready'; network: StreetcarNetwork;
  bins: StreetcarBin[]; site_bins: StreetcarSiteBin[]; quality: StreetcarQuality[];
  updated_at: string | null;
  method: { name: string; max_gap_seconds: number; feature_radius_meters: number;
    window_meters?: number;
    timestamp_quantization_seconds?: number;
    track_tolerance_meters: number; terminal_radius_meters: number; slow_mph: number; limitations: string[] };
}
