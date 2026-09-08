/** Source clocks are UTC epoch seconds. Null is evidence of missing data, never zero. */
export interface StudyObservation {
  source: 'sse' | 'lepass';
  observation_id: string;
  vehicle_id: string | null;
  provider_vehicle_id: string | null;
  route_id: string | null;
  trip_id: string | null;
  observed_at: number | null;
  received_at: number;
  lat: number | null;
  lon: number | null;
  speed_mph: number | null;
  off_route: boolean;
  in_service?: boolean | null;
  location_source: 'provider_gps' | 'estimated' | 'unknown';
  timestamp_precision_seconds: number;
  direction_id: string | null;
  pattern_id: string | null;
  mapping_confidence: 'verified' | 'candidate' | 'unknown';
}

export interface CollectedObservation { observation: StudyObservation; raw: Record<string, unknown>; }
export interface CollectionBatch {
  schema_version: 1;
  batch_id: string;
  source: StudyObservation['source'];
  received_at: string;
  observations: CollectedObservation[];
  predictions?: unknown[];
  provenance?: Record<string, unknown>;
}
