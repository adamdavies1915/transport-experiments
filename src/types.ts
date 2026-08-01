// Shared domain types for the NOLA transit scraper.

export type SegmentType = 'mixed_traffic' | 'dedicated_row';

export interface Segment {
  id: number;
  route: string;
  name: string;
  type: SegmentType;
  min_lat: number;
  max_lat: number;
  min_lon: number;
  max_lon: number;
  order: number;
}

// Result of matching a coordinate to a route segment.
export interface SegmentMatch {
  segment_id: number | null;
  segment_name: string | null;
  segment_type: SegmentType | null;
}

// A single vehicle entry as received in the SSE payload (raw string fields).
export interface RawVehicle {
  vid: string;
  tmstmp: string;
  lat: string;
  lon: string;
  hdg?: string;
  rt: string;
  tatripid?: string;
  des?: string;
  spd?: string;
  dly?: boolean;
  or?: boolean;
}

// A processed record ready to be inserted into MotherDuck.
export interface TransitRecord extends SegmentMatch {
  vid: string;
  timestamp: string;
  lat: number;
  lon: number;
  heading: number;
  route: string;
  trip_id: string | null;
  destination: string | null;
  speed: number;
  is_delayed: boolean;
  is_off_route: boolean;
}
