// Shapes returned by the dashboard API (server-postgres.ts) and consumed by the
// React app. These mirror the pre-computed Postgres aggregate tables.

export interface Summary {
  total_records: number;
  total_routes: number;
  total_vehicles: number;
  first_record: string;
  last_record: string;
}

export interface SegmentTypeRow {
  segment_type: string;
  readings: number;
  delayed: number;
  delay_pct: number;
  avg_speed: number;
}

export interface SegmentRow {
  segment_name: string;
  segment_type: string;
  readings: number;
  delay_pct: number;
  avg_speed: number;
}

export interface RouteRow {
  route: string;
  readings: number;
  delayed: number;
  delay_pct: number;
  on_time_pct: number;
  avg_speed: number;
}

export interface HourlyRow {
  hour: number;
  segment_type: string;
  delay_pct: number;
  avg_speed: number;
}

export interface DailyRow {
  date: string;
  readings: number;
  vehicles: number;
  delayed: number;
  on_time_pct: number;
  avg_speed: number;
}

export interface DailySegmentRow {
  date: string;
  segment_type: string;
  readings: number;
  on_time_pct: number;
  avg_speed: number;
}

// Aggregated payload assembled by the useTransitData hook.
export interface TransitData {
  summary: Summary;
  segmentType: SegmentTypeRow[];
  segments: SegmentRow[];
  routes: RouteRow[];
  hourly: HourlyRow[];
  daily: DailyRow[];
  dailySegments: DailySegmentRow[];
}
