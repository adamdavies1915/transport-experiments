import type { RowStudyData, SignalStudyData } from '../../src/transit-study-types';
import type { OtpData } from './otp-data';
import type { PriorityData } from './priority-data';
import type { StreetcarData } from './streetcar-data';
import type { DailyRow, DailySegmentRow, HourlyRow, RouteRow, SegmentRow, SegmentTypeRow, Summary } from './types';

export interface SourceQualityEntry {
  id: string;
  label: string;
  status: 'ready' | 'collecting' | 'degraded' | 'unavailable';
  /** Latest durable receipt/provider high-water marks; may be newer than the saved analysis. */
  last_received_at?: string | null;
  last_provider_at?: string | null;
  /** Inventory at summary publication, not live receipt counts or current study sample size. */
  observations?: number;
  from?: string | null;
  to?: string | null;
  message?: string;
}
export interface SourceQualityData {
  status: 'ready' | 'collecting' | 'degraded';
  sources: SourceQualityEntry[];
  archive?: { last_uploaded_at?: string | null; pending_records?: number; storage_bytes?: number; budget_bytes?: number };
  limitations?: string[];
  snapshot?: SummarySnapshotStatus;
}
export interface LegacySummary {
  summary?: Summary;
  segment_types?: SegmentTypeRow[];
  segments?: SegmentRow[];
  routes?: RouteRow[];
  hourly?: HourlyRow[];
  daily?: DailyRow[];
  daily_segments?: DailySegmentRow[];
  daily_routes?: Array<{ date: string; route: string; readings: number; avg_speed: number; not_flagged_pct: number }>;
  otp?: OtpData;
  streetcars?: Record<string, StreetcarData>;
  streetcar_priority?: Record<string, PriorityData>;
  errors?: Record<string, string>;
}
/** Collector-produced public data only. Authentication stays outside this envelope. */
export interface TransitSummaryEnvelope {
  schema_version: 1;
  generated_at: string;
  source_quality?: SourceQualityData;
  row_study?: RowStudyData;
  signal_study?: SignalStudyData;
  legacy?: LegacySummary;
}
export interface SummarySnapshotStatus {
  generated_at: string | null;
  received_at: string | null;
  origin: 'collector' | 'disk' | 'none';
  stale: boolean;
  refresh_error: string | null;
}
