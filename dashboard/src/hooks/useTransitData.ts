import { useState, useEffect } from 'react';
import type {
  TransitData, Summary, SegmentTypeRow, SegmentRow, RouteRow,
  HourlyRow, DailyRow, DailySegmentRow
} from '../types';
import type { OtpData } from '../otp-data';

const API_BASE = '/api';

interface UseTransitDataResult {
  data: TransitData | null;
  loading: boolean;
  error: string | null;
}

async function getJSON<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`);
  if (!res.ok) throw new Error(`Transit data request failed (${res.status})`);
  return res.json() as Promise<T>;
}

export function useTransitData(): UseTransitDataResult {
  const [data, setData] = useState<TransitData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    async function fetchData(): Promise<void> {
      try {
        const [summary, segmentTypes, segments, routes, hourly, daily, dailySegments, otp] = await Promise.all([
          getJSON<Summary & { error?: string }>('/summary'),
          getJSON<SegmentTypeRow[]>('/segment-types'),
          getJSON<SegmentRow[]>('/segments'),
          getJSON<RouteRow[]>('/routes'),
          getJSON<HourlyRow[]>('/hourly'),
          getJSON<DailyRow[]>('/daily'),
          getJSON<DailySegmentRow[]>('/daily-segments'),
          getJSON<OtpData>('/otp')
        ]);

        if (summary.error) throw new Error(summary.error);

        setData({
          otp,
          summary,
          segmentType: segmentTypes,
          segments,
          routes,
          hourly,
          daily,
          dailySegments
        });
        setLoading(false);
      } catch (err) {
        console.error('API error:', err);
        setError(err instanceof Error ? err.message : String(err));
        setLoading(false);
      }
    }

    fetchData();
  }, []);

  return { data, loading, error };
}
