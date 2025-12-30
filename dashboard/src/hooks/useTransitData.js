import { useState, useEffect } from 'react';

const API_BASE = import.meta.env.DEV ? 'http://localhost:3000/api' : '/api';

export function useTransitData() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    async function fetchData() {
      try {
        const [summary, segmentTypes, segments, routes, hourly] = await Promise.all([
          fetch(`${API_BASE}/summary`).then(r => r.json()),
          fetch(`${API_BASE}/segment-types`).then(r => r.json()),
          fetch(`${API_BASE}/segments`).then(r => r.json()),
          fetch(`${API_BASE}/routes`).then(r => r.json()),
          fetch(`${API_BASE}/hourly`).then(r => r.json())
        ]);

        if (summary.error) throw new Error(summary.error);

        setData({
          summary,
          segmentType: segmentTypes,
          segments,
          routes,
          hourly
        });
        setLoading(false);
      } catch (err) {
        console.error('API error:', err);
        setError(err.message);
        setLoading(false);
      }
    }

    fetchData();
  }, []);

  return { data, loading, error };
}
