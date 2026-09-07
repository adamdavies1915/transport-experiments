// Aggregate direct IDs, observed mappings, and labelled recurring sequence inference.
// Nearest-window block guesses remain diagnostics; inferred counts can be excluded by the UI.
export function otpDaysSql(database: string): string {
  const catalog = `"${database.replace(/"/g, '""')}"`;
  return `
      WITH valid_mappings AS (
        SELECT schedule_hash, route, legacy_id, service_id, MIN(gtfs_id) AS gtfs_id FROM ${catalog}.otp_trip_mappings
        GROUP BY schedule_hash, route, legacy_id, service_id HAVING COUNT(DISTINCT gtfs_id) = 1
      ), valid_sequences AS (
        SELECT s.schedule_hash, s.route, s.legacy_id, s.service_id, MIN(s.gtfs_id) AS gtfs_id
        FROM ${catalog}.otp_sequence_mappings s
        GROUP BY s.schedule_hash, s.route, s.legacy_id, s.service_id
        HAVING COUNT(DISTINCT s.gtfs_id) = 1 AND MAX(s.evidence_count) >= 2
          AND NOT EXISTS (SELECT 1 FROM ${catalog}.otp_trip_mappings p
            WHERE p.schedule_hash = s.schedule_hash AND p.route = s.route
              AND p.legacy_id = s.legacy_id AND p.service_id = s.service_id AND p.gtfs_id <> MIN(s.gtfs_id))
      ), accepted_events AS (
        SELECT e.* FROM ${catalog}.otp_events e LEFT JOIN valid_mappings m
          ON e.schedule_hash = m.schedule_hash AND e.route = m.route
          AND e.mapping_legacy_id = m.legacy_id AND e.trip_id = m.gtfs_id
        LEFT JOIN valid_sequences s ON e.schedule_hash = s.schedule_hash AND e.route = s.route
          AND e.mapping_legacy_id = s.legacy_id AND e.trip_id = s.gtfs_id
        WHERE e.method = 'timepoint-departure-v2'
          AND (e.match_method <> 'crosswalk' OR m.gtfs_id IS NOT NULL)
          AND (e.match_method <> 'sequence' OR s.gtfs_id IS NOT NULL)
      ), events AS (
        SELECT service_date, route,
          COUNT(*) FILTER (WHERE status = 'early' AND match_method IN ('trip_id', 'crosswalk', 'sequence'))::INTEGER AS early,
          COUNT(*) FILTER (WHERE status = 'on_time' AND match_method IN ('trip_id', 'crosswalk', 'sequence'))::INTEGER AS on_time,
          COUNT(*) FILTER (WHERE status = 'late' AND match_method IN ('trip_id', 'crosswalk', 'sequence'))::INTEGER AS late,
          COUNT(*) FILTER (WHERE status = 'uncertain' AND match_method IN ('trip_id', 'crosswalk', 'sequence'))::INTEGER AS uncertain,
          COUNT(*) FILTER (WHERE match_method = 'block')::INTEGER AS block_events,
          COUNT(*) FILTER (WHERE match_method = 'crosswalk')::INTEGER AS crosswalk_events,
          COUNT(*) FILTER (WHERE match_method = 'sequence')::INTEGER AS sequence_events,
          COUNT(*) FILTER (WHERE match_method = 'sequence' AND status = 'early')::INTEGER AS sequence_early,
          COUNT(*) FILTER (WHERE match_method = 'sequence' AND status = 'on_time')::INTEGER AS sequence_on_time,
          COUNT(*) FILTER (WHERE match_method = 'sequence' AND status = 'late')::INTEGER AS sequence_late,
          COUNT(*) FILTER (WHERE match_method = 'sequence' AND status = 'uncertain')::INTEGER AS sequence_uncertain
        FROM accepted_events
        GROUP BY service_date, route
      )
      SELECT CAST(c.service_date AS VARCHAR) AS date, c.route,
        c.scheduled_timepoints AS scheduled,
        COALESCE(e.early + e.on_time + e.late + e.uncertain, 0) AS observed,
        COALESCE(e.early + e.on_time + e.late, 0) AS classified,
        COALESCE(e.early, 0)::INTEGER AS early, COALESCE(e.on_time, 0)::INTEGER AS on_time,
        COALESCE(e.late, 0)::INTEGER AS late, COALESCE(e.uncertain, 0)::INTEGER AS uncertain,
        COALESCE(e.block_events, 0)::INTEGER AS block_events,
        COALESCE(e.crosswalk_events, 0)::INTEGER AS crosswalk_events,
        COALESCE(e.sequence_events, 0)::INTEGER AS sequence_events,
        COALESCE(e.sequence_early, 0)::INTEGER AS sequence_early,
        COALESCE(e.sequence_on_time, 0)::INTEGER AS sequence_on_time,
        COALESCE(e.sequence_late, 0)::INTEGER AS sequence_late,
        COALESCE(e.sequence_uncertain, 0)::INTEGER AS sequence_uncertain,
        c.observed_trips, c.matched_trips, c.block_matched_trips,
        CAST(c.updated_at AS VARCHAR) AS updated_at
      FROM ${catalog}.otp_coverage c LEFT JOIN events e USING(service_date, route)
      WHERE c.method = 'timepoint-departure-v2'
      ORDER BY c.service_date, c.route
    `;
}
