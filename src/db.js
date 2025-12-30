import pg from 'pg';

const { Pool } = pg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 10,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000,
});

pool.on('error', (err) => {
  console.error('Unexpected database error:', err);
});

export async function initializeDatabase() {
  const client = await pool.connect();
  try {
    await client.query(`
      -- Segment types for analysis
      DO $$ BEGIN
        CREATE TYPE segment_type AS ENUM ('dedicated_row', 'mixed_traffic');
      EXCEPTION
        WHEN duplicate_object THEN null;
      END $$;

      -- Route segments define geographic zones along routes
      CREATE TABLE IF NOT EXISTS route_segments (
        id SERIAL PRIMARY KEY,
        route VARCHAR(20) NOT NULL,
        name VARCHAR(100) NOT NULL,
        segment_type segment_type NOT NULL,
        -- Bounding box for the segment
        min_lat DECIMAL(10, 8) NOT NULL,
        max_lat DECIMAL(10, 8) NOT NULL,
        min_lon DECIMAL(11, 8) NOT NULL,
        max_lon DECIMAL(11, 8) NOT NULL,
        -- Optional: order along route for sequencing
        sequence_order INTEGER DEFAULT 0
      );

      CREATE TABLE IF NOT EXISTS vehicle_positions (
        id BIGSERIAL PRIMARY KEY,
        vid VARCHAR(20) NOT NULL,
        timestamp TIMESTAMPTZ NOT NULL,
        server_timestamp TIMESTAMPTZ NOT NULL,
        lat DECIMAL(10, 8) NOT NULL,
        lon DECIMAL(11, 8) NOT NULL,
        heading INTEGER,
        route VARCHAR(20),
        trip_id VARCHAR(30),
        block_id VARCHAR(30),
        destination VARCHAR(255),
        pattern_distance INTEGER,
        pattern_id INTEGER,
        speed INTEGER,
        is_delayed BOOLEAN DEFAULT FALSE,
        is_off_route BOOLEAN DEFAULT FALSE,
        segment_id INTEGER REFERENCES route_segments(id),
        created_at TIMESTAMPTZ DEFAULT NOW()
      );

      CREATE INDEX IF NOT EXISTS idx_vehicle_positions_vid ON vehicle_positions(vid);
      CREATE INDEX IF NOT EXISTS idx_vehicle_positions_timestamp ON vehicle_positions(timestamp);
      CREATE INDEX IF NOT EXISTS idx_vehicle_positions_route ON vehicle_positions(route);
      CREATE INDEX IF NOT EXISTS idx_vehicle_positions_delayed ON vehicle_positions(is_delayed) WHERE is_delayed = TRUE;
      CREATE INDEX IF NOT EXISTS idx_vehicle_positions_vid_timestamp ON vehicle_positions(vid, timestamp DESC);
      CREATE INDEX IF NOT EXISTS idx_vehicle_positions_segment ON vehicle_positions(segment_id);
      CREATE INDEX IF NOT EXISTS idx_route_segments_route ON route_segments(route);
    `);

    // Seed St. Charles streetcar segments if not exists
    await seedStreetcarSegments(client);

    console.log('Database initialized successfully');
  } finally {
    client.release();
  }
}

async function seedStreetcarSegments(client) {
  // Check if segments already exist
  const existing = await client.query(`SELECT COUNT(*) FROM route_segments WHERE route = '12'`);
  if (parseInt(existing.rows[0].count) > 0) return;

  // St. Charles Streetcar (Route 12) segments
  // These are approximate bounding boxes - you may want to refine these
  const segments = [
    // Canal Street - Mixed traffic (CBD to Cemeteries)
    {
      route: '12',
      name: 'Canal Street (CBD)',
      type: 'mixed_traffic',
      min_lat: 29.9495, max_lat: 29.9650,
      min_lon: -90.0800, max_lon: -90.0650,
      order: 1
    },
    // St. Charles - Lee Circle area (transitional, some mixed)
    {
      route: '12',
      name: 'Lee Circle / Downtown',
      type: 'mixed_traffic',
      min_lat: 29.9430, max_lat: 29.9495,
      min_lon: -90.0820, max_lon: -90.0700,
      order: 2
    },
    // St. Charles Ave - Lower Garden District (dedicated neutral ground)
    {
      route: '12',
      name: 'St. Charles - Lower Garden District',
      type: 'dedicated_row',
      min_lat: 29.9250, max_lat: 29.9430,
      min_lon: -90.0900, max_lon: -90.0750,
      order: 3
    },
    // St. Charles Ave - Garden District (dedicated neutral ground)
    {
      route: '12',
      name: 'St. Charles - Garden District',
      type: 'dedicated_row',
      min_lat: 29.9150, max_lat: 29.9250,
      min_lon: -90.1050, max_lon: -90.0900,
      order: 4
    },
    // St. Charles Ave - Uptown (dedicated neutral ground)
    {
      route: '12',
      name: 'St. Charles - Uptown',
      type: 'dedicated_row',
      min_lat: 29.9150, max_lat: 29.9350,
      min_lon: -90.1300, max_lon: -90.1050,
      order: 5
    },
    // Carrollton Ave - Riverbend (dedicated, curves to Carrollton)
    {
      route: '12',
      name: 'Carrollton - Riverbend',
      type: 'dedicated_row',
      min_lat: 29.9350, max_lat: 29.9550,
      min_lon: -90.1400, max_lon: -90.1250,
      order: 6
    },
    // S. Carrollton Ave (dedicated neutral ground to Claiborne)
    {
      route: '12',
      name: 'S. Carrollton Ave',
      type: 'dedicated_row',
      min_lat: 29.9550, max_lat: 29.9750,
      min_lon: -90.1350, max_lon: -90.1200,
      order: 7
    }
  ];

  for (const seg of segments) {
    await client.query(`
      INSERT INTO route_segments (route, name, segment_type, min_lat, max_lat, min_lon, max_lon, sequence_order)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
    `, [seg.route, seg.name, seg.type, seg.min_lat, seg.max_lat, seg.min_lon, seg.max_lon, seg.order]);
  }

  console.log('Seeded St. Charles streetcar segments');
}

// Cache segments in memory to avoid repeated queries
let segmentsCache = null;

async function loadSegments(client) {
  if (segmentsCache) return segmentsCache;

  const result = await client.query(`SELECT * FROM route_segments`);
  segmentsCache = result.rows;
  console.log(`Loaded ${segmentsCache.length} route segments into cache`);
  return segmentsCache;
}

function findSegment(segments, route, lat, lon) {
  for (const seg of segments) {
    if (seg.route !== route) continue;
    if (lat >= parseFloat(seg.min_lat) && lat <= parseFloat(seg.max_lat) &&
        lon >= parseFloat(seg.min_lon) && lon <= parseFloat(seg.max_lon)) {
      return seg.id;
    }
  }
  return null;
}

export async function insertVehiclePositions(vehicles) {
  if (!vehicles || vehicles.length === 0) return;

  const client = await pool.connect();
  try {
    const segments = await loadSegments(client);
    const values = [];
    const placeholders = [];
    let paramIndex = 1;

    for (const v of vehicles) {
      // Skip vehicles with invalid coordinates
      if (v.lat === '0' && v.lon === '0') continue;

      const lat = parseFloat(v.lat);
      const lon = parseFloat(v.lon);
      const segmentId = findSegment(segments, v.rt, lat, lon);

      placeholders.push(`($${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++})`);
      values.push(
        v.vid,
        v.tmstmp,
        v.srvtmstmp,
        lat,
        lon,
        parseInt(v.hdg) || 0,
        v.rt,
        v.tatripid,
        v.tablockid,
        v.des || null,
        parseInt(v.pdist) || 0,
        parseInt(v.pid) || null,
        parseInt(v.spd) || 0,
        v.dly === true,
        v.or === true,
        segmentId
      );
    }

    if (placeholders.length === 0) return;

    const query = `
      INSERT INTO vehicle_positions
        (vid, timestamp, server_timestamp, lat, lon, heading, route, trip_id, block_id, destination, pattern_distance, pattern_id, speed, is_delayed, is_off_route, segment_id)
      VALUES ${placeholders.join(', ')}
    `;

    await client.query(query, values);
    console.log(`Inserted ${placeholders.length} vehicle positions`);
  } finally {
    client.release();
  }
}

export async function getDelayStats(routeFilter = null, hoursBack = 24) {
  const client = await pool.connect();
  try {
    const query = `
      SELECT
        route,
        COUNT(*) as total_readings,
        SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) as delayed_readings,
        ROUND(100.0 * SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) / COUNT(*), 2) as delay_percentage
      FROM vehicle_positions
      WHERE timestamp > NOW() - INTERVAL '${hoursBack} hours'
        ${routeFilter ? `AND route = $1` : ''}
      GROUP BY route
      ORDER BY delay_percentage DESC
    `;

    const result = await client.query(query, routeFilter ? [routeFilter] : []);
    return result.rows;
  } finally {
    client.release();
  }
}

// KEY ANALYSIS: Compare delays by segment type (mixed traffic vs dedicated ROW)
export async function getDelaysBySegmentType(route = '12', hoursBack = 24) {
  const client = await pool.connect();
  try {
    const result = await client.query(`
      SELECT
        rs.segment_type,
        COUNT(*) as total_readings,
        SUM(CASE WHEN vp.is_delayed THEN 1 ELSE 0 END) as delayed_readings,
        ROUND(100.0 * SUM(CASE WHEN vp.is_delayed THEN 1 ELSE 0 END) / COUNT(*), 2) as delay_percentage,
        ROUND(AVG(vp.speed), 1) as avg_speed
      FROM vehicle_positions vp
      JOIN route_segments rs ON vp.segment_id = rs.id
      WHERE vp.route = $1
        AND vp.timestamp > NOW() - INTERVAL '${hoursBack} hours'
      GROUP BY rs.segment_type
      ORDER BY delay_percentage DESC
    `, [route]);
    return result.rows;
  } finally {
    client.release();
  }
}

// Detailed breakdown by individual segment
export async function getDelaysBySegment(route = '12', hoursBack = 24) {
  const client = await pool.connect();
  try {
    const result = await client.query(`
      SELECT
        rs.name as segment_name,
        rs.segment_type,
        rs.sequence_order,
        COUNT(*) as total_readings,
        SUM(CASE WHEN vp.is_delayed THEN 1 ELSE 0 END) as delayed_readings,
        ROUND(100.0 * SUM(CASE WHEN vp.is_delayed THEN 1 ELSE 0 END) / COUNT(*), 2) as delay_percentage,
        ROUND(AVG(vp.speed), 1) as avg_speed,
        ROUND(AVG(CASE WHEN vp.is_delayed THEN vp.speed ELSE NULL END), 1) as avg_speed_when_delayed
      FROM vehicle_positions vp
      JOIN route_segments rs ON vp.segment_id = rs.id
      WHERE vp.route = $1
        AND vp.timestamp > NOW() - INTERVAL '${hoursBack} hours'
      GROUP BY rs.id, rs.name, rs.segment_type, rs.sequence_order
      ORDER BY rs.sequence_order
    `, [route]);
    return result.rows;
  } finally {
    client.release();
  }
}

// Time-of-day analysis: When are delays worst by segment type?
export async function getDelaysByTimeOfDay(route = '12', hoursBack = 168) {
  const client = await pool.connect();
  try {
    const result = await client.query(`
      SELECT
        EXTRACT(HOUR FROM vp.timestamp AT TIME ZONE 'America/Chicago') as hour_of_day,
        rs.segment_type,
        COUNT(*) as total_readings,
        ROUND(100.0 * SUM(CASE WHEN vp.is_delayed THEN 1 ELSE 0 END) / COUNT(*), 2) as delay_percentage
      FROM vehicle_positions vp
      JOIN route_segments rs ON vp.segment_id = rs.id
      WHERE vp.route = $1
        AND vp.timestamp > NOW() - INTERVAL '${hoursBack} hours'
      GROUP BY EXTRACT(HOUR FROM vp.timestamp AT TIME ZONE 'America/Chicago'), rs.segment_type
      ORDER BY hour_of_day, rs.segment_type
    `, [route]);
    return result.rows;
  } finally {
    client.release();
  }
}

export { pool };
