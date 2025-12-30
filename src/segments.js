// St. Charles Streetcar (Route 12) segments
// These define geographic zones for mixed traffic vs dedicated right-of-way analysis

export const SEGMENTS = [
  {
    id: 1,
    route: '12',
    name: 'Canal Street (CBD)',
    type: 'mixed_traffic',
    min_lat: 29.9495, max_lat: 29.9650,
    min_lon: -90.0800, max_lon: -90.0650,
    order: 1
  },
  {
    id: 2,
    route: '12',
    name: 'Lee Circle / Downtown',
    type: 'mixed_traffic',
    min_lat: 29.9430, max_lat: 29.9495,
    min_lon: -90.0820, max_lon: -90.0700,
    order: 2
  },
  {
    id: 3,
    route: '12',
    name: 'St. Charles - Lower Garden District',
    type: 'dedicated_row',
    min_lat: 29.9250, max_lat: 29.9430,
    min_lon: -90.0900, max_lon: -90.0750,
    order: 3
  },
  {
    id: 4,
    route: '12',
    name: 'St. Charles - Garden District',
    type: 'dedicated_row',
    min_lat: 29.9150, max_lat: 29.9250,
    min_lon: -90.1050, max_lon: -90.0900,
    order: 4
  },
  {
    id: 5,
    route: '12',
    name: 'St. Charles - Uptown',
    type: 'dedicated_row',
    min_lat: 29.9150, max_lat: 29.9350,
    min_lon: -90.1300, max_lon: -90.1050,
    order: 5
  },
  {
    id: 6,
    route: '12',
    name: 'Carrollton - Riverbend',
    type: 'dedicated_row',
    min_lat: 29.9350, max_lat: 29.9550,
    min_lon: -90.1400, max_lon: -90.1250,
    order: 6
  },
  {
    id: 7,
    route: '12',
    name: 'S. Carrollton Ave',
    type: 'dedicated_row',
    min_lat: 29.9550, max_lat: 29.9750,
    min_lon: -90.1350, max_lon: -90.1200,
    order: 7
  }
];

export function findSegment(route, lat, lon) {
  for (const seg of SEGMENTS) {
    if (seg.route !== route) continue;
    if (lat >= seg.min_lat && lat <= seg.max_lat &&
        lon >= seg.min_lon && lon <= seg.max_lon) {
      return {
        segment_id: seg.id,
        segment_name: seg.name,
        segment_type: seg.type
      };
    }
  }
  return {
    segment_id: null,
    segment_name: null,
    segment_type: null
  };
}
