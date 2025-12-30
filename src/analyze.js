import {
  getDelaysBySegmentType,
  getDelaysBySegment,
  getDelaysByTimeOfDay,
  getDelayStats,
  pool
} from './db.js';

async function runAnalysis() {
  console.log('\n========================================');
  console.log('NOLA STREETCAR DELAY ANALYSIS');
  console.log('========================================\n');

  // 1. Overall delay comparison: Mixed Traffic vs Dedicated ROW
  console.log('📊 DELAYS BY SEGMENT TYPE (Mixed Traffic vs Dedicated Right-of-Way)');
  console.log('─'.repeat(60));
  const segmentTypeStats = await getDelaysBySegmentType('12', 168); // Last 7 days

  if (segmentTypeStats.length === 0) {
    console.log('No data yet. Let the scraper run for a few hours to collect data.\n');
  } else {
    console.table(segmentTypeStats);

    const mixed = segmentTypeStats.find(s => s.segment_type === 'mixed_traffic');
    const dedicated = segmentTypeStats.find(s => s.segment_type === 'dedicated_row');

    if (mixed && dedicated) {
      const difference = (parseFloat(mixed.delay_percentage) - parseFloat(dedicated.delay_percentage)).toFixed(2);
      console.log(`\n🔍 Finding: Streetcars in mixed traffic are delayed ${difference}% more often than in dedicated right-of-way\n`);
    }
  }

  // 2. Detailed breakdown by segment
  console.log('\n📍 DELAYS BY SPECIFIC SEGMENT');
  console.log('─'.repeat(60));
  const segmentStats = await getDelaysBySegment('12', 168);
  if (segmentStats.length > 0) {
    console.table(segmentStats.map(s => ({
      segment: s.segment_name,
      type: s.segment_type,
      readings: s.total_readings,
      delayed_pct: `${s.delay_percentage}%`,
      avg_speed: s.avg_speed
    })));
  } else {
    console.log('No data yet.\n');
  }

  // 3. Time of day analysis
  console.log('\n⏰ DELAYS BY TIME OF DAY');
  console.log('─'.repeat(60));
  const timeStats = await getDelaysByTimeOfDay('12', 168);
  if (timeStats.length > 0) {
    // Group by hour for display
    const byHour = {};
    for (const row of timeStats) {
      const hour = parseInt(row.hour_of_day);
      if (!byHour[hour]) byHour[hour] = {};
      byHour[hour][row.segment_type] = row.delay_percentage;
    }

    console.log('Hour  | Mixed Traffic | Dedicated ROW | Difference');
    console.log('─'.repeat(55));
    for (let hour = 0; hour < 24; hour++) {
      if (byHour[hour]) {
        const mixed = byHour[hour].mixed_traffic || '-';
        const dedicated = byHour[hour].dedicated_row || '-';
        const diff = (mixed !== '-' && dedicated !== '-')
          ? (parseFloat(mixed) - parseFloat(dedicated)).toFixed(1)
          : '-';
        const hourStr = `${hour.toString().padStart(2, '0')}:00`;
        console.log(`${hourStr} | ${String(mixed).padStart(13)}% | ${String(dedicated).padStart(13)}% | ${diff}%`);
      }
    }
  } else {
    console.log('No data yet.\n');
  }

  // 4. Overall route stats
  console.log('\n\n📈 ALL ROUTES DELAY STATS (Last 24 hours)');
  console.log('─'.repeat(60));
  const allStats = await getDelayStats(null, 24);
  if (allStats.length > 0) {
    console.table(allStats.slice(0, 10)); // Top 10
  }

  await pool.end();
}

runAnalysis().catch(err => {
  console.error('Analysis failed:', err);
  process.exit(1);
});
