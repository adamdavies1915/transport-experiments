import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import parquet from 'parquetjs';
import { createWriteStream, unlinkSync, readFileSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';

// Parquet schema for vehicle positions
const schema = new parquet.ParquetSchema({
  vid: { type: 'UTF8' },
  timestamp: { type: 'TIMESTAMP_MILLIS' },
  lat: { type: 'DOUBLE' },
  lon: { type: 'DOUBLE' },
  heading: { type: 'INT32' },
  route: { type: 'UTF8', optional: true },
  trip_id: { type: 'UTF8', optional: true },
  destination: { type: 'UTF8', optional: true },
  speed: { type: 'INT32' },
  is_delayed: { type: 'BOOLEAN' },
  is_off_route: { type: 'BOOLEAN' },
  segment_id: { type: 'INT32', optional: true },
  segment_name: { type: 'UTF8', optional: true },
  segment_type: { type: 'UTF8', optional: true }
});

// R2 client setup
const r2Client = new S3Client({
  region: 'auto',
  endpoint: `https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
  credentials: {
    accessKeyId: process.env.R2_ACCESS_KEY_ID,
    secretAccessKey: process.env.R2_SECRET_ACCESS_KEY
  }
});

const BUCKET = process.env.R2_BUCKET || 'nola-transit';

export async function writeParquetToR2(records) {
  if (!records || records.length === 0) return null;

  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
  const filename = `transit-${timestamp}.parquet`;
  const tempPath = join(tmpdir(), filename);

  try {
    // Write parquet file locally first
    const writer = await parquet.ParquetWriter.openFile(schema, tempPath);

    for (const record of records) {
      await writer.appendRow({
        vid: record.vid,
        timestamp: new Date(record.timestamp).getTime(),
        lat: record.lat,
        lon: record.lon,
        heading: record.heading,
        route: record.route || null,
        trip_id: record.trip_id || null,
        destination: record.destination || null,
        speed: record.speed,
        is_delayed: record.is_delayed,
        is_off_route: record.is_off_route,
        segment_id: record.segment_id || null,
        segment_name: record.segment_name || null,
        segment_type: record.segment_type || null
      });
    }

    await writer.close();

    // Upload to R2
    const fileBuffer = readFileSync(tempPath);
    const date = new Date();
    const key = `${date.getUTCFullYear()}/${String(date.getUTCMonth() + 1).padStart(2, '0')}/${filename}`;

    await r2Client.send(new PutObjectCommand({
      Bucket: BUCKET,
      Key: key,
      Body: fileBuffer,
      ContentType: 'application/octet-stream'
    }));

    // Cleanup temp file
    unlinkSync(tempPath);

    const sizeKB = Math.round(fileBuffer.length / 1024);
    console.log(`Uploaded ${key} (${sizeKB} KB, ${records.length} records)`);

    return key;
  } catch (err) {
    console.error('Error writing parquet to R2:', err.message);
    // Try to cleanup temp file on error
    try { unlinkSync(tempPath); } catch {}
    throw err;
  }
}
