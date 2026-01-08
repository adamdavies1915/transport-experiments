import 'dotenv/config';
import { S3Client, ListObjectsV2Command, GetObjectCommand, PutObjectCommand, DeleteObjectsCommand } from '@aws-sdk/client-s3';
import parquet from 'parquetjs';
import { Writable } from 'stream';
import fs from 'fs';
import path from 'path';
import os from 'os';

const R2_ACCOUNT_ID = process.env.R2_ACCOUNT_ID || process.env.VITE_R2_ACCOUNT_ID;
const R2_ACCESS_KEY_ID = process.env.R2_ACCESS_KEY_ID || process.env.VITE_R2_ACCESS_KEY_ID;
const R2_SECRET_ACCESS_KEY = process.env.R2_SECRET_ACCESS_KEY || process.env.VITE_R2_SECRET_ACCESS_KEY;
const R2_BUCKET = process.env.R2_BUCKET || process.env.VITE_R2_BUCKET || 'nola-transit';

const s3 = new S3Client({
  region: 'auto',
  endpoint: `https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
  credentials: {
    accessKeyId: R2_ACCESS_KEY_ID,
    secretAccessKey: R2_SECRET_ACCESS_KEY
  }
});

// Schema must match the scraper's schema
const schema = new parquet.ParquetSchema({
  vid: { type: 'UTF8' },
  timestamp: { type: 'TIMESTAMP_MILLIS' },
  lat: { type: 'DOUBLE' },
  lon: { type: 'DOUBLE' },
  route: { type: 'UTF8' },
  speed: { type: 'DOUBLE' },
  heading: { type: 'INT32' },
  is_delayed: { type: 'BOOLEAN' },
  segment_id: { type: 'INT32', optional: true },
  segment_name: { type: 'UTF8', optional: true },
  segment_type: { type: 'UTF8', optional: true }
});

async function listAllFiles() {
  const files = [];
  let continuationToken;

  do {
    const cmd = new ListObjectsV2Command({
      Bucket: R2_BUCKET,
      ContinuationToken: continuationToken
    });
    const res = await s3.send(cmd);
    for (const obj of res.Contents || []) {
      if (obj.Key.endsWith('.parquet')) {
        files.push(obj.Key);
      }
    }
    continuationToken = res.NextContinuationToken;
  } while (continuationToken);

  return files;
}

function getDateFromFilename(filename) {
  // Format: YYYY/MM/transit-YYYY-MM-DDTHH-MM-SS-MMMZ.parquet
  const match = filename.match(/transit-(\d{4}-\d{2}-\d{2})T/);
  if (match) {
    return match[1];
  }
  return null;
}

async function downloadFile(key) {
  const cmd = new GetObjectCommand({ Bucket: R2_BUCKET, Key: key });
  const res = await s3.send(cmd);

  const tmpFile = path.join(os.tmpdir(), `download_${Date.now()}_${Math.random().toString(36).slice(2)}.parquet`);
  const chunks = [];

  for await (const chunk of res.Body) {
    chunks.push(chunk);
  }

  fs.writeFileSync(tmpFile, Buffer.concat(chunks));
  return tmpFile;
}

async function readParquetFile(filePath) {
  const reader = await parquet.ParquetReader.openFile(filePath);
  const cursor = reader.getCursor();
  const records = [];

  let record;
  while (record = await cursor.next()) {
    records.push(record);
  }

  await reader.close();
  return records;
}

async function writeParquetFile(records, outputPath) {
  const writer = await parquet.ParquetWriter.openFile(schema, outputPath);

  for (const record of records) {
    await writer.appendRow(record);
  }

  await writer.close();
}

async function uploadFile(localPath, key) {
  const fileContent = fs.readFileSync(localPath);
  const cmd = new PutObjectCommand({
    Bucket: R2_BUCKET,
    Key: key,
    Body: fileContent,
    ContentType: 'application/octet-stream'
  });
  await s3.send(cmd);
}

async function deleteFiles(keys) {
  if (keys.length === 0) return;

  // S3 DeleteObjects has a limit of 1000 keys per request
  for (let i = 0; i < keys.length; i += 1000) {
    const batch = keys.slice(i, i + 1000);
    const cmd = new DeleteObjectsCommand({
      Bucket: R2_BUCKET,
      Delete: {
        Objects: batch.map(key => ({ Key: key }))
      }
    });
    await s3.send(cmd);
  }
}

async function consolidate() {
  console.log('Listing files in R2...');
  const allFiles = await listAllFiles();

  // Filter to only raw files (not already consolidated daily files)
  const hourlyFiles = allFiles.filter(f => f.match(/transit-\d{4}-\d{2}-\d{2}T/));
  const dailyFiles = allFiles.filter(f => f.startsWith('daily/'));
  console.log(`Found ${hourlyFiles.length} raw files, ${dailyFiles.length} consolidated daily files`);

  if (hourlyFiles.length === 0) {
    console.log('No hourly files to consolidate');
    return;
  }

  // Group by date
  const byDate = {};
  for (const file of hourlyFiles) {
    const date = getDateFromFilename(file);
    if (date) {
      if (!byDate[date]) byDate[date] = [];
      byDate[date].push(file);
    }
  }

  // Only consolidate dates with multiple files (complete days)
  // Skip today to avoid consolidating incomplete data
  const today = new Date().toISOString().split('T')[0];
  const datesToConsolidate = Object.keys(byDate)
    .filter(date => date !== today && byDate[date].length > 1)
    .sort();

  console.log(`Found ${datesToConsolidate.length} dates to consolidate`);

  for (const date of datesToConsolidate) {
    const files = byDate[date];
    console.log(`\nConsolidating ${date} (${files.length} files)...`);

    const allRecords = [];
    const tmpFiles = [];

    // Download and read all files for this date
    for (const file of files) {
      try {
        const tmpFile = await downloadFile(file);
        tmpFiles.push(tmpFile);
        const records = await readParquetFile(tmpFile);
        allRecords.push(...records);
        console.log(`  - ${file}: ${records.length} records`);
      } catch (err) {
        console.error(`  - Error reading ${file}:`, err.message);
      }
    }

    if (allRecords.length === 0) {
      console.log(`  Skipping ${date} - no records`);
      continue;
    }

    // Sort by timestamp
    allRecords.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

    // Write consolidated file
    const outputPath = path.join(os.tmpdir(), `daily_${date}.parquet`);
    await writeParquetFile(allRecords, outputPath);

    // Upload to R2 with daily/ prefix
    const dailyKey = `daily/${date}.parquet`;
    await uploadFile(outputPath, dailyKey);
    console.log(`  Uploaded ${dailyKey} (${allRecords.length} records)`);

    // Delete original hourly files
    await deleteFiles(files);
    console.log(`  Deleted ${files.length} hourly files`);

    // Cleanup temp files
    for (const tmp of tmpFiles) {
      try { fs.unlinkSync(tmp); } catch {}
    }
    try { fs.unlinkSync(outputPath); } catch {}
  }

  console.log('\nConsolidation complete!');
}

consolidate().catch(err => {
  console.error('Consolidation failed:', err);
  process.exit(1);
});
