// producer/src/producer.js
require('dotenv').config();
const path = require('path');
const fs = require('fs');
const { createClient } = require('./grpcClient');
const { listVideos, readFileChunks } = require('./filereader');
const { PRODUCER_THREADS, FOLDERS, GRPC_HOST, GRPC_PORT, BATCH_DELAY_MS } = require('./config');

if (FOLDERS.length < PRODUCER_THREADS) {
  console.warn('Warning: fewer folders than producer threads. Some threads may idle.');
}

const client = createClient(GRPC_HOST, GRPC_PORT);

function uploadFile(filePath, sourceId) {
  return new Promise((resolve, reject) => {
    const call = client.UploadVideo((err, status) => {
      if (err) return reject(err);
      resolve(status);
    });

    const filename = path.basename(filePath);
    const stat = fs.statSync(filePath);
    call.write({ meta: { filename, contentType: guessContentType(filename), sizeBytes: stat.size, sourceId } });

    const stream = readFileChunks(filePath);
    stream.on('data', chunk => call.write({ data: chunk }));
    stream.on('end', () => call.end());
    stream.on('error', reject);
  });
}

function guessContentType(filename) {
  if (filename.endsWith('.mp4')) return 'video/mp4';
  if (filename.endsWith('.webm')) return 'video/webm';
  if (filename.endsWith('.mov')) return 'video/quicktime';
  return 'application/octet-stream';
}

async function runThread(index, folder) {
  const sourceId = `producer-${index}`;
  console.log(`[${sourceId}] scanning ${folder}`);
  const files = listVideos(folder);
  
  // Upload all files concurrently (with small delay between each start)
  const uploadPromises = files.map((f, i) => 
    new Promise(resolve => {
      setTimeout(async () => {
        try {
          const status = await uploadFile(f, sourceId);
          console.log(`[${sourceId}] ${path.basename(f)} -> ${status.accepted ? 'ACCEPTED' : 'DROPPED'} (${status.message})`);
        } catch (e) {
          console.error(`[${sourceId}] upload error: ${e.message}`);
        }
        resolve();
      }, i * BATCH_DELAY_MS);
    })
  );
  
  await Promise.all(uploadPromises);
}

async function main() {
  const threads = [];
  for (let i = 0; i < PRODUCER_THREADS; i++) {
    const folder = FOLDERS[i] || FOLDERS[FOLDERS.length - 1];
    threads.push(runThread(i + 1, folder));
  }
  await Promise.all(threads);
  console.log('All producer threads completed.');
}

main().catch(err => {
  console.error('Producer failed:', err);
  process.exit(1);
});
