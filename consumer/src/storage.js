// consumer/src/storage.js
const fs = require('fs');
const path = require('path');

const MEDIA_DIR = process.env.MEDIA_DIR || path.join(__dirname, '..', 'media');

function ensureMediaDir() {
  if (!fs.existsSync(MEDIA_DIR)) fs.mkdirSync(MEDIA_DIR, { recursive: true });
}

function writeStreamToFile(filename) {
  ensureMediaDir();
  const safeName = path.basename(filename);
  const destPath = path.join(MEDIA_DIR, safeName);
  const writeStream = fs.createWriteStream(destPath);
  return { writeStream, destPath };
}

function listMedia() {
  ensureMediaDir();
  const files = fs.readdirSync(MEDIA_DIR);
  return files.map(f => {
    const full = path.join(MEDIA_DIR, f);
    const stat = fs.statSync(full);
    return {
      filename: f,
      path: full,
      sizeBytes: stat.size,
      contentType: guessContentType(f),
    };
  });
}

function guessContentType(filename) {
  if (filename.endsWith('.mp4')) return 'video/mp4';
  if (filename.endsWith('.webm')) return 'video/webm';
  if (filename.endsWith('.mov')) return 'video/quicktime';
  return 'application/octet-stream';
}

module.exports = { writeStreamToFile, listMedia, MEDIA_DIR };
