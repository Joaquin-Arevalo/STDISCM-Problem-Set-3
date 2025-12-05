// producer/src/fileReader.js
const fs = require('fs');
const path = require('path');

function listVideos(folder) {
  const files = fs.readdirSync(folder);
  return files
    .filter(f => /\.(mp4|webm|mov)$/i.test(f))
    .map(f => path.join(folder, f));
}

function readFileChunks(filePath, chunkSize = 64 * 1024) {
  return fs.createReadStream(filePath, { highWaterMark: chunkSize });
}

module.exports = { listVideos, readFileChunks };
