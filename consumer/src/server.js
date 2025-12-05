// consumer/src/server.js
require('dotenv').config();
const path = require('path');
const { BoundedQueue } = require('./queue');
const { writeStreamToFile, listMedia } = require('./storage');

const PROTO_PATH = path.join(__dirname, '..', '..', 'proto', 'media.proto');
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');

const pkgDef = protoLoader.loadSync(PROTO_PATH, { keepCase: true, longs: String });
const proto = grpc.loadPackageDefinition(pkgDef).media;

const qMax = parseInt(process.env.Q_MAX || '10', 10);
const concurrency = parseInt(process.env.CONSUMER_THREADS || '4', 10);
const queue = new BoundedQueue(qMax, concurrency);

function UploadVideo(call, callback) {
  let meta = null;
  let accepted = false;
  let writeStream = null;
  let storedPath = null;
  let writePromise = null;

  function finishUpload(statusMessage) {
    if (writeStream) writeStream.end();
    callback(null, { accepted, message: statusMessage, storedPath });
  }

  call.on('data', chunk => {
    if (chunk.meta) {
      if (meta) return; // ignore duplicate meta
      meta = chunk.meta;

      const job = {
        run: async () => {
          const { writeStream: ws, destPath } = writeStreamToFile(meta.filename);
          writeStream = ws;
          storedPath = path.basename(destPath);
          
          // Wait for the file write to complete
          writePromise = new Promise((resolve, reject) => {
            ws.on('finish', resolve);
            ws.on('error', reject);
          });
          
          // Return the promise so the queue waits for the entire file write
          return writePromise;
        }
      };

      accepted = queue.tryEnqueue(job);
      if (!accepted) {
        // Leaky bucket: drop gracefully
        finishUpload('Queue full. Upload dropped.');
        // End the call to stop receiving further data
      }
    } else if (chunk.data) {
      if (writeStream) {
        writeStream.write(chunk.data);
      } // else chunk arrives before meta or before job starts; safely ignore
    }
  });

  call.on('end', () => {
    finishUpload(accepted ? 'Upload completed.' : 'Upload not accepted.');
  });

  call.on('error', err => {
    finishUpload(`Upload error: ${err.message}`);
  });
}

function ListMedia(call, callback) {
  const items = listMedia().map(f => ({
    filename: f.filename,
    url: `/media/${encodeURIComponent(f.filename)}`,
    sizeBytes: f.sizeBytes,
    contentType: f.contentType,
  }));
  callback(null, { items });
}

function startGrpcServer() {
  const server = new grpc.Server();
  server.addService(proto.MediaUpload.service, { UploadVideo, ListMedia });
  const host = process.env.GRPC_HOST || '0.0.0.0';
  const port = parseInt(process.env.GRPC_PORT || '50051', 10);
  server.bindAsync(`${host}:${port}`, grpc.ServerCredentials.createInsecure(), () => {
    server.start();
    console.log(`gRPC consumer listening on ${host}:${port}`);
  });
}

startGrpcServer();
