// consumer/src/server.js
require('dotenv').config();
const path = require('path');
const fs = require('fs');
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
  let bufferedChunks = []; // Buffer chunks until writeStream is ready
  let jobStarted = false;
  let allDataReceived = false;
  let callEnded = false;

  function finishUpload(statusMessage) {
    if (!callEnded) {
      callEnded = true;
      callback(null, { accepted, message: statusMessage, storedPath });
    }
  }

  call.on('data', chunk => {
    if (chunk.meta) {
      if (meta) return;
      meta = chunk.meta;
      console.log(`[DATA_RECEIVED] Metadata for "${meta.filename}"`);

      const job = {
        run: async () => {
          jobStarted = true;
          console.log(`[JOB_START] Processing "${meta.filename}"`);
          const { writeStream: ws, destPath } = writeStreamToFile(meta.filename);
          writeStream = ws;
          storedPath = path.basename(destPath);
          
          // Write all buffered chunks that arrived before job started
          if (bufferedChunks.length > 0) {
            console.log(`[JOB_START] Writing ${bufferedChunks.length} buffered chunks for "${meta.filename}"`);
            for (const bufferedChunk of bufferedChunks) {
              writeStream.write(bufferedChunk);
            }
            bufferedChunks = [];
          }
          
          return new Promise((resolve, reject) => {
            ws.on('finish', () => {
              console.log(`[JOB_END] Finished writing "${meta.filename}"`);
              resolve();
            });
            ws.on('error', (err) => {
              console.log(`[JOB_ERROR] Error writing "${meta.filename}": ${err.message}`);
              reject(err);
            });
            
            // If all data already received before job started, end stream now
            if (allDataReceived) {
              console.log(`[JOB_START] All data already received, ending writeStream for "${meta.filename}"`);
              ws.end();
            }
          });
        }
      };

      accepted = queue.tryEnqueue(job);
      if (!accepted) {
        finishUpload('Queue full. Upload dropped.');
      }
    } else if (chunk.data) {
      if (writeStream && !writeStream.destroyed) {
        // Stream is ready, write directly
        writeStream.write(chunk.data);
      } else if (!jobStarted) {
        // Job hasn't started yet, buffer the chunk
        bufferedChunks.push(chunk.data);
      }
    }
  });

  call.on('end', () => {
    allDataReceived = true;
    console.log(`[STREAM_END] All data received for "${meta ? meta.filename : 'unknown'}"`);
    
    // If job already started and writeStream exists, end it now
    if (jobStarted && writeStream && !writeStream.destroyed) {
      console.log(`[STREAM_END] Ending writeStream for "${meta.filename}"`);
      writeStream.end();
    }
    
    // Wait for job to start before sending response
    if (!callEnded) {
      const checkInterval = setInterval(() => {
        if (jobStarted || !accepted) {
          clearInterval(checkInterval);
          finishUpload(accepted ? 'Upload completed.' : 'Upload not accepted.');
        }
      }, 50);
      
      // Timeout after 5 seconds
      setTimeout(() => {
        clearInterval(checkInterval);
        if (!callEnded) {
          finishUpload(accepted ? 'Upload completed.' : 'Upload not accepted.');
        }
      }, 5000);
    }
  });

  call.on('error', err => {
    if (!callEnded) {
      finishUpload(`Stream error: ${err.message}`);
    }
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
