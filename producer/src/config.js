// producer/src/config.js
require('dotenv').config();

module.exports = {
  PRODUCER_THREADS: parseInt(process.env.PRODUCER_THREADS || '2', 10),
  FOLDERS: (process.env.FOLDERS || '').split(',').map(s => s.trim()).filter(Boolean),
  GRPC_HOST: process.env.CONSUMER_HOST || '127.0.0.1',
  GRPC_PORT: parseInt(process.env.CONSUMER_PORT || '50051', 10),
  BATCH_DELAY_MS: parseInt(process.env.BATCH_DELAY_MS || '250', 10), // small delay between files
};
