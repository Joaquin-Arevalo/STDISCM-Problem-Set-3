// producer/src/grpcClient.js
const path = require('path');
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');

const PROTO_PATH = path.join(__dirname, '..', '..', 'proto', 'media.proto');
const pkgDef = protoLoader.loadSync(PROTO_PATH, { keepCase: true, longs: String });
const proto = grpc.loadPackageDefinition(pkgDef).media;

function createClient(host, port) {
  return new proto.MediaUpload(`${host}:${port}`, grpc.credentials.createInsecure());
}

module.exports = { createClient };
