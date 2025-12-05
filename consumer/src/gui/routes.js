// consumer/src/gui/routes.js
const express = require('express');
const router = express.Router();
const path = require('path');
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');

const PROTO_PATH = path.join(__dirname, '..', '..', '..', 'proto', 'media.proto');
const pkgDef = protoLoader.loadSync(PROTO_PATH, { keepCase: true, longs: String });
const proto = grpc.loadPackageDefinition(pkgDef).media;

const grpcHost = process.env.GRPC_CLIENT_HOST || '127.0.0.1';
const grpcPort = parseInt(process.env.GRPC_PORT || '50051', 10);
const client = new proto.MediaUpload(`${grpcHost}:${grpcPort}`, grpc.credentials.createInsecure());

router.get('/videos', (req, res) => {
  client.ListMedia({}, (err, response) => {
    if (err) return res.status(500).json({ error: err.message });
    res.json(response.items || []);
  });
});

module.exports = router;
