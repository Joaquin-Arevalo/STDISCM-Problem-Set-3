// consumer/src/gui/webServer.js
require('dotenv').config();
const express = require('express');
const path = require('path');
const { MEDIA_DIR } = require('../storage');
const routes = require('./routes');

const app = express();

// Static GUI
app.use('/', express.static(path.join(__dirname, '..', 'public')));
// Serve media files
app.use('/media', express.static(MEDIA_DIR));
// REST API
app.use('/api', routes);

const host = process.env.GUI_HOST || '0.0.0.0';
const port = parseInt(process.env.GUI_PORT || '8080', 10);
app.listen(port, host, () => {
  console.log(`GUI server listening on http://${host}:${port}`);
});
