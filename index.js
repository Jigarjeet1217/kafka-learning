let express = require('express');

const fs = require('fs');
const { sendMessage } = require('./pushDataEvents/kafka');
let app = express();

function setUpServer(env) {
  if (!env) console.log('No env found');
  else {
    require('dotenv').config({
      path: `./config/env/${env}.env`,
      override: true,
    });
  }
}

setUpServer('local');

app.get('/', async (req, res, next) => {
  await sendMessage();
  res.send('Hello from local');
});

app.listen(process.env.port, (err) => {
  console.log(`Server started, listening on port ${process.env.port}`);
});
