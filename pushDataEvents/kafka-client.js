let { Kafka } = require('kafkajs');

// handling kafka config / setup properties
let kafka = new Kafka({
  clientId: process.env.clientId,
  brokers: ['localhost:9092'],
  retry: {
    maxRetryTime: 20000, // how long to continue retry mechanism
    initialRetryTime: 100, // start retry after what time in ms
    retries: 7, // max number of retries - max value = 8
  },
});

module.exports = kafka;
