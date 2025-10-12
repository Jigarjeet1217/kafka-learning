let { Kafka, logLevel, Partitioners } = require('kafkajs');
const { resolve } = require('path');

// handling kafka config / setup properties
let kafka = new Kafka({
  clientId: process.env.clientId,
  brokers: ['localhost:9092'],
  retry: {
    maxRetryTime: 20000, // how long to continue retry mechanism
    initialRetryTime: 100, // start retry after what time in ms
    retries: 7, // max number of retries - max value = 8
  }
});

let producer = kafka.producer();

async function main() {
  try {
    await producer.connect();
    resolve('success');
  } catch (error) {
    console.log('error in producer connection', {
      name: error.name,
      message: error.message,
    });
  }
}

main().then((res) => {
  console.log('in then of main', res);
});

async function sendMessage() {
  try {
    let key = Date.now().toString();
    await producer.send({
      topic: 'Random' + process.env.clientId,
      messages: [
        {
          key,
          value: `Current time in ms: ${key}`,
        },
      ],
    });
  } catch (error) {
    console.log('Error in sending message : ', error);
  }
}

module.exports = { sendMessage };
