let { Kafka, logLevel, Partitioners } = require('kafkajs');
const { resolve } = require('path');

let kafka = new Kafka({
  clientId: process.env.clientId,
  brokers: ['localhost:9092'],
});
//
// Partitioners

let producer = kafka.producer();

async function main() {
  try {
    await producer.connect();
    resolve('success');
  } catch (error) {
    console.log('error in producer connection', error);
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
