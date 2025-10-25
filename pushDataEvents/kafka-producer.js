const kafka = require('./kafka-client');

let producer = kafka.producer({
  // default = true to allow auto topics creation,
  // if false dont allow auto topic creation error on new topic creation - This server does not host this topic-partition
  allowAutoTopicCreation: true,
  retry: {
    maxRetryTime: 20000, // how long to continue retry mechanism
    initialRetryTime: 100, // start retry after what time in ms
    retries: 8, // max number of retries - max value = 8
  },
  metadataMaxAge: 10000, // max time in ms to keep data or messages for
  // maxInFlightRequests: 5, // max number of requests that can be in progress at a time
});

// async function main() {
//   try {
//     await producer.connect();
//     resolve('success');
//   } catch (error) {
//     console.log('error in producer connection', {
//       name: error.name,
//       message: error.message,
//     });
//   }
// }

// main().then((res) => {
//   console.log('in then of main', res);
// });

async function createProducer() {
  return new Promise(async (resolve, reject) => {
    try {
      await producer.connect();
      resolve('success');
    } catch (error) {
      reject(error);
    }
  });
}

createProducer()
  .then((res) => console.log('producer connected!!!'))
  .catch((error) =>
    console.log('error in producer connection', {
      name: error.name,
      message: error.message,
    })
  );

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

// error handling in producer life cycle
producer.on(producer.events.CONNECT, (msg) => {
  console.log('Producer connection success', msg);
});

producer.on(producer.events.DISCONNECT, (msg) => {
  console.log('Producer disconnect ', msg);
});

// producer.on(producer.events.REQUEST, (msg) => {
//   console.log('Producer Request ', msg);
// });

module.exports = { sendMessage };
