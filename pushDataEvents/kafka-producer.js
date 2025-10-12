const kafka = require('./kafka-client');

let producer = kafka.producer();

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

module.exports = { sendMessage };
