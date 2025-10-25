const kafka = require('./kafka-client');
let msgKeys = ['admin', 'superadmin', 'user'];

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

function getRandomTopic(topics) {
  let random = Math.floor(Math.random() * topics.length);
  return topics[random];
}

function getDynamicKeys() {
  let random = Math.floor(Math.random() * msgKeys.length);
  return msgKeys[random];
}

/**
 * @description producing with key as null and no partition on message obj
 * 1. when producing successive message with key = null (and partition = null on msg obj) for same topic, partitions are assigned in roundrobin approach
 * 2. when key is provided on msg (and partition = null) then messages with same key for a topic will go to same partition
 * 3. when key = null and partition have some value, data go into mentioned partition
 * 4. when both key and partition are defined, partition will take preference
 */

async function sendMessage() {
  try {
    let existingTopics = global.topics;
    let topic = getRandomTopic(existingTopics),
      key = getDynamicKeys();

    await producer.send({
      topic,
      messages: [
        {
          key,
          value: `Current time in ms: ${key}`,
          // partition: 0,
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
