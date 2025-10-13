const kafka = require('./kafka-client');

let admin = kafka.admin();

async function createTopics() {
  try {
    await admin.connect();

    // get topic names
    let topics = process?.env?.topics?.split(',') ?? [];
    let topicsToCreate = [];
    for (let topic of topics) {
      let topicConfig = {
        topic, // name of topic
        numPartitions: 3, // number of partitions a topic shoul have
        replicationFactor: 1,
      };
      topicsToCreate.push(topicConfig);
    }
    await admin.createTopics({
      topics: topicsToCreate,
      waitForLeaders: true,
    });
    console.log('topic creation successfull!!');
  } catch (error) {
    console.log('Error in topics creation', error);
  } finally {
    // await admin.disconnect();
  }
}

async function listTopics() {
  try {
    // await admin.connect();
    let topics = await admin.listTopics();
    console.log('created topics : ', topics);
  } catch (error) {
    console.log('error in fetching topics!!!', error);
  } finally {
    await admin.disconnect();
  }
}

module.exports = { createTopics, listTopics };
