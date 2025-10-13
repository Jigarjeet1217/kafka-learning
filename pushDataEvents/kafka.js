const { createTopics, listTopics } = require('./kafka-admin');
const { sendMessage } = require('./kafka-producer');
(async () => {
  await createTopics();
  await listTopics();
})();

module.exports = { sendMessage };
