const { QUEUE, DONE_QUEUE, DEAD_QUEUE } = require('./config');

async function assertTopology(ch) {
  await ch.assertQueue(QUEUE, { durable: true });
  await ch.assertQueue(DONE_QUEUE, { durable: true });
  await ch.assertQueue(DEAD_QUEUE, { durable: true });
}

async function purgeAll(ch) {
  await Promise.all([QUEUE, DONE_QUEUE, DEAD_QUEUE].map((q) => ch.purgeQueue(q)));
}

module.exports = { assertTopology, purgeAll };
