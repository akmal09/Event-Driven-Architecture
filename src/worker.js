// Architecture B: queue consumer. Pulls orders from the broker at its own pace,
// retries transient failures and emits an "order completed" event.
const amqp = require('amqplib');
const { AMQP_URL, QUEUE, DONE_QUEUE, DEAD_QUEUE, CAPACITY, MAX_RETRIES } = require('./config');
const { processOrder } = require('./processor');
const { assertTopology } = require('./topology');

async function main() {
  const conn = await amqp.connect(AMQP_URL);
  const ch = await conn.createChannel();
  await assertTopology(ch);

  // Back-pressure: the broker only hands us as many unacked messages as we can actually process.
  await ch.prefetch(CAPACITY);

  await ch.consume(QUEUE, async (msg) => {
    const order = JSON.parse(msg.content.toString());
    const attempt = msg.properties.headers?.['x-attempt'] ?? 1;

    try {
      const result = await processOrder(order);
      const event = { ...result, attempt, sentAt: order.sentAt, completedAt: Date.now() };
      ch.sendToQueue(DONE_QUEUE, Buffer.from(JSON.stringify(event)), { persistent: true });
    } catch (err) {
      const target = attempt <= MAX_RETRIES ? QUEUE : DEAD_QUEUE;
      ch.sendToQueue(target, msg.content, {
        persistent: true,
        headers: { 'x-attempt': attempt + 1, 'x-error': err.message },
      });
    }
    ch.ack(msg);
  });

  console.log(`[worker] consuming "${QUEUE}" with prefetch=${CAPACITY}`);
  process.send?.('ready');
}

main().catch((err) => {
  console.error('[worker] failed:', err.message);
  process.exit(1);
});
