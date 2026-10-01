// The "business logic" shared by both architectures.
// It is bounded by CAPACITY concurrent slots (think: DB pool), so extra work has to wait.
const { CAPACITY, WORK_MS, FAIL_RATE } = require('./config');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

let active = 0;
const waiting = [];

async function acquire() {
  if (active < CAPACITY) {
    active++;
    return;
  }
  await new Promise((resolve) => waiting.push(resolve));
}

function release() {
  const next = waiting.shift();
  if (next) next(); // hand the slot straight to the next waiter
  else active--;
}

async function processOrder(order) {
  await acquire();
  try {
    await sleep(WORK_MS);
    if (Math.random() < FAIL_RATE) throw new Error('transient downstream failure');
    return { orderId: order.id, status: 'processed' };
  } finally {
    release();
  }
}

module.exports = { processOrder, sleep };
