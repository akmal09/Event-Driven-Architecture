// All knobs can be overridden with env vars, e.g. REQUESTS=500 CAPACITY=10 npm run bench
const num = (name, fallback) => (process.env[name] !== undefined ? Number(process.env[name]) : fallback);

module.exports = {
  AMQP_URL: process.env.AMQP_URL || 'amqp://guest:guest@localhost:5672',
  API_PORT: num('API_PORT', 3000),

  QUEUE: 'orders',
  DONE_QUEUE: 'orders.completed',
  DEAD_QUEUE: 'orders.dead',

  REQUESTS: num('REQUESTS', 100),          // how many requests / messages to send
  CAPACITY: num('CAPACITY', 5),            // downstream limit, e.g. DB connection pool size
  WORK_MS: num('WORK_MS', 100),            // time to process one order
  FAIL_RATE: num('FAIL_RATE', 0.05),       // chance a single attempt hits a transient error
  CLIENT_TIMEOUT_MS: num('CLIENT_TIMEOUT_MS', 1000), // how long a direct-API caller waits
  MAX_RETRIES: num('MAX_RETRIES', 3),      // broker-side retries before dead-lettering
};
