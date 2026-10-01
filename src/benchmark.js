// Fires REQUESTS concurrent calls at each architecture and compares what the caller experiences.
const { fork } = require('child_process');
const path = require('path');
const fs = require('fs');
const amqp = require('amqplib');
const cfg = require('./config');
const { sleep } = require('./processor');
const { assertTopology, purgeAll } = require('./topology');

// ---------- helpers ----------
function startChild(file) {
  return new Promise((resolve, reject) => {
    const child = fork(path.join(__dirname, file), { stdio: 'inherit' });
    child.once('message', (m) => m === 'ready' && resolve(child));
    child.once('exit', (code) => reject(new Error(`${file} exited early (code ${code})`)));
  });
}

function summarize(values) {
  if (values.length === 0) return { avg: 0, p50: 0, p95: 0, max: 0 };
  const s = [...values].sort((a, b) => a - b);
  const pct = (p) => s[Math.min(s.length - 1, Math.ceil((p / 100) * s.length) - 1)];
  return {
    avg: Math.round(s.reduce((a, b) => a + b, 0) / s.length),
    p50: pct(50),
    p95: pct(95),
    max: s[s.length - 1],
  };
}

async function waitFor(predicate, timeoutMs, label) {
  const start = Date.now();
  while (!(await predicate())) {
    if (Date.now() - start > timeoutMs) throw new Error(`Timed out waiting for ${label}`);
    await sleep(25);
  }
}

// ---------- Scenario A: direct synchronous API ----------
async function runDirect() {
  const api = await startChild('service-api.js');
  const url = `http://localhost:${cfg.API_PORT}`;
  const results = [];
  const t0 = Date.now();

  await Promise.all(
    Array.from({ length: cfg.REQUESTS }, async (_, i) => {
      const start = Date.now();
      try {
        const res = await fetch(`${url}/orders`, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ id: i + 1, sentAt: start }),
          signal: AbortSignal.timeout(cfg.CLIENT_TIMEOUT_MS),
        });
        await res.text();
        results.push({ ms: Date.now() - start, outcome: res.ok ? 'ok' : 'error' });
      } catch (err) {
        const outcome = err.name === 'TimeoutError' ? 'timeout' : 'error';
        results.push({ ms: Date.now() - start, outcome });
      }
    })
  );
  const callerDoneMs = Date.now() - t0;

  // Let the server drain the work it kept doing for callers that already gave up.
  let server;
  await waitFor(
    async () => {
      server = await (await fetch(`${url}/stats`)).json();
      return server.processed + server.failed >= cfg.REQUESTS;
    },
    60_000,
    'API to finish in-flight work'
  );
  const serverDoneMs = Date.now() - t0;
  api.kill();

  const count = (o) => results.filter((r) => r.outcome === o).length;
  return {
    name: 'Direct API',
    callerLatency: summarize(results.map((r) => r.ms)),
    callerFreedAfterMs: callerDoneMs,
    allWorkDoneMs: serverDoneMs,
    succeeded: count('ok'),
    failedForCaller: count('error'),
    timedOut: count('timeout'),
    wastedWork: server.wasted,
    retries: 0,
    deadLettered: 0,
  };
}

// ---------- Scenario B: publish to broker, worker consumes ----------
async function runBroker() {
  const conn = await amqp.connect(cfg.AMQP_URL);
  const ch = await conn.createConfirmChannel();
  await assertTopology(ch);
  await purgeAll(ch);

  const worker = await startChild('worker.js');

  const completed = [];
  const dead = [];
  await ch.consume(cfg.DONE_QUEUE, (msg) => {
    completed.push(JSON.parse(msg.content.toString()));
    ch.ack(msg);
  });
  await ch.consume(cfg.DEAD_QUEUE, (msg) => {
    dead.push(JSON.parse(msg.content.toString()));
    ch.ack(msg);
  });

  const publishMs = [];
  const t0 = Date.now();

  await Promise.all(
    Array.from(
      { length: cfg.REQUESTS },
      (_, i) =>
        new Promise((resolve, reject) => {
          const start = Date.now();
          const order = { id: i + 1, sentAt: start };
          // Resolves when the broker confirms it has safely stored the message.
          ch.sendToQueue(cfg.QUEUE, Buffer.from(JSON.stringify(order)), { persistent: true }, (err) => {
            if (err) return reject(err);
            publishMs.push(Date.now() - start);
            resolve();
          });
        })
    )
  );
  const callerDoneMs = Date.now() - t0;

  await waitFor(() => completed.length + dead.length >= cfg.REQUESTS, 60_000, 'worker to drain queue');
  const allWorkDoneMs = Date.now() - t0;

  worker.kill();
  await conn.close();

  return {
    name: 'Broker + Worker',
    callerLatency: summarize(publishMs),
    endToEndLatency: summarize(completed.map((e) => e.completedAt - e.sentAt)),
    callerFreedAfterMs: callerDoneMs,
    allWorkDoneMs,
    succeeded: completed.length,
    failedForCaller: 0,
    timedOut: 0,
    wastedWork: 0,
    retries: completed.reduce((n, e) => n + (e.attempt - 1), 0),
    deadLettered: dead.length,
  };
}

// ---------- report ----------
function printReport(a, b) {
  const rows = [
    ['Caller latency p50 (ms)', a.callerLatency.p50, b.callerLatency.p50],
    ['Caller latency p95 (ms)', a.callerLatency.p95, b.callerLatency.p95],
    ['Caller latency max (ms)', a.callerLatency.max, b.callerLatency.max],
    ['All callers released after (ms)', a.callerFreedAfterMs, b.callerFreedAfterMs],
    ['All work finished after (ms)', a.allWorkDoneMs, b.allWorkDoneMs],
    ['End-to-end p95 (ms)', a.callerLatency.p95 + ' *', b.endToEndLatency.p95],
    ['Succeeded', `${a.succeeded}/${cfg.REQUESTS}`, `${b.succeeded}/${cfg.REQUESTS}`],
    ['Timed out (caller gave up)', a.timedOut, b.timedOut],
    ['Errors returned to caller', a.failedForCaller, b.failedForCaller],
    ['Work done but answer lost', a.wastedWork, b.wastedWork],
    ['Automatic retries', a.retries, b.retries],
    ['Dead-lettered', a.deadLettered, b.deadLettered],
  ];
  const w = [34, 14, 16];
  const line = (cells) => cells.map((c, i) => String(c).padEnd(w[i])).join(' | ');

  console.log('\n' + '='.repeat(70));
  console.log(
    `${cfg.REQUESTS} requests | capacity=${cfg.CAPACITY} | work=${cfg.WORK_MS}ms | ` +
      `fail=${cfg.FAIL_RATE * 100}% | client timeout=${cfg.CLIENT_TIMEOUT_MS}ms`
  );
  console.log('='.repeat(70));
  console.log(line(['Metric', a.name, b.name]));
  console.log(line(w.map((n) => '-'.repeat(n))));
  rows.forEach((r) => console.log(line(r)));
  console.log('\n* Direct API: caller latency IS the end-to-end latency (the caller waits for processing).');
  console.log('  Broker: caller latency = time until the broker confirms the message is stored.\n');
}

async function main() {
  console.log('Running scenario A: direct API...');
  const direct = await runDirect();
  console.log('Running scenario B: broker + worker...');
  const broker = await runBroker();

  printReport(direct, broker);

  const outDir = path.join(__dirname, '..', 'results');
  fs.mkdirSync(outDir, { recursive: true });
  const file = path.join(outDir, 'latest.json');
  fs.writeFileSync(file, JSON.stringify({ config: cfg, direct, broker, at: new Date().toISOString() }, null, 2));
  console.log(`Raw results written to ${path.relative(process.cwd(), file)}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
