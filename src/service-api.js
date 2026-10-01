// Architecture A: synchronous REST service. The caller waits until processing is finished.
const http = require('http');
const { API_PORT } = require('./config');
const { processOrder } = require('./processor');

const stats = { processed: 0, failed: 0, wasted: 0 };

const server = http.createServer((req, res) => {
  if (req.method === 'GET' && req.url === '/stats') {
    res.writeHead(200, { 'content-type': 'application/json' });
    return res.end(JSON.stringify(stats));
  }

  if (req.method !== 'POST' || req.url !== '/orders') {
    res.writeHead(404);
    return res.end();
  }

  let body = '';
  req.on('data', (chunk) => (body += chunk));
  req.on('end', async () => {
    const order = JSON.parse(body);
    let status = 200;
    let payload;
    try {
      payload = await processOrder(order);
      stats.processed++;
    } catch (err) {
      stats.failed++;
      status = 500;
      payload = { orderId: order.id, error: err.message };
    }
    // The caller may have timed out already: the work was done but nobody got the answer.
    if (res.destroyed) {
      stats.wasted++;
      return;
    }
    res.writeHead(status, { 'content-type': 'application/json' });
    res.end(JSON.stringify(payload));
  });
});

server.listen(API_PORT, () => {
  console.log(`[api] listening on :${API_PORT}`);
  process.send?.('ready');
});
