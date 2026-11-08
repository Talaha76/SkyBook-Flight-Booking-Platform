/**
 * Minimal in-memory Amazon SQS stand-in speaking the AWS JSON 1.0 protocol used by AWS SDK v3.
 * Lets the bridge (and its outage test) run with no AWS account. NOT for production.
 *
 *   PORT=4566 node dist/scripts/fake-sqs.js
 *   GET  /stats                     -> { received, unique, duplicates, outage }
 *   POST /admin/outage?seconds=120  -> respond 503 to every SQS call for N seconds
 *   POST /admin/fail-rate?value=0.2 -> put ~20% of batch entries in Failed[] (transient)
 *   POST /admin/reset               -> clear counters
 */
import { createHash, randomUUID } from 'crypto';
import http from 'http';

const port = Number(process.env.PORT ?? 4566);
const seen = new Map<string, number>();
let received = 0;
let outageUntil = 0;
let failRate = Number(process.env.FAIL_RATE ?? 0);

const md5 = (s: string) => createHash('md5').update(s, 'utf8').digest('hex');

function send(res: http.ServerResponse, status: number, body: unknown, json = 'application/x-amz-json-1.0') {
  res.writeHead(status, { 'content-type': json, 'x-amzn-requestid': randomUUID() });
  res.end(JSON.stringify(body));
}

const server = http.createServer((req, res) => {
  const url = new URL(req.url ?? '/', `http://localhost:${port}`);
  let raw = '';
  req.on('data', (c) => (raw += c));
  req.on('end', () => {
    if (req.method === 'GET' && url.pathname === '/stats') {
      return send(res, 200, { received, unique: seen.size, duplicates: received - seen.size, outage: Date.now() < outageUntil, failRate }, 'application/json');
    }
    if (req.method === 'POST' && url.pathname === '/admin/outage') {
      outageUntil = Date.now() + Number(url.searchParams.get('seconds') ?? 60) * 1000;
      return send(res, 200, { outageUntil: new Date(outageUntil).toISOString() }, 'application/json');
    }
    if (req.method === 'POST' && url.pathname === '/admin/fail-rate') {
      failRate = Number(url.searchParams.get('value') ?? 0);
      return send(res, 200, { failRate }, 'application/json');
    }
    if (req.method === 'POST' && url.pathname === '/admin/reset') {
      seen.clear();
      received = 0;
      return send(res, 200, { ok: true }, 'application/json');
    }

    const target = String(req.headers['x-amz-target'] ?? '');
    if (Date.now() < outageUntil) {
      return send(res, 503, { __type: 'com.amazonaws.sqs#ServiceUnavailable', message: 'simulated outage' });
    }
    let body: any = {};
    try { body = raw ? JSON.parse(raw) : {}; } catch { return send(res, 400, { __type: 'com.amazon.coral.service#SerializationException' }); }

    if (target === 'AmazonSQS.SendMessageBatch') {
      const Successful: unknown[] = [];
      const Failed: unknown[] = [];
      for (const e of body.Entries ?? []) {
        if (Math.random() < failRate) {
          Failed.push({ Id: e.Id, SenderFault: false, Code: 'InternalError', Message: 'simulated transient failure' });
          continue;
        }
        received++;
        const id = e.MessageAttributes?.eventId?.StringValue ?? randomUUID();
        seen.set(id, (seen.get(id) ?? 0) + 1);
        Successful.push({ Id: e.Id, MessageId: randomUUID(), MD5OfMessageBody: md5(e.MessageBody) });
      }
      return send(res, 200, { Successful, Failed });
    }
    if (target === 'AmazonSQS.GetQueueAttributes') {
      return send(res, 200, { Attributes: { ApproximateNumberOfMessages: String(received) } });
    }
    return send(res, 400, { __type: 'com.amazonaws.sqs#UnsupportedOperation', message: target });
  });
});

server.listen(port, () => console.log(`fake-sqs listening on :${port}`));
