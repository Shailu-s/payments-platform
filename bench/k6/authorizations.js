// Open-model load for POST /network/authorizations, signed as the card network
// signs: hex HMAC-SHA256 of "<unix seconds>.<raw body>" keyed with NETWORK_SECRET.
// Arrivals are scheduled at a fixed rate whether or not earlier requests have
// finished, so a slow issuer shows up as latency and dropped iterations.
import http from 'k6/http';
import crypto from 'k6/crypto';
import exec from 'k6/execution';
import { SharedArray } from 'k6/data';
import { Counter } from 'k6/metrics';

const fixture = JSON.parse(open(__ENV.FIXTURE));
const cards = new SharedArray('cards', () => fixture.cards);

const BASE = __ENV.API_URL;
const MODE = __ENV.MODE || 'auth-spread';
const RATE = Number(__ENV.RATE || 100);
const RUN = __ENV.RUN_ID || `${Date.now()}`;
const OUT = __ENV.OUT;
const ROW_MODE = __ENV.ROW_MODE || MODE;
const SECRET = __ENV.NETWORK_SECRET;
if (!SECRET) throw new Error('NETWORK_SECRET is required');

// 503 is the issuer's "no answer": the network would retry, then stand in.
const TRACKED_STATUSES = [0, 200, 400, 401, 500, 503];
const statusCounters = Object.fromEntries(
  TRACKED_STATUSES.map((s) => [s, new Counter(`status_${s}`)]));
const otherStatus = new Counter('status_other');

export const options = {
  discardResponseBodies: true,
  scenarios: {
    authorizations: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: __ENV.DURATION || '30s',
      preAllocatedVUs: Math.max(50, RATE),
      maxVUs: Math.max(200, RATE * 4),
    },
  },
  summaryTrendStats: ['min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

http.setResponseCallback(http.expectedStatuses(200));

export default function () {
  const i = exec.scenario.iterationInTest;
  const card = MODE === 'auth-hot' ? fixture.hot_card : cards[Math.floor(Math.random() * cards.length)];
  const body = JSON.stringify({
    auth_id: `auth_bench_${RUN}_${i}`,
    card_id: card,
    amount: 100,
    currency: 'USD',
    merchant: { id: 'mer_coffee', name: 'Corner Coffee', mcc: '5814' },
    kind: 'initial',
    sent_at: new Date().toISOString(),
  });
  const timestamp = String(Math.floor(Date.now() / 1000));
  const signature = 'v1=' + crypto.hmac('sha256', SECRET, `${timestamp}.${body}`, 'hex');
  const res = http.post(`${BASE}/network/authorizations`, body, {
    headers: {
      'Content-Type': 'application/json',
      'X-Network-Timestamp': timestamp,
      'X-Network-Signature': signature,
    },
  });
  (statusCounters[res.status] || otherStatus).add(1);
}

export function handleSummary(data) {
  const m = data.metrics;
  const d = m.http_req_duration.values;
  const reqs = m.http_reqs ? m.http_reqs.values.count : 0;
  const failed = m.http_req_failed ? m.http_req_failed.values.passes : 0;
  const dropped = m.dropped_iterations ? m.dropped_iterations.values.count : 0;
  const statusCounts = {};
  for (const s of TRACKED_STATUSES) {
    const metric = m[`status_${s}`];
    if (metric && metric.values.count > 0) statusCounts[String(s)] = metric.values.count;
  }
  if (m.status_other && m.status_other.values.count > 0) statusCounts.other = m.status_other.values.count;
  const row = {
    mode: ROW_MODE,
    target_rps: RATE,
    achieved_rps: Number((m.http_reqs ? m.http_reqs.values.rate : 0).toFixed(1)),
    requests: reqs,
    non_200: failed,
    dropped,
    p50_ms: Number(d.med.toFixed(2)),
    p95_ms: Number(d['p(95)'].toFixed(2)),
    p99_ms: Number(d['p(99)'].toFixed(2)),
    max_ms: Number(d.max.toFixed(2)),
    status_counts: statusCounts,
  };
  const line = JSON.stringify(row);
  const outputs = { stdout: line + '\n' };
  if (OUT) outputs[OUT] = line + '\n';
  return outputs;
}
