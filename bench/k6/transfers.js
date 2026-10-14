// Open-model load for POST /v1/transfers. Arrivals are scheduled at a fixed rate
// whether or not earlier requests have finished, so a slow server shows up as
// latency and dropped iterations instead of being hidden (coordinated omission).
import http from 'k6/http';
import exec from 'k6/execution';
import { SharedArray } from 'k6/data';

const fixture = JSON.parse(open(__ENV.FIXTURE));
const keys = new SharedArray('keys', () => fixture.keys);
const sources = new SharedArray('sources', () => fixture.sources);
const destinations = new SharedArray('destinations', () => fixture.destinations);

const BASE = __ENV.API_URL;
const MODE = __ENV.MODE || 'spread';
const RATE = Number(__ENV.RATE || 100);
const RUN = __ENV.RUN_ID || `${Date.now()}`;
const OUT = __ENV.OUT;

export const options = {
  discardResponseBodies: true,
  scenarios: {
    transfers: {
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

http.setResponseCallback(http.expectedStatuses(202));

export default function () {
  const i = exec.scenario.iterationInTest;
  const pair = Math.floor(Math.random() * sources.length);
  const source = MODE === 'hot' ? fixture.hot_source : sources[pair];
  const body = JSON.stringify({
    source_account: source,
    destination_account: destinations[pair],
    amount: 100,
    currency: 'USD',
  });
  http.post(`${BASE}/v1/transfers`, body, {
    headers: {
      Authorization: `Bearer ${keys[i % keys.length]}`,
      'Idempotency-Key': `bench-${RUN}-${i}`,
      'Content-Type': 'application/json',
    },
  });
}

export function handleSummary(data) {
  const m = data.metrics;
  const d = m.http_req_duration.values;
  const reqs = m.http_reqs ? m.http_reqs.values.count : 0;
  const failed = m.http_req_failed ? m.http_req_failed.values.passes : 0;
  const dropped = m.dropped_iterations ? m.dropped_iterations.values.count : 0;
  const row = {
    mode: MODE,
    target_rps: RATE,
    achieved_rps: Number((m.http_reqs ? m.http_reqs.values.rate : 0).toFixed(1)),
    requests: reqs,
    non_202: failed,
    dropped,
    p50_ms: Number(d.med.toFixed(2)),
    p95_ms: Number(d['p(95)'].toFixed(2)),
    p99_ms: Number(d['p(99)'].toFixed(2)),
    max_ms: Number(d.max.toFixed(2)),
  };
  const line = JSON.stringify(row);
  const outputs = { stdout: line + '\n' };
  if (OUT) outputs[OUT] = line + '\n';
  return outputs;
}
