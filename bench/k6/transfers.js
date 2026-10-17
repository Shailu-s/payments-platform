// Open-model load for POST /v1/transfers. Arrivals are scheduled at a fixed rate
// whether or not earlier requests have finished, so a slow server shows up as
// latency and dropped iterations instead of being hidden (coordinated omission).
import http from 'k6/http';
import exec from 'k6/execution';
import { SharedArray } from 'k6/data';
import { Counter } from 'k6/metrics';

const fixture = JSON.parse(open(__ENV.FIXTURE));
const keys = new SharedArray('keys', () => fixture.keys);
const sources = new SharedArray('sources', () => fixture.sources);
const destinations = new SharedArray('destinations', () => fixture.destinations);

const BASE = __ENV.API_URL;
const MODE = __ENV.MODE || 'spread';
const RATE = Number(__ENV.RATE || 100);
const RUN = __ENV.RUN_ID || `${Date.now()}`;
const OUT = __ENV.OUT;
// Mixed runs start two processes; each reports under its own label and uses
// a different slice of the keys so they do not share a rate-limit window.
const ROW_MODE = __ENV.ROW_MODE || MODE;
const KEY_OFFSET = Number(__ENV.KEY_OFFSET || 0);

// A failure count alone cannot tell shedding (503) from breakage (500).
const TRACKED_STATUSES = [0, 202, 400, 422, 429, 500, 503];
const statusCounters = Object.fromEntries(
  TRACKED_STATUSES.map((s) => [s, new Counter(`status_${s}`)]));
const otherStatus = new Counter('status_other');

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
  const res = http.post(`${BASE}/v1/transfers`, body, {
    headers: {
      Authorization: `Bearer ${keys[(i + KEY_OFFSET) % keys.length]}`,
      'Idempotency-Key': `bench-${RUN}-${i}`,
      'Content-Type': 'application/json',
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
    non_202: failed,
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
