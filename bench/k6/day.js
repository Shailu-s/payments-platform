// A card day at fixed arrival rates, signed as the network signs: taps spread
// over every card, a few taps on the hot card, and authorizations that are
// reversed (one in five with the reversal sent first). Every request is tagged
// with its message so cmd/benchday can cut latency by time window from the
// per-request CSV output.
import http from 'k6/http';
import crypto from 'k6/crypto';
import exec from 'k6/execution';
import { SharedArray } from 'k6/data';

const fixture = JSON.parse(open(__ENV.FIXTURE));
const cards = new SharedArray('cards', () => fixture.cards);

const BASE = __ENV.API_URL;
const RUN = __ENV.RUN_ID || `${Date.now()}`;
const SECRET = __ENV.NETWORK_SECRET;
if (!SECRET) throw new Error('NETWORK_SECRET is required');
const DURATION = __ENV.DURATION || '60s';

function arrivals(rate, fn) {
  return {
    executor: 'constant-arrival-rate',
    exec: fn,
    rate,
    timeUnit: '1s',
    duration: DURATION,
    preAllocatedVUs: Math.max(20, rate),
    maxVUs: Math.max(100, rate * 4),
  };
}

export const options = {
  discardResponseBodies: true,
  scenarios: {
    taps: arrivals(Number(__ENV.TAP_RATE || 500), 'tap'),
    hot: arrivals(Number(__ENV.HOT_RATE || 20), 'hot'),
    reversals: arrivals(Number(__ENV.REVERSAL_RATE || 50), 'reversal'),
  },
};

http.setResponseCallback(http.expectedStatuses(200));

function post(path, payload, msg) {
  const body = JSON.stringify(payload);
  const timestamp = String(Math.floor(Date.now() / 1000));
  const signature = 'v1=' + crypto.hmac('sha256', SECRET, `${timestamp}.${body}`, 'hex');
  return http.post(`${BASE}${path}`, body, {
    headers: {
      'Content-Type': 'application/json',
      'X-Network-Timestamp': timestamp,
      'X-Network-Signature': signature,
    },
    tags: { msg },
  });
}

function authorization(authID, card) {
  return {
    auth_id: authID,
    card_id: card,
    amount: 100,
    currency: 'USD',
    merchant: { id: 'mer_coffee', name: 'Corner Coffee', mcc: '5814' },
    kind: 'initial',
    sent_at: new Date().toISOString(),
  };
}

function randomCard() {
  return cards[Math.floor(Math.random() * cards.length)];
}

export function tap() {
  const i = exec.scenario.iterationInTest;
  post('/network/authorizations', authorization(`auth_day_${RUN}_tap_${i}`, randomCard()), 'authorization');
}

export function hot() {
  const i = exec.scenario.iterationInTest;
  post('/network/authorizations', authorization(`auth_day_${RUN}_hot_${i}`, fixture.hot_card), 'authorization');
}

// The network retries an authorization once and redelivers a reversal until
// it hears 2xx; three attempts each is enough at these rates.
function deliver(path, payload, msg) {
  for (let attempt = 0; attempt < 3; attempt++) {
    if (post(path, payload, msg).status === 200) return;
  }
}

export function reversal() {
  const i = exec.scenario.iterationInTest;
  const authID = `auth_day_${RUN}_rev_${i}`;
  const auth = authorization(authID, randomCard());
  const rev = { reversal_id: `rev_day_${RUN}_${i}`, auth_id: authID, amount: 100, sent_at: new Date().toISOString() };
  if (i % 5 === 0) {
    deliver('/network/reversals', rev, 'reversal');
    deliver('/network/authorizations', auth, 'reversed_authorization');
  } else {
    deliver('/network/authorizations', auth, 'reversed_authorization');
    deliver('/network/reversals', rev, 'reversal');
  }
}

export function handleSummary() {
  return {};
}
