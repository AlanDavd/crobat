// k6 script: creates TASKS one-shot tasks at RATE/s through task-api, all
// due at the same instant AT, the "everyone schedules at 09:00:00" peak.
// Run it through loadtest/run.sh, which sets the env and measures the lag.
import http from 'k6/http';
import { check } from 'k6';
import exec from 'k6/execution';

const API = __ENV.API || 'http://task-api:8080';
const TARGET = __ENV.TARGET || 'http://echo/peak';
const RUN = __ENV.RUN;
const AT = __ENV.AT;
const RATE = Number(__ENV.RATE || 1000);
const TASKS = Number(__ENV.TASKS || 30000);
// First token is a noisy tenant (70% of tasks); the rest share 30%.
const TOKENS = __ENV.TOKENS.split(',');

// 5% critical, 15% high, 50% normal, 30% low.
const PRIOS = [].concat(
  Array(1).fill('critical'), Array(3).fill('high'), Array(10).fill('normal'), Array(6).fill('low'));

export const options = {
  scenarios: {
    create: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: `${Math.ceil(TASKS / RATE)}s`,
      preAllocatedVUs: 400,
      maxVUs: 1000,
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.001'],
    'http_req_duration{expected_response:true}': ['p(99)<500'],
  },
  summaryTrendStats: ['avg', 'med', 'p(95)', 'p(99)', 'max'],
};

export default function () {
  const i = exec.scenario.iterationInTest;
  // Tenant is chosen per block of 20 so every tenant gets the full priority mix.
  const b = Math.floor(i / PRIOS.length);
  const token = b % 10 < 7 || TOKENS.length === 1 ? TOKENS[0] : TOKENS[1 + (b % (TOKENS.length - 1))];
  const res = http.post(`${API}/tasks`, JSON.stringify({
    name: `load-${i}`,
    at: AT,
    priority: PRIOS[i % PRIOS.length],
    request: { method: 'POST', url: TARGET, headers: { Authorization: 'Bearer load-test' }, body: '{}' },
    timeout_ms: 5000,
    retry: { max_attempts: 3, backoff_ms: 1000 },
  }), {
    headers: {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json',
      'Idempotency-Key': `${RUN}-${i}`,
    },
  });
  check(res, { 'task created': (r) => r.status === 201 });
}
