// mixed — a weighted blend of the other four saturation profiles' workload shapes, running
// concurrently against the same engine.
//
// Design note: "Weighted blend of the above; the number a
// comparison page may eventually quote." Per the honesty rule (this directory's README, and
// the design doc's §7 opening line): no throughput/saturation number from ANY run of this
// profile is cited anywhere until it has actually been run on a committed profile — this
// comment is not a preemptive claim, it is a description of what gets measured once it is.
//
// *** WEIGHTS — the one config block, per the task contract. Fractions of TOTAL_RATE
// (constant-arrival-rate iterations/sec) each workload shape gets. Must sum to 1.0; the
// module-load assertion below fails loudly if they don't. ***
const WEIGHTS = {
    runToEnd: 0.4, // burst-start's shape: stateless spawn, no wait (see run-to-end.bpmn)
    holdRelayFresh: 0.2, // steady-park-resume's shape: fresh key, spawn + relay per iteration
    holdRelayPool: 0.3, // correlation-heavy's shape: many relays against a small key pool
    timerArm: 0.1, // a steady TRICKLE of timer arms — NOT timer-storm's tight burst; run
    // timer-storm.js separately for the burst-fire-drain shape, this is only
    // the "some background timer load" component of a blended workload.
};

import http from "k6/http";
import { check } from "k6";

const sumWeights = Object.values(WEIGHTS).reduce((a, b) => a + b, 0);
if (Math.abs(sumWeights - 1.0) > 1e-9) {
    throw new Error(`mixed.js WEIGHTS must sum to 1.0, got ${sumWeights}`);
}

const BASE_URL = __ENV.BASE_URL || "http://127.0.0.1:8080";
const API_KEY = __ENV.API_KEY || "saturation-bench-key";
const WORK_URL = __ENV.WORK_URL || "/channels/work-in";
const SPAWN_URL = __ENV.SPAWN_URL || "/channels/spawn-in";
const RELAY_URL = __ENV.RELAY_URL || "/channels/relay-in";
const TIMER_URL = __ENV.TIMER_URL || "/channels/timer-in";
// TOTAL_RATE is the combined offered iterations/sec across all four scenarios; each
// scenario's own rate is TOTAL_RATE * its weight (floored, minimum 1).
const TOTAL_RATE = parseInt(__ENV.TOTAL_RATE || "200", 10);
const DURATION = __ENV.DURATION || "5m";
// The correlation-heavy-shaped scenario's pool size (see holdRelayPool below).
const POOL_SIZE = parseInt(__ENV.POOL_SIZE || "20", 10);

const rateFor = (weight) => Math.max(1, Math.floor(TOTAL_RATE * weight));

export const options = {
    scenarios: {
        "run-to-end": {
            executor: "constant-arrival-rate",
            exec: "runToEnd",
            rate: rateFor(WEIGHTS.runToEnd),
            timeUnit: "1s",
            duration: DURATION,
            preAllocatedVUs: 20,
            maxVUs: 500,
            gracefulStop: "10s",
        },
        "hold-relay-fresh": {
            executor: "constant-arrival-rate",
            exec: "holdRelayFresh",
            rate: rateFor(WEIGHTS.holdRelayFresh),
            timeUnit: "1s",
            duration: DURATION,
            preAllocatedVUs: 20,
            maxVUs: 500,
            gracefulStop: "10s",
        },
        "hold-relay-pool": {
            executor: "constant-arrival-rate",
            exec: "holdRelayPool",
            rate: rateFor(WEIGHTS.holdRelayPool),
            timeUnit: "1s",
            duration: DURATION,
            preAllocatedVUs: 20,
            maxVUs: 500,
            gracefulStop: "10s",
        },
        "timer-arm": {
            executor: "constant-arrival-rate",
            exec: "timerArm",
            rate: rateFor(WEIGHTS.timerArm),
            timeUnit: "1s",
            duration: DURATION,
            preAllocatedVUs: 10,
            maxVUs: 200,
            gracefulStop: "10s",
        },
    },
    thresholds: {},
    summaryTrendStats: ["avg", "min", "med", "max", "p(90)", "p(95)", "p(99)"],
};

const headers = () => ({
    "Content-Type": "application/json",
    "X-Api-Key": API_KEY,
});

// setup() runs once, before any scenario's clock starts — seeds the holdRelayPool pool exactly
// like correlation-heavy.js.
export function setup() {
    for (let i = 0; i < POOL_SIZE; i++) {
        const key = `mixed-pool-${i}`;
        const res = http.post(`${BASE_URL}${SPAWN_URL}`, JSON.stringify({ key }), {
            headers: headers(),
            timeout: "10s",
        });
        check(res, { "setup spawn: status is 2xx": (r) => r.status >= 200 && r.status < 300 });
    }
    return { poolSize: POOL_SIZE };
}

// run-to-end.bpmn — see burst-start.js.
export function runToEnd() {
    const res = http.post(`${BASE_URL}${WORK_URL}`, JSON.stringify({ key: "mixed-run-to-end" }), {
        headers: headers(),
        timeout: "10s",
    });
    check(res, { "run-to-end: status is 2xx": (r) => r.status >= 200 && r.status < 300 });
}

// hold-relay.bpmn, fresh key per iteration — see steady-park-resume.js.
export function holdRelayFresh() {
    const key = `mixed-fresh-${__VU}-${__ITER}-${Date.now()}`;
    const spawnRes = http.post(`${BASE_URL}${SPAWN_URL}`, JSON.stringify({ key }), {
        headers: headers(),
        timeout: "10s",
    });
    check(spawnRes, { "hold-relay-fresh spawn: status is 2xx": (r) => r.status >= 200 && r.status < 300 });

    const relayRes = http.post(`${BASE_URL}${RELAY_URL}`, JSON.stringify({ key }), {
        headers: headers(),
        timeout: "10s",
    });
    check(relayRes, { "hold-relay-fresh relay: status is 2xx": (r) => r.status >= 200 && r.status < 300 });
}

// hold-relay.bpmn, small fixed pool with self-healing refill — see correlation-heavy.js.
export function holdRelayPool(data) {
    const idx = Math.floor(Math.random() * data.poolSize);
    const key = `mixed-pool-${idx}`;

    const relayRes = http.post(`${BASE_URL}${RELAY_URL}`, JSON.stringify({ key }), {
        headers: headers(),
        timeout: "10s",
    });
    // Informational only — see correlation-heavy.js for why a bounce/miss is not a failure.
    check(relayRes, { "hold-relay-pool relay: no 5xx / transport error": (r) => r.status < 500 && r.status !== 0 });

    if (relayRes.status >= 200 && relayRes.status < 300) {
        http.post(`${BASE_URL}${SPAWN_URL}`, JSON.stringify({ key }), {
            headers: headers(),
            timeout: "10s",
        });
    }
}

// timer-storm.bpmn, steady trickle (not a burst) — see timer-storm.js for the real storm shape.
export function timerArm() {
    const key = `mixed-timer-${__VU}-${__ITER}`;
    const res = http.post(`${BASE_URL}${TIMER_URL}`, JSON.stringify({ key }), {
        headers: headers(),
        timeout: "10s",
    });
    check(res, { "timer-arm: status is 2xx": (r) => r.status >= 200 && r.status < 300 });
}
