// correlation-heavy — a high relay:spawn ratio against a small, fixed pool of long-lived
// instances.
//
// Design: docs/design/execution-scale-out.md §7 — "High relay:spawn ratio to few long-lived
// instances; isolates the handoff path, hot-shard skew, claim-bounce behavior."
//
// Fixture: tools/sutra-load-test/fixtures/saturation/bpmn/hold-relay.bpmn (same BPMN as
// steady-park-resume.js — only the driving pattern differs here).
//
// The pool mechanic (why there is no shared-state library involved):
//   1. setup() seeds POOL_SIZE keys ONCE, sequentially, before the load phase starts —
//      pool-0 .. pool-{POOL_SIZE-1}, each spawned via spawn-in. This is the profile's only
//      spawn-in traffic besides self-healing refills (below), so the ratio of total relay-in
//      attempts to total spawn-in calls over a run is exactly the "high relay:spawn ratio" the
//      design names.
//   2. Every VU iteration in the load phase independently picks a RANDOM pool slot
//      (Math.random() over POOL_SIZE) and POSTs relay-in for that key. With RATE substantially
//      above POOL_SIZE, many concurrent iterations target the SAME live key at once: exactly
//      one wins the correlate + claim + resume + complete race, the rest either bounce
//      (CLAIM_HELD — the instance is mid-step on another shard/replica) or miss (the key
//      already completed and its alias retired). Both outcomes are useful signal, not errors —
//      see the checks below.
//   3. Whichever iteration's relay actually wins (2xx) immediately re-spawns the SAME key
//      (self-healing refill) so the pool stays occupied — "few long-lived instances" is
//      maintained by the traffic pattern itself, not by any single BPMN loop construct. A
//      refill racing another VU's refill of the same key is expected and harmless: it either
//      succeeds (key was free) or hits the correlate-collision path (key was already
//      refilled), which is exactly the "for now the second arrival is rejected" case documented
//      in hold-relay.bpmn's comment — never fatal to the run.
//
// Config block:
//   POOL_SIZE   number of concurrently-live keys (default 20 — "few")
//   RATE        offered relay-in attempts/sec (default 200 — "high" relative to POOL_SIZE)
//   DURATION    load-phase duration (default 3m)
//   BASE_URL / API_KEY — as every other profile in this directory.

import http from "k6/http";
import { check } from "k6";

const BASE_URL = __ENV.BASE_URL || "http://127.0.0.1:8080";
const API_KEY = __ENV.API_KEY || "saturation-bench-key";
const SPAWN_URL = __ENV.SPAWN_URL || "/channels/spawn-in";
const RELAY_URL = __ENV.RELAY_URL || "/channels/relay-in";
const POOL_SIZE = parseInt(__ENV.POOL_SIZE || "20", 10);
const RATE = parseInt(__ENV.RATE || "200", 10);
const DURATION = __ENV.DURATION || "3m";

export const options = {
    scenarios: {
        "correlation-heavy": {
            executor: "constant-arrival-rate",
            rate: RATE,
            timeUnit: "1s",
            duration: DURATION,
            preAllocatedVUs: 50,
            maxVUs: 1000,
            gracefulStop: "10s",
            // setup() runs before this scenario's clock starts.
        },
    },
    // Loose by design: a bounced (CLAIM_HELD) or missed (alias already retired) relay is an
    // EXPECTED outcome of racing many attempts against a small pool, not a failure. Only a
    // wholesale 5xx/timeout collapse should fail the run — see the explicit check below rather
    // than a blanket http_req_failed threshold (which would count expected 4xx bounces).
    thresholds: {},
    summaryTrendStats: ["avg", "min", "med", "max", "p(90)", "p(95)", "p(99)"],
};

const headers = () => ({
    "Content-Type": "application/json",
    "X-Api-Key": API_KEY,
});

export function setup() {
    for (let i = 0; i < POOL_SIZE; i++) {
        const key = `pool-${i}`;
        const res = http.post(`${BASE_URL}${SPAWN_URL}`, JSON.stringify({ key }), {
            headers: headers(),
            timeout: "10s",
        });
        check(res, { "setup spawn: status is 2xx": (r) => r.status >= 200 && r.status < 300 });
    }
    return { poolSize: POOL_SIZE };
}

export default function (data) {
    const idx = Math.floor(Math.random() * data.poolSize);
    const key = `pool-${idx}`;

    const relayRes = http.post(`${BASE_URL}${RELAY_URL}`, JSON.stringify({ key }), {
        headers: headers(),
        timeout: "10s",
    });

    // Informational, not a hard failure: how often a relay attempt actually wins the race.
    check(relayRes, { "relay: correlated (2xx)": (r) => r.status >= 200 && r.status < 300 });
    // The only real failure signal — a bounce (409/4xx miss) is expected contention noise, a
    // 5xx or transport error is not.
    check(relayRes, { "relay: no 5xx / transport error": (r) => r.status < 500 && r.status !== 0 });

    if (relayRes.status >= 200 && relayRes.status < 300) {
        // Self-healing refill: this VU's relay just retired `key` — put it straight back into
        // the pool. Best-effort: a racing refill from another VU hitting the same key is the
        // expected correlate-collision case and is not checked here.
        http.post(`${BASE_URL}${SPAWN_URL}`, JSON.stringify({ key }), {
            headers: headers(),
            timeout: "10s",
        });
    }
}
