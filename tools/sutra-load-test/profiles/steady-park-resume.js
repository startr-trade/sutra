// steady-park-resume — stateful flows: spawn -> park on a correlated wait -> relay -> complete.
//
// Design note: "Stateful flows: spawn -> park -> correlated
// relay -> complete; isolates step-commit cost, per-instance serialization overhead, handoff
// rate." At shard-count=1 (Phase 0's only mode) there is no handoff (the arrival shard IS the
// owner shard, design §1.1) — this run's number is the pre-handoff baseline every N>1 run
// (Phase 2+) is compared against for handoff/claim overhead.
//
// Fixture: tools/sutra-load-test/fixtures/saturation/bpmn/hold-relay.bpmn — spawn-in mints +
// parks an instance keyed by a `key` field (<q:alias unique="true" onConflict="correlate">);
// relay-in, carrying the SAME key, correlates and resumes it to completion. No <q:reply> node
// on either side — every request gets a 202 Accepted once its step commits.
//
// Per iteration: one FRESH key (never reused — this profile is the one-relay-per-spawn shape;
// correlation-heavy.js is the many-relays-to-a-small-pool shape), spawn it, then immediately
// relay it. Both legs are timed and checked independently, and both count toward
// http_req_duration — the reported p50/p95/p99 blends "commit a park" and "commit a resume +
// complete", which is the number the design's "step-commit cost" line means to capture. Split
// them post-hoc from the raw --out json=... samples if you need the two costs separately (the
// two request URLs differ, so per-URL breakdown is a single jq/awk pass).
//
// Config block:
//   RATE            offered iterations/sec (each iteration = 2 requests: spawn + relay)
//   DURATION        how long to sustain RATE (default 5m, matching sustained.js's headline
//                    window)
//   BASE_URL / API_KEY — as every other profile in this directory.

import http from "k6/http";
import { check } from "k6";

const BASE_URL = __ENV.BASE_URL || "http://127.0.0.1:8080";
const API_KEY = __ENV.API_KEY || "saturation-bench-key";
const SPAWN_URL = __ENV.SPAWN_URL || "/channels/spawn-in";
const RELAY_URL = __ENV.RELAY_URL || "/channels/relay-in";
const RATE = parseInt(__ENV.RATE || "50", 10);
const DURATION = __ENV.DURATION || "5m";

export const options = {
    scenarios: {
        "steady-park-resume": {
            executor: "constant-arrival-rate",
            rate: RATE,
            timeUnit: "1s",
            duration: DURATION,
            preAllocatedVUs: 20,
            maxVUs: 500,
            gracefulStop: "10s",
        },
    },
    thresholds: {
        http_req_failed: ["rate<0.01"],
    },
    summaryTrendStats: ["avg", "min", "med", "max", "p(90)", "p(95)", "p(99)"],
};

const headers = () => ({
    "Content-Type": "application/json",
    "X-Api-Key": API_KEY,
});

export default function () {
    // A fresh, globally-unique key per iteration: VU id + iteration count + a scenario-local
    // timestamp fragment keeps this collision-free across the whole run without any cross-VU
    // coordination.
    const key = `spr-${__VU}-${__ITER}-${Date.now()}`;

    const spawnRes = http.post(`${BASE_URL}${SPAWN_URL}`, JSON.stringify({ key }), {
        headers: headers(),
        timeout: "10s",
    });
    check(spawnRes, { "spawn: status is 2xx": (r) => r.status >= 200 && r.status < 300 });

    const relayRes = http.post(`${BASE_URL}${RELAY_URL}`, JSON.stringify({ key }), {
        headers: headers(),
        timeout: "10s",
    });
    check(relayRes, { "relay: status is 2xx": (r) => r.status >= 200 && r.status < 300 });
}
