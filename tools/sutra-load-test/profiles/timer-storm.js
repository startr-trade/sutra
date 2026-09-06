// timer-storm — arm k thousand short-duration timers due at (approximately) one instant;
// fire-to-complete drain is measured OUTSIDE this script.
//
// Design note: "Arm k*10^3 timers due in one instant;
// isolates poller fan-out (§5's bounded concurrency), shard convoying, claim-defer churn."
//
// Fixture: tools/sutra-load-test/fixtures/saturation/bpmn/timer-storm.bpmn — spawn, park on a
// fixed PT2S timer, done. No relay, no reply: every arm request gets a 202 Accepted once the
// park commits. Because every armed instance carries the SAME PT2S duration and this script
// arms them all in one tight burst (shared-iterations: every VU grabs the next of k total
// iterations as fast as it can), their due-at timestamps land within the same handful of
// poller ticks — the "storm".
//
// *** This script only measures the ARM phase (how fast k spawns can be POSTed and their
// steps committed). It intentionally does NOT wait for the timers to fire — k6 has no way to
// observe internal engine state. Fire-to-complete DRAIN (due-at -> every instance COMPLETED)
// is measured by the driver script, rust/bench/saturation-matrix.sh, which polls
// GET /sutra/instances?status=WAITING after this script exits until the count reaches zero
// (or a timeout) and records the elapsed time. Run this profile only via the driver script,
// or replicate its post-run poll by hand if driving it directly. ***
//
// NOTE — the poller's own ceiling: due timers fire in batches of 32 per 500 ms tick PER
// DEPLOYMENT (rust/crates/sutra-engine/src/timer.rs, TimerPollerConfig::default — cited in the
// design's §7 table). That is TODAY's single poller loop's ceiling (~64 fires/s), not engine
// saturation — a drain-rate plateau near there reflects the poller, not the shard count. Don't
// cite a timer-storm drain-rate number as an engine-saturation claim without checking it against
// this ceiling first.
//
// Config block:
//   K       total timers to arm (default 2000 = 2*10^3; the design's headline shape is
//           "k*10^3" — override K=5000, K=10000, etc. for the k-matrix)
//   VUS     concurrent arm-phase workers (default 100)
//   MAX_DURATION  arm-phase time budget safety cap (default 60s)

import http from "k6/http";
import { check } from "k6";

const BASE_URL = __ENV.BASE_URL || "http://127.0.0.1:8080";
const API_KEY = __ENV.API_KEY || "saturation-bench-key";
const TIMER_URL = __ENV.TIMER_URL || "/channels/timer-in";
const K = parseInt(__ENV.K || "2000", 10);
const VUS = parseInt(__ENV.VUS || "100", 10);
const MAX_DURATION = __ENV.MAX_DURATION || "60s";

export const options = {
    scenarios: {
        "timer-storm-arm": {
            executor: "shared-iterations",
            vus: VUS,
            iterations: K,
            maxDuration: MAX_DURATION,
        },
    },
    thresholds: {
        http_req_failed: ["rate<0.01"],
    },
    summaryTrendStats: ["avg", "min", "med", "max", "p(90)", "p(95)", "p(99)"],
};

export default function () {
    const key = `storm-${__VU}-${__ITER}`;
    const res = http.post(`${BASE_URL}${TIMER_URL}`, JSON.stringify({ key }), {
        headers: {
            "Content-Type": "application/json",
            "X-Api-Key": API_KEY,
        },
        timeout: "10s",
    });
    check(res, { "arm: status is 2xx": (r) => r.status >= 200 && r.status < 300 });
}
