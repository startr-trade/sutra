// burst-start — offered-RPS staircase of run-to-end spawns until the achieved rate flattens.
//
// Design: docs/design/execution-scale-out.md §7 — "Offered-RPS ramp of run-to-end spawns
// until achieved rate flattens; isolates the saturation knee: pure shard parallelism, no
// claims." At shard-count=1 (today's engine — Phase 0 has no sharding yet) there is exactly
// one actor thread, so this profile's real job in Phase 0 is recording the SINGLE-LANE
// saturation knee as the baseline every later N-shard run is compared against.
//
// Fixture: tools/sutra-load-test/fixtures/saturation/bpmn/run-to-end.bpmn — spawn, one FEEL
// data assignment, done. No wait node, no reply template: every request gets a 202 Accepted
// once its step commits (rust/crates/sutra-channels/src/http.rs), so this profile measures
// pure spawn-to-commit throughput with no correlation/claim machinery in play.
//
// Shape: a STAIRCASE, not a single ramp — each stage holds a fixed offered RPS long enough
// for the achieved rate to stabilise, then steps up. Reading the JSON output: plot
// http_reqs.rate (or, for per-timestamp resolution, re-run with `k6 run --out json=...`)
// against each stage's offered rate — the knee is the stage where achieved stops tracking
// offered. p95/p99 climbing while achieved plateaus is the same signal from the latency side.
//
// Config block — every knob overridable via env so a short proof run and the committed
// headline staircase share one profile:
//   STAGES        comma-separated offered-RPS list, ascending (default a wide staircase)
//   STAGE_DURATION  how long each stage holds its rate (default 30s)
//   BASE_URL / API_KEY / CHANNEL_URL — as every other profile in this directory.

import http from "k6/http";
import { check } from "k6";

const BASE_URL = __ENV.BASE_URL || "http://127.0.0.1:8080";
const API_KEY = __ENV.API_KEY || "saturation-bench-key";
const CHANNEL_URL = __ENV.CHANNEL_URL || "/channels/work-in";
const STAGE_DURATION = __ENV.STAGE_DURATION || "30s";
// Default staircase: 50 -> 100 -> 200 -> 400 -> 800 -> 1600 offered RPS. Override with e.g.
// STAGES=50,100,200 for a short proof run.
const STAGES = (__ENV.STAGES || "50,100,200,400,800,1600")
    .split(",")
    .map((s) => parseInt(s.trim(), 10));

const BODY = JSON.stringify({ key: "burst-start" });

export const options = {
    scenarios: {
        "burst-start": {
            executor: "ramping-arrival-rate",
            startRate: 0,
            timeUnit: "1s",
            preAllocatedVUs: 100,
            // Generous ceiling: if the runner exhausts VUs before the SUT saturates, that's
            // itself a finding (dropped_iterations > 0 in the JSON output) — bump maxVUs on
            // the load-generator host, don't silently cap the offered rate.
            maxVUs: 4000,
            stages: STAGES.flatMap((rate) => [{ duration: STAGE_DURATION, target: rate }]),
            gracefulStop: "10s",
        },
    },
    // Deliberately loose — the whole point is to drive past the green window and observe
    // where it breaks, not to gate on a fixed threshold. A sanity floor still catches a
    // fully-broken SUT (all 5xx) rather than reporting a silent flat line.
    thresholds: {
        http_req_failed: ["rate<0.5"],
    },
    summaryTrendStats: ["avg", "min", "med", "max", "p(90)", "p(95)", "p(99)"],
};

export default function () {
    const res = http.post(`${BASE_URL}${CHANNEL_URL}`, BODY, {
        headers: {
            "Content-Type": "application/json",
            "X-Api-Key": API_KEY,
        },
        timeout: "10s",
    });

    check(res, {
        // 202 is the expected ack (no <q:reply> node); 2xx generally in case a future edit
        // adds one.
        "status is 2xx": (r) => r.status >= 200 && r.status < 300,
    });
}
