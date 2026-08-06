// Burst load profile — 2 min total, 0→500 RPS ramp / 60 s hold / ramp-down.
//
// Purpose: simulate a peak-hour traffic spike. Captures how fast latency
// degrades under transient overload and how cleanly the SUT recovers when the
// load comes back off.
//
// Shape:
//   t=0   →  30s  ramp 0 → 500 RPS
//   t=30s →  90s  hold at 500 RPS (the "stress" window)
//   t=90s → 120s  ramp 500 → 0 RPS
//
// Total request budget: ~45,000 requests (triangular integral of the ramps
// plus the 60 s × 500 RPS hold).
//
// What to look for in the JSON output:
//   - http_req_duration during the hold window (90 s slice) — that's the
//     overload latency tail. Compare with sustained.js's 100 RPS tail.
//   - dropped_iterations — if non-zero, the runner ran out of VUs and the
//     offered load was below 500 RPS. Bump maxVUs or accept lower offered
//     RPS as the runner ceiling.
//   - http_req_duration during the ramp-down — should return to baseline
//     within a few seconds; a long tail here implies queueing in the engine.

import http from "k6/http";
import { check } from "k6";

const BASE_URL = __ENV.BASE_URL || "http://127.0.0.1:8080";
const API_KEY = __ENV.API_KEY || "hello-demo-key";
const PAYLOAD_PATH = __ENV.PAYLOAD || "../fixtures/hello.txt";
const BODY = open(PAYLOAD_PATH, "b");

export const options = {
    scenarios: {
        burst: {
            executor: "ramping-arrival-rate",
            startRate: 0,
            timeUnit: "1s",
            preAllocatedVUs: 50,
            maxVUs: 1000,
            stages: [
                { duration: "30s", target: 500 },
                { duration: "60s", target: 500 },
                { duration: "30s", target: 0 },
            ],
            gracefulStop: "10s",
        },
    },
    // Thresholds are deliberately looser than sustained — the whole point of
    // burst is to drive past the green window. We still want a sanity floor
    // on the error rate so a "broken SUT returning 500s" doesn't quietly
    // pass.
    thresholds: {
        http_req_failed: ["rate<0.05"],
        http_req_duration: ["p(95)<2000"],
    },
    // p(99) needs to be in summaryTrendStats so --summary-export emits it.
    summaryTrendStats: ["avg", "min", "med", "max", "p(90)", "p(95)", "p(99)"],
};

export default function () {
    const res = http.post(
        `${BASE_URL}/channels/hello-in`,
        BODY,
        {
            headers: {
                "Content-Type": "text/plain",
                "X-API-Key": API_KEY,
            },
            timeout: "10s",
        },
    );

    check(res, {
        "status is 2xx": (r) => r.status >= 200 && r.status < 300,
    });
}
