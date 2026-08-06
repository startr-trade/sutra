// Sustained load profile — 5 min @ 100 RPS sustained, 4 VUs base.
//
// Purpose: the headline number. Captures p50 / p95 / p99 latency, total
// throughput, and non-2xx rate over a long-enough window that warmup, cache,
// and async-buffer drains all stabilise.
//
// Used by:
//   - CI throughput runs against the engine
//   - on-demand workflow_dispatch runs
//   - operator hand-runs against a deployed instance (--target=external)
//
// Total request budget: 100 RPS × 300 s ≈ 30,000 requests.
//
// Why constant-arrival-rate (not constant-vus): k6's constant-vus mode lets
// the client thread back-pressure into the SUT — a slow response from the SUT
// reduces the offered load, which makes "did the engine handle 100 RPS?"
// impossible to answer. constant-arrival-rate fixes the offered RPS so we
// measure SUT behaviour, not client behaviour.

import http from "k6/http";
import { check } from "k6";

const BASE_URL = __ENV.BASE_URL || "http://127.0.0.1:8080";
const API_KEY = __ENV.API_KEY || "hello-demo-key";
const PAYLOAD_PATH = __ENV.PAYLOAD || "../fixtures/hello.txt";
const BODY = open(PAYLOAD_PATH, "b");
// Overridable so this profile can drive a real deployment's channel (e.g. the
// money-transfer `balance` channel) instead of only the sample `hello-in`.
// Both default to the original hardcoded values, so an unset env leaves behaviour unchanged.
const CHANNEL_URL = __ENV.CHANNEL_URL || "/channels/hello-in";
const CONTENT_TYPE = __ENV.CONTENT_TYPE || "text/plain";
// RATE / DURATION are env-overridable so a short proof run (e.g. RATE=100 DURATION=20s) and the
// headline 5-min run share one profile. Unset → the committed 100-RPS × 5-min headline window.
const RATE = parseInt(__ENV.RATE || "100", 10);
const DURATION = __ENV.DURATION || "5m";

export const options = {
    scenarios: {
        sustained: {
            executor: "constant-arrival-rate",
            rate: RATE,
            timeUnit: "1s",
            duration: DURATION,
            preAllocatedVUs: 4,
            // maxVUs guard: if the SUT slows to a crawl, k6 spawns more VUs
            // to keep offered load at 100 RPS. Cap so we don't exhaust the
            // runner — past this the offered rate degrades and we'll see it
            // in the "dropped iterations" metric in the JSON output.
            maxVUs: 200,
            gracefulStop: "10s",
        },
    },
    thresholds: {
        http_req_failed: ["rate<0.005"],
        http_req_duration: ["p(95)<500", "p(99)<1000"],
    },
    // p(99) needs to be in summaryTrendStats so --summary-export emits it.
    summaryTrendStats: ["avg", "min", "med", "max", "p(90)", "p(95)", "p(99)"],
};

export default function () {
    const res = http.post(
        `${BASE_URL}${CHANNEL_URL}`,
        BODY,
        {
            headers: {
                "Content-Type": CONTENT_TYPE,
                "X-API-Key": API_KEY,
            },
            timeout: "10s",
        },
    );

    check(res, {
        "status is 2xx": (r) => r.status >= 200 && r.status < 300,
    });
}
