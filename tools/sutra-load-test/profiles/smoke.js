// Smoke load profile — ~30 s @ 10 RPS, 1 VU.
//
// Purpose: cheapest possible "the SUT serves traffic correctly" check.
//   - Confirms the channel route is reachable and authenticated.
//   - Confirms the engine produces a successful (2xx) response per request.
//   - Confirms the harness JSON output (open(), check(), thresholds) is wired correctly.
//
// The cheapest profile — safe to run on every change. Total request budget: ~300 requests.
//
// Inputs read from env (set by run.sh):
//   BASE_URL   default http://127.0.0.1:8080
//   API_KEY    default hello-demo-key — the sample hello-in channel's api-key
//   PAYLOAD    absolute path to the request body (default tools/sutra-load-test/fixtures/hello.txt)
//
// Drives the sample hello-in HTTP channel (POST /channels/hello-in, text/plain body →
// "Hello, <body>!"). The path is passed in by the harness; we open() it at init time,
// which is k6's idiom for static request bodies.

import http from "k6/http";
import { check } from "k6";

const BASE_URL = __ENV.BASE_URL || "http://127.0.0.1:8080";
const API_KEY = __ENV.API_KEY || "hello-demo-key";
const PAYLOAD_PATH = __ENV.PAYLOAD || "../fixtures/hello.txt";

// open() is k6's init-time file-read; runs once before VUs start. The 'b' flag
// returns an ArrayBuffer so the body bytes are preserved exactly (no charset
// re-encoding surprises).
const BODY = open(PAYLOAD_PATH, "b");

export const options = {
    scenarios: {
        smoke: {
            executor: "constant-arrival-rate",
            rate: 10,                    // 10 RPS
            timeUnit: "1s",
            duration: "30s",
            preAllocatedVUs: 1,
            maxVUs: 4,
            gracefulStop: "5s",
        },
    },
    // run.sh applies its own exit-code thresholds on top of these (smoke is
    // permissive — anything green here means the wire path works).
    thresholds: {
        http_req_failed: ["rate<0.01"],
        http_req_duration: ["p(95)<500"],
    },
    // k6 defaults to "avg,min,med,max,p(90),p(95)" for trend summary stats.
    // We need p(99) in --summary-export so run.sh's MD summary + threshold
    // check can render / compare it.
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
            // k6 default is 60s; tighten so a hung SUT shows up as a timeout
            // instead of dragging the run.
            timeout: "10s",
        },
    );

    check(res, {
        "status is 2xx": (r) => r.status >= 200 && r.status < 300,
    });
}
