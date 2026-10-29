/*
 * relay — the full durable relay, end to end (fixtures/relay, driven by relay-bench.sh).
 *
 * Each iteration POSTs one order to `order-in`. The engine validates it, reads two data-store
 * rows, forwards it over a local channel to a second process, receives that process's answer
 * over a second local channel, and calls back the sink (sink/) over HTTP. The sink, not this
 * script, measures the settle: it timestamps the callback against the `sentAt` stamped here.
 * This script reports only the intake leg (POST -> 202).
 *
 * Knobs (env): BASE_URL, API_KEY, RATE (orders/s, default 20), DURATION (default 60s),
 * PRE_VUS (default 50), MAX_VUS (default 1000), RUN_ID (prefix for unique refs).
 */
import http from "k6/http";
import { check } from "k6";

const BASE_URL = __ENV.BASE_URL || "http://127.0.0.1:8080";
const API_KEY = __ENV.API_KEY || "relay-bench-key";
const RATE = parseInt(__ENV.RATE || "20", 10);
const DURATION = __ENV.DURATION || "60s";
const RUN_ID = __ENV.RUN_ID || `${Date.now()}`;
const PARTIES = ["party-a", "party-b"];

export const options = {
    scenarios: {
        relay: {
            executor: "constant-arrival-rate",
            rate: RATE,
            timeUnit: "1s",
            duration: DURATION,
            preAllocatedVUs: parseInt(__ENV.PRE_VUS || "50", 10),
            maxVUs: parseInt(__ENV.MAX_VUS || "1000", 10),
            gracefulStop: "10s",
        },
    },
    summaryTrendStats: ["avg", "min", "med", "max", "p(90)", "p(95)", "p(99)"],
};

export default function () {
    const ref = `r-${RUN_ID}-${__VU}-${__ITER}`;
    const body = JSON.stringify({
        ref,
        amount: 100 + (__ITER % 50),
        party: PARTIES[__ITER % PARTIES.length],
        sentAt: Date.now(),
    });
    const res = http.post(`${BASE_URL}/channels/order-in`, body, {
        headers: { "Content-Type": "application/json", "X-Api-Key": API_KEY },
        timeout: "10s",
    });
    check(res, { "intake: status is 2xx": (r) => r.status >= 200 && r.status < 300 });
}
