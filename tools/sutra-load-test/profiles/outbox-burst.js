/*
 * outbox-burst — the outbound burst (fixtures/burst, driven by relay-bench.sh BURST=<rows>).
 *
 * Starts ITEMS instances as fast as VUS virtual users allow (the bench sizes ITEMS so the
 * instances emit about BURST outbox rows, and checks the intake took under 5 s). Item i asks for
 * the slow destination when its hash falls under SLOW_SHARE, and for a repeat when i % 3 == 0.
 * The sink, not this script, measures delivery lag against the `created` stamp the engine puts
 * in each request.
 *
 * Knobs (env): BASE_URL, API_KEY, ITEMS, SLOW_SHARE (default 0.3), VUS (default 100), RUN_ID.
 */
import http from "k6/http";
import exec from "k6/execution";
import { check } from "k6";

const BASE_URL = __ENV.BASE_URL || "http://127.0.0.1:8080";
const API_KEY = __ENV.API_KEY || "relay-bench-key";
const ITEMS = parseInt(__ENV.ITEMS || "1500", 10);
const SLOW_SHARE = parseFloat(__ENV.SLOW_SHARE || "0.3");
const RUN_ID = __ENV.RUN_ID || `${Date.now()}`;

export const options = {
    scenarios: {
        burst: {
            executor: "shared-iterations",
            vus: parseInt(__ENV.VUS || "100", 10),
            iterations: ITEMS,
            maxDuration: "60s",
        },
    },
    summaryTrendStats: ["avg", "min", "med", "max", "p(90)", "p(95)", "p(99)"],
};

// A stable spread of item indexes over [0, 1): Knuth's multiplicative hash.
function share(i) {
    return ((i * 2654435761) % 4294967296) / 4294967296;
}

export default function () {
    const i = exec.scenario.iterationInTest;
    const body = JSON.stringify({
        ref: `b-${RUN_ID}-${i}`,
        slow: share(i) < SLOW_SHARE,
        repeat: i % 3 === 0,
    });
    const res = http.post(`${BASE_URL}/channels/burst-in`, body, {
        headers: { "Content-Type": "application/json", "X-Api-Key": API_KEY },
        timeout: "30s",
    });
    check(res, { "intake: status is 2xx": (r) => r.status >= 200 && r.status < 300 });
}
