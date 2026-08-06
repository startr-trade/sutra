# sutra-load-test — end-to-end load harness

Pure-shell k6 harness that exercises the full inbound-HTTP → BPMN-engine → audit-write
path under sustained concurrent load against a running Sutra deployment.

This is **not** a microbenchmark suite — those measure the FEEL parser/evaluator hot
path; this measures end-to-end throughput and latency from the operator's point of view.

**Why this is a shell module, not a workspace crate**: the harness drives a *deployed*
engine over HTTP with an external tool (k6). There is nothing to compile and no build
integration to gain, so it stays a standalone shell module.

**Tool**: [k6](https://k6.io/). Picked over wrk2 because the install story is
clean (single binary via `apt`/`brew`), JSON output is native (no Lua glue),
and the script language is plain JS. wrk2 would need C compile + Lua.

## Run locally

```bash
# 1. install k6 (Linux):
sudo gpg -k && sudo gpg --no-default-keyring --keyring /usr/share/keyrings/k6-archive-keyring.gpg --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys C5AD17C747E3415A3642D57D77C6C491D6AC1D69
echo "deb [signed-by=/usr/share/keyrings/k6-archive-keyring.gpg] https://dl.k6.io/deb stable main" | sudo tee /etc/apt/sources.list.d/k6.list
sudo apt-get update && sudo apt-get install -y k6

# 2. bring up the SUT yourself — e.g. build and run the engine image
#    (docker build -t sutra-rust-engine:dev -f rust/Dockerfile rust/), or deploy it
#    to a cluster. The harness never starts anything; it only generates load.

# 3. smoke run (30 s, ~300 requests) against the running deployment:
tools/sutra-load-test/run.sh --target=external --url=http://127.0.0.1:8081 --profile=smoke
```

`--target=external` is the only target: the harness points k6 at a base URL that already
serves the channel under test. Set `SUTRA_API_KEY=...` when the target deployment's
channel resolves an api-key other than the sample `hello-demo-key`, and `PAYLOAD=/abs/path`
for a request body other than `fixtures/hello.txt`.

## Layout

```
tools/sutra-load-test/
├── README.md          (this file)
├── run.sh             (harness: run k6 against a URL, capture results, exit code)
├── profiles/
│   ├── smoke.js       (~30 s, 10 RPS, 1 VU — PR feedback)
│   ├── sustained.js   (~5 min, 100 RPS, 4 VUs — headline number)
│   └── burst.js       (~2 min, 0→500 RPS ramp + hold + ramp-down — peak hour)
└── results/           (.gitignore'd; CI uploads as artifacts)
```

## Profiles at a glance

| Profile | Duration | Offered RPS | Budget | When |
|---|---|---|---|---|
| smoke     | 30 s   | 10                       | ~300 reqs    | every PR / pre-merge |
| sustained | 5 min  | 100                      | ~30,000 reqs | nightly CI / baselines |
| burst     | 2 min  | 0→500 ramp + hold + down | ~45,000 reqs | peak-hour stress, on-demand |

## Exit codes

| Code | Meaning |
|---|---|
| 0 | all thresholds green |
| 1 | error-rate threshold breached (`ERROR_RATE_THRESHOLD`, default 0.5%) |
| 2 | p99 latency threshold breached (`P99_MS_THRESHOLD`, default 500 ms) |
| 3 | harness error (bad arguments, k6 missing, no k6 output) |
