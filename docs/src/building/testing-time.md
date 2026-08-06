# Testing time: fast-forwarding durable timers

A flow that waits `PT24H` for a reminder, or fires an `R3/PT12H` schedule three times, is exactly
the kind of thing that is hard to test and therefore usually untested. Sutra's answer is a
**virtual clock**: the engine's notion of "now" becomes something a test drives explicitly, so a
day-long timer settles in wall-clock seconds against a real database, with real durable rows and
the real timer poller.

Two ways in: a CLI command for BPMN authors, and an embedded seam for anyone writing tests in
Rust.

## `sutra test simulate` — no test code required

```
sutra test simulate --deployments <DIR> --datasource <URL> \
    (--advance <DURATION> | --until-quiescent) [flags]
```

Boots a real engine on a dynamic port against a directory of sealed deployment archives with a
virtual clock installed, fast-forwards it, reports, and shuts down cleanly.

This is **unrelated to `sutra simulate`**, which is a dry-run routing report over a single BPMN
file and never boots anything. They are separate commands on purpose.

| Flag | Meaning |
|---|---|
| `--deployments <DIR>` | Directory of sealed `.sutra` archives to serve. Required. |
| `--datasource <URL>` (`--datasource-username` / `--datasource-password`) | Engine datasource. Required — the same canonical `SUTRA_DATASOURCE_*` env names a real engine container reads. |
| `--advance <DURATION>` | Fast-forward the virtual clock by this ISO-8601 duration, firing everything due along the way, then stop and report. |
| `--until-quiescent` | Fast-forward until nothing is armed and nothing is live, or `--timeout` elapses. |
| `--timeout <DURATION>` | **Real** wall-clock budget for the fast-forward loop, either mode. Default `PT30S`. |
| `--start <RFC3339>` | Virtual start instant. Default: the real current instant. |
| `--allow-existing-data` | Proceed even though the datasource already holds instances. |

Exactly one of `--advance` / `--until-quiescent` is required.

### It refuses to run against data it did not expect

Fast-forwarding a virtual clock against a database that already holds real, in-flight instances
would durably fire their **real** timers early. A datasource mixup must not silently do that, so
before booting anything the command checks that the target database holds no instances, and
refuses with exit `2` if it does.

`--allow-existing-data` is the explicit acknowledgement — and also the right shape for "seed one
instance externally, then fast-forward it".

`--datasource` being required is part of the same posture: a persistence-less run has no durable
timers to advance at all, so the command refuses rather than pretending to have done something.

### Quiescence, precisely

`--until-quiescent` stops when, simultaneously: no waiting row is an armed timer, no schedule is
armed, and no instance is still live.

A `FAILED` instance is deliberately *not* counted as finished — it needs an operator, not a clock.
So a fixture that fails under fast-forward correctly reports `timedOut: true` rather than a false
"quiescent", and the summary's `instancesFailed` says why.

### Output

Human-readable progress goes to **stderr**. The final summary is one JSON object on **stdout** and
nothing else touches stdout, so piping into `jq` is always safe:

```json
{
  "mode": "until-quiescent",
  "deployments": "/path/to/deployments",
  "allowExistingData": false,
  "timedOut": false,
  "quiescent": true,
  "preExistingInstances": 0,
  "virtualStart": "2026-08-06T00:00:00Z",
  "virtualEnd": "2026-08-07T00:00:03Z",
  "virtualSecondsAdvanced": 86403.2,
  "wallSeconds": 1.84,
  "instancesStarted": 1,
  "instancesCompleted": 1,
  "instancesFailed": 0,
  "instancesTerminated": 0,
  "instancesLive": 0,
  "timersFired": 1,
  "schedulesFired": 0
}
```

`schedulesFired` counts single-shot and bounded (`R<n>/…`) timer-start fires exactly. An unbounded
cycle (`R/…`) has no repeat budget to difference, so its fires show up in `instancesStarted` but
are not separately counted in this release.

Use it to prove, in your application's own CI, that a durable-timer or cyclic-schedule flow settles
correctly — without a wall-clock-length test run.

## The embedded seam

`sutra_engine::TestClock` is a manually-advanced virtual clock, and
`sutra_engine::fast_forward_until` is the paired driver. Install the clock on the engine config
before serving, and every temporal read in that boot uses it: timer park due instants, `<q:retry>`
backoff instants, the timer poller's per-tick claim instant, and schedule arming.

```rust
let clock = sutra_engine::TestClock::starting_now();
let engine = sutra_engine::serve(sutra_engine::EngineConfig {
    now_override: Some(clock.clone()),
    ..config
}).await?;

// park a PT24H timer, then:
let settled = sutra_engine::fast_forward_until(
    &pool, &clock, std::time::Duration::from_secs(10),
    || async { live_instance_count(&pool).await == 0 },
).await;
assert!(settled);
```

Every clone of the clock is a handle onto the *same* instant, so the code under test and the test
driving it always agree. `starting_now()` starts at the real current instant — only the
fast-forward is virtual — which keeps any absolute-time assertion elsewhere in the test (log
timestamps, audit rows) sane.

The driver loop is deliberately simple: jump the clock to the earliest armed due instant, give the
real timer poller a tick to claim and fire it, re-check your condition; repeat until the condition
holds or the **real** wall-clock timeout elapses. It only ever touches the clock and the database
— never the poller's internals. Set the poller's tick interval low for the test boot so a tick is
cheap.

### It is unreachable from a deployed engine — by construction

There is **no config key, no environment variable, and no CLI flag** on the engine binary that
installs a virtual clock. Wiring one in is always an explicit choice in Rust at the call site, so
an operator has no way to reach it in a deployed engine.

`sutra test simulate` is not a hole in that: it is a *different* binary that constructs an engine
configuration directly in code — exactly the sanctioned pattern, just packaged as a reusable tool
instead of a bespoke integration test per application. The engine's own configuration loading is
untouched by it.

## What it is good for

- A `timeDuration` catch timer or `<q:timeout>` boundary that would otherwise need a real wait.
- A `timeCycle` schedule firing its full repeat budget.
- A `<q:retry>` backoff curve running to exhaustion — every attempt's park is a real durable row,
  and fast-forward walks them all.

See [Retries, history, and schedules](retries-history-schedules.md) for what each of those
constructs does, and [Testing tiers](../contributing.md) for where a test like this belongs in the
suite.

## Next

- **[Retries, history, and schedules](retries-history-schedules.md)** — the timers being
  fast-forwarded.
- **[`sutra` CLI reference](../reference/cli.md#sutra-test-simulate)** — the command in the full
  CLI map.
