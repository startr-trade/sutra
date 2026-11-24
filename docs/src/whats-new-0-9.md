# What's new in 0.9

Engine 0.9.0 makes audit something a flow declares, and keeps it apart from the engine's database.
It changes the `q:` namespace (the `sink` and `target` attributes of `q:audit` are removed) and adds a
migration family of its own for audit. What changes on upgrade is listed in
[Upgrading to 0.9](operating/upgrading-0-9.md).

| Feature | What it gives you | Try it |
|---|---|---|
| **No tag, no audit** | A process is audited only when its BPMN carries `<q:audit>`. A bare tag is `events="points" capture="metadata"`; `events="all"` keeps the 0.8 trail. | [Declaring audit](building/q-namespace.md#declaring-audit-qaudit) |
| **Audit points** | `events="points"` records the nodes where something observable happens (events, waits, message exchanges, calls) and skips routing gateways and local computation. | [Recording audit points](building/recording-audit-points.md) |
| **Audit in a store of its own** | A database of its own (`sutra.audit.sql.url`) or JSONL files (`sutra.audit.jsonl.dir`); never the engine's database. `sutra migrate --audit-url` prepares it. | [Audit sinks](operating/configuration.md#audit-sinks) |
| **A durable spool** | Every event is fsynced to a local spool before its step commits, and shipped to the stores after. If the spool cannot be written, readiness goes DOWN rather than lose audit. | [Upgrading to 0.9](operating/upgrading-0-9.md#5-the-spool-mount-it-on-persistent-storage) |
| **Only committed steps are audited** | A step that did not commit never reaches a store; its re-run is recorded instead. Each event carries the pass that wrote it. | [Recording audit points](building/recording-audit-points.md#4-read-the-audit-files) |
| **Linter and codemod** | `sutra lint --audit` shows what is audited and why; `sutra lint --fix audit` adds the tag and moves `sink` / `target` into the manifest. | [`sutra lint`](reference/cli.md#sutra-lint) |
| **Fired boundary events are recorded** | An error, escalation, cancel or timer boundary that fires now records `NODE_LEFT`. | [Upgrading to 0.9](operating/upgrading-0-9.md#6-a-fired-boundary-event-now-records-node_left) |
