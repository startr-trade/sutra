# Upgrading to 0.9: audit is declared, and stored apart

0.9 changes what a package must say to be audited, and where audit is kept. Nothing about how
instances run changes. Work through the list once per package and once per engine.

## 1. No tag, no audit

In 0.8 every process was audited, whether or not it said so. From 0.9 a process is audited only
when its BPMN carries `<q:audit>`. The manifest and the engine configuration decide where audit
goes, never whether. A process without the tag deploys and runs, and its audit is silent. Deploy
logs one info line per such process (`SUTRA.DEPLOY.AUDIT.NOT_DECLARED`), and a deploy response
lists them under `diagnostics`.

To find them and add the tag:

```bash
sutra lint my-package --audit                      # which processes are undeclared, and why
sutra lint my-package --fix audit --dry-run        # the diff the codemod would apply
sutra lint my-package --fix audit --events all     # apply it; `all` reproduces 0.8's trail
```

`--events points` (the default) records fewer events: see the next item.

## 2. A bare tag has defaults

`<q:audit/>` means `events="points" capture="metadata"`. `points` records the nodes where something
observable happens (events, waits, message exchanges, calls) and skips routing gateways and local
computation. Use `events="all"` to keep 0.8's every-element trail. A tag that has only `capture`
now gets `events="points"`, which is a smaller trail than 0.8 wrote: add `events="all"` where you
need the old one (`--fix audit` does this when you pass `--events all`). The XSD default text for
`capture` is corrected to `metadata`, which is what the engine always did.

## 3. `sink` and `target` on `<q:audit>` are a load error

A flow no longer names its storage. `<q:audit sink="jsonl"/>` fails to load with
`SUTRA.BPMN.AUDIT.SINK_REMOVED`, at the process and at an element. Name the store in the manifest
instead (`audit.sink`, or `audit.sinks` for several), or set the engine's
`sutra.audit.default-sink`. `sutra lint --fix audit` moves the value for you. The manifest's
`audit.capture` was never honoured over the BPMN's; it is now ignored and the codemod removes it.

A process whose store cannot be resolved is not refused: deploy warns, and the events are counted
as `sutra.audit.undeliverable`.

## 4. Audit never lives in the engine's database

0.8 wrote `audit_event` into the engine's own database (`SUTRA_AUDIT_SQL=true`). 0.9 keeps audit
in a store of its own: a separate audit database, or the JSONL file store.

- `SUTRA_AUDIT_SQL=true` **without** `SUTRA_AUDIT_SQL_URL` is a startup error
  (`SUTRA.CONFIG.AUDIT.SQL_URL_REQUIRED`). Point `SUTRA_AUDIT_SQL_URL` at a database of its own and drop
  the old flag. The URL naming the engine's database is refused (`SUTRA.CONFIG.AUDIT.SHARES_ENGINE_DB`);
  another database on the same server only warns (`SUTRA.CONFIG.AUDIT.SAME_SERVER`). The
  examples' compose stacks use a second database on the engine's server to stay small; in
  production give the audit database an instance of its own.
- The engine applies the audit migrations to that database at boot. `sutra migrate --audit-url <URL>`
  does it ahead of time.
- To keep 0.8's history, copy it after 0.9's first start has migrated the audit database:
  `pg_dump --data-only -t audit_event <engine-db> | psql <audit-db>`. The engine's own copy of the
  table is left where it is.
- The features that read or amend audit need the SQL store. With only the file store,
  `/admin/instances/{id}/history` answers 409 (`SUTRA.ADMIN.AUDIT.NO_SQL_STORE`); subject erasure
  answers `auditRedacted: false` with the reason `no SQL audit store`; the retention sweeper deletes
  expired media objects but cannot mark the audit rows; an instance migration carries no journal
  (`auditRows: 0`). Read the files with `sutra audit-replay` or `jq`. The linter warns when a process
  captures `payload` or `media` into the file store, because those lines cannot be redacted later.

## 5. The spool: mount it on persistent storage

Each engine now appends every audit event to a local spool (`sutra.audit.spool.dir`, default
`/var/lib/sutra/audit-spool`) and syncs it before the step commits. A shipper loads the spool into
the stores. Mount that directory on a volume that outlives the container (a PVC per replica, under a
StatefulSet), or crash durability is only as good as the container's disk.

If the spool cannot be written, or holds more than `sutra.audit.spool.max-bytes` unshipped, the
engine's readiness goes DOWN and step commits fail until it recovers. This is deliberate: an event
is durable or its step did not commit, so audit is never lost. Watch `sutra.audit.spool.healthy`
and `sutra.audit.spool.unshipped_bytes`. A spool directory that cannot be opened at startup is a
startup error. A deployment with no audit store configured opens no spool.

## 6. A fired boundary event now records `NODE_LEFT`

0.8 never reported a boundary event that fired (error, escalation, cancel, timer). 0.9 records
`NODE_LEFT` for it, always, because it is an audit point. A trail with `events="all"` gains those
lines; a consumer that counted events per instance will see a few more.

## 7. Transport events and the JSONL files

Only committed steps are audited. A step's events reach the spool before it commits, and a
store receives them only once the step's commit is confirmed; a step that did not commit (its
commit failed, or the engine stopped before it) never reaches a store, and its re-run is
recorded instead. An engine event now carries the pass that recorded it (`attemptId`,
`attemptBase`, `attemptAt` in a JSONL line; `attempt_id`, `attempt_base`, `attempt_at` columns,
migration V206; `sutra.audit.attempt_*` OTLP attributes). In the rare case of two committed
passes from one point, the latest is the record; the SQL store and `sutra audit-replay` apply
that, a reader of the files or the OTLP stream applies it itself (see
[Recording audit points](../building/recording-audit-points.md)). 0.8 recorded attempts that never
committed.

Transport audit events (`<transport>.<EVENT>`) carry `seq` 0 and are never deduplicated. A JSONL
line can carry the extra keys `diagnosticJson` and `spoolRef`; readers ignore keys they do not know.
`sutra.audit.jsonl.path` still works but is deprecated: use `sutra.audit.jsonl.dir`, and not both.

The keys are listed under *Audit sinks* in [Configuration keys](configuration.md#audit-sinks).
For a first run, see [Recording audit points](../building/recording-audit-points.md).
