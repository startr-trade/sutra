# Recording audit points

This walkthrough declares audit in a process, checks it with the linter, runs one instance, and
reads what was recorded from the JSONL files. Audit goes to the file store, so it needs no database
beyond the engine's own.

## Before you start

You need the `approval-hold` example deployed on an engine with a database, as in its README.
Its `approval-hold` process already carries `<q:audit/>`. Start the engine with two more
settings: the directory the audit files go to, and a spool on a directory the engine can write:

```bash
SUTRA_AUDIT_JSONL_DIR=/var/lib/sutra/audit
SUTRA_AUDIT_SPOOL_DIR=/var/lib/sutra/audit-spool
```

The spool is where each event is made durable before its step commits. Put both directories on
persistent storage in anything but a trial.

## 1. Declare audit

In the process's `extensionElements`:

```xml
<bpmn:process id="approval-hold" isExecutable="true">
  <bpmn:extensionElements>
    <q:audit/>
  </bpmn:extensionElements>
```

The tag is the only switch: a process without it is not audited, whatever the engine is
configured with. A bare tag is `events="points" capture="metadata"`. Write `events="all"` to
record every element.

## 2. Lint

```bash
sutra lint examples/approval-hold/deployments-src/default--approval--1.0.0 --audit
```

For each process the report shows that audit is declared, the effective `events` and `capture`,
and for every element whether it is audited and the rule that decided it. Remove the tag and run it
again: the process is reported as `SUTRA.LINT.AUDIT.NOT_DECLARED`. Then
`sutra lint <dir> --fix audit --dry-run` prints the diff that would add it back, and without
`--dry-run` applies it.

## 3. Run an instance

Start one instance and resume it with a decision, as in the example's README:

```bash
curl -sS -X POST localhost:8080/channels/approval-request \
  -H 'Content-Type: application/json' -H 'X-Api-Key: approval-demo-key' \
  -d '{"ApprovalRequest":{"E2EId":"E2E-1","Amount":"1500.00"}}'
curl -sS -X POST localhost:8080/channels/approval-decision \
  -H 'Content-Type: application/json' -H 'X-Api-Key: approval-demo-key' \
  -d '{"ApprovalDecision":{"E2EId":"E2E-1","Decision":"APPROVE"}}'
```

Both answer `200`. Take the instance id from the first line of the audit files (next step).

## 4. Read the audit files

The files are private to the engine's user (mode `0600`): read them as that user, or copy them
out through a container that runs as it (the image's user is `10001`).

```bash
ls /var/lib/sutra/audit
# audit-<spoolId>-<time>.jsonl
jq -c 'select(.instanceId=="<INSTANCE_ID>") | [.at,.eventType,.nodeId]' /var/lib/sutra/audit/*.jsonl
```

Each line is one event. Retrace the instance in order, from the same files:

```bash
sutra audit-replay <INSTANCE_ID> --from-jsonl /var/lib/sutra/audit
```

Only steps that committed reach the files. A step's events are made durable in the spool before
the step commits, and the shipper writes them only once the spool records that the commit
succeeded; a step whose commit failed, or whose engine stopped before committing, is dropped
(counted in `sutra.audit.attempts_aborted`), and its re-run is recorded instead. Each line
carries the pass that wrote it (`attemptId`, `attemptBase`, `attemptAt`). If one step ever
committed twice from the same point (an engine stopped right after a commit and another replica
re-ran the step), `audit-replay` shows the later pass only; with `jq`, the same rule:

```bash
jq -s --arg id <INSTANCE_ID> '
  map(select(.instanceId == $id))
  | group_by([.deploymentId, .attemptBase])
  | map(if .[0].attemptId then (max_by([.attemptAt, .attemptId]) as $w
          | map(select(.attemptId == $w.attemptId))) else . end)
  | flatten | sort_by(.at)' /var/lib/sutra/audit/*.jsonl
```

The SQL audit store applies the same rule as it loads the rows.

## 5. See what the spool does

While the engine runs, `sutra.audit.spool.healthy` is 1 and `sutra.audit.spool.unshipped_bytes`
falls back to 0 after each batch.

Now stop the engine and start it again with the spool on a directory it cannot write:

```bash
mkdir -p /tmp/ro-spool && chmod 555 /tmp/ro-spool
SUTRA_AUDIT_SPOOL_DIR=/tmp/ro-spool  # with the settings above
```

The engine refuses to start and names the spool directory: with an audit store configured it
never runs unaudited. Point the spool back at its own directory and it starts again. A spool that
stops syncing while the engine runs (a full or failing disk), or that holds more than
`sutra.audit.spool.max-bytes` unshipped, takes readiness DOWN and fails each step commit until it
recovers; the delivery is retried, so audit is never lost and work resumes by itself.

To move audit to a database instead, set `SUTRA_AUDIT_SQL_URL` to a database of its own and run
`sutra migrate --audit-url <URL>`; see [Upgrading to 0.9](../operating/upgrading-0-9.md#4-audit-never-lives-in-the-engines-database).
