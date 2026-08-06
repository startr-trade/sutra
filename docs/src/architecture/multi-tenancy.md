# Multi-tenancy and isolation

Sutra runs many tenants behind one engine and one database, with isolation enforced at the
storage layer rather than by giving each tenant its own infrastructure.

## Tenant identity is a package label

A deployment package's `tenant` is an opaque label in `package.yaml` (alongside `module` and
`version` — see [Deployment packages](../building/deployment-packages.md)):

```yaml
labels:
  "module": "money-transfer"
  "tenant": "default"
  "version": "1.0.0"
```

The engine never interprets this string — it's a selector for observability, routing, and the
row-level-security partition key described below. Because a package is fully self-contained (no
shared resource tree, no inheritance between packages), a tenant's isolation boundary is simply
"whatever package it was labeled into" — there's no separate tenant-configuration document a
package's processes have to be matched against.

## Storage isolation: row-level filtering + PostgreSQL RLS

Every durable table the engine owns — instance state, the outbox, the inbox dedup table, the
alias index, the audit event log, the wait-state table, the dead-letter store — carries a tenant
column and a PostgreSQL row-level-security policy keyed on a session variable, applied by the
engine's own SQL migrations. Defense in depth, two layers:

1. **Application-level filtering** — every query goes through the persistence layer, which
   requires a tenant argument and injects the `WHERE` clause; there's no ad hoc database access
   path around it.
2. **PostgreSQL RLS** — `SET LOCAL app.current_tenant = '…'` at the start of each request
   transaction, with a `CREATE POLICY … USING (tenant_id = current_setting('app.current_tenant'))`
   on every table. Even a query that forgot its own `WHERE` clause cannot return another tenant's
   rows — the database itself is the second line of defense, not just the repository layer.

**A startup check closes the obvious way to defeat this.** RLS policies are silently bypassed for
any role with `BYPASSRLS`, `SUPERUSER`, or ownership of the target tables. The engine probes its
own connecting role at startup and **refuses to start** if either flag is set — an operator who
pointed the engine at `postgres` or a role minted with `BYPASSRLS` finds out at boot, not after a
cross-tenant leak. Production role setup:

```sql
CREATE ROLE sutra_app LOGIN PASSWORD '<from-secret>';
ALTER ROLE sutra_app NOBYPASSRLS;
GRANT SELECT, INSERT, UPDATE, DELETE ON <engine tables> TO sutra_app;
```

(Migrations themselves run as a separate, table-owning role that the engine runtime never uses.)

## Quotas

Two per-tenant ceilings, enforced by the channel dispatcher before an inbound reaches the
executor:

- **`maxConcurrentInstances`** — a hard cap on simultaneously in-flight instances for the tenant,
  checked against a live count in `instance_state` — globally coherent across every replica,
  since every replica queries the same table.
- **`maxInboundRatePerMinute`** — a sliding 60-second admission window. This one is
  **per-replica**, not fleet-wide: each replica tracks its own window in memory rather than paying
  a synchronous round-trip to the database on every inbound message. A tenant saturating one
  replica's window can still be admitted by a second replica until its window also fills — a
  deliberate latency/coherence trade-off. A hard global rate guarantee, if you need one, belongs
  in front of the engine (an API gateway or service-mesh L7 limiter), not in the dispatcher.

Both rejections surface as the same class of diagnostic the payload-size cap uses (see
[Limits and quotas](../operating/limits.md)), translated to the right per-transport signal (HTTP
429, a broker nack, …).

## Audit isolation

Audit rows carry the tenant column and are covered by the same RLS policy as every other engine
table. Where a JSONL audit sink is configured, it writes per-tenant directories, so a log shipper
can fan out per-tenant index/retention policy downstream without the engine knowing anything
about that policy itself.

## Next

- **[Replica semantics](replicas.md)** — the mechanisms (leases, row locks, `SKIP LOCKED`) that
  keep all of this correct across a multi-replica engine.
- **[Limits and quotas](../operating/limits.md)** — the operator-facing configuration surface for
  the ceilings above.
