# What's new in 0.8

Engine 0.8.0 adds one attribute to the `q:` namespace, so a flow can choose the status and headers
of a synchronous HTTP answer. The addition is backward compatible: a package written for 0.7 or 0.6
loads and runs unchanged. What changes on upgrade is listed in
[Upgrading to 0.8.0](operating/upgrading-0-8-0.md). 0.8.0 is also the first engine image since
0.6.25: 0.7.0 published no image, so everything in [What's new in 0.7](whats-new-0-7.md) arrives
with it.

| Feature | What it gives you | Try it |
|---|---|---|
| **Status and headers on a synchronous answer** | `<q:reply status="201">` (digits or FEEL) and the reply's `<q:header>` children set the HTTP status and response headers, so a flow can answer like a REST API or an auth check. | [Answering with a status and headers](building/reply-status-and-headers.md) |
| **Broker outages as messages** | With `sutra.engine.connectivity-events.channel` set, each outage start and end the AMQP transport sees on a deployment's channel reaches that deployment as a JSON message. | [Broker outages as messages](operating/connectivity-events.md) |
| **FEEL durations keep fractional seconds** | `duration("PT0.001S")` is one millisecond, not zero, through parsing, `string()`, arithmetic, comparison and `.seconds`. | [Durations with fractional seconds](building/rules.md#durations-with-fractional-seconds) |
| **No accepted message lost across a database restart** (0.8.2) | A data task that cannot reach its database (a restart breaks every pooled connection) now refuses the delivery for a retry (HTTP 503, broker redelivery, outbox backoff) instead of dead-lettering it, and a step that has run rides out a short outage before giving up, so it is recorded rather than run again. New code `SUTRA.RUNTIME.DATASTORE.UNAVAILABLE`. | [Data stores](building/data-stores.md) |
| **The AMQP 1.0 sink keeps its connections** (0.8.2) | Up to 8 connections per broker (`SUTRA_SINK_AMQP_MAX_CONNECTIONS`), each with one session and a link per target, instead of a connection per message: on a durable queue 22.3 → 227 messages/s one at a time. Delivery guarantee unchanged. | [IBM MQ](operating/ibm-mq.md) |
| **The SQL audit sink writes in batches** (0.8.1) | With `sutra.audit.sql` on, the audit writer commits once per batch of up to 256 events instead of once per event, so on a durable PostgreSQL the journal keeps up with a busy engine (on a 50 orders/s relay workload it had been 85,770 rows, about five minutes, behind). Still best-effort and behind execution; nothing to configure. | [Audit sinks](operating/configuration.md#audit-sinks) |
