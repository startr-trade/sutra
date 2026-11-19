# What's new in 0.7

Engine 0.7.0 adds one attribute to the `q:` namespace, so a process can delete a data-store row.
The addition is backward compatible: a package written for 0.6 loads and runs unchanged. What
changes on upgrade is listed in [Upgrading to 0.7.0](operating/upgrading-0-7-0.md).
0.7.0 published its CLI and crates but no engine image; the first image with these features is
0.8.0 (see [Upgrading to 0.8.0](operating/upgrading-0-8-0.md)).

| Feature | What it gives you | Try it |
|---|---|---|
| **Delete a data-store row from a process** | `<q:store key="…" delete="true"/>` on a data task's write removes the row; with `expect="unchanged"` it applies only if the row is unchanged since the instance read it. A row created again never reuses a revision, so no replica's read cache serves the old one. | [Deleting a row](building/data-stores.md#deleting-a-row) |
| **Answers no instance waits for** | A message that correlates to no live instance is logged and audited, and `uncorrelated: { start: … }` on its channel can send it to a process instead of refusing it. | [An answer no instance waits for](building/wait-states.md#an-answer-no-instance-waits-for) |
| **IBM MQ over AMQP 1.0** | The `amqp` transport works with an IBM MQ queue manager's AMQP channel: queues and topics, plain `MQSTR` messages with no MQRFH2 header, TLS and SASL, and logged outage windows. | [Connecting the engine to IBM MQ](operating/ibm-mq.md) |
| **Codecs that seal what they send** | `PayloadCodec::seal` and `bind_refs` let an extension codec sign outbound messages, template renders included, with keys named as references. | [Domain neutrality and the SPI model](architecture/neutrality-and-spi.md) |
