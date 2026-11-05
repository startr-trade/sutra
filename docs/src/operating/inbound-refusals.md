# Refused deliveries and connection references

This page covers what the engine does with a message it permanently refuses on a broker or file channel, what
changed in 0.6.18, and how references behave in a channel's connection properties. For the ack modes
themselves see [Acknowledgement modes](ack-modes.md).

## A message with no content type

Before 0.6.18 (back to 0.2.0-rc.1), a broker message with no content type on a channel that declares a codec
was deleted with nothing recorded: no instance, no log line, no audit record. The source filled in
`application/octet-stream`, the codec refused that, and the drop left no trace.

Now a message with no content type is decoded with the channel's codec. If the codec refuses the body, it
follows the normal reject path. A message that states a type the codec does not accept is still refused.
A channel with no codec reads a message with no type as raw bytes, as before. The one visible difference there
is that `event.contentType` is empty rather than `application/octet-stream`, the same as on HTTP and file
channels.

## What a permanent refusal leaves behind

The same silence covered every permanent refusal on these transports: a body the codec refused, a stated type it
does not accept, a message no start event matched, a tenant quota. HTTP, Dapr and Knative answer the sender with a
status, and the file transport moves the file to `.failed/`. Every other transport now leaves two things:

- A WARN log line with `code` `SUTRA.INBOUND.DELIVERY_DROPPED`, the refusal's own code as `cause_code`, the
  channel and the dedup key. It never carries the body.
- When an audit sink is on, a record `inbound.DELIVERY_DROPPED` in the deployment's journal. Its
  `diagnostic_code` is the refusal's, and its `diagnostic_json` names the channel. No instance owns it, so its
  `instance_id` is empty.

Two limits of this release:

- **`sutra.audit.sql` is off by default.** An engine with no audit sink writes no audit record, and the WARN is
  the only trace. Turn a sink on if you need the record. See [Logging and audit](logging.md#audit-sinks).
- **A refused body is not kept.** On SQS a refused message is deleted, and nothing in the engine can redrive it.
  A sender that needs the body back keeps its own copy.

What the refusal did to the message depends on the transport, and a copy may still exist:

| Transport | What happens | Where a copy can still be |
|---|---|---|
| `aws-sqs` | deleted | nowhere, unless the sender keeps its own |
| `kafka` | offset committed past it | in the topic, until retention removes it |
| `gcp-pubsub` | acked | in a snapshot or a seek window, if you keep one |
| `rabbitmq`, `amqp` | rejected without requeue | in the dead-letter route, if the broker has one |

A rejected credential gets the same audit record, beside the WARN the transport already wrote. A message settled
as a duplicate is not a drop: it logs at INFO with `SUTRA.INBOUND.DUPLICATE_DELIVERY`. A deferred nack for a
failed instance, `SUTRA.ACK.DEFERRED_NACKED`, is now WARN rather than INFO.

**To check whether you lost messages on an earlier version,** compare what your senders sent with the instances
and rows the engine produced for that channel over the time it ran. On Kafka, compare the topic's offsets with the
same counts, and replay what is missing with `content-type` set. The engine holds nothing to query, because
earlier versions wrote nothing.

## References in connection properties

An inbound `aws-sqs` channel now resolves `${NAME}`, `${NAME:default}`, `env:NAME` and `secret:` references in
`region`, `queue.url`, `endpoint-override`, `wait-time-seconds`, `max-messages` and `visibility-timeout-seconds`.
Earlier versions read them as written and polled a queue literally named `${...}`, retrying forever. An
unresolved reference now stops the channel at wiring with `SUTRA.INBOUND.AWS_SQS.CONFIG_INVALID`, naming the
property.

An illustrative channel (the keys match the SQS transport's properties; adjust the names and add your codec):

```yaml
channels:
  - name: notes-in
    transport: aws-sqs
    region: "${AWS_REGION:eu-west-1}"
    queue.url: "${NOTES_QUEUE_URL}"
    ack-mode: on-persist
```

**Only `aws-sqs` expands references in connection properties.** The other transports do not yet, so in 0.6.18 a
value containing `${` in one of the properties below is refused when the channel is wired, with that transport's
`CONFIG_INVALID` code (`SUTRA.INBOUND.KAFKA.CONFIG_INVALID`, for example). The message names the channel and the
property and says references there arrive in a later release; it does not repeat the value. Use literal values.

| Transport | Properties that refuse `${` | Code |
|---|---|---|
| `kafka` | `bootstrap.servers`, `topic`, `group.id` | `SUTRA.INBOUND.KAFKA.CONFIG_INVALID` |
| `rabbitmq` | `host`, `port`, `virtual-host`, `queue`, `exchange` | `SUTRA.INBOUND.RABBITMQ.CONFIG_INVALID` |
| `amqp` | `host`, `port`, `queue`, `topic` | `SUTRA.INBOUND.AMQP.CONFIG_INVALID` |
| `gcp-pubsub` | `project-id`, `subscription`, `topic`, `endpoint-override` | `SUTRA.INBOUND.GCP_PUBSUB.CONFIG_INVALID` |
| `file` | `spool.dir`, `directory` | `SUTRA.INBOUND.FILE.CONFIG_INVALID` |

What happens on refusal:

- At boot the engine does not start.
- On an activation in a running engine, the activation succeeds, that channel's consumer does not run, and the
  code is logged as a WARN. `aws-sqs` behaves the same way for an unresolved reference.
- This can stop a channel that ran before. Earlier versions used the text as a literal name. A Kafka `group.id`
  of `${NAME}` consumed under a group literally named that; a file `spool.dir` polled a directory with that name;
  an AMQP `queue` attached to that name. A new literal `group.id` is a new consumer group, so it starts from
  `auto.offset.reset` (default `earliest`), not from the old group's offsets.
- Only `${` is detected. `env:NAME` and `secret:` forms in these properties are still read as written.
- Credentials, `inbound-auth` and outbound `bind` resolve references as before.
