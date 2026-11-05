# Upgrading to 0.6.19

**Check these against what you run:**

- **A connection property that starts with a secret scheme and a colon.** `env:`, `secret:`, `vault:`, `aws-secrets:`, `azure-kv:` and `gcp-secret:` now mean a reference in the properties listed below, so a Kafka broker called `vault` written `bootstrap.servers: vault:9092`, or a queue called `env:orders`, now fails to resolve and stops the channel. Write such a value through a default: `${BROKER:vault:9092}`. Other text with a colon, such as `kafka:9092`, is unaffected.
- **A `${NAME}` that 0.6.18 refused.** It now resolves, so the channel starts. Before 0.6.18 such a value was used as a literal name, so if you move a Kafka `group.id` from a literal `${NAME}` to a resolved one, it is a new consumer group and starts from `auto.offset.reset` (default `earliest`), not from the old group's offsets.
- **`RAW_BODY_UNMASKED` and `RAW_BODY_STORED` warn in more places.** They now also fire on a flow that reads a store a masking flow wrote raw text to. These are warnings, and `RAW_BODY_UNMASKED` becomes an error in a later release.

There is no migration, archive format, deploymentId or snapshot format change.
