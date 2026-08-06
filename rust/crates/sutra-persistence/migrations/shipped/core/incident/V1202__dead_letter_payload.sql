-- Dead-letter REPLAY capture: the columns that turn V1201's write-only incident metadata into a
-- redrivable record. V1201 deliberately carried no payload ("a replay is not even reconstructible
-- from the row"); these five nullable columns close exactly that gap and nothing more —
-- everything the NORMAL intake path needs to re-dispatch the consumed message as a fresh delivery:
--
--   payload       the consumed body, TRUNCATED at the channel's effective payload cap
--                 (sutra.codec.max-payload-bytes / the per-channel override) so one dead letter can
--                 never store more than the engine was willing to accept in the first place;
--   headers_json  the inbound transport headers as a JSON object (same shape as
--                 outbox_entry.headers_json), replayed verbatim;
--   content_type  the declared inbound media type (broker deliveries do not always carry one in
--                 headers, and the codec selection needs it);
--   tenant        the delivering tenant, and
--   module_key    the "<tenant>/<module>/<version>" namespace key of the serving channel —
--                 together the (module_key, channel) pair the channel registry resolves a binding
--                 by, which deployment_id alone cannot address after a version flip.
--
-- All five are NULLABLE: rows written before this migration, and rows recorded by a path that
-- captured no payload (an outbound required-delivery incident has no inbound message at all), keep
-- NULL and the replay endpoint answers a structured "no payload captured" error rather than
-- fabricating one.
--
-- SENSITIVE-DATA POSTURE. `payload` and `headers_json` hold RAW BUSINESS DATA — the exact bytes a
-- caller sent, unredacted, unencrypted. Two protections apply and both are load-bearing:
--   1. Deployment isolation. The columns inherit V1201's RLS policy (dead_letter_deployment_iso,
--      keyed off the sutra.deployment_id GUC) plus the explicit deployment_id bind on every
--      statement — a tenant can never read another tenant's dead letters.
--   2. Admin-only exposure. The read surface (GET /admin/dead-letters…) lives ONLY on the
--      OIDC/key-gated /admin/* router, never on the unauthenticated /sutra/* operate routes, and
--      even there the payload bytes are NEVER rendered into a response body: the listing exposes
--      octet_length only, and the bytes leave the database on exactly one path — POST
--      /admin/dead-letters/{id}/replay, which feeds them straight back into intake.
-- Operators who cannot accept business data at rest here leave sutra.incident.sql off (the
-- default): no row is written at all and the tracing::error! floor remains the record.
--
-- pg-only, exactly like V1201 (and the rest of the incident family): the dead-letter store is one
-- of the engine's PostgreSQL system stores, so migrations_mysql/ and migrations_mssql/ carry no
-- incident folder to keep in parity.
ALTER TABLE dead_letter ADD COLUMN payload       BYTEA        NULL;
ALTER TABLE dead_letter ADD COLUMN headers_json  TEXT         NULL;
ALTER TABLE dead_letter ADD COLUMN content_type  VARCHAR(256) NULL;
ALTER TABLE dead_letter ADD COLUMN tenant        VARCHAR(128) NULL;
ALTER TABLE dead_letter ADD COLUMN module_key    VARCHAR(512) NULL;
