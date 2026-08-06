-- The PULL half of the outbox: a `pull://` delivery parks here as a FETCHABLE task instead of
-- being pushed at an endpoint. A worker fetches-and-locks it over HTTP
-- (`POST /sutra/external-tasks/fetch-and-lock`) and completes or fails it back through the
-- engine's ordinary inbound path — the row never resumes an instance by itself.
--
-- Why its own table rather than lock columns on `outbox_entry`: reusing that row would put a
-- second lifecycle (lock owner / lock expiry / worker retries) inside the claim predicate the
-- relay runs on every tick for every deployment, on three dialects, with the poison + backoff
-- semantics riding on it. Ownership TRANSFERS instead: the sink parks a row here and the relay
-- deletes the outbox row exactly as a delivered push would. `outbox_key` carries across as the
-- unique key, so a re-delivered claim parks idempotently.
--
-- `channel` is both the fetch topic and the inbound channel the completion is delivered to — a
-- `pull://<module_key>/<channel>` destination names its target the same way `local://` does.
--
-- FLAG FOR THE INTEGRATOR: V605 is the next free number in the outbox V6xx family on this
-- branch. Sibling wave branches may have claimed it too — renumber on merge if it collides.

CREATE TABLE external_task (
  task_id         UUID PRIMARY KEY,
  deployment_id   VARCHAR(64)  NOT NULL,
  instance_id     UUID         NOT NULL,
  channel         VARCHAR(255) NOT NULL,
  tenant          VARCHAR(255) NOT NULL,
  module_key      VARCHAR(512) NOT NULL,

  body            BYTEA        NOT NULL,
  content_type    VARCHAR(256),
  headers_json    TEXT         NOT NULL DEFAULT '{}',
  -- The originating row's `outbox_key`: the worker-visible task key AND the inbox dedup key the
  -- completion re-enters the engine under (delivery stays at-least-once, deduped).
  outbox_key      VARCHAR(128) NOT NULL,
  traceparent     VARCHAR(64),

  created_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  -- Not fetchable before this instant: a failure with retries left defers the next fetch.
  fetchable_at    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  -- Lock ownership. An EXPIRED lock is fetchable again by the claim predicate itself, so no
  -- sweeper is needed to release abandoned work.
  lock_owner      VARCHAR(255),
  lock_expires_at TIMESTAMP WITH TIME ZONE,

  attempt_count   INTEGER      NOT NULL DEFAULT 0,
  retries_left    INTEGER      NOT NULL DEFAULT 3,
  -- TERMINAL, not gone — the outbox `poisoned` posture applied to pull: a task that exhausted
  -- its retries is never fetched again but stays inspectable, and its incident is durable.
  failed          BOOLEAN      NOT NULL DEFAULT FALSE,
  last_error      TEXT
);

-- Idempotent parking: a re-delivered outbox row must not create a second task.
CREATE UNIQUE INDEX external_task_key ON external_task (deployment_id, outbox_key);
-- The fetch-and-lock claim's access path (live rows only).
CREATE INDEX external_task_fetchable ON external_task (deployment_id, channel, fetchable_at)
    WHERE NOT failed;
CREATE INDEX external_task_instance ON external_task (deployment_id, instance_id);

ALTER TABLE external_task ENABLE ROW LEVEL SECURITY;
CREATE POLICY external_task_deployment_iso ON external_task
  USING (deployment_id = current_setting('sutra.deployment_id', true));
