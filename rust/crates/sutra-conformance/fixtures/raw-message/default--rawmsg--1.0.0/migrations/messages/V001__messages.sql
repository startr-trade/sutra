-- The message is kept as text in `body`; the other columns are ordinary typed columns.
CREATE TABLE IF NOT EXISTS messages (
  store_key    VARCHAR(512) NOT NULL,
  message_id   VARCHAR(64)  NOT NULL,
  content_type VARCHAR(128),
  body         TEXT         NOT NULL,
  rev          BIGINT       NOT NULL DEFAULT 1,
  updated_at   TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (store_key)
);
