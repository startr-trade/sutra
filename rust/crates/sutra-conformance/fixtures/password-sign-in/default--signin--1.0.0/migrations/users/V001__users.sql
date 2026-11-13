-- One row per user; the password is kept only as an argon2id hash in PHC form.
CREATE TABLE IF NOT EXISTS users (
  store_key     VARCHAR(512) NOT NULL,
  user_id       VARCHAR(64)  NOT NULL,
  password_hash VARCHAR(256) NOT NULL,
  rev           BIGINT       NOT NULL DEFAULT 1,
  updated_at    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (store_key)
);
-- The seeded user: the fixed argon2id test vector (password "correct horse").
INSERT INTO users (store_key, user_id, password_hash)
VALUES ('ada', 'ada',
        '$argon2id$v=19$m=19456,t=2,p=1$AAECAwQFBgcICQoLDA0ODw$0AuQxA+RjPiJeA19Op7DwrFxnfh9xeTU2T6mFj2wThc')
ON CONFLICT (store_key) DO NOTHING;
