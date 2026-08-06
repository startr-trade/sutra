-- Wrapped data-encryption-key (DEK) store for the KEK-wrap envelope KeyProvider.
-- One row per crypto key_id: the DEK
-- SEALED under the tenant/deployment KEK (sutra_crypto::WrappedDataKey — nonce||ct+tag, the
-- KEK never leaves the KMS/envref boundary). At boot, EnvelopeKeyProvider materialises the
-- whole key_id -> WrappedDataKey map from this table and unwraps on demand under a KEK
-- resolved through the envref/secret registry (sutra.crypto.envelope.kek).
--
-- A WrappedDataKey is ciphertext — explicitly safe to store/log/transmit — so an ordinary row
-- suffices; the confidentiality boundary is the KEK, not this table. NOT deployment-scoped:
-- key_id is the migration-stable crypto identity (the same label HkdfKeyProvider/
-- EnvelopeKeyProvider key on), engine infra like deployment_archive/lease — no GUC, no RLS.
-- Postgres-only, matching subject_index (V1101): the engine's own datasource is always a PgPool.
CREATE TABLE data_key (
  key_id       VARCHAR(256) NOT NULL,
  wrapped_dek  BYTEA        NOT NULL,
  created_at   TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  rotated_at   TIMESTAMP WITH TIME ZONE,
  PRIMARY KEY (key_id)
);
