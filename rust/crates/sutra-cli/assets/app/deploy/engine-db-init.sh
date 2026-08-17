#!/bin/sh
# generated-by: sutra create app (edit freely — this file is yours)
#
# Creates the role the ENGINE connects as. Runs once, inside the postgres container, the first
# time its data volume is initialised (`/docker-entrypoint-initdb.d/`); on later boots the
# volume already exists and this is not re-run.
#
# WHY THIS FILE EXISTS AT ALL: the engine refuses to start against a role that can bypass
# row-level security (SUTRA.STARTUP.RLS_BYPASS_RISK), and the postgres image's POSTGRES_USER is
# a SUPERUSER — superusers bypass RLS unconditionally, which makes the deployment-isolation
# policies on the engine tables silently inert. Rather than switch that check off for local
# development, this dev stack gives the engine the same kind of credential a production
# deployment gives it: a plain login role, NOSUPERUSER and NOBYPASSRLS.
#
# It owns the schema because it MIGRATES the schema — the engine applies its own migrations at
# boot through the one datasource it is given, so the role it connects as must be able to
# create tables. That leaves one gap against a fully hardened deployment, and the engine says
# so at startup (SUTRA.STARTUP.RLS_INERT_POSTURE): PostgreSQL exempts a table's OWNER from its
# policies unless the table carries FORCE ROW LEVEL SECURITY. Closing it needs the migrating
# identity and the serving identity to be different roles, which is a deployment-topology
# decision — see docs/operating for the hardened shape.
set -eu

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<SQL
-- NOSUPERUSER NOBYPASSRLS is the whole point: with either of them reversed the engine's
-- startup check fails closed and the stack does not come up.
CREATE ROLE "${ENGINE_DB_USER}"
    LOGIN PASSWORD '${ENGINE_DB_PASSWORD}'
    NOSUPERUSER NOBYPASSRLS NOCREATEROLE NOCREATEDB NOREPLICATION;

-- Owns what it migrates, and nothing wider: this database and its schema.
ALTER DATABASE "${POSTGRES_DB}" OWNER TO "${ENGINE_DB_USER}";
ALTER SCHEMA public OWNER TO "${ENGINE_DB_USER}";
GRANT ALL ON SCHEMA public TO "${ENGINE_DB_USER}";
SQL

echo "engine role '${ENGINE_DB_USER}' created (NOSUPERUSER, NOBYPASSRLS; owns schema public)"
