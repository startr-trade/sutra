-- MySQL/MariaDB dialect of the outbox emitting-node column (PostgreSQL reference:
-- V606__outbox_node.sql). Links each entry to the BPMN node that emitted it, so a channel-call
-- <q:retry> backoff park can WITHDRAW the dead attempt's request rows and the poison wake can be
-- verified against durable evidence — both keyed (deployment_id, instance_id, node_id).
-- NULL on pre-F1 rows: they are simply never matched.

ALTER TABLE outbox_entry ADD COLUMN node_id VARCHAR(255);
