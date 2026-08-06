-- Extract the tenant id from the JSONL audit file path and inject it as a
-- record field, so the Fluent Bit ES output can route the record to a
-- per-tenant Elasticsearch index via Logstash_Prefix_Key.
--
-- Path convention (from docs/multi-tenancy.md):
--   /var/log/sutra-audit/<tenantId>/<yyyy>/<mm>/<dd>/<instance-id>.jsonl
--
-- The first segment under /var/log/sutra-audit/ is treated as the tenant id.
-- Records that don't match the expected layout get tenant_id="unknown" so they
-- are still indexed (under sutra-audit-unknown-*) rather than dropped.
--
-- Return contract for Fluent Bit's Lua filter: (code, ts, record)
--   code == 2  → keep record, accept the modified record
--   code == 1  → keep record, accept the original ts/record
--   code == 0  → keep record as-is
--   code == -1 → drop record

local function extract_tenant(tag, ts, record)
    local path = record["file_path"] or record["file"] or record["log_path"] or ""
    local tenant = path:match("/var/log/sutra[-_]audit/([^/]+)/")
    if not tenant or tenant == "" then
        record["tenant_id"] = "unknown"
    else
        record["tenant_id"] = tenant
    end
    return 2, ts, record
end

return extract_tenant
