# call-log-load — a CSV batch, XSD-validated whole, transformed row by row, stored as typed rows

A worked deployment package. A **CSV batch of phone-call detail records** is POSTed over HTTP;
the codec validates **every row and every cell** against an inbound XSD before anything runs; a
**Handlebars template** transforms each record into a *different* XSD; and each becomes a row in
a **projected data store** — a real table whose columns are that second XSD's type. The upload is
answered immediately and the load runs detached.

One channel, one process, two schemas.

## The flow

```
POST /channels/cdr-upload        cdr-upload    codec: urn:cdr   (manifest: formats: [csv])
   │
   ├── the CODEC validates the WHOLE FILE, row by row, cell by cell, against
   │   schemas/cdr/cdr.xsd — patterns, enumerations, xs:dateTime, numeric range.
   │   A bad cell anywhere ⇒ the batch is REFUSED here, with the full issue list
   │   rendered in the caller's own content-type. No instance starts.
   ▼
 cdr-load.bpmn
   Prep     rows := payload.value, rowCount := count(payload.value)
   Accept   Handlebars receipt + <q:reply continue="true">   ──►  caller gets its answer HERE
   PerRow   multi-instance, ONE ITERATION PER RECORD              (everything below is detached)
     Map        <scriptTask> call-log-entry.hbs    CallDetailRecord ─► CallLogEntry
     Persist    data task                          call_log[entry.entryId]
   End
                                    │
                                    ▼
                   call_log — one typed COLUMN per scalar declared by
                   schemas/call-log/call-log.xsd
```

## How each requirement is met

| Requirement | Where |
|---|---|
| CSV in an HTTP POST body | `cdr-upload` binds `codec: urn:cdr`, whose `codec-manifest.yaml` declares `formats: [csv]` |
| Parsed against an XSD, **all rows and cells, in the codec, before processing** | A tabular body is a *batch*: the codec validates each row as one `CallDetailRecord` against `cdr.xsd` in a single decode, reporting every violation with a row-indexed path (`value[3].durationSec`) |
| Data store loading to a structured table matching the XSD type | `datastores.yaml` → `structure: {schema: urn:call-log, type: CallLogEntry}`. That XSD **is** the table; `migrations/call_log/V001__call_log.sql` is the author's own DDL and `sutra lint` checks the two agree |
| Async inbound (long-running load) | `ack-mode: on-persist` **plus** `<q:reply continue="true"/>` on `Accept`: the caller gets a receipt document as soon as the rows are counted, then the instance parks and the engine self-resumes to run the load |
| Inbound XSD → storage XSD transform before storing | `scripts/call-log-entry.hbs`, run by the `Map` script task |
| Incremental transformation | One iteration, one transform, one committed row **per record**. No whole-batch document is ever built |

## The two schemas, and what the transform does

`schemas/cdr/cdr.xsd` (inbound) and `schemas/call-log/call-log.xsd` (storage) are deliberately not
a rename apart:

| Inbound (`urn:sutra:cdr`) | Storage (`urn:sutra:call-log`) | |
|---|---|---|
| `recordId` | `entryId` | renamed |
| `msisdn` | `subscriber` | renamed |
| `peerMsisdn` | `counterparty` | renamed |
| `startTime` | `startedAt` | renamed |
| `durationSec` | `durationSeconds` | renamed |
| `cellId` | `cellSite` | renamed, optional both sides |
| `chargeAmount` | `ratedAmount` | renamed |
| `direction` (`originated`/`received`) | `bearing` (`outgoing`/`incoming`) | **vocabulary mapped** |
| — | `billable` | **derived** |
| `rateCode` | — | **dropped** |

## Two wire forms, one schema

Switches hand over call records as CSV or as fixed-width text depending on the vendor, and both
are the same records — so the codec declares **both**, and `cdr.xsd` types either. Their content
types are disjoint, so an upload selects its parser unambiguously:

| `Content-Type` | Parser | Sample |
|---|---|---|
| `text/csv` | csv | `sample/call-logs.csv` |
| `text/plain` | fixed-width | `sample/call-logs.fixed-width.txt` |

Nothing downstream knows or cares which arrived: the process, the transform and the store are
identical either way. Adding a vendor with a different delimiter is a manifest edit.

```yaml
schemaKind: xsd
formats: [csv, fixed-width]
csv:
  delimiter: ","
  header: true
fixed-width:
  fields:
    - {name: recordId,     width: 12}
    - {name: msisdn,       width: 16}
    - {name: peerMsisdn,   width: 16}
    - {name: startTime,    width: 22}
    - {name: durationSec,  width:  6}
    - {name: direction,    width: 11}
    - {name: cellId,       width: 10}
    - {name: chargeAmount, width: 10}
    - {name: rateCode,     width:  8}
```

`sample/call-logs.fixed-width.txt` is the same four records in that layout; posting it with
`Content-Type: text/plain` loads identically. Unlike CSV — whose column names arrive in the header
at runtime — a fixed-width layout is configuration, so it is checked against `cdr.xsd` at **package
time**: a column the type does not declare, or a required element with no column, fails
`sutra lint` (`SUTRA.CONFIG.CODEC_MANIFEST.REJECTED`) rather than every row at runtime.

## Three design points worth the detour

**Why the CSV binds a schema codec, not the bare `csv` format.** A bare format is a parser: it
would hand the flow a bag of strings with nothing asserted. Binding `urn:cdr` with
`formats: [csv]` makes the file a *typed* message — the XSD's leaf types are applied to the cells,
so `durationSec` arrives as a number, and the facets do real work on real data. Validation
happens once, at the door, for the whole file.

**Why the transform is a `<bpmn:scriptTask>` and not a template service task.** Both run the same
Handlebars engine on the same kind of `.hbs` file. The difference is what the render becomes: a
*template* render is a reply — bytes on the wire — and a template task may not carry data-store
associations at all (`SUTRA.PARSE.DATA_ASSOCIATION_UNSUPPORTED`). A *script* render must be a
JSON object, and its entries merge **typed** into the instance variables. That is what makes
`entry` a record the very next node can persist, and it is why this example needs one process
rather than a second one whose only job would be to turn bytes back into a record.

**Why nothing re-validates after the transform.** The transform's shape is checked statically:
`<q:variable name="entry" schema="call-log"/>` binds it to the storage type, and the store's
`structure` block is lint-verified against the migrations' DDL. At runtime the projected store is
the backstop — a field the type does not declare is a fail-closed write error, and on first use
the provider refuses a table that cannot satisfy the projection. `sutra lint` / `sutra package`
run all of it before deployment.

## Running it

```bash
sutra lint    deployments-src/default--call-log--1.0.0     # 0 errors, 0 warnings
sutra package --out ./out deployments-src/default--call-log--1.0.0

export CALL_LOG_DB_URL=postgres://localhost:5432/calllog
export CALL_LOG_DB_USER=... CALL_LOG_DB_PASSWORD=...
export CDR_UPLOAD_API_KEY=...

curl -sS -X POST http://localhost:<port>/channels/cdr-upload \
     -H 'Content-Type: text/csv' -H "X-Api-Key: $CDR_UPLOAD_API_KEY" \
     -H 'X-Request-Id: batch-2026-09-06-01' \
     --data-binary @sample/call-logs.csv
# {"batchId":"…","rowsAccepted":4,"status":"loading"}
```

```sql
SELECT entry_id, subscriber, counterparty, started_at,
       duration_seconds, bearing, cell_site, rated_amount, billable
  FROM call_log ORDER BY started_at;
```

```
 entry_id   | subscriber   | counterparty  | started_at             | duration_seconds | bearing  | cell_site | rated_amount | billable
------------+--------------+---------------+------------------------+------------------+----------+-----------+--------------+---------
 CDR-100001 | +14155550101 | +14155550187  | 2026-09-06 09:14:02+00 |              182 | outgoing | CELL-0042 |       4.5500 | t
 CDR-100002 | +14155550101 | +442071838750 | 2026-09-06 09:31:47+00 |               45 | incoming | CELL-0042 |       0.0000 | f
 CDR-100003 | +14155550188 | +14155550101  | 2026-09-06 10:02:19+00 |              930 | incoming | CELL-0117 |       0.0000 | f
 CDR-100004 | +14155550101 | +919845012345 | 2026-09-06 11:47:55+00 |             1204 | outgoing | CELL-0203 |      21.6720 | t
```

Typed columns are the point: that is why the migration can index `(subscriber, started_at)`.

### What a bad batch does

`sample/call-logs-with-a-bad-row.csv` has five rows; three violate the inbound XSD:

| Row | Cell | Facet violated |
|---|---|---|
| `CDR-100011` | `4155550999` — no leading `+` | `Msisdn` pattern |
| `CDR-100012` | `not-a-timestamp` | `xs:dateTime` |
| `CDR-100013` | `sideways` | `Direction` enumeration |

**The whole batch is refused** — `<q:onValidation mode="reject"/>` — and because the caller posted
`text/csv`, the RFC 7807 problem document comes back **as a table**, one row per issue, which can
be diffed against the file that was sent. Nothing is stored, and no instance runs. Changing that
one attribute to `mode="route"` would instead let the batch through with the issues available at
`payload.validation.issues` for the flow to triage.

## Layout

```
deployments-src/default--call-log--1.0.0/
  package.yaml
  channels.yaml                        the one channel
  datastores.yaml                      the projected `call_log` store
  bpmn/cdr-load.bpmn                   the one process
  schemas/cdr/                         INBOUND: codec-manifest.yaml (formats: [csv]) + cdr.xsd
  schemas/call-log/                    STORAGE: the store's declared row type
  scripts/call-log-entry.hbs           the transform
  templates/batch-accepted.hbs         the receipt
  migrations/call_log/V001__call_log.sql   the author's own DDL
sample/
  call-logs.csv                        four good records
  call-logs-with-a-bad-row.csv         five records, three bad cells
  call-logs.fixed-width.txt            the same four records, fixed-width
```

## Scaling note

`PerRow` is a multi-instance loop, and a loop iteration is not a parkable token position — the
whole load runs inside one durable step. Each row's store write commits on its own, so nothing is
lost and the load is genuinely incremental, but for a very large file that step is long. If
batches outgrow what one step should own, the natural next move is to make the upload channel a
file or broker transport delivering chunks; the BPMN names channels only, never transports, so
nothing in the flow changes.
