# Worked example: loading a batch file

`examples/call-log-load/` is a package that takes a **file** of records over HTTP — CSV or
fixed-width, the same records either way — validates every row against an XSD *before the process
starts*, transforms each record into a second XSD, and writes it as a typed database row.

One channel, one process, two schemas, two wire forms.

## The shape

```
POST /channels/cdr-upload        codec: urn:cdr   (formats: [csv, fixed-width])
   │
   ├── the CODEC validates the WHOLE FILE, row by row, cell by cell
   ▼
 cdr-load.bpmn
   Prep      rowCount := count(payload.value)
   Accept    Handlebars receipt + <q:reply continue="true">   ──►  202 + receipt HERE
   PerRow    multi-instance over payload.value                     (the load runs detached)
     Map       <scriptTask> — one CallDetailRecord ► one CallLogEntry
     Persist   data task     — call_log[entry.entryId]
   End
```

## Answering now, loading later

The channel declares `ack-mode: on-persist`: the caller is answered as soon as the upload is
durably captured, never held for the load. That alone would send a bare `202`. What makes it a
*useful* `202` is `<q:reply continue="true">` on `Accept` — the process renders its receipt, that
render becomes the response body, and the process then parks and resumes detached to run the rows:

```json
HTTP/1.1 202 Accepted
Content-Type: application/json

{"batchId":"d36d2c59-0fea-4349-b410-a4b4c7715ab9","rowsAccepted":4,"status":"loading"}
```

The two declarations are independent and both are needed. The ack mode decides *when* the caller is
answered; `continue="true"` decides that the reply happens at the park instead of at the end of the
process. Drop the reply and the caller gets an empty `202` it cannot tell from a lost message; drop
`continue="true"` and the receipt waits for the whole load. See
[Acknowledgement modes](../operating/ack-modes.md).

## Binding a tabular format to a schema

A bare `codec: csv` on a channel is a *parser*: it would hand the flow a bag of untyped strings
with nothing asserted. Binding a schema codec whose manifest declares the tabular format is what
makes the file a typed message:

```yaml
# schemas/cdr/codec-manifest.yaml
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

The schema those columns are checked against is an ordinary XSD — the facets do the work:

```xml
<xs:simpleType name="Msisdn">
  <xs:restriction base="xs:string"><xs:pattern value="\+[0-9]{8,15}"/></xs:restriction>
</xs:simpleType>
<xs:simpleType name="Direction">
  <xs:restriction base="xs:string">
    <xs:enumeration value="originated"/><xs:enumeration value="received"/>
  </xs:restriction>
</xs:simpleType>

<xs:element name="CallDetailRecord">
  <xs:complexType><xs:sequence>
    <xs:element name="recordId"    type="RecordId"/>
    <xs:element name="msisdn"      type="Msisdn"/>
    <xs:element name="startTime"   type="xs:dateTime"/>
    <xs:element name="durationSec" type="DurationSeconds"/>   <!-- xs:int, 0..86400 -->
    <xs:element name="direction"   type="Direction"/>
    <xs:element name="rateCode"    type="RateCode" minOccurs="0"/>
  </xs:sequence></xs:complexType>
</xs:element>
```

## Two wire forms, one schema

Both files carry the same four records. The content-type selects the parser; nothing downstream
knows which arrived.

```csv
recordId,msisdn,peerMsisdn,startTime,durationSec,direction,cellId,chargeAmount,rateCode
CDR-100001,+14155550101,+14155550187,2026-09-06T09:14:02Z,182,originated,CELL-0042,4.5500,PEAK
CDR-100003,+14155550188,+14155550101,2026-09-06T10:02:19Z,930,received,CELL-0117,0.0000,
```

```text
CDR-100001  +14155550101    +14155550187    2026-09-06T09:14:02Z  182   originated CELL-0042 4.5500    PEAK
CDR-100003  +14155550188    +14155550101    2026-09-06T10:02:19Z  930   received   CELL-0117 0.0000
```

```bash
curl -X POST .../channels/cdr-upload -H 'Content-Type: text/csv'   --data-binary @sample/call-logs.csv
curl -X POST .../channels/cdr-upload -H 'Content-Type: text/plain' --data-binary @sample/call-logs.fixed-width.txt
```

Adding a vendor with a different delimiter is a manifest edit. The BPMN never changes.

## The nuances

### A batch projects under `value`

An array root projects under `value`, so the flow reads `payload.value` and a row is
`payload.value[0].recordId`. The loop iterates it **in place**:

```xml
<bpmn:multiInstanceLoopCharacteristics isSequential="true">
  <bpmn:loopDataInputRef>payload.value</bpmn:loopDataInputRef>
  <bpmn:inputDataItem name="row"/>
</bpmn:multiInstanceLoopCharacteristics>
```

`loopDataInputRef` is a FEEL *expression*, not a variable name. Copying the collection into a
variable first would put a second copy of the whole batch in the instance snapshot on every park.

### The XSD's leaf types reach the cells

A CSV cell is text on the wire, but the bound type decides what it *is*. `durationSec` is declared
`xs:int`, so it arrives as a **number** — which is why the transform can render it unquoted into
JSON and the store can write it into an `INTEGER` column without a cast anywhere.

### An empty cell is absence — for an optional element

Row `CDR-100003` leaves `rateCode` empty. A tabular row has a cell for every column whether or not
it carries a value, so an empty cell for an element the type declares `minOccurs="0"` reads as
**absent**. Without that rule the empty cell would become `<rateCode></rateCode>` and fail the
element's own enumeration — and every optional column would have to be populated in every row.

An empty cell for a **required** element is untouched: that is a genuine data error and is still
reported.

### A bad cell names its row

One decode validates every row and every cell, and each violation carries a row-indexed path:

```
value[1]   value '4155550999' does not match pattern '\+[0-9]{8,15}'
value[2]   the value of element 'startTime' is not valid
value[3]   value 'sideways' is not one of the enumerated values [originated, received]
```

The row index is the part that locates the problem; the message names the element. Rows 0 and 4 are
not reported.

### The whole batch is refused, in the caller's own format

`<q:onValidation mode="reject"/>` makes a partly-bad file an all-or-nothing refusal. Because the
caller posted `text/csv`, the RFC 7807 problem document comes back **as a table** — one row per
issue, diffable against the file that was sent, rather than JSON to re-parse looking for row 4,217.

### A fixed-width layout is checked at package time

A fixed-width record's columns *are* its only field names, so they must agree with the bound type
or every row would fail at runtime for what is really a configuration mistake. `sutra lint` catches
it:

```
[ERROR] SUTRA.CONFIG.CODEC_MANIFEST.REJECTED — codec 'cdr': fixed-width layout declares
column(s) ["recordIdTYPO"] that type 'CallDetailRecord' does not declare …
```

A csv codec gets no equivalent check: its column names arrive in the header, at runtime, so there
is nothing to compare against beforehand. That asymmetry is the price of a self-describing format.

## What to know before scaling it up

The multi-instance loop is **one durable step**. That has three consequences worth stating plainly:

- **The batch is the unit of recovery.** A crash part-way through replays the loop from the *first*
  record rather than resuming where it stopped. Here that converges, because every per-record
  effect is a store write keyed by `entry.entryId` and the write is an upsert — the same rows are
  rewritten, not duplicated. Add a per-record `<q:send>`, an append or a counter and that is no
  longer true.
- **Say so.** `<q:process idempotent="true"/>` is the assertion that re-running converges. Without
  it the fail-closed default applies and a mid-load failure is dead-lettered and consumed rather
  than retried — the wrong posture for a file load, and wrong by silence.
- **The decoded batch lives in the snapshot.** That is unavoidable; what is avoidable is
  *duplicating* it. Iterate `payload.value` in place, and mark per-iteration scratch `transient` so
  it never reaches the database.

If batches outgrow what one step should own, the move is to deliver them in chunks — a file or
broker transport carrying N records per message. The BPMN names channels, never transports, so
nothing in the flow changes.

## Run it

The example ships its own two-service stack — engine and PostgreSQL, every secret already set —
so trying it is `up`, `package`, `curl`. You need the [`sutra` CLI](../getting-started/installation.md)
and Docker; this is the same local model as the [Quickstart](../getting-started/quickstart.md),
pointed at a package that already exists.

### 1. Start the engine

```bash
docker compose -f examples/call-log-load/deploy/compose.yaml up -d
```

### 2. Seal the package into the watched directory

```bash
sutra package examples/call-log-load/deployments-src/default--call-log--1.0.0 \
     --out examples/call-log-load/deploy/deployments
```

```
packaged …/default--call-log--1.0.0.sutra (deploymentId dep-397f20f9669c0e2eaabd0e74)
```

`package` lints on the way in — the fixed-width layout is checked against the bound XSD, the
store's `structure` against the migration's DDL — so a mistake in either fails here rather than on
the first upload. The engine watches that directory and activates the archive without a restart.

The host port is dynamic, because nothing assumes 8080 is free on your machine:

```bash
ENGINE=$(docker compose -f examples/call-log-load/deploy/compose.yaml port engine 8080)
curl -s http://$ENGINE/sutra/health/ready
```

```json
{"status":"UP","checks":[{"name":"sutra-loader","status":"UP","data":{"deployments":1,"shards":1}}]}
```

### 3. Upload a batch

```bash
curl -s -X POST http://$ENGINE/channels/cdr-upload \
  -H 'Content-Type: text/csv' \
  -H 'X-Api-Key: dev-only-cdr-key' \
  --data-binary @examples/call-log-load/sample/call-logs.csv
```

```json
{"batchId":"45217d89-a520-4d7b-ba2a-453d1d9fa38a","rowsAccepted":4,"status":"loading"}
```

`202 Accepted`, in about 30 ms. The load has not finished — it has barely started. That is
[`ack-mode: on-persist` plus `<q:reply continue="true">`](#answering-now-loading-later) doing
exactly what the section above describes.

### 4. Read the rows back

```bash
docker compose -f examples/call-log-load/deploy/compose.yaml exec engine-db \
  psql -U sutra_engine -d sutra -c \
  'select entry_id, subscriber, bearing, duration_seconds, rated_amount, billable from call_log order by entry_id'
```

```
  entry_id  |  subscriber  | bearing  | duration_seconds | rated_amount | billable
------------+--------------+----------+------------------+--------------+----------
 CDR-100001 | +14155550101 | outgoing |              182 |       4.5500 | t
 CDR-100002 | +14155550101 | incoming |               45 |       0.0000 | f
 CDR-100003 | +14155550188 | incoming |              930 |       0.0000 | f
 CDR-100004 | +14155550101 | outgoing |             1204 |      21.6720 | t
```

Typed columns, not a JSON blob: `duration_seconds` is an integer and `rated_amount` a `NUMERIC`
because the XSD's leaf types were applied to the CSV's untyped cells at decode. `bearing` and
`billable` do not exist in the uploaded file at all — the transform derived them from `direction`.

`psql` needs no password here: `sutra_engine` is the engine's own role (owner of `sutra`), and
inside the container the local socket trusts it. The bootstrap superuser is `postgres`, which you
only need for creating roles or databases.

### 5. Watch it refuse a bad batch

```bash
curl -i -X POST http://$ENGINE/channels/cdr-upload \
  -H 'Content-Type: text/csv' \
  -H 'X-Api-Key: dev-only-cdr-key' \
  --data-binary @examples/call-log-load/sample/call-logs-with-a-bad-row.csv
```

```
HTTP/1.1 400 Bad Request
content-type: text/csv

field,value
type,urn:bpm:diag:SUTRA.INBOUND.VALIDATION_REJECT
status,400
detail,"Inbound rejected on intake node Start of process cdr-load: 6 validation issue(s); mode=reject."
issueCount,6
```

Three things at once. It is a **400**, not a 500 — a malformed file is the caller's to fix, and a
5xx would invite a retry of identical bytes. It comes back as **CSV**, because that is what was
posted. And re-running the query from step 4 still shows four rows: validation runs at intake over
the *whole* file, so a batch with one bad cell writes nothing at all.

Drop the `X-Api-Key` header and you get `401` instead — the channel declares `apikey` auth, and the
engine will not wire an unauthenticated HTTP intake.

### 6. The same records, as fixed-width

```bash
curl -s -X POST http://$ENGINE/channels/cdr-upload \
  -H 'Content-Type: text/plain' \
  -H 'X-Api-Key: dev-only-cdr-key' \
  --data-binary @examples/call-log-load/sample/call-logs.fixed-width.txt
```

Another `202` with a receipt, and the table still holds four rows — same schema, same keys, so the
same records upserted rather than appended. One channel, one XSD, [two wire
forms](#two-wire-forms-one-schema), chosen by content-type.

### Clean up

```bash
docker compose -f examples/call-log-load/deploy/compose.yaml down -v
```

### Into an app you already have

If you are already running a scaffolded app rather than this stack, copy the package into it and
give the engine the secrets the package names:

```bash
cp -r examples/call-log-load/deployments-src/default--call-log--1.0.0 \
      my-app/packages/call-log-load
```

```bash
# my-app/deploy/.env — read by the engine service (env_file), gitignored by the scaffold
CDR_UPLOAD_API_KEY=dev-only-cdr-key
CALL_LOG_DB_URL=postgres://engine-db:5432/sutra
CALL_LOG_DB_USER=sutra_engine
CALL_LOG_DB_PASSWORD=sutra-dev-only
```

Then `sutra package packages/call-log-load --out deploy/deployments` and recreate the engine
(`docker compose -f deploy/compose.yaml up -d --force-recreate engine`) so it picks the new
environment up.

**This step is not optional and it fails loudly.** `channels.yaml` and `datastores.yaml` name their
secrets by *reference* — `env:CDR_UPLOAD_API_KEY`, `env:CALL_LOG_DB_URL` — and the **engine**
resolves them, in its own environment. Exporting them in your shell does nothing; the engine is not
your shell. An unresolvable channel-auth reference is fatal:

```
startup failed — refusing to serve
  HTTP channel auth value could not be resolved: secret-ref 'env:CDR_UPLOAD_API_KEY'
  resolves to no value (environment variable 'CDR_UPLOAD_API_KEY' is not set).
```

The engine will not open an unauthenticated port instead. An unresolvable *store* reference is
narrower — that store is not registered and the rest of the deployment still serves — so watch the
startup log for `store NOT registered` too.

## Next

- [The q: namespace](q-namespace.md) — `q:onValidation`, `q:process idempotent`, `q:variables`
- [Deployment packages](deployment-packages.md#tabular-formats) — the manifest reference
- [Data stores](data-stores.md) — the projected store the rows land in
