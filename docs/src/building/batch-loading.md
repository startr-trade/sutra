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
   Accept    Handlebars receipt + <q:reply continue="true">   ──►  caller answered HERE
   PerRow    multi-instance over payload.value                     (the load runs detached)
     Map       <scriptTask> — one CallDetailRecord ► one CallLogEntry
     Persist   data task     — call_log[entry.entryId]
   End
```

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

## Try it

```bash
sutra lint    examples/call-log-load/deployments-src/default--call-log--1.0.0
sutra package --out ./out examples/call-log-load/deployments-src/default--call-log--1.0.0
```

## Next

- [The q: namespace](q-namespace.md) — `q:onValidation`, `q:process idempotent`, `q:variables`
- [Deployment packages](deployment-packages.md#tabular-formats) — the manifest reference
- [Data stores](data-stores.md) — the projected store the rows land in
