# Escaping free text in an XML template

A template renders `{{x}}` verbatim. That is right for values you control, and wrong for free text
a caller sent you: a customer called `O'Brien & Sons` or a note with a `<` in it makes the message
malformed XML. `{{xmlEscape x}}` (new in 0.6.25; reference in
[Template helpers](../reference/template-helpers.md#xmlescape)) escapes the value for XML.

This walkthrough answers a JSON order with an XML receipt twice, once escaped and once verbatim. It
was run against the 0.6.25 engine and PostgreSQL 16, with the output shown below.

## 1. The package

`package.yaml` labels the module `receipt`, version `1.0.0`. `channels.yaml` declares two JSON
channels, one per template:

```yaml
channels:
  - name: order
    transport: http
    bind: "POST /channels/order"
    codec: json
    cloudevents-mode: none
    auth: { scheme: apikey, apikey: { value: env:RECEIPT_API_KEY, header: X-Api-Key } }
  - name: order-raw
    transport: http
    bind: "POST /channels/order-raw"
    codec: json
    cloudevents-mode: none
    auth: { scheme: apikey, apikey: { value: env:RECEIPT_API_KEY, header: X-Api-Key } }
```

`bpmn/receipt.bpmn` binds the request to `order` and answers with a template:

```xml
<bpmn:process id="receipt" isExecutable="true">
  <bpmn:extensionElements>
    <q:variables>
      <q:variable name="order" type="object"/>
    </q:variables>
  </bpmn:extensionElements>
  <bpmn:startEvent id="Start">
    <bpmn:extensionElements><q:source channel="order" name="order"/></bpmn:extensionElements>
  </bpmn:startEvent>
  <bpmn:serviceTask id="Answer" implementation="receipt.hbs">
    <bpmn:extensionElements><q:reply mode="native" contentType="application/xml"/></bpmn:extensionElements>
  </bpmn:serviceTask>
  <bpmn:endEvent id="Done"/>
  <bpmn:sequenceFlow id="F1" sourceRef="Start" targetRef="Answer"/>
  <bpmn:sequenceFlow id="F2" sourceRef="Answer" targetRef="Done"/>
</bpmn:process>
```

`bpmn/receipt-raw.bpmn` is the same process with the id `receipt-raw`, the channel `order-raw`
and the template `receipt-raw.hbs`.

`templates/receipt.hbs` escapes every value taken from the request:

```handlebars
<Receipt customer="{{xmlEscape order.customer}}"><Note>{{xmlEscape order.note}}</Note><Ref>{{xmlEscape order.ref}}</Ref></Receipt>
```

`templates/receipt-raw.hbs` does not:

```handlebars
<Receipt customer="{{order.customer}}"><Note>{{order.note}}</Note></Receipt>
```

## 2. Package it and start the engine

```bash
sutra lint receipt
sutra package receipt -o deployments
```

Lint reports no errors. It warns that the `json` channels declare no message types, which is
expected for a schema-less channel.

Start the engine with `SUTRA_DEPLOYMENTS_DIR` pointing at `deployments`, `SUTRA_DATASOURCE_URL`
and its credentials pointing at PostgreSQL, and `RECEIPT_API_KEY` set. Set `B` to the engine's
address.

## 3. Send free text

```bash
cat > order.json <<'JSON'
{"customer":"O'Brien & Sons","note":"Leave at the door if <2 kg. \"Fragile\" — 壊れ物","ref":"A-1042"}
JSON
curl -s -X POST "$B/channels/order" -H 'X-Api-Key: <your key>' \
  -H 'Content-Type: application/json' -d @order.json
```

```xml
<Receipt customer="O&apos;Brien &amp; Sons"><Note>Leave at the door if &lt;2 kg. &quot;Fragile&quot; — 壊れ物</Note><Ref>A-1042</Ref></Receipt>
```

The five XML special characters are escaped; the dash and the Japanese text pass through unchanged.

The same order on the verbatim channel:

```bash
curl -s -X POST "$B/channels/order-raw" -H 'X-Api-Key: <your key>' \
  -H 'Content-Type: application/json' -d @order.json
```

```xml
<Receipt customer="O'Brien & Sons"><Note>Leave at the door if <2 kg. "Fragile" — 壊れ物</Note></Receipt>
```

Save each answer to a file and give it to an XML parser. Python's `xml.dom.minidom` accepts the
first and refuses the second:

```text
receipt.xml:     well-formed
receipt-raw.xml: not well-formed (invalid token): line 1, column 28
```

## 4. A missing value

Leave `ref` out:

```bash
curl -s -X POST "$B/channels/order" -H 'X-Api-Key: <your key>' \
  -H 'Content-Type: application/json' -d '{"customer":"Ada","note":"none"}'
```

```xml
<Receipt customer="Ada"><Note>none</Note><Ref></Ref></Receipt>
```

`xmlEscape` renders a missing or null value empty. A plain `{{order.note}}` on a missing value
fails the render instead, as strict mode requires. Leaving `note` out on `order-raw` answers HTTP
500:

```text
unresolved template reference {{order.note}} (strict rendering: every referenced value must be present; use {{coalesce x "default"}} for optional values)
```

If an element must not be empty, check the value in the flow before the template runs.
