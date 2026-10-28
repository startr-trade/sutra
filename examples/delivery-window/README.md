# delivery-window — a timer whose interval is data

An order opens a **delivery window**: the process waits at a receive task for a delivery, and an
interrupting timer boundary flags the order late when the window closes. The window is not a
literal. The boundary's `<timeDuration>` is a FEEL expression:

```xml
<bpmn:timeDuration>if payload.window = null then config.defaultWindow else payload.window</bpmn:timeDuration>
```

An order may name its own window (`"window": "PT3S"`). Otherwise the window is
`config.defaultWindow`, a deployment property declared in `package.yaml`:

```yaml
properties:
  defaultWindow: "${DEFAULT_WINDOW:PT10S}"
```

The archive seals that text unchanged. The engine resolves `DEFAULT_WINDOW` from its own
environment when it activates the deployment (`compose.yaml` sets it to `PT8S`). The expression is
evaluated once, when the receive task parks, and only the instant it resolves to is stored. A
window that is not a usable duration (`"window": "soon"`) is refused with
`SUTRA.RUNTIME.TIMER.EXPRESSION_INVALID`.

## The flow

```
POST /channels/orders             { orderId, window? }
  AwaitDelivery   receive task, answered on POST /channels/orders/delivered { orderId }
                  boundary WindowClosed: payload.window, else config.defaultWindow
  -> Delivered    a delivery inside the window
  -> Late         the window closed first
```

## Run it

Needs Docker, the `sutra` CLI and an engine image, both 0.6.4 or later (set `SUTRA_ENGINE_IMAGE`
for a locally built image). From the repository root:

```bash
C=examples/delivery-window/deploy/compose.yaml
docker compose -f $C up -d --wait
sutra package examples/delivery-window/deployments-src/default--window--1.0.0 \
    --out examples/delivery-window/deploy/deployments
PORT=$(docker compose -f $C port engine 8080 | cut -d: -f2)
H=(-H 'Content-Type: application/json' -H 'X-Api-Key: dev-only-window-key')
until curl -sf "http://127.0.0.1:$PORT/sutra/deployments" | grep -q '"phase":"Active"'; do sleep 1; done
send() { curl -s -w " HTTP %{http_code}\n" -X POST "http://127.0.0.1:$PORT/channels/orders$1" "${H[@]}" -d "$2"; }

send "" '{"orderId":"o-1","window":"PT3S"}'      # closes after 3 s -> Late
send "" '{"orderId":"o-2"}'                      # config.defaultWindow (PT8S)
send /delivered '{"orderId":"o-2"}'              # inside the window -> Delivered
send "" '{"orderId":"o-3","window":"soon"}'      # refused: not a duration
docker compose -p sutra-delivery-window down -v
```

The book's *Retries, history, and schedules* chapter walks through it with the output to expect.
