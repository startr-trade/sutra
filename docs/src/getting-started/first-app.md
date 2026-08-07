# Anatomy of an app

The [quickstart](quickstart.md) ran a scaffolded app end to end. This page opens it up: what
each file does, how a message travels through them, and how to make your first real change —
including watching the engine refuse a change that would have broken in production.

If you skipped the quickstart, scaffold one now:

```bash
sutra create app my-first-app && cd my-first-app
```

## The package is the unit

```
packages/my-first-app-main/       ← the deployment package: what you ship
├── package.yaml                  identity (tenant, module, version) — its slot
├── channels.yaml                 which channels this package binds, and how
├── bpmn/sample.bpmn              the process
├── schemas/sample/
│   ├── sample.xsd                the message contract
│   └── codec-manifest.yaml       which schema serves which message type
├── templates/*.hbs               reply bodies
└── datastores.yaml               business data stores this package declares
```

Everything outside `packages/` is scaffolding *around* the app — `deploy/compose.yaml`,
`deploy/deployments/` (the watched directory), `deploy/k8s/`, `deploy/smoke.sh`. You own those
and can delete them; the package is the part the engine consumes.

One package directory seals into one `.sutra` archive. Its identity is
`tenant--module--version` (its **slot**) plus a content hash of everything inside — which is why
a re-deploy of identical bytes is a no-op and a changed package replaces its slot atomically.

## How the pieces connect

Follow one message:

1. **`channels.yaml`** declares `sample-in` as an HTTP channel bound to `POST /channels/sample-in`,
   with a codec. The channel is the *doorway*: it decides how bytes become a message.
2. The **codec** (`schemas/sample/`) decodes the XML and validates it against `sample.xsd`
   before any process runs. A violation here is data, not an exception.
3. **`bpmn/sample.bpmn`** has a start event bound to that channel, so a valid message starts an
   instance — with the payload already typed. The gateway branches on the validation outcome;
   the accept and reject paths render different templates.
4. **`templates/*.hbs`** produce the reply body, returned on the same HTTP connection because
   the process declares `<q:reply>`.

The important property: **steps 2 and 3 are separate on purpose.** The process never parses
anything. Change the wire format later — XML to JSON, HTTP to Kafka — and the diagram is
untouched, because format and transport are channel concerns.

Inspect any of this without running an engine:

```bash
sutra describe packages/my-first-app-main        # what the engine sees: channels, processes, bindings
sutra dispatch-graph packages/my-first-app-main  # which channel reaches which process
```

## Make it yours

Add a field to the contract and use it. First, `schemas/sample/sample.xsd` — add an `amount`
alongside `note`:

```xml
<xs:element name="amount" type="xs:decimal" minOccurs="0"/>
```

Then use it in `templates/sample-accepted.hbs`:

```handlebars
<Accepted><note>{{payload.note}}</note><amount>{{payload.amount}}</amount></Accepted>
```

Re-package, and the engine's deploy-time verification runs:

```bash
sutra package packages/my-first-app-main --out deploy/deployments
```

Now break it deliberately — change the template to `{{payload.amont}}` (typo) and re-package:

```
SUTRA.LINT.NAVIGATION.UNKNOWN_FIELD
  templates/sample-accepted.hbs:1 — 'amont' is not a field of SampleRequest
  did you mean: amount
```

**Packaging fails.** That typo never reaches an engine, never waits for the one request that
happens to take that branch at 2 a.m. Every FEEL expression and template path in the package is
checked against the schema it claims to read — this is the design's central bet, and it costs
you nothing to see it work.

Fix the typo, re-package, and the running engine picks up the new archive and flips to it with
in-flight instances left to finish on the old one.

## Where each concern lives

| Want to… | Go to |
|---|---|
| Understand the model behind all of this | [Concepts](../building/concepts.md) |
| Bind a broker instead of HTTP | [Channels](../building/channels.md) |
| Decode a different format, or your own | [Codecs](../building/codecs.md) |
| Wait for a human decision or a reply | [Wait states](../building/wait-states.md) |
| Write decisions as tables or rules | [Decisions](../building/decisions.md) |
| Retry, schedule, or time-box work | [Retries, history, schedules](../building/retries-history-schedules.md) |
| Test a 30-day timer in milliseconds | [Testing time](../building/testing-time.md) |

## Next

**[Your first deployment](first-deploy.md)** — getting the archive onto an engine you did not
start with `docker compose`, and what "Active" really means.
