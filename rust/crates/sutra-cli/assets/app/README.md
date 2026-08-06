<!-- generated-by: sutra create app (edit freely — this file is yours) -->
# %%APP%%

A sutra application workspace: **standalone deployment packages** in, one generic engine image
out. There is no application build — you author declarative packages, seal each into a `.sutra`
archive with the `sutra` CLI, and drop the archives into the engine's deployments directory.

## Layout

```
%%APP%%/
├── packages/                  # authoring: one directory per deployment package
│   └── %%PACKAGE%%/           # the sample package (self-contained, archive-shaped)
│       ├── package.yaml       # authoring metadata: name, labels, engine contract, entry processes
│       ├── bpmn/              # process definitions (ids are package-local)
│       │   └── sample.bpmn    # sample process with the validation-gateway wiring
│       ├── rules/             # decision rulesets (*.dmn) referenced by <q:validators> etc.
│       ├── templates/         # reply/render templates (*.hbs)
│       ├── scripts/           # script-task sources (*.hbs)
│       ├── schemas/           # message schemas; each folder is one codec
│       │   └── sample/        # the `sample` codec (schemas/sample — bound by channels.yaml)
│       ├── channels.yaml      # transport bindings (how messages reach the processes)
│       ├── datastores.yaml    # data stores the package OWNS (each with its own connection)
│       └── migrations/        # per-store idempotent SQL (migrations/<store>/)
└── deploy/
    ├── compose.yaml           # local run: engine image + engine database + deployments mount
    ├── deployments/           # drop packaged .sutra archives here (the engine watches it)
    ├── k8s/engine.yaml        # the same deploy as Kubernetes manifests
    └── smoke.sh               # health-gated smoke: /sutra/health/* + a sample channel POST
```

Every package directory mirrors the sealed archive layout 1:1. A package is **fully
self-contained** — no shared library tree, no inheritance: a variant of a package (say, a second
tenant) is an explicit copy (`sutra create deployment <name> --from packages/%%PACKAGE%%`)
edited independently. At package time the directory is sealed into an immutable `.sutra` archive
whose manifest (per-file digests, the content-addressed deployment id) is derived — never author
a manifest by hand, and never edit an archive: re-package instead.

## Workflow

```
# 1. author (or evolve) a package
sutra create deployment my-flow            # a fresh package skeleton under packages/
sutra create bpmn my-process --package packages/my-flow --validation fatal

# 2. seal it
sutra package packages/%%PACKAGE%%         # -> %%PACKAGE%%.sutra (validated fail-closed)

# 3. run the engine and deploy
docker compose -f deploy/compose.yaml up -d
cp %%PACKAGE%%.sutra deploy/deployments/   # add = deploy, remove = undeploy

# 4. prove it
./deploy/smoke.sh
```

The smoke script waits on `/sutra/health/ready`, checks `/sutra/health/live`, POSTs a
`SampleRequest` to the sample channel and expects the `<Accepted…>` reply.

## The sample package

`packages/%%PACKAGE%%` handles one message end to end:

- `POST /channels/sample-in` receives XML/JSON/YAML decoded by the `schemas/sample` codec
  (`SampleRequest`, declared in `schemas/sample/sample.xsd`).
- `bpmn/sample.bpmn` routes on the intake validation summary at a visible
  **`Validation outcome?`** gateway: reject-worthy outcomes render
  `templates/sample-rejected.hbs`, everything else renders `templates/sample-accepted.hbs`.
- No data store is needed; `datastores.yaml` documents how to declare one when you are ready
  (each store owns its connection via `env:` references and its `migrations/<store>/` SQL).

Try it once the engine is up (`docker compose -f deploy/compose.yaml port engine 8080` prints
the dynamic host port):

```
curl -X POST http://<host:port>/channels/sample-in \
  -H 'Content-Type: application/xml' \
  --data '<SampleRequest><note>hello</note></SampleRequest>'
```

## Path coverage (compliance metric)

Declare the routes you care about directly in a process and let the running engine tick them
off (`docs: path-coverage-metrics`). The CLI seeds and checks them:

```
sutra coverage init packages/%%PACKAGE%%/bpmn/sample.bpmn    # declare <q:coverage> routes +
                                                             # scaffold report/reset admin flows
sutra coverage check packages/%%PACKAGE%%/bpmn/sample.bpmn   # drift lint (routes still valid)
```

## Engine configuration

The engine reads canonical `sutra.*` keys — as environment variables (`SUTRA_*`) or from an
optional properties file named by `SUTRA_CONFIG` (default `sutra.properties` in its working
directory). The deploy assets set exactly these:

| key                        | env                        | meaning                                   |
|----------------------------|----------------------------|-------------------------------------------|
| `sutra.resource-root`      | `SUTRA_DEPLOYMENTS_DIR`      | deployment source root (the archive mount) |
| `sutra.http.port`          | `SUTRA_HTTP_PORT`          | listen port (`0` = dynamic)                |
| `sutra.datasource.url`     | `SUTRA_DATASOURCE_URL`     | engine-internal database                   |
| `sutra.datasource.username`| `SUTRA_DATASOURCE_USERNAME`| —                                          |
| `sutra.datasource.password`| `SUTRA_DATASOURCE_PASSWORD`| —                                          |

The engine-internal database stores instances/outbox/leases/audit. It is never a package's
business store — packages own those in `datastores.yaml`, connection and migrations included.

## Secrets

Configuration and channel auth use `${ENV}` / `env:NAME` references resolved at startup —
never literal secrets in authored files. Package-time validation rejects secret literals;
for a throwaway local demo you may relax that check at lint time (dev profile), but keep
references in anything you commit.
