# Upgrading to 0.6.18

Two changes in 0.6.18 can break a client that worked on 0.6.17: reads of `GET /sutra/instances*` now need the
admin key, and a synchronous HTTP flow with no `<q:reply>` now answers an empty body. This page covers those
two and points to the rest. No archive format, deploymentId or snapshot format changes. Two additive migrations
run at boot: V304 (a nullable `inbox_seen.instance_id`) and V1002 (a `deployment_endpoint_holder` table).
The tier-3 (Kubernetes) suites were not run for this release.

## Reading instances takes the admin key

`GET /sutra/instances`, its `?alias=` search and `GET /sutra/instances/{id}` now sit behind the admin gate
when admin auth is on (`sutra.admin.auth.*` or `sutra.admin.oidc.*`).

- No credential, or a wrong one, gets `401`. A valid OIDC token without the admin role gets `403`.
- Send what `/admin/*` takes: the admin key in `X-API-Key` (or the header you set with
  `SUTRA_ADMIN_AUTH_HEADER`), or the bearer token.
- `POST /sutra/instances/{id}/cancel` is not part of this change.

```console
$ curl -si "$B/sutra/instances?alias=orderId:O-100"
HTTP/1.1 401 Unauthorized

{"error":"unauthorized","reason":"missing or invalid admin auth key/secret"}
```

**An engine with no admin configuration at all still answers these routes without a key.** The gate applies
only when admin auth is configured, so an engine that never set it up behaves as before. Anyone who can reach
it can list instances and read one by id, including a value found through an alias. Either configure admin auth
(see [Configuration reference](configuration.md)) or keep that engine where only operators can reach it.

## A flow with no `<q:reply>` answers an empty body

On a synchronous HTTP channel (`ack-mode: on-complete`, the default), a flow with no `<q:reply>` now answers
`200` with an empty body. Every earlier version, from 0.2.0-rc.1 through 0.6.17, returned the instance's whole
variable map as JSON: `event.body`, the request's headers including the caller's `X-Api-Key`, and every variable
the flow set. A declared reply, and status codes, are unchanged.

What to do:

1. List the HTTP channels whose flow has no `<q:reply>` and whose callers read the answer.
2. Add a `<q:reply>` that returns exactly what the caller needs, and change the client to read only that.
3. If a log, a proxy or a client stored the old answers, they hold those credentials. Rotate the keys.

## Also changed

- A `202` from an `on-persist` HTTP channel that used to be empty now carries a JSON body when the flow replied
  nothing: [Acceptance receipts](../building/acceptance-receipts.md).
- A second deployment on a taken route, queue or consumer group is refused, and a new version drains the old one
  whole: [One endpoint, one deployment](../building/endpoint-claims.md).
- A declared `<q:source dedupKey>` now always dedupes. The recognised forms are `header.<name>`, `ce.id`,
  `body.<path>` and `amqp.message-id`. A new package with any other form is refused with
  `SUTRA.PARSE.Q_SOURCE.DEDUP_KEY_UNRECOGNISED`; one already deployed keeps running, dedupes nothing as before,
  and logs the code as an error. A repeat that used to run now answers as a duplicate. Don't key on a secret
  header: the value is stored in `inbox_seen` and can appear in refusal text and logs.
- Messages with no content type, refused deliveries, and references in connection properties:
  [Refused deliveries and connection references](inbound-refusals.md).
- A package with no `tenant` or `module` label on the `db` source gets a slot of its own and no longer replaces
  another unlabeled package. Undeploy the old one first, or label it.
