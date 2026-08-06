# Your first app

The `sutra` CLI scaffolds a complete, deployable application — BPMN process, channel bindings,
a sample message type, and an OpenTofu deploy — so you can watch a typed message flow through
the engine end to end.

```bash
# Scaffold a new app
cd rust && cargo run -p sutra-cli -- create app my-first-app
```

This produces an app with:

- a **BPMN process** wired to a **channel** and a **message type**,
- a **codec** (format × schema) that decodes and validates the inbound payload, and
- a **reply** on the inbound channel.

From there you **package** the app into a sealed `.sutra` archive and **deploy** it:

```bash
sutra package ./my-first-app
sutra deploy  ./my-first-app.sutra
```

Then send a message on the bound channel and watch it decode, validate, route, and reply.

> The runnable, end-to-end walkthroughs live under [`examples/`](https://github.com/startr-trade/sutra/tree/main/examples)
> — each has its own README with the exact `package` / `deploy` / `curl` lines. Start with
> `money-transfer` or `approval-hold`.

## Next

Continue to **[Your first deployment](first-deploy.md)** to deploy that archive over the
synchronous API and see exactly what "Active" means. Or skip ahead to
**[Concepts](../building/concepts.md)** to understand why the message, not a REST call, is the
contract.
