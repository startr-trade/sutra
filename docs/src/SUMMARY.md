# Summary

[Introduction](introduction.md)

# Getting Started

- [Installation](getting-started/installation.md)
- [Quickstart](getting-started/quickstart.md)
- [Anatomy of an app](getting-started/first-app.md)
- [Your first deployment](getting-started/first-deploy.md)

# Building BPMN Solutions

- [Concepts](building/concepts.md)
- [Deployment packages](building/deployment-packages.md)
- [Channels and transports](building/channels.md)
- [The q: namespace](building/q-namespace.md)
- [Rules: DMN, FEEL, and .srl](building/rules.md)
- [Data stores](building/data-stores.md)
- [Wait states and human tasks](building/wait-states.md)
- [External tasks: the pull worker surface](building/external-tasks.md)
- [Retries, history, and schedules](building/retries-history-schedules.md)
- [Testing time: fast-forwarding durable timers](building/testing-time.md)
- [Coverage: declared routes as the compliance signal](building/coverage.md)
- [Worked example: money-transfer](building/worked-example.md)

# Architecture

- [Engine layering](architecture/overview.md)
- [Domain neutrality and the SPI model](architecture/neutrality-and-spi.md)
- [Deployment model](architecture/deployment-model.md)
- [Multi-tenancy and isolation](architecture/multi-tenancy.md)
- [Replica semantics](architecture/replicas.md)
- [Execution lanes](architecture/execution-lanes.md)
- [Observability](architecture/observability.md)

# Design & Internals

- [Durable execution: snapshots and typed values](internals/durable-execution.md)
- [Ownership and claims](internals/ownership-and-claims.md)
- [Execution lanes: the design](internals/execution-lanes-design.md)
- [Retry machinery](internals/retry-machinery.md)
- [Migration internals](internals/migration-internals.md)
- [The pull surface: design](internals/pull-surface-design.md)

# Operating

- [Configuration reference](operating/configuration.md)
- [Acknowledgement modes](operating/ack-modes.md)
- [Limits and quotas](operating/limits.md)
- [Deploy, hot-deploy, and rollback](operating/deploy-rollback.md)
- [Instance migration](operating/instance-migration.md)
- [Logging and audit](operating/logging.md)
- [Troubleshooting BPMN solutions](operating/troubleshooting.md)

# Reference

- [`sutra` CLI](reference/cli.md)
- [Crates: sutra-feel and sutra-dmn](reference/crates.md)
- [DMN-TCK conformance](reference/dmn-tck.md)

# Project

- [Contributing](contributing.md)
- [Debugging the engine](debugging-the-engine.md)
