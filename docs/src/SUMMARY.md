# Summary

[Introduction](introduction.md)
[What's new in 0.9](whats-new-0-9.md)
[What's new in 0.8](whats-new-0-8.md)
[What's new in 0.7](whats-new-0-7.md)
[What's new in 0.6](whats-new-0-6.md)

# Getting Started

- [Installation](getting-started/installation.md)
- [Quickstart](getting-started/quickstart.md)
- [Anatomy of an app](getting-started/first-app.md)
- [Your first deployment](getting-started/first-deploy.md)

# Building BPMN Solutions

- [Concepts](building/concepts.md)
- [Deployment packages](building/deployment-packages.md)
- [Channels and transports](building/channels.md)
- [Calling a language model](building/llm-channels.md)
- [The q: namespace](building/q-namespace.md)
- [Rules: DMN, FEEL, and .srl](building/rules.md)
- [Data stores](building/data-stores.md)
- [Signing a user in against a store](building/signing-in-against-a-store.md)
- [Recordings and media references](building/recordings-and-media.md)
- [Wait states and human tasks](building/wait-states.md)
- [One endpoint, one deployment](building/endpoint-claims.md)
- [A reply answers the instance waiting for it](building/waits-first.md)
- [Upgrading a deployment in place](building/upgrading-in-place.md)
- [Recording audit points](building/recording-audit-points.md)
- [Acceptance receipts](building/acceptance-receipts.md)
- [Answering with a status and headers](building/reply-status-and-headers.md)
- [External tasks: the pull worker surface](building/external-tasks.md)
- [Retries, history, and schedules](building/retries-history-schedules.md)
- [Escaping free text in an XML template](building/escaping-xml-in-templates.md)
- [Testing time: fast-forwarding durable timers](building/testing-time.md)
- [Coverage: declared routes as the compliance signal](building/coverage.md)
- [Worked example: money-transfer](building/worked-example.md)
- [Worked example: loading a batch file](building/batch-loading.md)

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
- [Refused deliveries and connection references](operating/inbound-refusals.md)
- [References in connection properties](operating/connection-references.md)
- [Connecting to a TLS and SASL broker](operating/broker-tls-sasl.md)
- [Connecting the engine to IBM MQ](operating/ibm-mq.md)
- [Broker outages as messages](operating/connectivity-events.md)
- [Tuning for throughput](operating/throughput-tuning.md)
- [Limits and quotas](operating/limits.md)
- [Capacity pools and sessions](operating/capacity-pools.md)
- [Deploy, hot-deploy, and rollback](operating/deploy-rollback.md)
- [Instance migration](operating/instance-migration.md)
- [Logging and audit](operating/logging.md)
- [Troubleshooting BPMN solutions](operating/troubleshooting.md)
- [Upgrading to 0.9](operating/upgrading-0-9.md)
- [Upgrading to 0.6.18](operating/upgrading-0-6-18.md)
- [Upgrading to 0.6.19](operating/upgrading-0-6-19.md)
- [Upgrading to 0.6.20](operating/upgrading-0-6-20.md)
- [Upgrading to 0.6.21](operating/upgrading-0-6-21.md)
- [Upgrading to 0.6.22](operating/upgrading-0-6-22.md)
- [Upgrading to 0.6.25](operating/upgrading-0-6-25.md)
- [Upgrading to 0.8.0](operating/upgrading-0-8-0.md)
- [Upgrading to 0.7.0](operating/upgrading-0-7-0.md)

# Reference

- [BPMN element support](reference/bpmn-elements.md)
- [`sutra` CLI](reference/cli.md)
- [Template helpers](reference/template-helpers.md)
- [Crates: sutra-feel and sutra-dmn](reference/crates.md)
- [DMN-TCK conformance](reference/dmn-tck.md)

# Project

- [Contributing](contributing.md)
- [Debugging the engine](debugging-the-engine.md)
