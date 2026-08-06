# Third-party notices

This repository is licensed MIT OR Apache-2.0 (see `LICENSE-MIT` / `LICENSE-APACHE`).
The following third-party material is included or referenced:

## Vendored standards schemas: none

No message-definition schema published by any standards body is vendored in this
repository — not as a runtime resource, not as a build input, and not as a test fixture.
Every `.xsd` under `rust/crates/**/tests/` is authored for this project (each such
directory carries a `provenance.md` recording that), and the example deployments under
`examples/` ship their own module schemas.

The XSD compiler (`sutra-xsd`) and the schema generator (`sutra-schema-gen`) are
domain-neutral tools: they take arbitrary schema and output paths and name no standard.
The generator does recognise one input FORMAT — schemas whose target namespace follows the
`urn:iso:std:iso:20022:tech:xsd:<message-type>` shape, from which it derives module names —
but that is a namespace pattern, not published content, and the fixtures exercising it use
invented message codes.

## Deliberately not included

The following material carries its owner's own license or confidentiality terms and is
never shipped here. Supply it yourself at deployment time if your integration needs it
— a deployment package can carry its own schemas (see the schema-bundle mechanism in
`docs/src/building/deployment-packages.md`):

- Message-definition schemas published by standards bodies and registration authorities.
  Many are freely licensed for implementation use, but redistributing them is a decision
  to make against their terms.
- SWIFT MyStandards "enriched" usage-guideline schemas, including the per-release
  editions market-infrastructure rails publish — licensed MyStandards products.
- `admi.998.001.02.xsd` and its supplementary-data schema — SWIFT-licensed even in
  base form.
- The FedNow Service envelope and key-exchange XSDs — Federal Reserve Banks
  confidential material.

## Fixture provenance

Message-shaped test fixtures, sample payloads and schema fixtures in this repository are
authored for this project, modeled on the structural idioms the relevant standards and
their published illustrative samples describe; no licensed or confidential material is
reproduced.
