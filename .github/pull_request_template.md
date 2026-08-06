<!--
Thanks for contributing! Keep the PR focused on one logical change — small PRs get
reviewed faster. See CONTRIBUTING.md for the full flow.
-->

## What and why

<!-- What does this change, and what problem does it solve? -->

Closes #

## How it was verified

<!-- Which tiers you ran, and anything you could not run locally. -->

- [ ] `cargo fmt --all --check`
- [ ] `make lint` — clippy `-D warnings` + the domain-neutrality gate
- [ ] `make test` — tier-1 (no Docker)
- [ ] `make test-docker` (or `P=<crate>`) — tier-2, if this change touches persistence,
      channels, transports or codecs
- [ ] New or updated tests cover the change

## Checklist

- [ ] `CHANGELOG.md` updated under `[Unreleased]` (user-visible changes only)
- [ ] Documentation updated (`docs/`) if behaviour, configuration or the CLI changed
- [ ] No breaking change — or the break is called out below with a migration note
- [ ] Commit messages follow `type(area): summary`

## Notes for reviewers

<!-- Anything worth knowing: trade-offs taken, follow-ups deferred, areas you want scrutinised. -->
