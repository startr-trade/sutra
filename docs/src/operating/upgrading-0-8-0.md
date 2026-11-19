# Upgrading to 0.8.0

**2026-11-19.** **A synchronous HTTP answer sets its own status and headers** with
`<q:reply status="…">` and the reply's `<q:header>` children. It adds an attribute to the `q:`
namespace (`urn:sutra:q:1.0`), which is why this release is 0.8.0. The addition is backward
compatible: existing packages load and run unchanged, and an answer without the attribute or
headers is unchanged. **Broker outages can reach the deployment as messages.** **FEEL durations
keep fractional seconds.** No migration, and no archive or snapshot format change.

## Coming from the 0.6.25 image: there is no 0.7.0 image

The v0.7.0 tag, its CLI archives and its crates.io crates were published, but its release run
stopped at the engine image scan on a base-image advisory published the same day
(CVE-2026-95690, below), so there is no `ghcr.io/startr-trade/sutra:0.7.0` image and no 0.7.0
GitHub Release. 0.8.0 carries every 0.7.0 change. If you run the 0.6.25 image, move straight to
0.8.0 and read [Upgrading to 0.7.0](upgrading-0-7-0.md) as well: its migration (V103) and its
behaviour changes (data-store revisions seeded from the database clock, uncorrelated messages
logged and audited) arrive with this image.

## Before you upgrade

**Check these against what you run:**

- **A FEEL duration with a fraction on a unit other than seconds is now refused.**
  `duration("PT1.5H")` used to be cut to `PT1H`; now `duration()` cannot parse it, as XML Schema
  and the FEEL specification require. Write `PT1H30M` or `PT5400S`. See
  [Durations with fractional seconds](../building/rules.md#durations-with-fractional-seconds).
- **`.seconds` on a days-and-time duration returns the fraction.** `duration("PT4.5S").seconds` is
  `4.5`, where it was `4`. An expression that needs a whole number should apply `floor()`.
- **A fractional seconds value is no longer dropped.** `duration("PT0.001S")` was zero, so a timer
  whose FEEL expression returned it fired at once; it now waits 1 ms. `string()` keeps the fraction.

## Status and headers on a synchronous answer

On an HTTP channel that answers synchronously (`ack-mode: on-complete`, the default), a
`<q:reply>` with no `destination` can set the status (digits, or a FEEL expression), and its
`<q:header>` children become the response's headers. A flow can answer 201 with a `Location`, 404
or 409, or act as an auth check that answers 200 with identity headers or 401 with a challenge.
Statuses outside 200–599, a 3xx without a `Location`, and headers a flow may not set are refused
at load by `sutra lint`, sealing and deploy alike; a computed status or header value that cannot be
sent is a 500 that names the header, never its value.

Reference: [Status and response headers](../building/q-namespace.md#status-and-response-headers-080).
Walkthrough: [Answering with a status and headers](../building/reply-status-and-headers.md).

## Broker outages as messages

Set `sutra.engine.connectivity-events.channel` (env `SUTRA_CONNECTIVITY_EVENTS_CHANNEL`) to a
channel name, declare that channel in a package (`transport: local`, `codec: json`), and each
broker outage start and end the AMQP 1.0 transport sees on one of the package's channels arrives
as a JSON message there. Unset, nothing changes.

Reference: [Broker connectivity events](configuration.md#broker-connectivity-events).
Walkthrough: [Broker outages as messages](connectivity-events.md).

## For transport authors

`sutra_channels::connectivity` (`ConnectivityReporter`, `ConnectivityEvent`, `current_send()`,
re-exported by `sutra-transport-spi`) and the default method `InboundIntake::connectivity()`, a
no-op unless the engine installs a sink. Existing transports compile unchanged and report nothing.

## Image scan

CVE-2026-95690 (libssl3, DTLS) in the distroless base has no fixed Debian package. The engine
never loads libssl3: on the 0.8.0 binary, `readelf -d` lists libgcc_s, libm, libc and the loader as
its only libraries and the binary names no libssl to load at run time, and nothing in it speaks
DTLS. The entry is time-boxed in `.trivyignore.yaml` to 2026-12-16 with the other base-image
entries.
