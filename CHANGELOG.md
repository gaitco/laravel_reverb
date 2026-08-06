## 0.6.0

Cleanup. No new capability, and nothing an existing app has to change.

- **`ReverbFake.emitFrame`.** `emit` builds an application event, which left
  two things unreachable from a test: seeding a presence roster, which arrives
  as a `pusher_internal:subscription_succeeded` frame, and connection-level
  frames such as `pusher:error`, which carry no channel. `emitFrame` sends a
  frame verbatim. The README shows roster seeding.
- **`ReverbFake` explains itself before `connect()`.** Calling `emit` or
  `drop` on a fake that has not connected threw a null-check error from
  inside the fake; both now throw a `StateError` naming the mistake, and the
  new `emitFrame` shares the same guard.
- **Each mixin owns its own state.** 0.5.0 split `Reverb` into three mixins but
  left all 29 fields on the shared base, so any mixin could still mutate any
  field. The registry and both epoch counters now live with the channel code,
  liveness with the health code, and the connect loop's state with the connect
  loop — leaving only constructor-injected values and the socket itself shared.
  Internal throughout: no public API changed and no test changed.
- Test coverage for an all-slashes `path`, cache misses on `private-cache-` and
  `presence-cache-` channels, and an exports check that catches an accidental
  addition rather than only an accidental removal.

## 0.5.0

Observability and testability, on a `reverb.dart` that no longer does
everything itself. All additive — a 0.4.0 app that changes nothing behaves
identically.

- **`ReverbFake`, for testing your app.** `package:laravel_reverb/testing.dart`
  hands you a real client on an in-memory socket: `emit` an event as the server
  would, assert your listener fired, inspect everything the client `sent`, and
  `drop()` the socket to exercise your `onReconnected`. Subscribe, dispatch and
  teardown all run the production code paths — only the socket is fake. Private
  and presence channels authorize against a canned signature, so no auth
  endpoint is needed.
- **`reverb.metrics`.** A snapshot of connection quality: the last ping/pong
  round trip, how many drops have been recovered from, how stale the socket is,
  and when the current one connected. Read it when you paint — nothing is
  streamed, because latency changes on every ping.
- **`reverb.dart` split into part files.** The connect loop, channel registry
  and health tracking now live in three mixins in their own files, with a base
  class holding the fields and naming the cross-boundary calls explicitly. No
  public API changed and no test changed; this is groundwork that made the two
  features above small edits to focused files instead of more weight on an
  787-line one.
- **New test seam:** the `Reverb` constructor takes an optional `now`, joining
  `socketFactory`, `random` and `httpClientFactory`. It lets a test drive
  latency and staleness without real time passing.

## 0.4.0

Reverb-native gaps, closed. All additive — a 0.3.0 app that changes nothing
behaves identically.

- **Custom server path.** `path` mirrors Reverb's `REVERB_SERVER_PATH`, so a
  server behind a reverse proxy at `/ws` is reachable: the client dials
  `/ws/app/KEY` instead of `/app/KEY`. Leading and trailing slashes are
  optional. Defaults to empty, which is the 0.3.0 URL exactly.
- **Cache channels.** `cache-`, `private-cache-` and `presence-cache-` channels
  now work end to end. A cache hit always arrived as an ordinary event; the
  miss did not arrive at all, because `pusher:cache_miss` was namespaced into
  an event name no server sends. Any `pusher:`-prefixed name now resolves
  literally, so `listen('pusher:cache_miss', ...)` reaches a listener.
- **Documented local development** — emulator hosts, Android cleartext, and
  why self-signed TLS is not worth it locally.
- **Scope, stated plainly.** Encrypted channels and Pusher clusters are out of
  scope because Reverb does not implement them, not because they are pending.
  The README says so instead of listing them as shortfalls.

## 0.3.0

Hardening drawn from a production Reverb client running in a realtime game.
All additive — a 0.2.0 app that changes nothing behaves identically.

- **Configurable keepalive.** `pingInterval` and `watchdogTimeout` let an app
  notice a half-open socket in seconds instead of the ~60s a server-driven
  `activity_timeout` allows. Both default to null, which is exactly the 0.2.0
  behaviour. `watchdogTimeout` must exceed `pingInterval`, or the constructor
  throws `ArgumentError`.
- **Per-channel health.** `channelHealth` is a `Stream<ChannelHealth>` of
  per-channel up/down transitions, and `isSubscribed(wireName)` reports the
  current value. A connected socket whose channel authorization was rejected
  is now observable, which is what degraded-mode polling should gate on.
- **Presence roster snapshots.** `members(roster: ...)` receives the full
  member set on subscribe and after every join or leave, and
  `channel.currentMembers` exposes it synchronously. The existing `here`,
  `joining` and `leaving` callbacks are unchanged.
- **`disconnect(forget: true)`** drops every channel and makes handles created
  before the call inert, so a screen holding the previous user's channel
  cannot resubscribe it under the next user's session. Plain `disconnect()` is
  unchanged.
- **Potentially breaking, for uncommon usage.** `Channel`, `PrivateChannel` and
  `PresenceChannel` are exported, and their constructors gained a required
  `clientEpoch` parameter, plus a new public `clientEpoch` getter. Applications
  get channels from `Reverb`, never by construction, so this is very unlikely
  to affect normal usage.

## 0.2.0

Behavioural fixes and API polish from the 0.1.0 review backlog.

- **A dead channel handle now comes back to life.** Re-listening on a handle
  whose last listener was cancelled used to silently receive nothing
  forever; it now resends `pusher:subscribe` (re-authorizing private and
  presence channels against the current socket id) and goes straight back to
  work — as long as nothing else has since claimed the same name. Stick to
  one pattern per channel name: keep reusing a handle, or always ask
  `channel()`/`private()`/`presence()` for a fresh one, rather than mixing
  the two — only the first handle to claim a name is ever live.
- **A failing authorizer now retries.** If `Authorizer` throws while
  subscribing a private or presence channel, `laravel_reverb` retries with
  the same exponential backoff used for reconnects, up to three attempts,
  reporting every failure — including the last — through `onError`. A
  transient 500 or a token that is about to expire no longer disables a
  channel until the next reconnect or a fresh listen.
- **A common `ReverbException` base for every error type.** `ReverbFatalError`,
  `ReverbConnectionClosed`, `ReverbAuthException`, `ReverbProtocolError` and
  `ReverbSubscriptionError` all now extend the sealed `ReverbException`, so
  host applications can `switch` over `onError`'s argument exhaustively
  instead of writing an `is` chain.
- **The default HTTP client is closed on `dispose()`.** The `http.Client`
  created implicitly for `authEndpoint` is now owned by `Reverb` and closed
  when you call `dispose()`. A client you pass in yourself (via a custom
  `authorizer`) is never touched — that one is still yours to close.
- **Potentially breaking, for uncommon usage.** The five error types are now
  `final` classes, so code that did `implements ReverbAuthException` (etc.),
  as a test fake might, no longer compiles — `extends` still works.
  `Channel`'s (exported) constructor also gained a required `onFirst`
  parameter, so any code constructing a `Channel` directly rather than
  through `Reverb.channel()`/`private()`/`presence()` needs to supply one.
  Neither is expected to affect normal usage.
- Fixed a stale doc comment on `Connection.frames`: it also carries non-fatal
  `pusher:error` and `pusher:subscription_error` frames, which `Reverb`
  filters out before anything reaches a channel.
- Added test coverage for presence events flowing end-to-end through
  `Connection` and `Reverb` (not just dispatched directly on a `Channel`),
  and for the `disconnect()`/`connect()` race that a fast pause/resume can
  trigger.

## 0.1.0

Initial release.

- Laravel Echo-style API: `Reverb`, `channel()`/`private()`/`presence()`,
  chainable and cancelable `Subscription`s.
- Pure-Dart Pusher wire protocol client — no native plugins.
- Automatic reconnection with exponential backoff and jitter, and full
  re-authorization of every private/presence channel on the new socket id.
- Presence channels, with `members(here:, joining:, leaving:)`.
- Client events (whispers) on private and presence channels.
- Ref-counted channel teardown: a channel unsubscribes automatically once
  its last listener is cancelled.
- App lifecycle handling: disconnects on background, reconnects on
  foreground, via `handleAppLifecycle`.
- Public entry point (`package:laravel_reverb/laravel_reverb.dart`), example app and CI.
