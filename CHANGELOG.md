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
