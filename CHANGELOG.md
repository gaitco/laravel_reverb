## 0.2.0

Behavioural fixes and API polish from the 0.1.0 review backlog.

- **A dead channel handle now comes back to life.** Re-listening on a handle
  whose last listener was cancelled used to silently receive nothing
  forever; it now resends `pusher:subscribe` (re-authorizing private and
  presence channels against the current socket id) and goes straight back to
  work. Getting a fresh handle from `channel()`/`private()`/`presence()`
  still works too, but is no longer necessary.
- **A failing authorizer now retries.** If `Authorizer` throws while
  subscribing a private or presence channel, `laravel_reverb` retries with
  the same exponential backoff used for reconnects, up to three attempts,
  reporting every failure — including the last — through `onError`. A
  transient 500 or a token that is about to expire no longer disables a
  channel for the rest of the session.
- **A common `ReverbException` base for every error type.** `ReverbFatalError`,
  `ReverbConnectionClosed`, `ReverbAuthException`, `ReverbProtocolError` and
  `ReverbSubscriptionError` all now extend the sealed `ReverbException`, so
  host applications can `switch` over `onError`'s argument exhaustively
  instead of writing an `is` chain.
- **The default HTTP client is closed on `dispose()`.** The `http.Client`
  created implicitly for `authEndpoint` is now owned by `Reverb` and closed
  when you call `dispose()`. A client you pass in yourself (via a custom
  `authorizer`) is never touched — that one is still yours to close.
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
