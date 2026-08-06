# Deferred to 0.2.0

Findings from the 0.1.0 reviews that were judged real but not release-blocking. Each is
documented in the README or dartdoc where it affects users today.

## Behaviour

- **No "first listener added" hook on `Channel`.** When the last listener is cancelled the
  channel is removed from the registry, but re-listening on a handle the caller still holds
  bumps the handler count 0→1 without sending `pusher:subscribe`, so it silently receives
  nothing. Currently documented; the fix is an `onFirst` hook mirroring `onEmpty`.
- **An authorizer failure disables that channel for the session.** `_subscribe` reports through
  `onError` and returns; nothing retries. Workaround is documented (cancel all listeners to
  force removal, then re-request the channel). A bounded retry belongs here.
- **`pusher:subscription_error` reports but does not evict the dead channel** from the registry.
- **`httpAuthorizer`'s default `http.Client` is never closed** and `Reverb.dispose()` cannot
  reach it. Harmless for an app-lifetime singleton; leaks a connection pool if a host creates
  and disposes a `Reverb` per session.

## API

- **The error types are a flat set.** `ReverbFatalError`, `ReverbConnectionClosed`,
  `ReverbAuthException`, `ReverbProtocolError` and `ReverbSubscriptionError` are all reachable
  through `onError` with no common base. A sealed `ReverbException` would let hosts switch
  exhaustively.
- **`Reverb.clientVersion` duplicates the version in `pubspec.yaml`** with only a comment
  keeping them in sync.

## Housekeeping

- `Connection.frames`' doc comment says "application-level frames only", but the stream also
  carries `pusher:error` and `pusher:subscription_error`. `Reverb` filters both before channel
  dispatch, so nothing leaks to application code — the comment is simply stale.
- `@visibleForTesting` on the `socketFactory` and `random` constructor parameters is enforced by
  the analyzer at any call site outside this package's `test/` directory. Call sites inside the
  declaring library (`lib/src/reverb.dart`) are exempt, which is why
  `package:laravel_reverb/testing.dart` cannot call `Reverb(...)` directly with a fake
  `socketFactory` — it routes through `buildTestReverb`, a same-library bridge, to keep the
  annotation intact for every other caller.

## Test coverage

- `exports_test.dart` is a hand-maintained compile check; it cannot notice an accidental extra
  export.
