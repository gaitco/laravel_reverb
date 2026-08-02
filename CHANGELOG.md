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
- Public entry point (`package:flutter_reverb/flutter_reverb.dart`), example app and CI.
