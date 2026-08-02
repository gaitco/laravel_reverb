# flutter_reverb — Design

**Date:** 2026-08-02
**Status:** Approved
**Package:** `flutter_reverb` (public, pub.dev, MIT)

## Problem

Laravel Reverb speaks the Pusher WebSocket protocol, so Flutter apps today reach for
`pusher_channels_flutter` and hand-write the Laravel-specific glue around it. Two real
integrations in this codebase (`bidi/flutter`, `dahab/mobile`) independently grew the
same workarounds:

- **No channel ref-counting.** A bare `unsubscribe()` tears a channel down even when
  another screen still needs it, so each app maintains its own `Map<String, int>` of
  channel owners.
- **Native init race.** The iOS plugin holds a force-unwrapped `Pusher!`; any
  subscribe/connect reaching the platform side before init completes hard-crashes the
  app. Worked around with a `Completer` gate.
- **Manual `/broadcasting/auth` wiring** for every private channel.
- **A single large `switch`** mapping event names to handlers, because there is no
  per-event listener registry.
- **Reconnect leaves a correctness gap.** Reverb does not replay events missed while
  disconnected, so apps need a reliable "we are fully back" signal to reconcile via REST.

`flutter_reverb` closes all five in the package instead of in every app.

## Decisions

| Decision | Choice | Why |
|---|---|---|
| Transport | Pure Dart over `web_socket_channel` | No native code, so no iOS init race; identical on iOS/Android/web/desktop; fully unit-testable; reconnect is ours to fix |
| API shape | Laravel Echo-style chaining | Laravel developers already know it; backend docs translate 1:1 |
| Auth | Built-in HTTP POST, with optional `authorizer` override | Default removes boilerplate; override lets apps reuse Dio interceptors / token refresh / cert pinning |
| Lifecycle | `listen()` returns a cancelable handle; ref-counting internal | Makes the ref-count bug impossible by construction, with no new concepts |

Scope for v1: auto-reconnect + `onReconnected`, app lifecycle handling, presence
channels, client events (whisper).

## Architecture

Five files. No interfaces with a single implementation.

```
lib/flutter_reverb.dart      # public exports only
lib/src/reverb.dart          # Reverb: channel registry, reconnect policy, app lifecycle
lib/src/connection.dart      # WebSocket + Pusher wire protocol, ping/pong, backoff
lib/src/channel.dart         # Channel / PrivateChannel / PresenceChannel + Subscription
lib/src/auth.dart            # default /broadcasting/auth POST
```

Dependencies: `web_socket_channel`, `http`, `flutter`. Nothing else.

**Responsibilities**

- `Connection` owns the socket and speaks protocol. It knows nothing about Laravel.
  It exposes: `connect()`, `send(Map)`, a `Stream` of decoded frames, and the current
  `socketId`. Testable in isolation by feeding frames.
- `Reverb` owns the channel map and decides *when* to talk: what to subscribe, when to
  re-authorize, when to reconnect, when to fire `onReconnected`.
- `Channel` owns its listeners and its ref-count. It converts Echo-style event names to
  wire event names and decodes payloads.
- `auth.dart` is one function: given endpoint, headers, channel, and socket id, return
  the auth payload.

The socket is injectable as a plain function type
(`WebSocketChannel Function(Uri)`, defaulting to `WebSocketChannel.connect`) solely so
tests can drive frames without a server. This is a function parameter, not an
abstraction layer.

## Public API

```dart
final reverb = Reverb(
  host: 'api.example.com',
  port: 443,
  appKey: 'app-key',
  useTls: true,
  authEndpoint: 'https://api.example.com/broadcasting/auth',
  authHeaders: () async => {'Authorization': 'Bearer ${token()}'},
  // authorizer: (channel, socketId) async => ...  // full override; package skips HTTP
);

await reverb.connect();

final sub = reverb.private('users.$id')
  .listen('OrderCreated', (data) => ...)
  .listen('.order.status.changed', (data) => ...);

reverb.presence('room.5')
  ..here((members) => ...)
  ..joining((member) => ...)
  ..leaving((member) => ...);

channel.whisper('typing', {'user': id});
channel.listenForWhisper('typing', (data) => ...);

reverb.onReconnected(() => refetchEverything());
reverb.connectionState;   // Stream<ReverbState>

sub.cancel();   // channel unsubscribes only when its last listener is gone
```

**Channel names.** Callers pass the bare name (`'users.1'`). The package adds the
`private-` / `presence-` prefix. Passing an already-prefixed name is not supported and
is documented as such.

**Event names (Echo-compatible).** A name that does not begin with `.` or `\` is
resolved against the default namespace `App\Events`, so `listen('OrderCreated')` matches
the wire event `App\Events\OrderCreated`. A leading `.` means a literal `broadcastAs()`
name: `listen('.order.created')` matches the wire event `order.created`. The namespace
is configurable via a `namespace` option on `Reverb`.

## Protocol details

These are the parts an implementation gets wrong by default and must be explicit about.

- **Endpoint:** `ws(s)://host:port/app/{appKey}?protocol=7&client=flutter&version={pkg}`.
  `wss` when `useTls` is true.
- **Handshake:** server sends `pusher:connection_established`; its `data` field carries
  `socket_id` and `activity_timeout`.
- **`data` is double-encoded.** In `pusher:connection_established` and in application
  events, `data` is a JSON *string* containing JSON, not a nested object. It must be
  decoded twice. Some servers/events send it already as an object, so the decoder
  handles both shapes.
- **Subscribe:** `{"event":"pusher:subscribe","data":{"channel": name}}`, plus `auth`
  for private channels and `auth` + `channel_data` for presence channels.
- **Auth is bound to `socket_id`.** The signature is only valid for the socket it was
  issued for. On every reconnect the socket id changes, so all private and presence
  channels must be **re-authorized**, not merely re-subscribed. This is the single most
  important constraint in the reconnect path.
- **Keepalive:** reply to `pusher:ping` with `pusher:pong`; send our own `pusher:ping`
  when the connection has been idle longer than `activity_timeout`, and treat a missing
  pong as a dead socket.
- **Presence internals:** `pusher_internal:subscription_succeeded` carries the initial
  member list; `pusher_internal:member_added` / `member_removed` carry deltas.
- **Client events** must be prefixed `client-` and are only permitted on private and
  presence channels.
- **Errors:** `pusher:error` carries a code; 4000-series codes indicate the connection
  must not be retried as-is (bad app key, unsupported protocol) and are surfaced rather
  than retried in a loop.

## Data flow

1. `connect()` opens the socket.
2. Server sends `pusher:connection_established`; `Reverb` stores `socket_id`.
3. For each channel awaiting subscription: public channels send `pusher:subscribe`
   immediately; private and presence channels call the authorizer with
   `(channelName, socketId)` first, then send `pusher:subscribe` carrying `auth`
   (and `channel_data` for presence).
4. Incoming frames: decode the outer JSON, decode the inner `data`, route by `channel`
   then by `event` to that channel's listeners.
5. Channels created before the socket is established queue their subscription and are
   flushed at step 3. There is no "not ready yet" error surface for callers.

## Reconnect

Backoff 1s → 2s → 4s → 8s → 16s → 30s with jitter, capped at 30s and reset on a
successful handshake. On reconnect, every channel is re-established through the full
step 3 path, including fresh authorization against the new socket id.

`onReconnected` fires **only after every previously-live channel is subscribed again**,
so an app's REST reconcile cannot race a half-restored socket.

App lifecycle: `AppLifecycleState.paused` disconnects cleanly (no backoff, no error);
`resumed` reconnects through the same path. This behaviour is on by default and can be
disabled with a constructor flag for apps that manage it themselves.

## Error handling

Nothing thrown by the package escapes into application code.

- Authorizer failure or a channel-level `pusher:error` marks that channel failed and
  reports it through an `onError` callback. The socket and all other channels stay up.
- Socket-level failures enter the backoff loop, except for non-retryable 4000-series
  codes, which stop retrying and report.
- Rationale: realtime is an enhancement. A dead socket must never break a screen that
  also loads over REST.

An optional `onLog` callback replaces `print`/`log` so hosts control logging.

## Testing

Unit tests drive a fake socket function — no server, no platform channels:

- handshake parses `socket_id` and `activity_timeout`
- double-encoded `data` decodes correctly, and an already-decoded object also works
- event-name resolution: bare name → namespaced, leading `.` → literal
- ref-counted teardown: two listeners on one channel, cancel one, channel stays
  subscribed; cancel the second, channel unsubscribes
- reconnect re-authorizes with the **new** socket id (asserts the authorizer is called
  again and receives the new id)
- `onReconnected` fires only after all channels are re-subscribed
- whisper sends a `client-` prefixed event and is rejected on public channels
- backoff sequence and its reset on success
- presence member list and add/remove deltas

## Deliverables

- The package as described above
- `example/` — a minimal app connecting to a local Reverb instance
- `README.md` — install, configure, the four common recipes (public, private, presence,
  whisper), and a migration note from `pusher_channels_flutter`
- `CHANGELOG.md`, MIT `LICENSE`, `analysis_options.yaml`
- CI: `dart format --set-exit-if-changed`, `flutter analyze`, `flutter test`

## Explicitly out of scope for v1

- Native transport option (pure Dart only; no transport interface until a second
  implementation actually exists)
- Encrypted (`private-encrypted-`) channels
- Automatic event-to-model deserialization; `listen` yields `Map<String, dynamic>`
- Pusher Channels cloud specifics such as clusters; this targets self-hosted Reverb
