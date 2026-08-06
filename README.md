![laravel_reverb](https://raw.githubusercontent.com/gaitco/laravel_reverb/main/assets/banner.png)

[![pub package](https://img.shields.io/pub/v/laravel_reverb.svg)](https://pub.dev/packages/laravel_reverb)
[![license: MIT](https://img.shields.io/badge/license-MIT-2a3154.svg)](LICENSE)
[![publisher](https://img.shields.io/pub/publisher/laravel_reverb.svg)](https://pub.dev/publishers/gaitco.com)

A Laravel Reverb realtime client for Flutter. It speaks the Pusher wire
protocol in pure Dart — no native plugins — with a Laravel Echo-style API:
chainable, cancelable listeners, automatic reconnection with re-authorization,
presence channels, and client events (whispers).

It exists because of three specific problems, each of which cost us a
production bug in a shipped app:

- **A channel outlives one screen.** Two screens listening to the same channel
  is normal, and a bare `unsubscribe()` from either one tears it down for both.
  Here, `listen()` returns a handle and the channel only unsubscribes when its
  **last** listener is cancelled — so ref-counting is an invariant of the
  package, not a `Map<String, int>` every app maintains by hand.
- **Backgrounded apps hold dead sockets.** iOS silently kills a socket the app
  isn't watching, and the client never notices. `handleAppLifecycle` (on by
  default) disconnects on background and reconnects on foreground.
- **Event names are Laravel's, not the wire's.** `listen('OrderCreated')`
  resolves to `App\Events\OrderCreated`, and `listen('.order.created')` to a
  `broadcastAs()` name — the same rules Laravel Echo uses, so your backend docs
  translate directly.

## Is this the right package for you?

This is a client for **self-hosted Laravel Reverb**, and its scope is
deliberately that. It does not support Pusher's hosted service, clusters or
API-key configuration, and it does not implement encrypted channels
(`private-encrypted-`) — Reverb has no server-side support for them either, so
they are a Pusher-hosted feature rather than something missing here.

If you are on Pusher's hosted service,
[`pusher_reverb_flutter`](https://pub.dev/packages/pusher_reverb_flutter) is a
mature, actively maintained alternative and the better choice.

Pick this one if you run Reverb yourself and want ref-counted channel teardown,
app lifecycle handling, or Echo-compatible event names. Both are MIT and speak
the same protocol, so switching either direction is a mechanical change.

## Install

```bash
flutter pub add laravel_reverb
```

## Laravel setup

This package targets a self-hosted [Reverb](https://reverb.laravel.com)
server, not Pusher's hosted service. In your Laravel app's `.env`, set:

```
BROADCAST_CONNECTION=reverb
REVERB_APP_KEY=your-reverb-app-key
REVERB_HOST=api.example.com
REVERB_PORT=443
REVERB_SERVER_PATH=
```

These map directly onto the `Reverb` constructor: `REVERB_APP_KEY` to
`appKey`, `REVERB_HOST` to `host`, `REVERB_PORT` to `port`, and
`REVERB_SERVER_PATH` to `path` — set that last one only if your Reverb server
sits behind a reverse proxy on a subpath, such as `/ws`.

## Local development

A Reverb server started with `php artisan reverb:start` listens on
`0.0.0.0:8080` over plain HTTP. Three things bite in that setup, all of them
platform quirks rather than package behaviour:

**The host differs per platform.** `localhost` on a device means the device,
not your Mac:

| Target | `host` |
|---|---|
| iOS simulator | `localhost` |
| Android emulator | `10.0.2.2` |
| Physical device | Your machine's LAN IP, e.g. `192.168.1.20` |

```dart
// `Platform` comes from dart:io.
final host = Platform.isAndroid ? '10.0.2.2' : 'localhost';

final reverb = Reverb(
  host: host,
  port: 8080,
  appKey: 'your-reverb-app-key',
  useTls: false,
  authEndpoint: 'http://$host:8000/broadcasting/auth',
);
```

**Android blocks cleartext.** On API 28 and above, `ws://` is refused before
the socket is ever opened, which surfaces as a connection failure with no
server-side log. Allow it for debug builds only, in
`android/app/src/debug/AndroidManifest.xml`:

```xml
<manifest xmlns:android="http://schemas.android.com/apk/res/android">
    <application android:usesCleartextTraffic="true" />
</manifest>
```

Putting this in the debug manifest keeps release builds cleartext-free.

**Self-signed TLS is not worth it locally.** If you terminate TLS in front of
Reverb with a self-signed certificate, Dart rejects it, and there is no hook
in this package to override that — `useTls: false` over the local network is
the simpler answer. Use real certificates in staging and production, where
`useTls: true` and `port: 443` are the defaults.

### It connects on iOS but not Android

In order, the three usual causes:

1. `host` is `localhost` — an Android emulator needs `10.0.2.2`.
2. Cleartext is blocked — add the debug manifest above.
3. `authEndpoint` still points at `localhost` — it needs the same
   platform-specific host as the socket.

Pass `onLog: print` and `onError: (e, _) => print(e)` to the constructor while
debugging; between them, every connection attempt and every failure is
visible.

## How it fits together

![Architecture: your Flutter app talks to laravel_reverb, which speaks the Pusher wire protocol to your Reverb server and authorizes private channels against /broadcasting/auth](https://raw.githubusercontent.com/gaitco/laravel_reverb/main/assets/architecture.png)

## Quick start

```dart
final reverb = Reverb(
  host: 'api.example.com',
  port: 443,
  appKey: 'your-reverb-app-key',
  useTls: true,
  authEndpoint: 'https://api.example.com/broadcasting/auth',
  authHeaders: () async => {'Authorization': 'Bearer $token'},
);

await reverb.connect();
```

## Errors, state and teardown

- `onError` (constructor parameter) is how the package reports runtime
  failures it handled without throwing — a dropped socket, a rejected
  subscription, a failed authorizer, a non-fatal protocol error. There is no
  other channel for these; if you don't pass it, they are reported nowhere.
- `reverb.states` is a `Stream<ReverbState>` of every connection state
  change (`connecting`, `connected`, `reconnecting`, `disconnected`,
  `failed`); `reverb.state` is the current value.
- `handleAppLifecycle` (default `true`) disconnects on background and
  reconnects on foreground, so iOS doesn't hold a socket the OS will
  silently kill. Set it to `false` if your app manages the socket itself.
- Call `reverb.dispose()` from your app's teardown (e.g. alongside other
  singletons at shutdown). It disconnects, stops observing app lifecycle
  events, closes the `states` and `channelHealth` streams, and closes the
  `http.Client` it created for `authEndpoint` (if any) — skipping it leaks
  the socket, the lifecycle
  observer and that client's connection pool. A client you passed in
  yourself via a custom `authorizer` is never touched — that one is yours.
- `reverb.disconnect()` closes the socket and stops reconnecting, keeping
  channels and listeners so a later `connect()` restores them — this is what
  the app-lifecycle pause/resume path uses. Pass `disconnect(forget: true)`
  on logout instead: it also drops every channel, clears registered
  `onReconnected` callbacks, and makes every handle created before the call
  permanently inert, so a screen still holding the previous user's channel
  can't resubscribe it — or whisper on it — under the next user's session.

## Recipes

### Public channel

```dart
final subscription = reverb.channel('orders').listen(
  'OrderCreated',
  (Map<String, dynamic> data) => print(data),
);
```

### Private channel

`listen` returns a chainable `Subscription`; cancel it in `dispose` to remove
every listener registered through the chain.

```dart
class _OrderScreenState extends State<OrderScreen> {
  Subscription? _subscription;

  @override
  void initState() {
    super.initState();
    _subscription = reverb
        .private('users.1')
        .listen('OrderShipped', (data) => print(data))
        .listen('OrderCancelled', (data) => print(data));
  }

  @override
  void dispose() {
    _subscription?.cancel();
    super.dispose();
  }
}
```

A channel unsubscribes once its last listener is cancelled, but the handle
itself is never left dead: calling `listen` on it again resends
`pusher:subscribe` (re-authorizing private and presence channels against the
current socket id) and puts it straight back to work — as long as nothing
else has since claimed the same name. Stick to one pattern per channel name:
either keep reusing the handle you already have, or always ask `reverb` for
a fresh one (`reverb.private('users.1')`, etc.). Mixing the two for the same
name — holding an old, emptied handle while also asking for a new one — means
only the one holding the name is live. The other stays inert, and it does not
wake up on its own when the occupant releases the name: it only reclaims it on
its own next 0-to-1 listener transition.

### Presence channel

```dart
final channel = reverb.presence('chat.1');
channel.members(
  here: (members) => print('online: $members'),
  joining: (member) => print('joined: ${member.id}'),
  leaving: (member) => print('left: ${member.id}'),
);
```

`members` also takes a `roster` callback, which receives the full member set
on subscribe and after every join or leave — reach for it instead of
`here`/`joining`/`leaving` when you want a live set rather than deltas:

```dart
channel.members(roster: (members) => updateSeatMap(members.values));
```

`channel.currentMembers` exposes the same set synchronously, for whenever you
need it outside a callback. Membership does not survive a socket drop: it is
cleared when the connection is lost and re-seeded from the server once the
channel resubscribes.

### Whisper (client events)

Whispers are only available on private and presence channels — they never
reach the application server, so they suit ephemeral signals like typing
indicators.

```dart
final channel = reverb.private('chat.1');
channel.listenForWhisper('typing', (data) => print('${data['user']} is typing'));
channel.whisper('typing', {'user': 'Alice'});
```

### Cache channel

Reverb's `cache-`, `private-cache-` and `presence-cache-` channels replay the
last event broadcast on them to each new subscriber, so a screen that opens
after the fact still gets current state without a REST round-trip.

A cache hit arrives as an ordinary event — nothing special to write:

```dart
reverb.channel('cache-scoreboard').listen(
  'ScoreUpdated',
  (Map<String, dynamic> data) => setState(() => score = data['score'] as int),
);
```

A cache **miss** — nothing has been broadcast on that channel yet — arrives as
the protocol event `pusher:cache_miss`. Listen for it when "no value yet" needs
different handling from "still connecting":

```dart
reverb.channel('cache-scoreboard').listen(
  'pusher:cache_miss',
  (_) => setState(() => score = 0),
);
```

## Event names

A bare event name is namespaced against `App\Events` (or whatever `namespace`
you passed to the constructor), so `listen('OrderCreated')` matches the wire
event `App\Events\OrderCreated`. A leading dot means a literal
`broadcastAs()` name: `listen('.order.created')` matches an event broadcast as
`order.created`.

Names beginning with `pusher:` are protocol events, not application events, and
are never namespaced — `listen('pusher:cache_miss')` matches that wire event
exactly.

## Reconnection

When the socket drops, `laravel_reverb` retries with exponential backoff —
1s, 2s, 4s, 8s, 16s, capped at 30s, with jitter so that clients dropped by the
same outage don't all reconnect in lockstep. On reconnect, every private and
presence channel is re-authorized against the new socket id, because a Pusher
auth signature is bound to the socket id it was issued for.

Reverb does not replay events missed while disconnected — this is exactly
what `onReconnected` is for. It fires only after every previously-live
channel has resubscribed (so a refetch can't race a half-restored socket),
and it does not fire on the first successful connect — but an explicit
`disconnect()` followed by `connect()` does fire it, since that is a real
restore too. The exception is `disconnect(forget: true)`: it clears every
registered `onReconnected` callback, so the following `connect()` has none
left to run:

```dart
reverb.onReconnected(() => refetch());
```

If an `Authorizer` throws for a given private or presence channel, that
failure is reported through `onError` and `laravel_reverb` retries it with
the same exponential backoff used for reconnects, up to three attempts in
total. Every failure — including the last — is reported through `onError`,
so a transient 500 or a token that is momentarily expired is never silent.

Once the last attempt fails, the channel is left registered but subscribed to
nothing — re-listening on the same handle, or requesting it again, does
**not** retry on its own, since from the registry's point of view the channel
is still there and already has its listener. Cancel every listener on it
first, so it actually unsubscribes and is dropped from the registry, and
*then* listen again (or request it again) to force a fresh authorization
attempt:

```dart
subscription.cancel();
final channel = reverb.private('users.1'); // retries authorization
```

## Keepalive

By default, `laravel_reverb` pings only after the server's advertised
`activity_timeout` of silence, and treats a missed pong as a dead socket —
this is exactly 0.2.0's behaviour, and it can take close to a minute to
notice a half-open connection. For an app that needs to notice much faster —
a realtime game, a live dashboard — set `pingInterval` and `watchdogTimeout`:

```dart
final reverb = Reverb(
  host: 'api.example.com',
  appKey: 'your-reverb-app-key',
  pingInterval: Duration(seconds: 10),
  watchdogTimeout: Duration(seconds: 15),
);
```

Once the handshake lands, death detection is always on, in every
configuration:

- **Neither set (the default):** the legacy idle-ping, backed by a pong
  deadline.
- **`pingInterval` alone:** a periodic ping on that schedule, still backed by
  a pong deadline.
- **Both set:** a periodic ping plus an inbound-frame watchdog. The watchdog
  resets on any frame from the server, but deliberately **not** on our own
  outbound sends — a socket that can still write but never receives is
  exactly the failure it exists to catch.

`watchdogTimeout` must be longer than `pingInterval`, or the constructor
throws `ArgumentError` — a shorter watchdog would fire between pings and the
client would reconnect forever.

None of these cover the window *before* the handshake: a socket whose upgrade
succeeds but which never sends `pusher:connection_established` is not yet
watched, and `connect()` waits on it. Set a connect timeout at your HTTP or
platform layer if that matters to you.

## Channel health

`reverb.channelHealth` is a `Stream<ChannelHealth>` of per-channel up/down
transitions, deliberately separate from `reverb.states`: a connected socket
whose channel authorization was rejected is `ReverbState.connected` and
unhealthy here, which is what degraded-mode polling should gate on rather
than the socket state alone.

```dart
reverb.channelHealth.listen((health) {
  if (!health.healthy) startPollingFallbackFor(health.channel);
});
```

`health.channel` is the wire name, including any `private-`/`presence-`
prefix. `reverb.isSubscribed(wireName)` reports the current value for one
channel without subscribing to the stream.

## Custom authorizer

For apps that need their own HTTP client, interceptors, token refresh or
certificate pinning, pass `authorizer` instead of `authEndpoint`. The package
then makes no HTTP requests of its own:

```dart
final reverb = Reverb(
  host: 'api.example.com',
  appKey: 'your-reverb-app-key',
  authorizer: (String channelName, String socketId) async {
    final response = await myHttpClient.post(
      Uri.parse('https://api.example.com/broadcasting/auth'),
      body: {'socket_id': socketId, 'channel_name': channelName},
    );
    return ReverbAuth(auth: jsonDecode(response.body)['auth'] as String);
  },
);
```

## Migrating from pusher_channels_flutter

| pusher_channels_flutter | laravel_reverb |
|---|---|
| `init(...)` + `connect()` | `Reverb(...)` constructor + `connect()` |
| `subscribe(channelName: 'private-users.1')` | `private('users.1')` — the prefix is added for you |
| `onAuthorizer` | `authEndpoint`/`authHeaders`, or `authorizer` for a full override |
| Manual ref-counting / `unsubscribe` | `Subscription.cancel()` — the channel tears itself down once its last listener is gone |
| `trigger(eventName, channelName, data)` | `channel.whisper(eventName, data)` |

## Publisher

Published under the pub.dev verified publisher **gaitco.com**.
