# flutter_reverb

[![pub package](https://img.shields.io/pub/v/flutter_reverb.svg)](https://pub.dev/packages/flutter_reverb)

A Laravel Reverb realtime client for Flutter. It speaks the Pusher wire
protocol in pure Dart — no native plugins — with a Laravel Echo-style API:
chainable, cancelable listeners, automatic reconnection with re-authorization,
presence channels, and client events (whispers).

## Install

```bash
flutter pub add flutter_reverb
```

## Laravel setup

This package targets a self-hosted [Reverb](https://reverb.laravel.com)
server, not Pusher's hosted service. In your Laravel app's `.env`, set:

```
BROADCAST_CONNECTION=reverb
REVERB_APP_KEY=your-reverb-app-key
REVERB_HOST=api.example.com
REVERB_PORT=443
```

These map directly onto the `Reverb` constructor: `REVERB_APP_KEY` to
`appKey`, `REVERB_HOST` to `host`, `REVERB_PORT` to `port`.

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

### Presence channel

```dart
final channel = reverb.presence('chat.1');
channel.members(
  here: (members) => print('online: $members'),
  joining: (member) => print('joined: ${member.id}'),
  leaving: (member) => print('left: ${member.id}'),
);
```

### Whisper (client events)

Whispers are only available on private and presence channels — they never
reach the application server, so they suit ephemeral signals like typing
indicators.

```dart
final channel = reverb.private('chat.1');
channel.listenForWhisper('typing', (data) => print('${data['user']} is typing'));
channel.whisper('typing', {'user': 'Alice'});
```

## Event names

A bare event name is namespaced against `App\Events` (or whatever `namespace`
you passed to the constructor), so `listen('OrderCreated')` matches the wire
event `App\Events\OrderCreated`. A leading dot means a literal
`broadcastAs()` name: `listen('.order.created')` matches an event broadcast as
`order.created`.

## Reconnection

When the socket drops, `flutter_reverb` retries with exponential backoff —
1s, 2s, 4s, 8s, 16s, capped at 30s, with jitter so that clients dropped by the
same outage don't all reconnect in lockstep. On reconnect, every private and
presence channel is re-authorized against the new socket id, because a Pusher
auth signature is bound to the socket id it was issued for.

Reverb does not replay events missed while disconnected — this is exactly
what `onReconnected` is for. It fires only after every previously-live
channel has resubscribed (so a refetch can't race a half-restored socket),
and it does not fire on the first successful connect:

```dart
reverb.onReconnected(() => refetch());
```

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
      '/broadcasting/auth',
      data: {'socket_id': socketId, 'channel_name': channelName},
    );
    return ReverbAuth(auth: response.data['auth']);
  },
);
```

## Migrating from pusher_channels_flutter

| pusher_channels_flutter | flutter_reverb |
|---|---|
| `init(...)` + `connect()` | `Reverb(...)` constructor + `connect()` |
| `subscribe(channelName: 'private-users.1')` | `private('users.1')` — the prefix is added for you |
| `onAuthorizer` | `authEndpoint`/`authHeaders`, or `authorizer` for a full override |
| Manual ref-counting / `unsubscribe` | `Subscription.cancel()` — the channel tears itself down once its last listener is gone |
| `trigger(eventName, channelName, data)` | `channel.whisper(eventName, data)` |

## Publisher

Published under the pub.dev verified publisher **gaitco.com**.
