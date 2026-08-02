import 'dart:async';
import 'dart:convert';

import 'package:laravel_reverb/src/auth.dart';
import 'package:laravel_reverb/src/channel.dart';
import 'package:laravel_reverb/src/connection.dart';
import 'package:laravel_reverb/src/reverb.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:http/http.dart' as http;

import 'support/fake_socket.dart';

/// A minimal `http.Client` whose [close] is observable, so a test can prove
/// `Reverb` actually closed the client it implicitly created — and, just as
/// importantly, that it left a caller-supplied client alone.
class _TrackingClient extends http.BaseClient {
  int closeCalls = 0;

  @override
  Future<http.StreamedResponse> send(http.BaseRequest request) {
    throw UnsupportedError('not used in this test');
  }

  @override
  void close() {
    closeCalls++;
    super.close();
  }
}

Reverb reverbFor(
  FakeSocket socket, {
  Authorizer? authorizer,
  void Function(Object, StackTrace?)? onError,
}) {
  return Reverb(
    host: 'localhost',
    port: 8080,
    appKey: 'key',
    useTls: false,
    authorizer: authorizer ??
        (String channel, String socketId) async =>
            const ReverbAuth(auth: 'key:sig'),
    socketFactory: factoryFor(socket),
    onError: onError,
  );
}

Future<void> settle() => Future<void>.delayed(Duration.zero);

void main() {
  // Task 7 makes Reverb a WidgetsBindingObserver, which touches
  // WidgetsBinding.instance during connect(). Initialize the test binding here
  // so these tests keep passing once that lands.
  TestWidgetsFlutterBinding.ensureInitialized();

  test('subscribes to a public channel after the handshake', () async {
    final socket = FakeSocket();
    final reverb = reverbFor(socket);

    final connected = reverb.connect();
    socket.emitJson(handshakeFrame());
    await connected;

    reverb.channel('orders').listen('OrderCreated', (_) {});
    await settle();

    expect(socket.sentJson.last, <String, dynamic>{
      'event': 'pusher:subscribe',
      'data': <String, dynamic>{'channel': 'orders'},
    });
  });

  test('queues channels created before the handshake and flushes them',
      () async {
    final socket = FakeSocket();
    final reverb = reverbFor(socket);

    reverb.channel('orders').listen('OrderCreated', (_) {});
    expect(socket.sentJson, isEmpty);

    final connected = reverb.connect();
    socket.emitJson(handshakeFrame());
    await connected;
    await settle();

    expect(
      socket.sentJson.map((Map<String, dynamic> f) => f['event']),
      contains('pusher:subscribe'),
    );
  });

  test('adds the private prefix and the auth signature', () async {
    final socket = FakeSocket();
    final captured = <String>[];
    final reverb = reverbFor(
      socket,
      authorizer: (String channel, String socketId) async {
        captured.add('$channel@$socketId');
        return const ReverbAuth(auth: 'key:sig');
      },
    );

    final connected = reverb.connect();
    socket.emitJson(handshakeFrame(socketId: '77.88'));
    await connected;

    reverb.private('users.1').listen('OrderCreated', (_) {});
    await settle();

    expect(captured, <String>['private-users.1@77.88']);
    expect(socket.sentJson.last, <String, dynamic>{
      'event': 'pusher:subscribe',
      'data': <String, dynamic>{
        'channel': 'private-users.1',
        'auth': 'key:sig'
      },
    });
  });

  test('sends channel_data for presence channels', () async {
    final socket = FakeSocket();
    final reverb = reverbFor(
      socket,
      authorizer: (String channel, String socketId) async =>
          const ReverbAuth(auth: 'key:sig', channelData: '{"user_id":"7"}'),
    );

    final connected = reverb.connect();
    socket.emitJson(handshakeFrame());
    await connected;

    reverb.presence('room.5').members(here: (_) {});
    await settle();

    expect(
      socket.sentJson.last['data'],
      <String, dynamic>{
        'channel': 'presence-room.5',
        'auth': 'key:sig',
        'channel_data': '{"user_id":"7"}',
      },
    );
  });

  test('returns the same channel object for the same name', () async {
    final socket = FakeSocket();
    final reverb = reverbFor(socket);

    final connected = reverb.connect();
    socket.emitJson(handshakeFrame());
    await connected;

    final first = reverb.channel('orders')..listen('A', (_) {});
    final second = reverb.channel('orders')..listen('B', (_) {});
    await settle();

    expect(identical(first, second), isTrue);
    expect(
      socket.sentJson
          .where((Map<String, dynamic> f) => f['event'] == 'pusher:subscribe')
          .length,
      1,
    );
  });

  test('routes an incoming event to the right channel', () async {
    final socket = FakeSocket();
    final reverb = reverbFor(socket);

    final connected = reverb.connect();
    socket.emitJson(handshakeFrame());
    await connected;

    Map<String, dynamic>? received;
    reverb
        .channel('orders')
        .listen('OrderCreated', (Map<String, dynamic> d) => received = d);
    await settle();

    socket.emitJson(<String, dynamic>{
      'event': r'App\Events\OrderCreated',
      'channel': 'orders',
      'data': '{"id":7}',
    });
    await settle();

    expect(received, <String, dynamic>{'id': 7});
  });

  test('unsubscribes when the last listener is cancelled', () async {
    final socket = FakeSocket();
    final reverb = reverbFor(socket);

    final connected = reverb.connect();
    socket.emitJson(handshakeFrame());
    await connected;

    final sub = reverb.channel('orders').listen('OrderCreated', (_) {});
    await settle();
    sub.cancel();
    await settle();

    expect(socket.sentJson.last, <String, dynamic>{
      'event': 'pusher:unsubscribe',
      'data': <String, dynamic>{'channel': 'orders'},
    });
  });

  test('an auth failure reports via onError and leaves the socket up',
      () async {
    final socket = FakeSocket();
    final errors = <Object>[];
    final reverb = reverbFor(
      socket,
      authorizer: (String channel, String socketId) async =>
          throw const ReverbAuthException('private-users.1', 403, 'nope'),
      onError: (Object e, StackTrace? _) => errors.add(e),
    );

    final connected = reverb.connect();
    socket.emitJson(handshakeFrame());
    await connected;

    reverb.private('users.1').listen('OrderCreated', (_) {});
    await settle();

    expect(errors.single, isA<ReverbAuthException>());
    expect(reverb.state, ReverbState.connected);
    expect(socket.closed, isFalse);
  });

  test('private throws immediately when no authorizer is configured', () {
    final reverb = Reverb(
      host: 'localhost',
      appKey: 'key',
      socketFactory: factoryFor(FakeSocket()),
    );

    expect(() => reverb.private('users.1'), throwsStateError);
  });

  test('emits connection states', () async {
    final socket = FakeSocket();
    final reverb = reverbFor(socket);
    final states = <ReverbState>[];
    reverb.states.listen(states.add);

    final connected = reverb.connect();
    socket.emitJson(handshakeFrame());
    await connected;
    await settle();

    expect(
        states, <ReverbState>[ReverbState.connecting, ReverbState.connected]);
  });

  test(
      'cancelling while the authorizer is pending never sends a stale '
      'subscribe', () async {
    final socket = FakeSocket();
    final authCompleter = Completer<ReverbAuth>();
    final reverb = reverbFor(
      socket,
      authorizer: (String channel, String socketId) => authCompleter.future,
    );

    final connected = reverb.connect();
    socket.emitJson(handshakeFrame());
    await connected;

    final sub = reverb.private('users.1').listen('OrderCreated', (_) {});
    await settle();
    sub.cancel();
    await settle();

    // The authorizer resolves only after the channel has already been
    // unregistered by the cancel above.
    authCompleter.complete(const ReverbAuth(auth: 'key:sig'));
    await settle();

    final events =
        socket.sentJson.map((Map<String, dynamic> f) => f['event']).toList();
    expect(events, contains('pusher:unsubscribe'));
    expect(events, isNot(contains('pusher:subscribe')));
  });

  test('disconnect closes the socket and sets state to disconnected', () async {
    final socket = FakeSocket();
    final reverb = reverbFor(socket);

    final connected = reverb.connect();
    socket.emitJson(handshakeFrame());
    await connected;

    await reverb.disconnect();

    expect(reverb.state, ReverbState.disconnected);
    expect(socket.closed, isTrue);
  });

  test('dispose is safe to call and emits no further states', () async {
    final socket = FakeSocket();
    final reverb = reverbFor(socket);

    final connected = reverb.connect();
    socket.emitJson(handshakeFrame());
    await connected;

    final states = <ReverbState>[];
    reverb.states.listen(states.add);

    reverb.dispose();
    await settle();

    expect(states, isEmpty);
    expect(() => reverb.dispose(), returnsNormally);
  });

  test(
      'a rejected subscription reports through onError instead of vanishing '
      'silently', () async {
    final socket = FakeSocket();
    final errors = <Object>[];
    final reverb = reverbFor(socket, onError: (e, _) => errors.add(e));

    final connected = reverb.connect();
    socket.emitJson(handshakeFrame());
    await connected;

    reverb.private('users.1').listen('OrderCreated', (_) {});
    await settle();

    socket.emitJson(<String, dynamic>{
      'event': 'pusher:subscription_error',
      'channel': 'private-users.1',
      'data': '{"type":"AuthError","error":"Invalid signature"}',
    });
    await settle();

    expect(errors.single, isA<ReverbSubscriptionError>());
    final error = errors.single as ReverbSubscriptionError;
    expect(error.channelName, 'private-users.1');
    expect(error.reason['error'], 'Invalid signature');
  });

  test(
      'a non-fatal pusher:error reports through onError without failing the '
      'connection', () async {
    final socket = FakeSocket();
    final errors = <Object>[];
    final reverb = reverbFor(socket, onError: (e, _) => errors.add(e));

    final connected = reverb.connect();
    socket.emitJson(handshakeFrame());
    await connected;

    socket.emitJson(<String, dynamic>{
      'event': 'pusher:error',
      'data': '{"message":"Unsupported client protocol version"}',
    });
    await settle();

    expect(errors.single, isA<ReverbProtocolError>());
    expect((errors.single as ReverbProtocolError).code, isNull);
    expect(reverb.state, ReverbState.connected);
  });

  test(
      'listening again on a handle whose last listener was cancelled '
      'resubscribes it instead of leaving it dead', () async {
    final socket = FakeSocket();
    final reverb = reverbFor(socket);

    final connected = reverb.connect();
    socket.emitJson(handshakeFrame());
    await connected;

    final channel = reverb.channel('orders');
    final sub = channel.listen('OrderCreated', (_) {});
    await settle();
    sub.cancel();
    await settle();

    expect(
      socket.sentJson.map((Map<String, dynamic> f) => f['event']),
      containsAllInOrder(<String>['pusher:subscribe', 'pusher:unsubscribe']),
    );

    // Re-listening on the same, already-emptied handle — not a fresh handle
    // from reverb.channel() — must resend pusher:subscribe.
    Map<String, dynamic>? received;
    channel.listen(
      'OrderCreated',
      (Map<String, dynamic> data) => received = data,
    );
    await settle();

    expect(socket.sentJson.last, <String, dynamic>{
      'event': 'pusher:subscribe',
      'data': <String, dynamic>{'channel': 'orders'},
    });

    socket.emitJson(<String, dynamic>{
      'event': r'App\Events\OrderCreated',
      'channel': 'orders',
      'data': '{"id":9}',
    });
    await settle();

    expect(received, <String, dynamic>{'id': 9});
  });

  test(
      'reviving a dead private channel handle re-authorizes against the '
      'current socket id', () async {
    final socket = FakeSocket();
    final captured = <String>[];
    final reverb = reverbFor(
      socket,
      authorizer: (String channel, String socketId) async {
        captured.add('$channel@$socketId');
        return const ReverbAuth(auth: 'key:sig');
      },
    );

    final connected = reverb.connect();
    socket.emitJson(handshakeFrame(socketId: '1.1'));
    await connected;

    final channel = reverb.private('users.1');
    final sub = channel.listen('A', (_) {});
    await settle();
    sub.cancel();
    await settle();

    channel.listen('B', (_) {});
    await settle();

    expect(captured, <String>['private-users.1@1.1', 'private-users.1@1.1']);
    expect(socket.sentJson.last['event'], 'pusher:subscribe');
  });

  test(
      'a stale handle cannot hijack the registry slot of a live channel '
      'under the same name', () async {
    final socket = FakeSocket();
    final reverb = reverbFor(socket);

    final connected = reverb.connect();
    socket.emitJson(handshakeFrame());
    await connected;

    // `a` is the emptied, stale handle; `b` is a fresh, live one for the
    // same name, obtained the ordinary way after `a` was dropped.
    final a = reverb.channel('orders');
    final subA = a.listen('OrderCreated', (_) {});
    await settle();
    subA.cancel();
    await settle();

    final b = reverb.channel('orders');
    expect(identical(a, b), isFalse);
    Map<String, dynamic>? receivedByB;
    final subB = b.listen(
      'OrderCreated',
      (Map<String, dynamic> data) => receivedByB = data,
    );
    await settle();

    // The stale handle tries to come back to life while `b` is live and has
    // listeners. It must not evict `b` from the registry.
    final rejectedRevival = a.listen('OrderCreated', (_) {});
    await settle();

    socket.emitJson(<String, dynamic>{
      'event': r'App\Events\OrderCreated',
      'channel': 'orders',
      'data': '{"id":1}',
    });
    await settle();
    expect(receivedByB, <String, dynamic>{'id': 1});

    // `a` still has its own (never-registered) listener from the rejected
    // revival above. Cancelling it must not touch `b`'s live registry entry
    // either — `_unsubscribe` may only act on the channel that actually
    // occupies its name.
    final sentBeforeCancel = socket.sentJson.length;
    rejectedRevival.cancel();
    await settle();
    expect(socket.sentJson.length, sentBeforeCancel); // nothing sent for `a`.

    // Cancelling b's only listener must unsubscribe b (the actual occupant),
    // not steal `a`'s non-existent slot out from under it.
    subB.cancel();
    await settle();
    expect(socket.sentJson.last, <String, dynamic>{
      'event': 'pusher:unsubscribe',
      'data': <String, dynamic>{'channel': 'orders'},
    });
  });

  test(
      'reviving a channel while its original subscribe is still awaiting '
      'authorization sends only the fresh subscribe, not the stale one',
      () async {
    final socket = FakeSocket();
    final authCompleters = <Completer<ReverbAuth>>[];
    final reverb = reverbFor(
      socket,
      authorizer: (String channel, String socketId) {
        final completer = Completer<ReverbAuth>();
        authCompleters.add(completer);
        return completer.future;
      },
    );

    final connected = reverb.connect();
    socket.emitJson(handshakeFrame());
    await connected;

    final channel = reverb.private('users.1');
    final sub = channel.listen('A', (_) {});
    await settle();
    expect(authCompleters, hasLength(1)); // the initial subscribe's auth.

    // Unsubscribes immediately; auth #1 is now stale but still in flight.
    sub.cancel();
    await settle();

    // Revives the same object; starts a second, independent subscribe.
    channel.listen('B', (_) {});
    await settle();
    expect(authCompleters, hasLength(2));

    // Resolve the stale one first, then the fresh one — the order a slow
    // /broadcasting/auth response plus a fast revival produces in practice.
    authCompleters[0].complete(const ReverbAuth(auth: 'stale'));
    await settle();
    authCompleters[1].complete(const ReverbAuth(auth: 'fresh'));
    await settle();

    final subscribes = socket.sentJson
        .where((Map<String, dynamic> f) => f['event'] == 'pusher:subscribe')
        .toList();
    expect(subscribes, hasLength(1));
    expect(subscribes.single['data']['auth'], 'fresh');
  });

  test(
      'presence member events survive Connection and Reverb routing to '
      'reach the channel', () async {
    final socket = FakeSocket();
    final reverb = reverbFor(
      socket,
      authorizer: (String channel, String socketId) async =>
          const ReverbAuth(auth: 'key:sig', channelData: '{"user_id":"7"}'),
    );

    final connected = reverb.connect();
    socket.emitJson(handshakeFrame());
    await connected;

    PresenceMember? joined;
    reverb
        .presence('room.5')
        .members(joining: (PresenceMember m) => joined = m);
    await settle();

    // Driven through the FakeSocket (not dispatched directly on the
    // channel), so this exercises Connection's event switch and
    // Reverb._onFrame's routing, not just Channel.dispatch in isolation.
    socket.emitJson(<String, dynamic>{
      'event': 'pusher_internal:member_added',
      'channel': 'presence-room.5',
      'data': jsonEncode(<String, dynamic>{
        'user_id': '9',
        'user_info': <String, dynamic>{'name': 'Zoe'},
      }),
    });
    await settle();

    expect(joined?.id, '9');
    expect(joined?.info, <String, dynamic>{'name': 'Zoe'});
  });

  test('dispose closes the http.Client it created for authEndpoint', () async {
    final tracker = _TrackingClient();
    final reverb = Reverb(
      host: 'localhost',
      appKey: 'key',
      authEndpoint: 'https://api.test/broadcasting/auth',
      httpClientFactory: () => tracker,
    );

    reverb.dispose();

    expect(tracker.closeCalls, 1);
  });

  test('dispose does not close a client the caller supplied via authorizer',
      () async {
    final tracker = _TrackingClient();
    final reverb = Reverb(
      host: 'localhost',
      appKey: 'key',
      authorizer: httpAuthorizer(
        endpoint: 'https://api.test/broadcasting/auth',
        client: tracker,
      ),
    );

    reverb.dispose();

    expect(tracker.closeCalls, 0);
  });

  test('a failed connection attempt sets state to failed and reports onError',
      () async {
    // A fatal pusher:error (4000 series) is used here rather than a plain
    // socket close: since Task 6, connect() retries non-fatal errors with
    // backoff instead of giving up, so only a fatal error settles to
    // ReverbState.failed after a single attempt.
    final socket = FakeSocket();
    final errors = <Object>[];
    final reverb = reverbFor(
      socket,
      onError: (Object e, StackTrace? _) => errors.add(e),
    );

    final connected = reverb.connect();
    socket.emitJson(<String, dynamic>{
      'event': 'pusher:error',
      'data': '{"code":4001,"message":"Application does not exist"}',
    });
    await connected;

    expect(reverb.state, ReverbState.failed);
    expect(errors, isNotEmpty);
  });

  test('rejects a watchdog that cannot outlive the ping interval', () {
    expect(
      () => Reverb(
        host: 'localhost',
        appKey: 'key',
        pingInterval: const Duration(seconds: 15),
        watchdogTimeout: const Duration(seconds: 15),
      ),
      throwsArgumentError,
    );
    expect(
      () => Reverb(
        host: 'localhost',
        appKey: 'key',
        pingInterval: const Duration(seconds: 20),
        watchdogTimeout: const Duration(seconds: 15),
      ),
      throwsArgumentError,
    );
  });

  test('accepts a watchdog longer than the ping interval', () {
    expect(
      () => Reverb(
        host: 'localhost',
        appKey: 'key',
        pingInterval: const Duration(seconds: 10),
        watchdogTimeout: const Duration(seconds: 15),
      ),
      returnsNormally,
    );
  });

  test('reports a channel healthy once the server acknowledges it', () async {
    final socket = FakeSocket();
    final reverb = reverbFor(socket);
    final health = <ChannelHealth>[];
    reverb.channelHealth.listen(health.add);

    final connected = reverb.connect();
    socket.emitJson(handshakeFrame());
    await connected;

    reverb.channel('orders').listen('OrderCreated', (_) {});
    await settle();
    expect(reverb.isSubscribed('orders'), isFalse);

    socket.emitJson(<String, dynamic>{
      'event': 'pusher_internal:subscription_succeeded',
      'channel': 'orders',
      'data': '{}',
    });
    await settle();

    expect(reverb.isSubscribed('orders'), isTrue);
    expect(health.single.channel, 'orders');
    expect(health.single.healthy, isTrue);
  });

  test('reports a channel unhealthy on subscription_error', () async {
    final socket = FakeSocket();
    final reverb = reverbFor(socket);
    final health = <ChannelHealth>[];
    reverb.channelHealth.listen(health.add);

    final connected = reverb.connect();
    socket.emitJson(handshakeFrame());
    await connected;

    reverb.channel('orders').listen('OrderCreated', (_) {});
    await settle();
    socket.emitJson(<String, dynamic>{
      'event': 'pusher_internal:subscription_succeeded',
      'channel': 'orders',
      'data': '{}',
    });
    await settle();

    socket.emitJson(<String, dynamic>{
      'event': 'pusher:subscription_error',
      'channel': 'orders',
      'data': '{"type":"AuthError","status":403}',
    });
    await settle();

    expect(reverb.isSubscribed('orders'), isFalse);
    expect(health.last.healthy, isFalse);
    expect(reverb.state, ReverbState.connected);
  });

  test('marks every live channel unhealthy when the socket drops', () async {
    final socket = FakeSocket();
    final reverb = reverbFor(socket);
    final health = <ChannelHealth>[];

    final connected = reverb.connect();
    socket.emitJson(handshakeFrame());
    await connected;

    reverb.channel('orders').listen('A', (_) {});
    reverb.channel('stock').listen('B', (_) {});
    await settle();
    for (final String name in <String>['orders', 'stock']) {
      socket.emitJson(<String, dynamic>{
        'event': 'pusher_internal:subscription_succeeded',
        'channel': name,
        'data': '{}',
      });
    }
    await settle();

    reverb.channelHealth.listen(health.add);
    await socket.serverClose();
    await settle();

    expect(
      health.map((ChannelHealth h) => h.channel).toSet(),
      <String>{'orders', 'stock'},
    );
    expect(health.every((ChannelHealth h) => !h.healthy), isTrue);
    expect(reverb.isSubscribed('orders'), isFalse);
  });

  test('presence rosters clear when the socket drops', () async {
    final socket = FakeSocket();
    final reverb = reverbFor(socket);

    final connected = reverb.connect();
    socket.emitJson(handshakeFrame());
    await connected;

    final channel = reverb.presence('room.5')..members(roster: (_) {});
    await settle();
    socket.emitJson(<String, dynamic>{
      'event': 'pusher_internal:subscription_succeeded',
      'channel': 'presence-room.5',
      'data': '{"presence":{"ids":["1"],"hash":{"1":{"name":"Ann"}}}}',
    });
    await settle();
    expect(channel.currentMembers, isNotEmpty);

    await socket.serverClose();
    await settle();

    expect(channel.currentMembers, isEmpty);
  });

  test('disconnect(forget: true) drops every channel', () async {
    // Each FakeSocket's underlying stream can only ever be listened to once
    // (a plain, non-broadcast StreamController), matching the real
    // WebSocketChannel a reconnect gets from the OS. So — as
    // reconnect_test.dart already does throughout — reconnecting after
    // disconnect() needs a fresh socket per connect cycle, not the same one
    // reused; reusing it makes Connection.open()'s second `listen` throw
    // "Stream has already been listened to", which Reverb's retry loop
    // treats as an ordinary drop and backs off on forever.
    final sockets = <FakeSocket>[FakeSocket(), FakeSocket()];
    var index = 0;
    final reverb = Reverb(
      host: 'localhost',
      port: 8080,
      appKey: 'key',
      useTls: false,
      authorizer: (String channel, String socketId) async =>
          const ReverbAuth(auth: 'key:sig'),
      socketFactory: (Uri _) => sockets[index++].channel,
    );

    final connected = reverb.connect();
    sockets[0].emitJson(handshakeFrame());
    await connected;

    reverb.channel('orders').listen('OrderCreated', (_) {});
    await settle();

    await reverb.disconnect(forget: true);
    await settle();

    expect(reverb.isSubscribed('orders'), isFalse);

    // Reconnecting the original client must not resubscribe the old channel.
    final again = reverb.connect();
    sockets[1].emitJson(handshakeFrame(socketId: 'second'));
    await again;
    await settle();

    expect(
      sockets[1]
          .sentJson
          .where((Map<String, dynamic> f) => f['event'] == 'pusher:subscribe'),
      isEmpty,
    );
  });

  test('a handle held across disconnect(forget: true) cannot revive', () async {
    // Two sockets, as above: this test reconnects for real, under a second
    // "session", and proves a handle from the first session cannot land a
    // subscribe on it.
    final sockets = <FakeSocket>[FakeSocket(), FakeSocket()];
    var index = 0;
    final reverb = Reverb(
      host: 'localhost',
      port: 8080,
      appKey: 'key',
      useTls: false,
      authorizer: (String channel, String socketId) async =>
          const ReverbAuth(auth: 'key:sig'),
      socketFactory: (Uri _) => sockets[index++].channel,
    );

    final connected = reverb.connect();
    sockets[0].emitJson(handshakeFrame());
    await connected;

    final stale = reverb.channel('orders');
    final sub = stale.listen('OrderCreated', (_) {});
    await settle();

    await reverb.disconnect(forget: true);
    await settle();

    // The next user logs in on a fresh socket.
    final again = reverb.connect();
    sockets[1].emitJson(handshakeFrame(socketId: 'second'));
    await again;
    await settle();

    // The screen holding `stale` rebuilds: its old listener goes away and it
    // listens again, exactly how reviving a handle ordinarily works.
    sub.cancel();
    stale.listen('OrderCreated', (_) {});
    await settle();

    expect(
      sockets[1]
          .sentJson
          .where((Map<String, dynamic> f) => f['event'] == 'pusher:subscribe'),
      isEmpty,
    );
    expect(reverb.isSubscribed('orders'), isFalse);
  });

  test(
      'a stale handle cannot whisper onto the next session after '
      'disconnect(forget: true)', () async {
    final sockets = <FakeSocket>[FakeSocket(), FakeSocket()];
    var index = 0;
    final reverb = Reverb(
      host: 'localhost',
      port: 8080,
      appKey: 'key',
      useTls: false,
      authorizer: (String channel, String socketId) async =>
          const ReverbAuth(auth: 'key:sig'),
      socketFactory: (Uri _) => sockets[index++].channel,
    );

    final connected = reverb.connect();
    sockets[0].emitJson(handshakeFrame());
    await connected;

    final stale = reverb.private('users.1')..listen('OrderCreated', (_) {});
    await settle();

    await reverb.disconnect(forget: true);
    await settle();

    // The next user logs in on a fresh socket.
    final again = reverb.connect();
    sockets[1].emitJson(handshakeFrame(socketId: 'second'));
    await again;
    await settle();

    // Whisper never goes through listen/cancel or _resubscribe at all, so it
    // needs its own check that the send is bound to the epoch the channel was
    // created under.
    stale.whisper('typing', <String, dynamic>{'user': 'A'});
    await settle();

    expect(sockets[1].sentJson, isEmpty);
  });

  test(
      'disconnect(forget: true) discards onReconnected callbacks registered '
      'by the previous session', () async {
    final sockets = <FakeSocket>[FakeSocket(), FakeSocket()];
    var index = 0;
    final reverb = Reverb(
      host: 'localhost',
      port: 8080,
      appKey: 'key',
      useTls: false,
      socketFactory: (Uri _) => sockets[index++].channel,
    );

    final connected = reverb.connect();
    sockets[0].emitJson(handshakeFrame());
    await connected;

    var fired = 0;
    reverb.onReconnected(() => fired++);

    await reverb.disconnect(forget: true);
    await settle();

    // The next user logs in. Their first connect must not run the previous
    // user's onReconnected closures — those may refetch data scoped to the
    // old session (e.g. `api.refetchOrdersFor(userA)`), and there is no API
    // to unregister them, so the client itself must drop them on forget.
    final again = reverb.connect();
    sockets[1].emitJson(handshakeFrame(socketId: 'second'));
    await again;
    await settle();

    expect(fired, 0);
  });

  test('plain disconnect still restores channels on reconnect', () async {
    // See the note in 'disconnect(forget: true) drops every channel' above:
    // a fresh socket per connect cycle, since each FakeSocket's stream can
    // only be listened to once.
    final sockets = <FakeSocket>[FakeSocket(), FakeSocket()];
    var index = 0;
    final reverb = Reverb(
      host: 'localhost',
      port: 8080,
      appKey: 'key',
      useTls: false,
      authorizer: (String channel, String socketId) async =>
          const ReverbAuth(auth: 'key:sig'),
      socketFactory: (Uri _) => sockets[index++].channel,
    );

    final connected = reverb.connect();
    sockets[0].emitJson(handshakeFrame());
    await connected;

    reverb.channel('orders').listen('OrderCreated', (_) {});
    await settle();

    await reverb.disconnect();
    final again = reverb.connect();
    sockets[1].emitJson(handshakeFrame(socketId: 'second'));
    await again;
    await settle();

    expect(
      <Map<String, dynamic>>[...sockets[0].sentJson, ...sockets[1].sentJson]
          .where((Map<String, dynamic> f) => f['event'] == 'pusher:subscribe')
          .length,
      2,
    );
  });
}
