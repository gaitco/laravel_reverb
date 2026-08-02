import 'dart:async';

import 'package:flutter_reverb/src/auth.dart';
import 'package:flutter_reverb/src/reverb.dart';
import 'package:flutter_test/flutter_test.dart';

import 'support/fake_socket.dart';

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

  test('a failed connection attempt sets state to failed and reports onError',
      () async {
    final socket = FakeSocket();
    final errors = <Object>[];
    final reverb = reverbFor(
      socket,
      onError: (Object e, StackTrace? _) => errors.add(e),
    );

    final connected = reverb.connect();
    await socket.serverClose();
    await connected;

    expect(reverb.state, ReverbState.failed);
    expect(errors, isNotEmpty);
  });
}
