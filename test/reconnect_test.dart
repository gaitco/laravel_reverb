import 'package:fake_async/fake_async.dart';
import 'package:flutter_reverb/src/auth.dart';
import 'package:flutter_reverb/src/connection.dart';
import 'package:flutter_reverb/src/reverb.dart';
import 'package:flutter_test/flutter_test.dart';

import 'support/fake_socket.dart';

void main() {
  // Task 7 makes Reverb a WidgetsBindingObserver, which touches
  // WidgetsBinding.instance during connect(). Initialize the test binding here
  // so these tests keep passing once that lands.
  TestWidgetsFlutterBinding.ensureInitialized();

  test('reconnects after a drop and re-authorizes with the new socket id', () {
    fakeAsync((async) {
      final sockets = <FakeSocket>[FakeSocket(), FakeSocket()];
      var index = 0;
      final authCalls = <String>[];

      final reverb = Reverb(
        host: 'localhost',
        port: 8080,
        appKey: 'key',
        useTls: false,
        authorizer: (String channel, String socketId) async {
          authCalls.add(socketId);
          return const ReverbAuth(auth: 'key:sig');
        },
        socketFactory: (Uri _) => sockets[index++].channel,
      );

      reverb.connect();
      async.flushMicrotasks();
      sockets[0].emitJson(handshakeFrame(socketId: 'first'));
      async.flushMicrotasks();

      reverb.private('users.1').listen('OrderCreated', (_) {});
      async.flushMicrotasks();
      expect(authCalls, <String>['first']);

      sockets[0].serverClose();
      async.flushMicrotasks();
      expect(reverb.state, ReverbState.reconnecting);

      async.elapse(const Duration(seconds: 2));
      async.flushMicrotasks();
      sockets[1].emitJson(handshakeFrame(socketId: 'second'));
      async.flushMicrotasks();

      expect(authCalls, <String>['first', 'second']);
      expect(reverb.state, ReverbState.connected);
      expect(
        sockets[1].sentJson.last['data'],
        <String, dynamic>{'channel': 'private-users.1', 'auth': 'key:sig'},
      );
    });
  });

  test('onReconnected fires only after every channel is resubscribed', () {
    fakeAsync((async) {
      final sockets = <FakeSocket>[FakeSocket(), FakeSocket()];
      var index = 0;
      var reconnected = 0;
      var subscribedAtCallback = -1;

      final reverb = Reverb(
        host: 'localhost',
        port: 8080,
        appKey: 'key',
        useTls: false,
        authorizer: (String channel, String socketId) async {
          // Authorization is asynchronous in reality; make sure the callback
          // waits for it rather than firing on handshake.
          await Future<void>.delayed(const Duration(milliseconds: 50));
          return const ReverbAuth(auth: 'key:sig');
        },
        socketFactory: (Uri _) => sockets[index++].channel,
      );

      reverb.onReconnected(() {
        reconnected++;
        subscribedAtCallback = sockets[1]
            .sentJson
            .where((Map<String, dynamic> f) => f['event'] == 'pusher:subscribe')
            .length;
      });

      reverb.connect();
      async.flushMicrotasks();
      sockets[0].emitJson(handshakeFrame(socketId: 'first'));
      async.elapse(const Duration(milliseconds: 100));

      reverb.private('users.1').listen('A', (_) {});
      reverb.private('orders.9').listen('B', (_) {});
      async.elapse(const Duration(milliseconds: 100));
      expect(reconnected, 0);

      sockets[0].serverClose();
      async.elapse(const Duration(seconds: 3));
      sockets[1].emitJson(handshakeFrame(socketId: 'second'));
      async.elapse(const Duration(milliseconds: 200));

      expect(reconnected, 1);
      expect(subscribedAtCallback, 2);
    });
  });

  test('backs off between failed attempts', () {
    fakeAsync((async) {
      final sockets = <FakeSocket>[
        FakeSocket(),
        FakeSocket(),
        FakeSocket(),
      ];
      var index = 0;

      final reverb = Reverb(
        host: 'localhost',
        port: 8080,
        appKey: 'key',
        useTls: false,
        socketFactory: (Uri _) => sockets[index++].channel,
      );

      reverb.connect();
      async.flushMicrotasks();
      sockets[0].emitJson(handshakeFrame());
      async.flushMicrotasks();

      sockets[0].serverClose();
      async.flushMicrotasks();

      // Second attempt fails immediately; a third socket must not be taken
      // before the backoff delay has elapsed.
      async.elapse(const Duration(seconds: 2));
      sockets[1].serverClose();
      async.flushMicrotasks();
      expect(index, 2);

      async.elapse(const Duration(seconds: 3));
      expect(index, 3);
    });
  });

  test('stops retrying on a fatal error code', () {
    fakeAsync((async) {
      final socket = FakeSocket();
      var created = 0;
      final errors = <Object>[];

      final reverb = Reverb(
        host: 'localhost',
        port: 8080,
        appKey: 'key',
        useTls: false,
        socketFactory: (Uri _) {
          created++;
          return socket.channel;
        },
        onError: (Object e, StackTrace? _) => errors.add(e),
      );

      reverb.connect();
      async.flushMicrotasks();
      socket.emitJson(<String, dynamic>{
        'event': 'pusher:error',
        'data': '{"code":4001,"message":"Application does not exist"}',
      });
      async.elapse(const Duration(seconds: 60));

      expect(created, 1);
      expect(reverb.state, ReverbState.failed);
      expect(errors.single, isA<ReverbFatalError>());
    });
  });

  test('an explicit disconnect does not trigger a reconnect', () {
    fakeAsync((async) {
      final socket = FakeSocket();
      var created = 0;

      final reverb = Reverb(
        host: 'localhost',
        port: 8080,
        appKey: 'key',
        useTls: false,
        socketFactory: (Uri _) {
          created++;
          return socket.channel;
        },
      );

      reverb.connect();
      async.flushMicrotasks();
      socket.emitJson(handshakeFrame());
      async.flushMicrotasks();

      reverb.disconnect();
      async.elapse(const Duration(seconds: 60));

      expect(created, 1);
      expect(reverb.state, ReverbState.disconnected);
    });
  });
}
