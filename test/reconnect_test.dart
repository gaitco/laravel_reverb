import 'dart:async';

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

  test(
      'connect is not re-entrant: a second call while one is already in '
      'flight opens no second socket', () {
    fakeAsync((async) {
      final sockets = <FakeSocket>[FakeSocket(), FakeSocket()];
      var index = 0;

      final reverb = Reverb(
        host: 'localhost',
        port: 8080,
        appKey: 'key',
        useTls: false,
        socketFactory: (Uri _) => sockets[index++].channel,
      );

      // Both calls happen before any microtask runs, mirroring a
      // double-tapped connect button.
      reverb.connect();
      reverb.connect();
      async.flushMicrotasks();
      sockets[0].emitJson(handshakeFrame());
      async.flushMicrotasks();

      expect(index, 1);
      expect(reverb.state, ReverbState.connected);
    });
  });

  test(
      'onReconnected does not fire when the first connect needed retries, '
      'but does fire after a later drop', () {
    fakeAsync((async) {
      final sockets = <FakeSocket>[FakeSocket(), FakeSocket(), FakeSocket()];
      var index = 0;
      var reconnected = 0;

      final reverb = Reverb(
        host: 'localhost',
        port: 8080,
        appKey: 'key',
        useTls: false,
        socketFactory: (Uri _) => sockets[index++].channel,
      );

      reverb.onReconnected(() => reconnected++);

      reverb.connect();
      async.flushMicrotasks();
      // The first attempt dies before the handshake completes; the retry
      // below happens inside the same original connect() call, before the
      // client was ever connected.
      sockets[0].serverClose();
      async.elapse(const Duration(seconds: 2));
      sockets[1].emitJson(handshakeFrame(socketId: 'first'));
      async.flushMicrotasks();

      expect(reverb.state, ReverbState.connected);
      expect(reconnected, 0);

      sockets[1].serverClose();
      async.elapse(const Duration(seconds: 2));
      sockets[2].emitJson(handshakeFrame(socketId: 'second'));
      async.flushMicrotasks();

      expect(reconnected, 1);
    });
  });

  test(
      'disconnect() then connect() during a pending backoff never opens a '
      'second, redundant socket', () {
    fakeAsync((async) {
      // A third socket sitting unused proves the stale backoff timer did not
      // resume and take it: the review's repro measured exactly this, a
      // third socket taken by a backoff loop that should have been
      // superseded.
      final sockets = <FakeSocket>[FakeSocket(), FakeSocket(), FakeSocket()];
      var index = 0;
      var delivered = 0;

      final reverb = Reverb(
        host: 'localhost',
        port: 8080,
        appKey: 'key',
        useTls: false,
        socketFactory: (Uri _) => sockets[index++].channel,
      );

      reverb.connect();
      async.flushMicrotasks();
      expect(index, 1);

      // The first attempt dies before the handshake completes, so connect()
      // is now backing off (~1-1.25s) before it would normally retry.
      sockets[0].serverClose();
      async.flushMicrotasks();

      // Well within that backoff window — a lifecycle pause/resume under a
      // second, or a logout immediately followed by a login — disconnect()
      // flips _shouldRun false and then connect() flips it back true, both
      // before the original timer ever fires.
      unawaited(reverb.disconnect());
      reverb.connect();
      async.flushMicrotasks();
      expect(index, 2); // the explicit connect() took the second socket.

      reverb.channel('orders').listen('OrderCreated', (_) => delivered++);
      async.flushMicrotasks();

      sockets[1].emitJson(handshakeFrame(socketId: 'second'));
      async.flushMicrotasks();
      expect(reverb.state, ReverbState.connected);

      // Let the original backoff timer's delay fully elapse. Without a
      // generation guard, this resumes the original loop and takes a third
      // socket, leaving two live connections both wired to dispatch frames.
      async.elapse(const Duration(seconds: 2));
      expect(index, 2);

      sockets[1].emitJson(<String, dynamic>{
        'event': r'App\Events\OrderCreated',
        'channel': 'orders',
        'data': '{"id":1}',
      });
      async.flushMicrotasks();

      expect(delivered, 1);
    });
  });

  test(
      'disconnect() during a pending handshake completes the in-flight '
      'connect() instead of hanging forever', () {
    fakeAsync((async) {
      final socket = FakeSocket();
      final reverb = Reverb(
        host: 'localhost',
        port: 8080,
        appKey: 'key',
        useTls: false,
        socketFactory: (Uri _) => socket.channel,
      );

      var completed = false;
      unawaited(reverb.connect().then((_) => completed = true));
      async.flushMicrotasks();
      expect(completed, isFalse); // still mid-handshake, nothing delivered.

      unawaited(reverb.disconnect());
      // 120 simulated seconds is the exact figure from the review's repro:
      // without the fix, connect()'s future never resolves, so this would
      // simply time out the test rather than reach the assertions below.
      async.elapse(const Duration(seconds: 120));

      expect(completed, isTrue);
      expect(reverb.state, ReverbState.disconnected);
    });
  });

  test(
      'onReconnected does not fire when the reconnected socket drops again '
      'before resubscription finishes', () {
    fakeAsync((async) {
      final sockets = <FakeSocket>[FakeSocket(), FakeSocket(), FakeSocket()];
      var index = 0;
      var reconnected = 0;
      final secondAuth = Completer<ReverbAuth>();

      final reverb = Reverb(
        host: 'localhost',
        port: 8080,
        appKey: 'key',
        useTls: false,
        authorizer: (String channel, String socketId) {
          // The initial subscribe (on 'first') resolves immediately; only
          // the resubscribe attempt after reconnecting (on 'second') is held
          // open, so it is still in flight when that socket drops again.
          if (socketId == 'second') return secondAuth.future;
          return Future<ReverbAuth>.value(const ReverbAuth(auth: 'key:sig'));
        },
        socketFactory: (Uri _) => sockets[index++].channel,
      );

      reverb.onReconnected(() => reconnected++);

      reverb.connect();
      async.flushMicrotasks();
      sockets[0].emitJson(handshakeFrame(socketId: 'first'));
      async.flushMicrotasks();

      reverb.private('users.1').listen('A', (_) {});
      async.flushMicrotasks();

      // First drop: the ordinary reconnect path takes over.
      sockets[0].serverClose();
      async.elapse(const Duration(seconds: 2));
      sockets[1].emitJson(handshakeFrame(socketId: 'second'));
      async.flushMicrotasks();

      // The new socket dies again while the resubscribe authorizer for
      // 'second' is still in flight.
      sockets[1].serverClose();
      async.flushMicrotasks();

      // The authorizer finally resolves, but there is no live socket left
      // to subscribe on — _subscribe's own socket-id check bails out.
      secondAuth.complete(const ReverbAuth(auth: 'key:sig'));
      async.flushMicrotasks();

      expect(
        sockets[1].sentJson.where(
            (Map<String, dynamic> f) => f['event'] == 'pusher:subscribe'),
        isEmpty,
      );
      expect(reconnected, 0);
      expect(reverb.state, ReverbState.reconnecting);
    });
  });

  test(
      'a fatal error arriving mid-session settles to failed instead of '
      'retrying forever', () {
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
      socket.emitJson(handshakeFrame());
      async.flushMicrotasks();
      expect(reverb.state, ReverbState.connected);

      socket.emitJson(<String, dynamic>{
        'event': 'pusher:error',
        'data': '{"code":4001,"message":"Application does not exist"}',
      });
      async.elapse(const Duration(seconds: 60));

      expect(reverb.state, ReverbState.failed);
      expect(errors.single, isA<ReverbFatalError>());
      expect(created, 1);
    });
  });
}
