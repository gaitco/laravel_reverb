import 'dart:async';
import 'dart:convert';

import 'package:fake_async/fake_async.dart';
import 'package:laravel_reverb/src/connection.dart';
import 'package:flutter_test/flutter_test.dart';

import 'support/fake_socket.dart';

Connection connectionFor(FakeSocket socket) => Connection(
      url: Uri.parse('ws://localhost:8080/app/key'),
      socketFactory: factoryFor(socket),
    );

void main() {
  test('open completes with the socket id from the handshake', () async {
    final socket = FakeSocket();
    final connection = connectionFor(socket);

    final opened = connection.open();
    socket.emitJson(handshakeFrame(socketId: '999.111'));

    expect(await opened, '999.111');
    expect(connection.socketId, '999.111');
  });

  test('application frames are forwarded to listeners', () async {
    final socket = FakeSocket();
    final connection = connectionFor(socket);
    final received = <ReverbFrame>[];
    connection.frames.listen(received.add);

    final opened = connection.open();
    socket.emitJson(handshakeFrame());
    await opened;

    socket.emitJson(<String, dynamic>{
      'event': r'App\Events\OrderCreated',
      'channel': 'private-users.1',
      'data': '{"id":7}',
    });
    await Future<void>.delayed(Duration.zero);

    expect(received.single.event, r'App\Events\OrderCreated');
    expect(received.single.data, <String, dynamic>{'id': 7});
  });

  test('handshake and keepalive frames are not forwarded as app frames',
      () async {
    final socket = FakeSocket();
    final connection = connectionFor(socket);
    final received = <ReverbFrame>[];
    connection.frames.listen(received.add);

    final opened = connection.open();
    socket.emitJson(handshakeFrame());
    await opened;

    socket.emitJson(<String, dynamic>{'event': 'pusher:ping', 'data': '{}'});
    await Future<void>.delayed(Duration.zero);

    expect(received, isEmpty);
  });

  test('replies to a server ping with a pong', () async {
    final socket = FakeSocket();
    final connection = connectionFor(socket);

    final opened = connection.open();
    socket.emitJson(handshakeFrame());
    await opened;

    socket.emitJson(<String, dynamic>{'event': 'pusher:ping', 'data': '{}'});
    await Future<void>.delayed(Duration.zero);

    expect(
      socket.sentJson.last,
      <String, dynamic>{'event': 'pusher:pong', 'data': <String, dynamic>{}},
    );
  });

  test('sends its own ping once the activity timeout elapses', () {
    fakeAsync((async) {
      final socket = FakeSocket();
      final connection = connectionFor(socket);

      connection.open();
      socket.emitJson(handshakeFrame(activityTimeout: 30));
      async.flushMicrotasks();

      async.elapse(const Duration(seconds: 31));

      expect(socket.sentJson.last['event'], 'pusher:ping');
    });
  });

  test('closes the socket when a ping goes unanswered', () {
    fakeAsync((async) {
      final socket = FakeSocket();
      final connection = connectionFor(socket);

      connection.open();
      socket.emitJson(handshakeFrame(activityTimeout: 30));
      async.flushMicrotasks();

      async.elapse(const Duration(seconds: 31));
      async.elapse(const Duration(seconds: 31));
      async.flushMicrotasks();

      expect(socket.closed, isTrue);
    });
  });

  test('open fails with ReverbFatalError on a 4000 series error', () async {
    final socket = FakeSocket();
    final connection = connectionFor(socket);

    final opened = connection.open();
    socket.emitJson(<String, dynamic>{
      'event': 'pusher:error',
      'data': '{"code":4001,"message":"Application does not exist"}',
    });

    await expectLater(opened, throwsA(isA<ReverbFatalError>()));
  });

  test('open fails when the server closes before the handshake', () async {
    final socket = FakeSocket();
    final connection = connectionFor(socket);

    final opened = connection.open();
    await socket.serverClose();

    await expectLater(opened, throwsA(isA<Exception>()));
  });

  test('closed completes when the server drops the socket', () async {
    final socket = FakeSocket();
    final connection = connectionFor(socket);

    final opened = connection.open();
    socket.emitJson(handshakeFrame());
    await opened;

    var done = false;
    unawaited(connection.closed.then((_) => done = true));
    await socket.serverClose();
    await Future<void>.delayed(Duration.zero);

    expect(done, isTrue);
  });

  test('send writes an encoded frame to the socket', () async {
    final socket = FakeSocket();
    final connection = connectionFor(socket);

    final opened = connection.open();
    socket.emitJson(handshakeFrame());
    await opened;

    connection.send(<String, dynamic>{
      'event': 'pusher:subscribe',
      'data': <String, dynamic>{'channel': 'orders'},
    });

    expect(socket.sentJson.last['event'], 'pusher:subscribe');
  });

  test('a stream error fails open and completes closed', () async {
    final socket = FakeSocket();
    final connection = connectionFor(socket);

    final opened = connection.open();
    var done = false;
    unawaited(connection.closed.then((_) => done = true));

    socket.emitError(Exception('boom'));

    await expectLater(opened, throwsA(isA<Exception>()));
    await Future<void>.delayed(Duration.zero);
    expect(done, isTrue);
  });

  test('a frame arriving after close is ignored and does not re-arm timers',
      () {
    fakeAsync((async) {
      final socket = FakeSocket();
      final connection = connectionFor(socket);

      connection.open();
      socket.emitJson(handshakeFrame());
      async.flushMicrotasks();

      unawaited(connection.close());
      async.flushMicrotasks();

      expect(
        () => socket.emitJson(<String, dynamic>{
          'event': r'App\Events\OrderCreated',
          'data': '{}',
        }),
        returnsNormally,
      );
      async.flushMicrotasks();

      expect(async.pendingTimers, isEmpty);
    });
  });

  test(
      'close fails an in-flight handshake instead of leaving it pending '
      'forever', () async {
    final socket = FakeSocket();
    final connection = connectionFor(socket);

    final opened = connection.open();
    unawaited(connection.close());

    await expectLater(opened, throwsA(isA<ReverbConnectionClosed>()));
  });

  test('a handshake missing socket_id tears the connection down', () {
    fakeAsync((async) {
      final socket = FakeSocket();
      final connection = connectionFor(socket);

      final opened = connection.open();
      unawaited(expectLater(opened, throwsA(isA<Exception>())));

      socket.emitJson(<String, dynamic>{
        'event': 'pusher:connection_established',
        'data': jsonEncode(<String, dynamic>{'activity_timeout': 30}),
      });
      async.flushMicrotasks();

      expect(async.pendingTimers, isEmpty);
    });
  });

  test('pings on the configured interval regardless of server activity', () {
    fakeAsync((async) {
      final socket = FakeSocket();
      final connection = Connection(
        url: Uri.parse('ws://localhost:8080/app/key'),
        socketFactory: factoryFor(socket),
        pingInterval: const Duration(seconds: 10),
        // Deliberately far outside the 31s window elapsed below: this test's
        // subject is the ping loop, not the watchdog (that has its own tests
        // further down). A watchdogTimeout of 15s — the value literally in
        // the task brief — is shorter than 31s, and since this fake server
        // never sends anything back, the watchdog would close the socket at
        // t=15s and cancel the ping timer with it, so only 1 of the 3
        // expected pings would ever go out. See task-1-report.md for detail.
        watchdogTimeout: const Duration(seconds: 100),
      );

      connection.open();
      socket.emitJson(handshakeFrame(activityTimeout: 120));
      async.flushMicrotasks();

      async.elapse(const Duration(seconds: 31));

      final pings = socket.sentJson
          .where((Map<String, dynamic> f) => f['event'] == 'pusher:ping')
          .length;
      expect(pings, 3);
    });
  });

  test('the watchdog closes a silent socket', () {
    fakeAsync((async) {
      final socket = FakeSocket();
      final connection = Connection(
        url: Uri.parse('ws://localhost:8080/app/key'),
        socketFactory: factoryFor(socket),
        watchdogTimeout: const Duration(seconds: 15),
      );

      connection.open();
      socket.emitJson(handshakeFrame(activityTimeout: 120));
      async.flushMicrotasks();

      async.elapse(const Duration(seconds: 16));
      async.flushMicrotasks();

      expect(socket.closed, isTrue);
    });
  });

  test('any inbound frame defers the watchdog', () {
    fakeAsync((async) {
      final socket = FakeSocket();
      final connection = Connection(
        url: Uri.parse('ws://localhost:8080/app/key'),
        socketFactory: factoryFor(socket),
        watchdogTimeout: const Duration(seconds: 15),
      );

      connection.open();
      socket.emitJson(handshakeFrame(activityTimeout: 120));
      async.flushMicrotasks();

      // Three 10s gaps, each broken by a frame, must not trip a 15s watchdog.
      for (var i = 0; i < 3; i++) {
        async.elapse(const Duration(seconds: 10));
        socket
            .emitJson(<String, dynamic>{'event': 'pusher:pong', 'data': '{}'});
        async.flushMicrotasks();
      }

      expect(socket.closed, isFalse);

      async.elapse(const Duration(seconds: 16));
      async.flushMicrotasks();
      expect(socket.closed, isTrue);
    });
  });

  test('defaults reproduce the 0.2.0 server-driven timing', () {
    fakeAsync((async) {
      final socket = FakeSocket();
      final connection = Connection(
        url: Uri.parse('ws://localhost:8080/app/key'),
        socketFactory: factoryFor(socket),
      );

      connection.open();
      socket.emitJson(handshakeFrame(activityTimeout: 30));
      async.flushMicrotasks();

      // No ping before the activity timeout elapses.
      async.elapse(const Duration(seconds: 29));
      expect(
        socket.sentJson
            .where((Map<String, dynamic> f) => f['event'] == 'pusher:ping'),
        isEmpty,
      );

      async.elapse(const Duration(seconds: 2));
      expect(socket.sentJson.last['event'], 'pusher:ping');

      // And the socket dies one more activity timeout later, not sooner.
      async.elapse(const Duration(seconds: 31));
      async.flushMicrotasks();
      expect(socket.closed, isTrue);
    });
  });

  test(
      'pingInterval alone still detects a dead socket instead of pinging '
      'forever', () {
    fakeAsync((async) {
      final socket = FakeSocket();
      final connection = Connection(
        url: Uri.parse('ws://localhost:8080/app/key'),
        socketFactory: factoryFor(socket),
        pingInterval: const Duration(seconds: 10),
      );

      connection.open();
      // 25s, not a multiple of the 10s ping interval, so the pong deadline
      // armed by the first ping (at t=10, firing at t=35) never lands on the
      // same tick as a scheduled ping — keeps the assertions below exact.
      socket.emitJson(handshakeFrame(activityTimeout: 25));
      async.flushMicrotasks();

      // The socket never replies to anything. Pings go out on schedule, but
      // each one after the first must find a deadline already ticking and
      // decline to push it back out — otherwise this socket would be pinged
      // forever with no death detection at all.
      async.elapse(const Duration(seconds: 34));
      expect(socket.closed, isFalse);
      expect(
        socket.sentJson
            .where((Map<String, dynamic> f) => f['event'] == 'pusher:ping')
            .length,
        3, // t=10, 20, 30
      );

      async.elapse(const Duration(seconds: 2));
      async.flushMicrotasks();
      expect(socket.closed, isTrue);
    });
  });

  test(
      'the primary configuration keeps a healthy socket alive and closes a '
      'truly dead one', () {
    fakeAsync((async) {
      final socket = FakeSocket();
      final connection = Connection(
        url: Uri.parse('ws://localhost:8080/app/key'),
        socketFactory: factoryFor(socket),
        pingInterval: const Duration(seconds: 10),
        watchdogTimeout: const Duration(seconds: 15),
      );

      connection.open();
      socket.emitJson(handshakeFrame(activityTimeout: 120));
      async.flushMicrotasks();

      // A real Reverb server echoes every ping with a pong. Six round trips
      // comfortably outlast the 15s watchdog, proving the periodic ping plus
      // the inbound pong keep resetting it indefinitely — not just in
      // isolation (the ping-loop and watchdog tests above), but together.
      for (var i = 0; i < 6; i++) {
        async.elapse(const Duration(seconds: 10));
        async.flushMicrotasks();
        expect(socket.sentJson.last['event'], 'pusher:ping');
        socket
            .emitJson(<String, dynamic>{'event': 'pusher:pong', 'data': '{}'});
        async.flushMicrotasks();
      }

      expect(socket.closed, isFalse);
      expect(
        socket.sentJson
            .where((Map<String, dynamic> f) => f['event'] == 'pusher:ping')
            .length,
        6,
      );

      // The server goes dark. The watchdog, not the ping loop, notices.
      async.elapse(const Duration(seconds: 16));
      async.flushMicrotasks();
      expect(socket.closed, isTrue);
    });
  });
}
