import 'dart:async';

import 'package:fake_async/fake_async.dart';
import 'package:flutter_reverb/src/connection.dart';
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
}
