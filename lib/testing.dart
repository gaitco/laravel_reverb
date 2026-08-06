/// Test helpers for applications built on `laravel_reverb`.
///
/// Import this from your own tests to exercise listeners without a running
/// Reverb server:
///
/// ```dart
/// final fake = ReverbFake();
/// await fake.connect();
/// fake.reverb.channel('orders').listen('OrderCreated', handler);
/// fake.emit('orders', r'App\Events\OrderCreated', {'id': 7});
/// ```
library;

import 'dart:async';

import 'package:stream_channel/stream_channel.dart';

import 'src/auth.dart';
import 'src/reverb.dart';
import 'src/testing/in_memory_socket.dart';

/// A real [Reverb] client wired to an in-memory socket you control.
///
/// The client is genuine — subscribe, dispatch and teardown all run the
/// production code paths. Only the socket is fake, so a test proves the same
/// thing a server would, without one.
class ReverbFake {
  /// Creates a fake and its client.
  ///
  /// [namespace] matches the `Reverb` constructor's, so an application that
  /// configures a custom event namespace can mirror it here.
  ///
  /// [onError] surfaces client-side failures the same way the `Reverb`
  /// constructor's `onError` does. Pass it in tests that need to notice a
  /// client-side error — without it, a failure the client handled internally
  /// (for example, a reconnect that cannot complete) is invisible: nothing
  /// throws, nothing fails the test, the listener under test simply never
  /// fires.
  ReverbFake({
    String namespace = r'App\Events',
    void Function(Object error, StackTrace? stackTrace)? onError,
  }) {
    _reverb = buildTestReverb(
      host: 'localhost',
      port: 8080,
      appKey: 'fake',
      useTls: false,
      namespace: namespace,
      handleAppLifecycle: false,
      authorizer: (String channel, String socketId) async =>
          const ReverbAuth(auth: 'fake:signature'),
      socketFactory: _connectSocket,
      onError: onError,
    );
  }

  late final Reverb _reverb;

  /// The socket for the connection attempt currently in flight or live, or
  /// null before the first one.
  InMemorySocket? _socket;

  /// Frames sent on every socket this fake has already moved on from, so
  /// [sent] can report everything ever sent rather than just the current
  /// socket's share of it.
  final List<Map<String, dynamic>> _archivedSent = <Map<String, dynamic>>[];

  /// The [SocketFactory] behind every connection attempt the client makes,
  /// including reconnects — `Connection` calls this itself on each retry, so
  /// a single fixed socket would hand a reconnect the same, already-closed
  /// instance the first attempt used. Each call mints a fresh socket instead
  /// and immediately answers its handshake, so the client comes back on its
  /// own after [drop] exactly as it would against a real server.
  ///
  /// The handshake frame is queued on the fresh socket before `Connection`
  /// has subscribed to it, which is safe: the underlying stream is
  /// single-subscription, so an event added before `listen()` is buffered
  /// and delivered the moment a listener attaches, rather than dropped.
  StreamChannel<dynamic> _connectSocket(Uri _) {
    final outgoing = _socket;
    if (outgoing != null) _archivedSent.addAll(outgoing.sentJson);

    final fresh = InMemorySocket();
    _socket = fresh;
    fresh.emitJson(handshakeFrame());
    return fresh.channel;
  }

  /// The client under test. Wire your application to this.
  Reverb get reverb => _reverb;

  /// Every frame the client has sent, decoded.
  ///
  /// Assert against this to prove your code subscribed to what you expected,
  /// or whispered what you expected. Accumulates across a [drop] and
  /// reconnect rather than resetting, so an assertion after a reconnect sees
  /// everything sent on every socket, not just the current one.
  List<Map<String, dynamic>> get sent => <Map<String, dynamic>>[
        ..._archivedSent,
        ...?_socket?.sentJson,
      ];

  /// Connects the client and completes the handshake.
  Future<void> connect() => _reverb.connect();

  /// Delivers [event] on [channel] as the server would.
  ///
  /// [channel] is the wire name, including any prefix — `'private-users.1'`,
  /// not `'users.1'`. [event] is also the wire name: an application listening
  /// for `'OrderCreated'` under the default namespace receives
  /// `r'App\Events\OrderCreated'`.
  void emit(
    String channel,
    String event, [
    Map<String, dynamic> data = const <String, dynamic>{},
  ]) {
    _socket!.emitJson(<String, dynamic>{
      'event': event,
      'channel': channel,
      'data': data,
    });
  }

  /// Drops the socket from the server side.
  ///
  /// The client reconnects on its own backoff schedule, re-answering the
  /// handshake automatically, so a test that wants to observe the reconnect
  /// need only wait it out.
  Future<void> drop() => _socket!.serverClose();

  /// Releases the client. Call this at the end of every test.
  void dispose() => _reverb.dispose();
}
