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
  ReverbFake({String namespace = r'App\Events'}) {
    _socket = InMemorySocket();
    _reverb = Reverb(
      host: 'localhost',
      port: 8080,
      appKey: 'fake',
      useTls: false,
      namespace: namespace,
      handleAppLifecycle: false,
      authorizer: (String channel, String socketId) async =>
          const ReverbAuth(auth: 'fake:signature'),
      socketFactory: factoryFor(_socket),
    );
  }

  late final InMemorySocket _socket;
  late final Reverb _reverb;

  /// The client under test. Wire your application to this.
  Reverb get reverb => _reverb;

  /// Every frame the client has sent, decoded.
  ///
  /// Assert against this to prove your code subscribed to what you expected,
  /// or whispered what you expected.
  List<Map<String, dynamic>> get sent => _socket.sentJson;

  /// Connects the client and completes the handshake.
  Future<void> connect() async {
    final connected = _reverb.connect();
    _socket.emitJson(handshakeFrame());
    await connected;
  }

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
    _socket.emitJson(<String, dynamic>{
      'event': event,
      'channel': channel,
      'data': data,
    });
  }

  /// Drops the socket from the server side.
  ///
  /// The client reconnects on its own backoff schedule, so a test that wants
  /// to observe the reconnect must wait it out.
  Future<void> drop() => _socket.serverClose();

  /// Releases the client. Call this at the end of every test.
  void dispose() => _reverb.dispose();
}
