import 'package:flutter/widgets.dart';
import 'package:flutter_reverb/src/reverb.dart';
import 'package:flutter_test/flutter_test.dart';

import 'support/fake_socket.dart';

void main() {
  TestWidgetsFlutterBinding.ensureInitialized();

  test('disconnects on pause and reconnects on resume', () async {
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

    reverb.didChangeAppLifecycleState(AppLifecycleState.paused);
    await Future<void>.delayed(Duration.zero);
    expect(reverb.state, ReverbState.disconnected);
    expect(sockets[0].closed, isTrue);

    reverb.didChangeAppLifecycleState(AppLifecycleState.resumed);
    await Future<void>.delayed(Duration.zero);
    sockets[1].emitJson(handshakeFrame());
    await Future<void>.delayed(Duration.zero);

    expect(reverb.state, ReverbState.connected);
    expect(index, 2);
  });

  test('does not reconnect on resume if it was never connected', () async {
    var index = 0;
    final reverb = Reverb(
      host: 'localhost',
      port: 8080,
      appKey: 'key',
      useTls: false,
      socketFactory: (Uri _) {
        index++;
        return FakeSocket().channel;
      },
    );

    reverb.didChangeAppLifecycleState(AppLifecycleState.resumed);
    await Future<void>.delayed(Duration.zero);

    expect(index, 0);
  });

  test('ignores lifecycle events when handling is disabled', () async {
    final socket = FakeSocket();
    final reverb = Reverb(
      host: 'localhost',
      port: 8080,
      appKey: 'key',
      useTls: false,
      handleAppLifecycle: false,
      socketFactory: (Uri _) => socket.channel,
    );

    final connected = reverb.connect();
    socket.emitJson(handshakeFrame());
    await connected;

    reverb.didChangeAppLifecycleState(AppLifecycleState.paused);
    await Future<void>.delayed(Duration.zero);

    expect(reverb.state, ReverbState.connected);
  });
}
