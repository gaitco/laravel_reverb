import 'package:flutter_test/flutter_test.dart';
import 'package:laravel_reverb/laravel_reverb.dart';
import 'package:laravel_reverb/src/connection.dart';

import 'support/fake_socket.dart';

void main() {
  TestWidgetsFlutterBinding.ensureInitialized();

  test('a fresh client reports no latency and no reconnects', () {
    const metrics = ReverbMetrics();

    expect(metrics.lastLatency, isNull);
    expect(metrics.reconnectCount, 0);
    expect(metrics.sinceLastFrame, isNull);
    expect(metrics.connectedSince, isNull);
  });

  test('records the round trip between a ping and its pong', () async {
    final socket = FakeSocket();
    var now = DateTime(2026);

    final connection = Connection(
      url: Uri.parse('ws://localhost:8080/app/key'),
      socketFactory: factoryFor(socket),
      pingInterval: const Duration(seconds: 5),
      watchdogTimeout: const Duration(seconds: 20),
      now: () => now,
    );

    final opened = connection.open();
    socket.emitJson(handshakeFrame());
    await opened;

    expect(connection.lastLatency, isNull);

    connection.sendPingForTest();
    now = now.add(const Duration(milliseconds: 40));
    socket.emitJson(<String, dynamic>{'event': 'pusher:pong'});
    await Future<void>.delayed(Duration.zero);

    expect(connection.lastLatency, const Duration(milliseconds: 40));

    await connection.close();
  });

  test('counts a restored socket as one reconnect', () async {
    final sockets = <FakeSocket>[FakeSocket(), FakeSocket()];
    var index = 0;
    var now = DateTime(2026);

    final reverb = Reverb(
      host: 'localhost',
      port: 8080,
      appKey: 'key',
      useTls: false,
      socketFactory: (Uri _) => sockets[index++].channel,
      now: () => now,
    );

    final connected = reverb.connect();
    sockets[0].emitJson(handshakeFrame());
    await connected;

    expect(reverb.metrics.reconnectCount, 0);
    expect(reverb.metrics.connectedSince, DateTime(2026));

    now = now.add(const Duration(seconds: 1));
    // Subscribed before the drop, not after: reverb.states is a broadcast
    // stream that does not replay, so listening after sockets[0].serverClose()
    // could miss the reconnecting transition if it fires first and hang this
    // await for the rest of the test's timeout.
    final reconnecting = reverb.states.firstWhere(
      (state) => state == ReverbState.reconnecting,
    );
    await sockets[0].serverClose();
    await reconnecting.timeout(const Duration(seconds: 5));
    // sockets[1]'s stream is single-subscription: this is buffered and
    // delivered as soon as the retry's Connection subscribes to it, whenever
    // that backoff timer fires — no need to wait for it first.
    sockets[1].emitJson(handshakeFrame());

    // The real (unseeded, by design — see this test's history) backoff can
    // take up to ~1250ms for the first attempt, so poll rather than sleep a
    // fixed amount.
    final deadline = DateTime.now().add(const Duration(seconds: 5));
    while (reverb.metrics.reconnectCount == 0 &&
        DateTime.now().isBefore(deadline)) {
      await Future<void>.delayed(const Duration(milliseconds: 20));
    }

    expect(reverb.metrics.reconnectCount, 1);

    reverb.dispose();
  });

  test('sinceLastFrame reflects the clock advancing since the last frame',
      () async {
    final socket = FakeSocket();
    var now = DateTime(2026);

    final reverb = Reverb(
      host: 'localhost',
      port: 8080,
      appKey: 'key',
      useTls: false,
      socketFactory: factoryFor(socket),
      now: () => now,
    );

    expect(reverb.metrics.sinceLastFrame, isNull);

    final connected = reverb.connect();
    socket.emitJson(handshakeFrame());
    await connected;

    expect(reverb.metrics.sinceLastFrame, Duration.zero);

    now = now.add(const Duration(seconds: 45));

    expect(reverb.metrics.sinceLastFrame, const Duration(seconds: 45));

    reverb.dispose();
  });
}
