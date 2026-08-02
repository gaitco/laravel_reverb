import 'package:flutter/widgets.dart';
import 'package:laravel_reverb/src/reverb.dart';
import 'package:flutter_test/flutter_test.dart';

import 'support/fake_socket.dart';

/// A [Reverb] that counts how many times the binding actually dispatched
/// [didChangeAppLifecycleState] to it, so a double-registered observer (or a
/// registration that silently never happened) is observable even though
/// [connect] and [disconnect] are otherwise idempotent.
class _CountingReverb extends Reverb {
  _CountingReverb({
    required super.host,
    required super.appKey,
    super.port,
    super.useTls,
    super.socketFactory,
  });

  int dispatchCount = 0;

  @override
  void didChangeAppLifecycleState(AppLifecycleState state) {
    dispatchCount++;
    super.didChangeAppLifecycleState(state);
  }
}

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
    addTearDown(reverb.dispose);

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
    addTearDown(reverb.dispose);

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
    addTearDown(reverb.dispose);

    final connected = reverb.connect();
    socket.emitJson(handshakeFrame());
    await connected;

    reverb.didChangeAppLifecycleState(AppLifecycleState.paused);
    await Future<void>.delayed(Duration.zero);

    expect(reverb.state, ReverbState.connected);
  });

  test('a background/foreground cycle while failed does not resurrect it',
      () async {
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
    addTearDown(reverb.dispose);

    final connected = reverb.connect();
    socket.emitJson(<String, dynamic>{
      'event': 'pusher:error',
      'data': '{"code":4001,"message":"Application does not exist"}',
    });
    await connected;
    expect(reverb.state, ReverbState.failed);

    reverb.didChangeAppLifecycleState(AppLifecycleState.paused);
    await Future<void>.delayed(Duration.zero);
    reverb.didChangeAppLifecycleState(AppLifecycleState.resumed);
    await Future<void>.delayed(Duration.zero);

    expect(reverb.state, ReverbState.failed);
    expect(created, 1);
  });

  test('a binding-dispatched pause disconnects the real observer', () async {
    final socket = FakeSocket();
    final reverb = Reverb(
      host: 'localhost',
      port: 8080,
      appKey: 'key',
      useTls: false,
      socketFactory: (Uri _) => socket.channel,
    );
    addTearDown(reverb.dispose);

    final connected = reverb.connect();
    socket.emitJson(handshakeFrame());
    await connected;

    // Dispatch through the real binding rather than calling the method
    // directly, so this actually exercises connect()'s addObserver call
    // rather than assuming it happened.
    WidgetsBinding.instance
        .handleAppLifecycleStateChanged(AppLifecycleState.paused);
    await reverb.states.firstWhere((s) => s == ReverbState.disconnected);

    expect(reverb.state, ReverbState.disconnected);
    expect(socket.closed, isTrue);
  });

  test(
      'a second connect after a disconnect does not double-register, so one '
      'binding-dispatched event is one transition', () async {
    final sockets = <FakeSocket>[FakeSocket(), FakeSocket()];
    var index = 0;
    final reverb = _CountingReverb(
      host: 'localhost',
      port: 8080,
      appKey: 'key',
      useTls: false,
      socketFactory: (Uri _) => sockets[index++].channel,
    );
    addTearDown(reverb.dispose);

    // _observing only guards repeat registration across separate connect()
    // calls, not a double-tap before any microtask runs (that is _shouldRun's
    // job, already covered by reconnect_test.dart). So drive a full
    // connect -> disconnect -> connect cycle here, which is the case where a
    // dropped `!_observing` guard would actually add a second observer.
    final firstConnect = reverb.connect();
    sockets[0].emitJson(handshakeFrame());
    await firstConnect;

    await reverb.disconnect();

    final secondConnect = reverb.connect();
    sockets[1].emitJson(handshakeFrame());
    await secondConnect;

    WidgetsBinding.instance
        .handleAppLifecycleStateChanged(AppLifecycleState.paused);
    await reverb.states.firstWhere((s) => s == ReverbState.disconnected);

    // A double-registered observer would appear twice in WidgetsBinding's
    // observer list, so the binding would call didChangeAppLifecycleState on
    // this instance twice for one event.
    expect(reverb.dispatchCount, 1);
  });

  test('dispose removes the observer so a later binding event is a no-op',
      () async {
    final socket = FakeSocket();
    final reverb = _CountingReverb(
      host: 'localhost',
      port: 8080,
      appKey: 'key',
      useTls: false,
      socketFactory: (Uri _) => socket.channel,
    );

    final connected = reverb.connect();
    socket.emitJson(handshakeFrame());
    await connected;

    reverb.dispose();

    WidgetsBinding.instance
        .handleAppLifecycleStateChanged(AppLifecycleState.paused);
    await Future<void>.delayed(Duration.zero);

    expect(reverb.dispatchCount, 0);
  });
}
