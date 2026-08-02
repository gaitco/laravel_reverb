# flutter_reverb Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `flutter_reverb`, a pure-Dart Laravel Reverb client for Flutter with a Laravel Echo-style API, auto-reconnect, presence channels, and client events.

**Architecture:** A `Connection` speaks the Pusher wire protocol over an injectable `StreamChannel`; a `Reverb` facade owns the channel registry, authorization, and the reconnect policy; `Channel` objects own their listeners and ref-count themselves so a channel unsubscribes only when its last listener is gone. All pure wire-format logic lives in `protocol.dart` and is tested without any socket.

**Tech Stack:** Dart/Flutter, `web_socket_channel`, `stream_channel`, `http`. Tests use `flutter_test` plus `fake_async` for timer-driven paths.

## Global Constraints

- Package name `flutter_reverb`, version `0.1.0`, MIT licence, published publicly to pub.dev.
- Dependencies are exactly: `flutter`, `http`, `stream_channel`, `web_socket_channel`. Do not add others.
- Dev dependencies are exactly: `flutter_test`, `flutter_lints`, `fake_async`.
- Dart SDK `^3.5.0`, Flutter `>=3.24.0`.
- `analysis_options.yaml` is `flutter_lints` and nothing else. Every public member in `lib/` still gets a `///` doc comment — enforced by review, not by a lint, because `public_member_api_docs` and `unawaited_futures` both fire on test code where neither is wanted.
- No public API throws into application code at runtime. Programming errors (whisper on a public channel, private channel with no authorizer configured) throw immediately at the call site; runtime conditions (auth failure, socket error) are reported through `onError`.
- Channel names given by callers are bare (`'users.1'`). The package adds `private-` / `presence-` prefixes.
- Event names: no leading `.` means namespaced under `App\Events`; a leading `.` means a literal `broadcastAs()` name.
- Commit after every task using the message given in that task's final step.
- After every task, `dart format .`, `flutter analyze` (zero issues), and `flutter test` (all pass) must succeed. Never mark a task done otherwise.

---

### Task 1: Package scaffold and protocol primitives

Creates the package and the pure wire-format functions everything else builds on. Scaffolding lives here because the protocol tests are the first thing that needs a runnable package.

**Files:**
- Create: `pubspec.yaml`
- Create: `analysis_options.yaml`
- Create: `.gitignore`
- Create: `LICENSE`
- Create: `CHANGELOG.md`
- Create: `lib/src/protocol.dart`
- Test: `test/protocol_test.dart`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `class ReverbFrame { final String event; final String? channel; final Map<String, dynamic> data; static ReverbFrame? parse(String raw); }`
  - `Map<String, dynamic> decodeData(Object? raw)`
  - `Uri buildSocketUrl({required String host, required int port, required String appKey, required bool useTls, required String clientVersion})`
  - `Duration backoffDelay(int attempt, math.Random random)`
  - `String resolveEventName(String name, String namespace)`
  - `bool isFatalErrorCode(int code)`

- [ ] **Step 1: Create the package metadata files**

`pubspec.yaml`:

```yaml
name: flutter_reverb
description: Laravel Reverb realtime client for Flutter. Echo-style API, pure Dart Pusher protocol, auto-reconnect, presence channels and client events.
version: 0.1.0
repository: https://github.com/abdullahghanem/flutter_reverb

environment:
  sdk: ^3.5.0
  flutter: ">=3.24.0"

dependencies:
  flutter:
    sdk: flutter
  http: ^1.2.0
  stream_channel: ^2.1.2
  web_socket_channel: ^3.0.0

dev_dependencies:
  flutter_test:
    sdk: flutter
  flutter_lints: ^5.0.0
  fake_async: ^1.3.1
```

`analysis_options.yaml`:

```yaml
include: package:flutter_lints/flutter.yaml
```

`.gitignore`:

```
.dart_tool/
.packages
build/
pubspec.lock
.flutter-plugins
.flutter-plugins-dependencies
```

`CHANGELOG.md`:

```markdown
## 0.1.0

- Initial release.
```

`LICENSE`: the standard MIT licence text, `Copyright (c) 2026 Abdullah Ghanem`.

- [ ] **Step 2: Write the failing tests**

`test/protocol_test.dart`:

```dart
import 'dart:math' as math;

import 'package:flutter_reverb/src/protocol.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('decodeData', () {
    test('decodes the double-encoded string Pusher actually sends', () {
      expect(decodeData('{"id":5,"name":"a"}'), {'id': 5, 'name': 'a'});
    });

    test('accepts a payload that is already a map', () {
      expect(decodeData(<String, dynamic>{'id': 5}), {'id': 5});
    });

    test('returns an empty map for a null payload', () {
      expect(decodeData(null), isEmpty);
    });

    test('wraps a non-object payload under the data key', () {
      expect(decodeData('"hello"'), {'data': 'hello'});
      expect(decodeData('not json'), {'data': 'not json'});
    });
  });

  group('ReverbFrame.parse', () {
    test('parses event, channel and double-encoded data', () {
      final frame = ReverbFrame.parse(
        '{"event":"App\\\\Events\\\\OrderCreated",'
        '"channel":"private-users.1","data":"{\\"id\\":7}"}',
      );

      expect(frame!.event, r'App\Events\OrderCreated');
      expect(frame.channel, 'private-users.1');
      expect(frame.data, {'id': 7});
    });

    test('leaves channel null for connection-level frames', () {
      final frame = ReverbFrame.parse('{"event":"pusher:ping","data":"{}"}');

      expect(frame!.event, 'pusher:ping');
      expect(frame.channel, isNull);
      expect(frame.data, isEmpty);
    });

    test('returns null for text that is not a frame', () {
      expect(ReverbFrame.parse('garbage'), isNull);
      expect(ReverbFrame.parse('{"no":"event"}'), isNull);
    });
  });

  group('buildSocketUrl', () {
    test('builds a wss url with the protocol query parameters', () {
      final url = buildSocketUrl(
        host: 'api.example.com',
        port: 443,
        appKey: 'abc',
        useTls: true,
        clientVersion: '0.1.0',
      );

      expect(url.scheme, 'wss');
      expect(url.path, '/app/abc');
      expect(url.queryParameters['protocol'], '7');
      expect(url.queryParameters['client'], 'flutter');
      expect(url.queryParameters['version'], '0.1.0');
    });

    test('uses ws when tls is disabled', () {
      final url = buildSocketUrl(
        host: 'localhost',
        port: 8080,
        appKey: 'abc',
        useTls: false,
        clientVersion: '0.1.0',
      );

      expect(url.scheme, 'ws');
      expect(url.port, 8080);
    });
  });

  group('backoffDelay', () {
    test('doubles per attempt and caps at 30 seconds', () {
      final zero = _ZeroRandom();

      expect(backoffDelay(0, zero).inMilliseconds, 1000);
      expect(backoffDelay(1, zero).inMilliseconds, 2000);
      expect(backoffDelay(2, zero).inMilliseconds, 4000);
      expect(backoffDelay(3, zero).inMilliseconds, 8000);
      expect(backoffDelay(4, zero).inMilliseconds, 16000);
      expect(backoffDelay(5, zero).inMilliseconds, 30000);
      expect(backoffDelay(99, zero).inMilliseconds, 30000);
    });

    test('adds at most 25 percent jitter', () {
      final random = math.Random(1);

      for (var attempt = 0; attempt < 8; attempt++) {
        final base = backoffDelay(attempt, _ZeroRandom()).inMilliseconds;
        final jittered = backoffDelay(attempt, random).inMilliseconds;

        expect(jittered, greaterThanOrEqualTo(base));
        expect(jittered, lessThanOrEqualTo((base * 1.25).round()));
      }
    });
  });

  group('resolveEventName', () {
    test('namespaces a bare event name', () {
      expect(
        resolveEventName('OrderCreated', r'App\Events'),
        r'App\Events\OrderCreated',
      );
    });

    test('treats a leading dot as a literal broadcastAs name', () {
      expect(resolveEventName('.order.created', r'App\Events'), 'order.created');
    });

    test('treats a leading backslash as a fully qualified name', () {
      expect(
        resolveEventName(r'\Domain\Events\Paid', r'App\Events'),
        r'Domain\Events\Paid',
      );
    });

    test('leaves the name alone when the namespace is empty', () {
      expect(resolveEventName('OrderCreated', ''), 'OrderCreated');
    });
  });

  group('isFatalErrorCode', () {
    test('treats 4000 series as fatal and others as retryable', () {
      expect(isFatalErrorCode(4001), isTrue);
      expect(isFatalErrorCode(4099), isTrue);
      expect(isFatalErrorCode(4100), isFalse);
      expect(isFatalErrorCode(4200), isFalse);
    });
  });
}

class _ZeroRandom implements math.Random {
  @override
  bool nextBool() => false;
  @override
  double nextDouble() => 0;
  @override
  int nextInt(int max) => 0;
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `flutter pub get && flutter test test/protocol_test.dart`
Expected: FAIL — `Target of URI doesn't exist: 'package:flutter_reverb/src/protocol.dart'`

- [ ] **Step 4: Write the implementation**

`lib/src/protocol.dart`:

```dart
import 'dart:convert';
import 'dart:math' as math;

/// A single decoded frame received from a Reverb server.
class ReverbFrame {
  /// Creates a frame.
  const ReverbFrame({
    required this.event,
    required this.data,
    this.channel,
  });

  /// The wire event name, such as `pusher:ping` or `App\Events\OrderCreated`.
  final String event;

  /// The wire channel name, or null for connection-level frames.
  final String? channel;

  /// The decoded payload. Non-object payloads are wrapped under a `data` key.
  final Map<String, dynamic> data;

  /// Parses a raw text frame, returning null if it is not a usable frame.
  static ReverbFrame? parse(String raw) {
    final decoded = _tryDecode(raw);
    if (decoded is! Map) return null;

    final event = decoded['event'];
    if (event is! String) return null;

    final channel = decoded['channel'];
    return ReverbFrame(
      event: event,
      channel: channel is String ? channel : null,
      data: decodeData(decoded['data']),
    );
  }
}

/// Decodes Pusher's `data` field, which arrives as a JSON string containing
/// JSON rather than as a nested object.
///
/// Already-decoded maps pass through unchanged. Anything that is neither a map
/// nor a JSON object string is wrapped as `{'data': value}` so callers always
/// receive a map.
Map<String, dynamic> decodeData(Object? raw) {
  if (raw == null) return const <String, dynamic>{};
  if (raw is Map) return raw.cast<String, dynamic>();
  if (raw is String) {
    final decoded = _tryDecode(raw);
    if (decoded is Map) return decoded.cast<String, dynamic>();
    return <String, dynamic>{'data': decoded ?? raw};
  }
  return <String, dynamic>{'data': raw};
}

/// Builds the Reverb socket URL for [appKey].
Uri buildSocketUrl({
  required String host,
  required int port,
  required String appKey,
  required bool useTls,
  required String clientVersion,
}) {
  return Uri(
    scheme: useTls ? 'wss' : 'ws',
    host: host,
    port: port,
    path: '/app/$appKey',
    queryParameters: <String, String>{
      'protocol': '7',
      'client': 'flutter',
      'version': clientVersion,
    },
  );
}

/// The delay before reconnect attempt [attempt] (zero-based).
///
/// Doubles from one second, caps at thirty, and adds up to 25 percent jitter so
/// that clients dropped by the same server outage do not reconnect in lockstep.
Duration backoffDelay(int attempt, math.Random random) {
  final seconds = math.min(1 << attempt.clamp(0, 5), 30);
  return Duration(
    milliseconds: seconds * 1000 + random.nextInt(seconds * 250 + 1),
  );
}

/// Resolves an Echo-style event name to the name that appears on the wire.
///
/// A bare name is namespaced (`OrderCreated` becomes `App\Events\OrderCreated`).
/// A leading `.` marks a literal `broadcastAs()` name, and a leading `\` marks a
/// fully qualified class name; both are returned with the marker stripped.
String resolveEventName(String name, String namespace) {
  if (name.startsWith('.') || name.startsWith(r'\')) return name.substring(1);
  if (namespace.isEmpty) return name;
  return '$namespace\\$name';
}

/// Whether a `pusher:error` [code] means the client must stop reconnecting.
///
/// The 4000 series covers unrecoverable conditions such as an unknown app key
/// or unsupported protocol version; retrying those in a loop only hammers the
/// server. Higher codes are transient and are retried with backoff.
bool isFatalErrorCode(int code) => code >= 4000 && code < 4100;

Object? _tryDecode(String raw) {
  try {
    return jsonDecode(raw);
  } on FormatException {
    return null;
  }
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `dart format . && flutter analyze && flutter test`
Expected: format clean, `No issues found!`, all protocol tests PASS

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat: package scaffold and Pusher protocol primitives"
```

---

### Task 2: Connection — socket lifecycle, handshake and keepalive

**Files:**
- Create: `lib/src/connection.dart`
- Create: `test/support/fake_socket.dart`
- Test: `test/connection_test.dart`

**Interfaces:**
- Consumes: `ReverbFrame`, `decodeData`, `isFatalErrorCode` from `lib/src/protocol.dart`.
- Produces:
  - `typedef SocketFactory = StreamChannel<dynamic> Function(Uri url)`
  - `class ReverbFatalError implements Exception { final int code; final String message; }`
  - `class Connection { Connection({required Uri url, required SocketFactory socketFactory, void Function(String)? onLog}); String? get socketId; Stream<ReverbFrame> get frames; Future<void> get closed; Future<String> open(); void send(Map<String, dynamic> message); Future<void> close(); }`
- Test helpers produced: `class FakeSocket { StreamChannel<dynamic> get channel; List<Map<String, dynamic>> get sentJson; bool closed; void emitJson(Map<String, dynamic> frame); void emitRaw(String raw); Future<void> serverClose(); }` and `SocketFactory factoryFor(FakeSocket socket)` and `Map<String, dynamic> handshakeFrame({String socketId, int activityTimeout})`.

- [ ] **Step 1: Write the fake socket test helper**

`test/support/fake_socket.dart`:

```dart
import 'dart:convert';

import 'package:flutter_reverb/src/connection.dart';
import 'package:stream_channel/stream_channel.dart';

/// A controllable stand-in for a WebSocket, used to drive [Connection] in tests.
///
/// The package under test talks to [channel]; the test drives the other end.
class FakeSocket {
  /// Creates a connected fake socket.
  FakeSocket() {
    _controller = StreamChannelController<dynamic>(sync: true);
    _controller.foreign.stream.listen(
      _sent.add,
      onDone: () => closed = true,
    );
  }

  late final StreamChannelController<dynamic> _controller;
  final List<dynamic> _sent = <dynamic>[];

  /// Whether the package closed its side of the socket.
  bool closed = false;

  /// The end of the socket handed to the package under test.
  StreamChannel<dynamic> get channel => _controller.local;

  /// Everything the package has sent, decoded from JSON.
  List<Map<String, dynamic>> get sentJson => _sent
      .map((dynamic e) => jsonDecode(e as String) as Map<String, dynamic>)
      .toList();

  /// Delivers [frame] to the package as a server message.
  void emitJson(Map<String, dynamic> frame) => emitRaw(jsonEncode(frame));

  /// Delivers raw text to the package as a server message.
  void emitRaw(String raw) => _controller.foreign.sink.add(raw);

  /// Closes the socket from the server side.
  Future<void> serverClose() => _controller.foreign.sink.close();
}

/// A [SocketFactory] that always returns [socket].
SocketFactory factoryFor(FakeSocket socket) =>
    (Uri _) => socket.channel;

/// The handshake frame a Reverb server sends immediately after connecting.
Map<String, dynamic> handshakeFrame({
  String socketId = '123.456',
  int activityTimeout = 30,
}) {
  return <String, dynamic>{
    'event': 'pusher:connection_established',
    'data': jsonEncode(<String, dynamic>{
      'socket_id': socketId,
      'activity_timeout': activityTimeout,
    }),
  };
}
```

- [ ] **Step 2: Write the failing tests**

`test/connection_test.dart`:

```dart
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
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `flutter test test/connection_test.dart`
Expected: FAIL — `Target of URI doesn't exist: 'package:flutter_reverb/src/connection.dart'`

- [ ] **Step 4: Write the implementation**

`lib/src/connection.dart`:

```dart
import 'dart:async';
import 'dart:convert';

import 'package:stream_channel/stream_channel.dart';

import 'protocol.dart';

export 'protocol.dart' show ReverbFrame;

/// Opens a socket to [url].
///
/// Typed as [StreamChannel] rather than `WebSocketChannel` so that tests can
/// substitute a controllable channel without implementing a WebSocket.
typedef SocketFactory = StreamChannel<dynamic> Function(Uri url);

/// An error that must not be retried, such as an unknown application key.
class ReverbFatalError implements Exception {
  /// Creates a fatal error.
  const ReverbFatalError(this.code, this.message);

  /// The `pusher:error` code.
  final int code;

  /// The human readable message from the server.
  final String message;

  @override
  String toString() => 'ReverbFatalError($code): $message';
}

/// Raised when the socket closes before the handshake completes.
class ReverbConnectionClosed implements Exception {
  /// Creates the exception.
  const ReverbConnectionClosed();

  @override
  String toString() => 'ReverbConnectionClosed: socket closed before handshake';
}

/// One socket to a Reverb server, speaking the Pusher wire protocol.
///
/// A [Connection] handles the handshake and keepalive only. It knows nothing
/// about channels, authorization or reconnection; those belong to `Reverb`.
class Connection {
  /// Creates a connection to [url]. Call [open] to actually connect.
  Connection({
    required this.url,
    required this.socketFactory,
    this.onLog,
  });

  /// The socket URL, including the protocol query parameters.
  final Uri url;

  /// Creates the underlying socket.
  final SocketFactory socketFactory;

  /// Optional log sink, so the host application controls logging.
  final void Function(String message)? onLog;

  final StreamController<ReverbFrame> _frames =
      StreamController<ReverbFrame>.broadcast();
  final Completer<void> _closed = Completer<void>();

  StreamChannel<dynamic>? _socket;
  Completer<String>? _handshake;
  Timer? _idleTimer;
  Timer? _pongTimer;
  Duration _activityTimeout = const Duration(seconds: 120);

  /// The socket id assigned by the server, or null before the handshake.
  String? socketId;

  /// Application-level frames. Handshake and keepalive frames are consumed
  /// internally and never appear here.
  Stream<ReverbFrame> get frames => _frames.stream;

  /// Completes when this connection is finished, from either side.
  Future<void> get closed => _closed.future;

  /// Connects and completes with the server-assigned socket id.
  ///
  /// Throws [ReverbFatalError] if the server rejects the connection outright,
  /// or [ReverbConnectionClosed] if the socket dies before the handshake.
  Future<String> open() {
    final handshake = Completer<String>();
    _handshake = handshake;

    final socket = socketFactory(url);
    _socket = socket;
    socket.stream.listen(
      _onMessage,
      onError: _onSocketError,
      onDone: _onSocketDone,
      cancelOnError: true,
    );

    return handshake.future;
  }

  /// Sends [message] as an encoded frame. No-op once the socket is gone.
  void send(Map<String, dynamic> message) {
    final socket = _socket;
    if (socket == null) return;
    socket.sink.add(jsonEncode(message));
  }

  /// Closes the socket from this side.
  Future<void> close() async {
    _stopTimers();
    final socket = _socket;
    _socket = null;
    if (socket != null) await socket.sink.close();
    _finish();
  }

  void _onMessage(dynamic raw) {
    _restartIdleTimer();
    _pongTimer?.cancel();
    _pongTimer = null;

    if (raw is! String) return;
    final frame = ReverbFrame.parse(raw);
    if (frame == null) {
      onLog?.call('reverb: dropped unparseable frame');
      return;
    }

    switch (frame.event) {
      case 'pusher:connection_established':
        _onEstablished(frame);
      case 'pusher:ping':
        send(<String, dynamic>{
          'event': 'pusher:pong',
          'data': <String, dynamic>{},
        });
      case 'pusher:pong':
        break;
      case 'pusher:error':
        _onProtocolError(frame);
      default:
        _frames.add(frame);
    }
  }

  void _onEstablished(ReverbFrame frame) {
    final id = frame.data['socket_id'];
    if (id is! String) {
      _failHandshake(const ReverbConnectionClosed());
      return;
    }

    socketId = id;
    final timeout = frame.data['activity_timeout'];
    if (timeout is int) _activityTimeout = Duration(seconds: timeout);

    _restartIdleTimer();
    onLog?.call('reverb: connected as $id');
    _handshake?.complete(id);
    _handshake = null;
  }

  void _onProtocolError(ReverbFrame frame) {
    final code = frame.data['code'];
    final message = frame.data['message']?.toString() ?? 'unknown error';
    onLog?.call('reverb: server error $code $message');

    if (code is int && isFatalErrorCode(code)) {
      _failHandshake(ReverbFatalError(code, message));
      unawaited(close());
    }
  }

  void _onSocketError(Object error, StackTrace stackTrace) {
    onLog?.call('reverb: socket error $error');
    _failHandshake(error);
    _stopTimers();
    _finish();
  }

  void _onSocketDone() {
    _failHandshake(const ReverbConnectionClosed());
    _stopTimers();
    _finish();
  }

  void _failHandshake(Object error) {
    final handshake = _handshake;
    if (handshake == null || handshake.isCompleted) return;
    _handshake = null;
    handshake.completeError(error);
  }

  /// Sends a ping after [_activityTimeout] of silence, then treats a missing
  /// pong within the same window as a dead socket. Reverb never notices a
  /// half-open TCP connection on its own, so without this a backgrounded
  /// device can sit on a socket that will never deliver another event.
  void _restartIdleTimer() {
    _idleTimer?.cancel();
    _idleTimer = Timer(_activityTimeout, () {
      send(<String, dynamic>{
        'event': 'pusher:ping',
        'data': <String, dynamic>{},
      });
      _pongTimer = Timer(_activityTimeout, () {
        onLog?.call('reverb: ping timed out, closing socket');
        unawaited(close());
      });
    });
  }

  void _stopTimers() {
    _idleTimer?.cancel();
    _idleTimer = null;
    _pongTimer?.cancel();
    _pongTimer = null;
  }

  void _finish() {
    if (!_closed.isCompleted) _closed.complete();
    if (!_frames.isClosed) unawaited(_frames.close());
  }
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `dart format . && flutter analyze && flutter test`
Expected: format clean, `No issues found!`, all tests PASS

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat: socket connection with handshake and keepalive"
```

---

### Task 3: Authorization

**Files:**
- Create: `lib/src/auth.dart`
- Test: `test/auth_test.dart`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  - `class ReverbAuth { const ReverbAuth({required String auth, String? channelData}); final String auth; final String? channelData; }`
  - `typedef Authorizer = Future<ReverbAuth> Function(String channelName, String socketId)`
  - `class ReverbAuthException implements Exception { final String channelName; final int statusCode; final String body; }`
  - `Authorizer httpAuthorizer({required String endpoint, Future<Map<String, String>> Function()? headers, http.Client? client})`

- [ ] **Step 1: Write the failing tests**

`test/auth_test.dart`:

```dart
import 'dart:convert';

import 'package:flutter_reverb/src/auth.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:http/http.dart' as http;
import 'package:http/testing.dart';

void main() {
  test('posts socket id and channel name and returns the signature', () async {
    late http.Request captured;
    final client = MockClient((http.Request request) async {
      captured = request;
      return http.Response(
        jsonEncode(<String, dynamic>{'auth': 'key:sig'}),
        200,
      );
    });

    final authorizer = httpAuthorizer(
      endpoint: 'https://api.test/broadcasting/auth',
      client: client,
    );
    final result = await authorizer('private-users.1', '123.456');

    final body = jsonDecode(captured.body) as Map<String, dynamic>;
    expect(body['socket_id'], '123.456');
    expect(body['channel_name'], 'private-users.1');
    expect(result.auth, 'key:sig');
    expect(result.channelData, isNull);
  });

  test('returns channel_data for presence channels', () async {
    final client = MockClient((http.Request request) async {
      return http.Response(
        jsonEncode(<String, dynamic>{
          'auth': 'key:sig',
          'channel_data': '{"user_id":"7"}',
        }),
        200,
      );
    });

    final authorizer = httpAuthorizer(
      endpoint: 'https://api.test/broadcasting/auth',
      client: client,
    );
    final result = await authorizer('presence-room.5', '123.456');

    expect(result.channelData, '{"user_id":"7"}');
  });

  test('applies headers from the callback on every call', () async {
    var calls = 0;
    final seen = <String?>[];
    final client = MockClient((http.Request request) async {
      seen.add(request.headers['authorization']);
      return http.Response(jsonEncode(<String, dynamic>{'auth': 'a'}), 200);
    });

    final authorizer = httpAuthorizer(
      endpoint: 'https://api.test/broadcasting/auth',
      headers: () async => <String, String>{'Authorization': 'Bearer ${++calls}'},
      client: client,
    );

    await authorizer('private-a', '1.1');
    await authorizer('private-b', '1.1');

    expect(seen, <String>['Bearer 1', 'Bearer 2']);
  });

  test('throws ReverbAuthException on a non-200 response', () async {
    final client = MockClient((http.Request request) async {
      return http.Response('Forbidden', 403);
    });

    final authorizer = httpAuthorizer(
      endpoint: 'https://api.test/broadcasting/auth',
      client: client,
    );

    await expectLater(
      authorizer('private-users.1', '123.456'),
      throwsA(
        isA<ReverbAuthException>()
            .having((ReverbAuthException e) => e.statusCode, 'statusCode', 403)
            .having((ReverbAuthException e) => e.channelName, 'channelName',
                'private-users.1'),
      ),
    );
  });

  test('throws ReverbAuthException when the response has no auth field',
      () async {
    final client = MockClient((http.Request request) async {
      return http.Response(jsonEncode(<String, dynamic>{'nope': 1}), 200);
    });

    final authorizer = httpAuthorizer(
      endpoint: 'https://api.test/broadcasting/auth',
      client: client,
    );

    await expectLater(
      authorizer('private-users.1', '123.456'),
      throwsA(isA<ReverbAuthException>()),
    );
  });
}
```

`package:http/testing.dart` ships with `http`; no new dependency is needed.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `flutter test test/auth_test.dart`
Expected: FAIL — `Target of URI doesn't exist: 'package:flutter_reverb/src/auth.dart'`

- [ ] **Step 3: Write the implementation**

`lib/src/auth.dart`:

```dart
import 'dart:convert';

import 'package:http/http.dart' as http;

/// The signature returned by Laravel's broadcasting auth endpoint.
class ReverbAuth {
  /// Creates an auth result.
  const ReverbAuth({required this.auth, this.channelData});

  /// The `auth` signature, in `appKey:signature` form.
  final String auth;

  /// The `channel_data` blob, present only for presence channels.
  final String? channelData;
}

/// Produces the auth signature for [channelName] bound to [socketId].
///
/// Supply your own to reuse an existing HTTP client, interceptors, token
/// refresh or certificate pinning; the package then makes no requests itself.
typedef Authorizer = Future<ReverbAuth> Function(
  String channelName,
  String socketId,
);

/// Raised when the broadcasting auth endpoint refuses or returns nonsense.
class ReverbAuthException implements Exception {
  /// Creates the exception.
  const ReverbAuthException(this.channelName, this.statusCode, this.body);

  /// The channel that failed to authorize.
  final String channelName;

  /// The HTTP status code, or 200 when the body was unusable.
  final int statusCode;

  /// The raw response body, for diagnostics.
  final String body;

  @override
  String toString() =>
      'ReverbAuthException($channelName, $statusCode): $body';
}

/// The default authorizer: POSTs JSON to Laravel's `/broadcasting/auth`.
///
/// [headers] is called per request rather than captured once, so a token that
/// is refreshed between subscriptions is picked up without recreating the
/// client.
Authorizer httpAuthorizer({
  required String endpoint,
  Future<Map<String, String>> Function()? headers,
  http.Client? client,
}) {
  final httpClient = client ?? http.Client();

  return (String channelName, String socketId) async {
    final response = await httpClient.post(
      Uri.parse(endpoint),
      headers: <String, String>{
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        ...?await headers?.call(),
      },
      body: jsonEncode(<String, dynamic>{
        'socket_id': socketId,
        'channel_name': channelName,
      }),
    );

    if (response.statusCode != 200) {
      throw ReverbAuthException(
        channelName,
        response.statusCode,
        response.body,
      );
    }

    Object? decoded;
    try {
      decoded = jsonDecode(response.body);
    } on FormatException {
      throw ReverbAuthException(channelName, 200, response.body);
    }

    if (decoded is! Map || decoded['auth'] is! String) {
      throw ReverbAuthException(channelName, 200, response.body);
    }

    final channelData = decoded['channel_data'];
    return ReverbAuth(
      auth: decoded['auth'] as String,
      channelData: channelData is String ? channelData : null,
    );
  };
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `dart format . && flutter analyze && flutter test`
Expected: format clean, `No issues found!`, all tests PASS

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: broadcasting auth with pluggable authorizer"
```

---

### Task 4: Channels, subscriptions and ref-counting

The core of the package. A channel counts its handlers; when the count reaches zero it reports itself empty so the owner can unsubscribe. Two screens listening to the same channel is therefore safe by construction.

**Files:**
- Create: `lib/src/channel.dart`
- Test: `test/channel_test.dart`

**Interfaces:**
- Consumes: `resolveEventName` from `lib/src/protocol.dart`.
- Produces:
  - `typedef ReverbEventCallback = void Function(Map<String, dynamic> data)`
  - `class Subscription { Subscription listen(String event, ReverbEventCallback callback); Subscription listenForWhisper(String event, ReverbEventCallback callback); void cancel(); }`
  - `class Channel { Channel({required String name, required String namespace, required void Function(Map<String, dynamic>) send, required void Function(Channel) onEmpty}); final String name; Subscription listen(String event, ReverbEventCallback callback); void dispatch(String wireEvent, Map<String, dynamic> data); }`
  - `class PrivateChannel extends Channel { void whisper(String event, Map<String, dynamic> data); }`
  - `class PresenceMember { const PresenceMember({required String id, required Map<String, dynamic> info}); final String id; final Map<String, dynamic> info; }`
  - `class PresenceChannel extends PrivateChannel { Subscription members({void Function(List<PresenceMember>)? here, void Function(PresenceMember)? joining, void Function(PresenceMember)? leaving}); }`

- [ ] **Step 1: Write the failing tests**

`test/channel_test.dart`:

```dart
import 'package:flutter_reverb/src/channel.dart';
import 'package:flutter_test/flutter_test.dart';

class Harness {
  final List<Map<String, dynamic>> sent = <Map<String, dynamic>>[];
  final List<Channel> emptied = <Channel>[];

  Channel public(String name) => Channel(
        name: name,
        namespace: r'App\Events',
        send: sent.add,
        onEmpty: emptied.add,
      );

  PrivateChannel private(String name) => PrivateChannel(
        name: name,
        namespace: r'App\Events',
        send: sent.add,
        onEmpty: emptied.add,
      );

  PresenceChannel presence(String name) => PresenceChannel(
        name: name,
        namespace: r'App\Events',
        send: sent.add,
        onEmpty: emptied.add,
      );
}

void main() {
  test('dispatches a namespaced event to its listener', () {
    final harness = Harness();
    final channel = harness.public('orders');
    Map<String, dynamic>? received;

    channel.listen('OrderCreated', (Map<String, dynamic> data) {
      received = data;
    });
    channel.dispatch(r'App\Events\OrderCreated', <String, dynamic>{'id': 7});

    expect(received, <String, dynamic>{'id': 7});
  });

  test('dispatches a broadcastAs event registered with a leading dot', () {
    final harness = Harness();
    final channel = harness.public('orders');
    var calls = 0;

    channel.listen('.order.created', (_) => calls++);
    channel.dispatch('order.created', <String, dynamic>{});

    expect(calls, 1);
  });

  test('ignores events with no listener', () {
    final harness = Harness();
    final channel = harness.public('orders');

    expect(
      () => channel.dispatch('Nothing', <String, dynamic>{}),
      returnsNormally,
    );
  });

  test('chained listen calls share one cancelable handle', () {
    final harness = Harness();
    final channel = harness.public('orders');
    var created = 0;
    var edited = 0;

    final sub = channel
        .listen('OrderCreated', (_) => created++)
        .listen('OrderEdited', (_) => edited++);

    channel.dispatch(r'App\Events\OrderCreated', <String, dynamic>{});
    channel.dispatch(r'App\Events\OrderEdited', <String, dynamic>{});
    expect(<int>[created, edited], <int>[1, 1]);

    sub.cancel();
    channel.dispatch(r'App\Events\OrderCreated', <String, dynamic>{});
    channel.dispatch(r'App\Events\OrderEdited', <String, dynamic>{});
    expect(<int>[created, edited], <int>[1, 1]);
  });

  test('channel survives while another listener remains', () {
    final harness = Harness();
    final channel = harness.public('orders');

    final first = channel.listen('OrderCreated', (_) {});
    final second = channel.listen('OrderCreated', (_) {});

    first.cancel();
    expect(harness.emptied, isEmpty);

    second.cancel();
    expect(harness.emptied, <Channel>[channel]);
  });

  test('cancelling twice does not double-decrement the ref count', () {
    final harness = Harness();
    final channel = harness.public('orders');

    final first = channel.listen('OrderCreated', (_) {});
    final second = channel.listen('OrderCreated', (_) {});

    first.cancel();
    first.cancel();

    expect(harness.emptied, isEmpty);
    second.cancel();
    expect(harness.emptied, <Channel>[channel]);
  });

  test('whisper sends a client prefixed event', () {
    final harness = Harness();
    final channel = harness.private('private-room.1');

    channel.whisper('typing', <String, dynamic>{'user': 7});

    expect(harness.sent.single, <String, dynamic>{
      'event': 'client-typing',
      'channel': 'private-room.1',
      'data': <String, dynamic>{'user': 7},
    });
  });

  test('listenForWhisper receives client events', () {
    final harness = Harness();
    final channel = harness.private('private-room.1');
    Map<String, dynamic>? received;

    channel.listen('X', (_) {}).listenForWhisper(
      'typing',
      (Map<String, dynamic> data) => received = data,
    );
    channel.dispatch('client-typing', <String, dynamic>{'user': 7});

    expect(received, <String, dynamic>{'user': 7});
  });

  test('presence reports the initial member list', () {
    final harness = Harness();
    final channel = harness.presence('presence-room.5');
    List<PresenceMember>? members;

    channel.members(here: (List<PresenceMember> m) => members = m);
    channel.dispatch('pusher_internal:subscription_succeeded', <String, dynamic>{
      'presence': <String, dynamic>{
        'ids': <String>['1', '2'],
        'hash': <String, dynamic>{
          '1': <String, dynamic>{'name': 'Ann'},
          '2': <String, dynamic>{'name': 'Bo'},
        },
      },
    });

    expect(members!.map((PresenceMember m) => m.id), <String>['1', '2']);
    expect(members!.first.info, <String, dynamic>{'name': 'Ann'});
  });

  test('presence reports joining and leaving members', () {
    final harness = Harness();
    final channel = harness.presence('presence-room.5');
    PresenceMember? joined;
    PresenceMember? left;

    channel.members(
      joining: (PresenceMember m) => joined = m,
      leaving: (PresenceMember m) => left = m,
    );

    channel.dispatch('pusher_internal:member_added', <String, dynamic>{
      'user_id': '3',
      'user_info': <String, dynamic>{'name': 'Cy'},
    });
    channel.dispatch(
      'pusher_internal:member_removed',
      <String, dynamic>{'user_id': '3'},
    );

    expect(joined!.id, '3');
    expect(joined!.info, <String, dynamic>{'name': 'Cy'});
    expect(left!.id, '3');
    expect(left!.info, isEmpty);
  });

  test('presence members handle counts toward the ref count', () {
    final harness = Harness();
    final channel = harness.presence('presence-room.5');

    final sub = channel.members(here: (_) {});
    expect(harness.emptied, isEmpty);

    sub.cancel();
    expect(harness.emptied, <Channel>[channel]);
  });
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `flutter test test/channel_test.dart`
Expected: FAIL — `Target of URI doesn't exist: 'package:flutter_reverb/src/channel.dart'`

- [ ] **Step 3: Write the implementation**

`lib/src/channel.dart`:

```dart
import 'protocol.dart';

/// Receives the decoded payload of a broadcast event.
typedef ReverbEventCallback = void Function(Map<String, dynamic> data);

/// A cancelable handle over one or more listeners on a single channel.
///
/// [listen] returns the same handle, so a chain of calls produces one handle
/// whose [cancel] removes every listener in that chain.
class Subscription {
  Subscription._(this._channel);

  final Channel _channel;
  final List<void Function()> _removers = <void Function()>[];

  /// Adds an event listener to the same channel and returns this handle.
  Subscription listen(String event, ReverbEventCallback callback) {
    _removers.add(_channel._addListener(_channel._wireEvent(event), callback));
    return this;
  }

  /// Adds a client event (whisper) listener and returns this handle.
  Subscription listenForWhisper(String event, ReverbEventCallback callback) {
    _removers.add(_channel._addListener('client-$event', callback));
    return this;
  }

  void _register(void Function() remover) => _removers.add(remover);

  /// Removes every listener registered through this handle.
  ///
  /// The channel itself is only torn down once its last listener is gone, so
  /// cancelling one screen's subscription never disconnects another's.
  void cancel() {
    final removers = List<void Function()>.of(_removers);
    _removers.clear();
    for (final void Function() remove in removers) {
      remove();
    }
  }
}

/// A public channel: events in, no authorization, no client events.
class Channel {
  /// Creates a channel. Applications get channels from `Reverb`, not directly.
  Channel({
    required this.name,
    required String namespace,
    required void Function(Map<String, dynamic> message) send,
    required void Function(Channel channel) onEmpty,
  })  : _namespace = namespace,
        _send = send,
        _onEmpty = onEmpty;

  /// The wire channel name, including any `private-` or `presence-` prefix.
  final String name;

  final String _namespace;
  final void Function(Map<String, dynamic> message) _send;
  final void Function(Channel channel) _onEmpty;
  final Map<String, List<ReverbEventCallback>> _listeners =
      <String, List<ReverbEventCallback>>{};

  int _handlerCount = 0;

  /// Listens for [event] and returns a chainable, cancelable handle.
  Subscription listen(String event, ReverbEventCallback callback) =>
      Subscription._(this).listen(event, callback);

  /// Routes an incoming wire event to its listeners.
  void dispatch(String wireEvent, Map<String, dynamic> data) {
    final callbacks = _listeners[wireEvent];
    if (callbacks == null) return;
    for (final ReverbEventCallback callback
        in List<ReverbEventCallback>.of(callbacks)) {
      callback(data);
    }
  }

  String _wireEvent(String event) => resolveEventName(event, _namespace);

  void _sendMessage(Map<String, dynamic> message) => _send(message);

  /// Registers [callback] and returns an idempotent remover.
  ///
  /// The remover guards against being called twice so that a double `cancel()`
  /// cannot drop the ref count below the number of live listeners and tear
  /// down a channel someone else is still using.
  void Function() _addListener(String wireEvent, ReverbEventCallback callback) {
    (_listeners[wireEvent] ??= <ReverbEventCallback>[]).add(callback);
    _handlerCount++;

    var removed = false;
    return () {
      if (removed) return;
      removed = true;

      _listeners[wireEvent]?.remove(callback);
      _handlerCount--;
      if (_handlerCount == 0) _onEmpty(this);
    };
  }
}

/// A private channel. Requires authorization and permits client events.
class PrivateChannel extends Channel {
  /// Creates a private channel.
  PrivateChannel({
    required super.name,
    required super.namespace,
    required super.send,
    required super.onEmpty,
  });

  /// Sends a client event directly to other subscribers.
  ///
  /// Client events never reach the application server, so they suit ephemeral
  /// signals such as typing indicators. The `client-` prefix is protocol
  /// mandated and added here.
  void whisper(String event, Map<String, dynamic> data) {
    _sendMessage(<String, dynamic>{
      'event': 'client-$event',
      'channel': name,
      'data': data,
    });
  }
}

/// A subscriber of a presence channel.
class PresenceMember {
  /// Creates a member.
  const PresenceMember({required this.id, required this.info});

  /// The member id, from Laravel's presence channel authorization.
  final String id;

  /// Arbitrary member info returned alongside the id.
  final Map<String, dynamic> info;
}

/// A presence channel: a private channel that also tracks who is subscribed.
class PresenceChannel extends PrivateChannel {
  /// Creates a presence channel.
  PresenceChannel({
    required super.name,
    required super.namespace,
    required super.send,
    required super.onEmpty,
  });

  /// Registers membership callbacks and returns one cancelable handle.
  ///
  /// A single entry point rather than three chainable methods, because the
  /// three callbacks have different signatures and are almost always wanted
  /// together.
  Subscription members({
    void Function(List<PresenceMember> members)? here,
    void Function(PresenceMember member)? joining,
    void Function(PresenceMember member)? leaving,
  }) {
    final subscription = Subscription._(this);

    if (here != null) {
      subscription._register(
        _addListener(
          'pusher_internal:subscription_succeeded',
          (Map<String, dynamic> data) => here(_parseMembers(data)),
        ),
      );
    }
    if (joining != null) {
      subscription._register(
        _addListener(
          'pusher_internal:member_added',
          (Map<String, dynamic> data) => joining(_parseMember(data)),
        ),
      );
    }
    if (leaving != null) {
      subscription._register(
        _addListener(
          'pusher_internal:member_removed',
          (Map<String, dynamic> data) => leaving(_parseMember(data)),
        ),
      );
    }

    return subscription;
  }

  List<PresenceMember> _parseMembers(Map<String, dynamic> data) {
    final presence = data['presence'];
    if (presence is! Map) return const <PresenceMember>[];

    final ids = presence['ids'];
    final hash = presence['hash'];
    if (ids is! List) return const <PresenceMember>[];

    return ids.map((dynamic id) {
      final key = id.toString();
      final info = hash is Map ? hash[key] : null;
      return PresenceMember(
        id: key,
        info: info is Map
            ? info.cast<String, dynamic>()
            : const <String, dynamic>{},
      );
    }).toList();
  }

  PresenceMember _parseMember(Map<String, dynamic> data) {
    final info = data['user_info'];
    return PresenceMember(
      id: data['user_id'].toString(),
      info:
          info is Map ? info.cast<String, dynamic>() : const <String, dynamic>{},
    );
  }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `dart format . && flutter analyze && flutter test`
Expected: format clean, `No issues found!`, all tests PASS

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: channels, chainable subscriptions and ref-counted teardown"
```

---

### Task 5: Reverb facade — registry, subscribe flow and state

Wires `Connection`, `Channel` and the authorizer together. Reconnect is deliberately left to Task 6; this task connects once.

**Files:**
- Create: `lib/src/reverb.dart`
- Test: `test/reverb_test.dart`

**Interfaces:**
- Consumes: `Connection`, `SocketFactory`, `ReverbFatalError` (Task 2); `Authorizer`, `ReverbAuth`, `httpAuthorizer` (Task 3); `Channel`, `PrivateChannel`, `PresenceChannel` (Task 4); `buildSocketUrl`, `backoffDelay` (Task 1).
- Produces:
  - `enum ReverbState { disconnected, connecting, connected, reconnecting, failed }`
  - `class Reverb { Reverb({required String host, required String appKey, int? port, bool useTls = true, String? authEndpoint, Future<Map<String, String>> Function()? authHeaders, Authorizer? authorizer, String namespace, bool handleAppLifecycle, void Function(Object, StackTrace?)? onError, void Function(String)? onLog, SocketFactory? socketFactory, math.Random? random}); Future<void> connect(); Future<void> disconnect(); String? get socketId; ReverbState get state; Stream<ReverbState> get states; Channel channel(String name); PrivateChannel private(String name); PresenceChannel presence(String name); void onReconnected(void Function() callback); void dispose(); }`

- [ ] **Step 1: Write the failing tests**

`test/reverb_test.dart`:

```dart
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
      'data': <String, dynamic>{'channel': 'private-users.1', 'auth': 'key:sig'},
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

    expect(states, <ReverbState>[ReverbState.connecting, ReverbState.connected]);
  });
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `flutter test test/reverb_test.dart`
Expected: FAIL — `Target of URI doesn't exist: 'package:flutter_reverb/src/reverb.dart'`

- [ ] **Step 3: Write the implementation**

`lib/src/reverb.dart`:

```dart
import 'dart:async';
import 'dart:math' as math;

import 'package:web_socket_channel/web_socket_channel.dart';

import 'auth.dart';
import 'channel.dart';
import 'connection.dart';
import 'protocol.dart';

/// The connection lifecycle of a [Reverb] client.
enum ReverbState {
  /// Not connected and not trying to be.
  disconnected,

  /// The first connection attempt is in flight.
  connecting,

  /// Connected, with the handshake complete.
  connected,

  /// The socket dropped and a retry is in flight.
  reconnecting,

  /// Stopped permanently, for example on an unknown application key.
  failed,
}

/// A Laravel Reverb client.
///
/// Owns the channel registry, authorization and the connection lifecycle.
/// Create one per application and keep it alive.
class Reverb {
  /// Creates a client. Call [connect] to open the socket.
  ///
  /// Provide either [authorizer] or [authEndpoint] to use private and presence
  /// channels; public channels need neither.
  Reverb({
    required String host,
    required String appKey,
    int? port,
    bool useTls = true,
    String? authEndpoint,
    Future<Map<String, String>> Function()? authHeaders,
    Authorizer? authorizer,
    String namespace = r'App\Events',
    this.onError,
    this.onLog,
    SocketFactory? socketFactory,
    math.Random? random,
  })  : _namespace = namespace,
        _socketFactory = socketFactory ?? WebSocketChannel.connect,
        _random = random ?? math.Random(),
        _authorizer = authorizer ??
            (authEndpoint == null
                ? null
                : httpAuthorizer(
                    endpoint: authEndpoint,
                    headers: authHeaders,
                  )),
        _url = buildSocketUrl(
          host: host,
          port: port ?? (useTls ? 443 : 80),
          appKey: appKey,
          useTls: useTls,
          clientVersion: clientVersion,
        );

  /// The package version reported to the server in the socket URL.
  static const String clientVersion = '0.1.0';

  /// Reports runtime failures that the package handled without throwing.
  final void Function(Object error, StackTrace? stackTrace)? onError;

  /// Optional log sink, so the host application controls logging.
  final void Function(String message)? onLog;

  final Uri _url;
  final String _namespace;
  final SocketFactory _socketFactory;
  final Authorizer? _authorizer;
  final math.Random _random;

  final Map<String, Channel> _channels = <String, Channel>{};
  final StreamController<ReverbState> _states =
      StreamController<ReverbState>.broadcast();

  Connection? _connection;
  ReverbState _state = ReverbState.disconnected;

  /// The socket id assigned by the server, or null while disconnected.
  String? get socketId => _connection?.socketId;

  /// The current connection state.
  ReverbState get state => _state;

  /// Connection state changes.
  Stream<ReverbState> get states => _states.stream;

  /// Opens the socket and subscribes to any channels created beforehand.
  Future<void> connect() async {
    if (_state == ReverbState.connected) return;
    _setState(ReverbState.connecting);

    final connection = Connection(
      url: _url,
      socketFactory: _socketFactory,
      onLog: onLog,
    );
    _connection = connection;
    connection.frames.listen(_onFrame);

    try {
      await connection.open();
    } on Object catch (error, stackTrace) {
      _setState(ReverbState.failed);
      onError?.call(error, stackTrace);
      return;
    }

    _setState(ReverbState.connected);
    await _subscribeAll();
  }

  /// Closes the socket. Channels and listeners are kept.
  Future<void> disconnect() async {
    await _connection?.close();
    _connection = null;
    _setState(ReverbState.disconnected);
  }

  /// Returns the public channel named [name], creating it on first use.
  Channel channel(String name) => _register(
        name,
        () => Channel(
          name: name,
          namespace: _namespace,
          send: _send,
          onEmpty: _unsubscribe,
        ),
      );

  /// Returns the private channel for the bare [name], adding the prefix.
  PrivateChannel private(String name) {
    _requireAuthorizer('private');
    return _register(
      'private-$name',
      () => PrivateChannel(
        name: 'private-$name',
        namespace: _namespace,
        send: _send,
        onEmpty: _unsubscribe,
      ),
    );
  }

  /// Returns the presence channel for the bare [name], adding the prefix.
  PresenceChannel presence(String name) {
    _requireAuthorizer('presence');
    return _register(
      'presence-$name',
      () => PresenceChannel(
        name: 'presence-$name',
        namespace: _namespace,
        send: _send,
        onEmpty: _unsubscribe,
      ),
    );
  }

  /// Releases the state stream. Call from the host application's teardown.
  void dispose() {
    unawaited(disconnect());
    unawaited(_states.close());
  }

  T _register<T extends Channel>(String wireName, T Function() create) {
    final existing = _channels[wireName];
    if (existing != null) return existing as T;

    final channel = create();
    _channels[wireName] = channel;
    unawaited(_subscribe(channel));
    return channel;
  }

  void _requireAuthorizer(String kind) {
    if (_authorizer != null) return;
    throw StateError(
      'flutter_reverb: $kind channels need authorization. Pass either '
      'authEndpoint or authorizer to the Reverb constructor.',
    );
  }

  /// Subscribes [channel], authorizing first when the channel is private.
  ///
  /// Silently returns when there is no live socket: channels created before
  /// connecting are flushed by [_subscribeAll] once the handshake lands, so
  /// callers never see a "not connected yet" error.
  Future<void> _subscribe(Channel channel) async {
    final connection = _connection;
    final socketId = connection?.socketId;
    if (connection == null || socketId == null) return;

    final payload = <String, dynamic>{'channel': channel.name};

    if (channel is PrivateChannel) {
      try {
        final auth = await _authorizer!(channel.name, socketId);
        payload['auth'] = auth.auth;
        if (auth.channelData != null) {
          payload['channel_data'] = auth.channelData;
        }
      } on Object catch (error, stackTrace) {
        onError?.call(error, stackTrace);
        return;
      }

      // The signature is bound to the socket id it was issued for. If the
      // socket was replaced while the request was in flight, this signature is
      // already void and the reconnect path will authorize again.
      if (_connection?.socketId != socketId) return;
    }

    connection.send(<String, dynamic>{
      'event': 'pusher:subscribe',
      'data': payload,
    });
  }

  Future<void> _subscribeAll() async {
    await Future.wait(_channels.values.map(_subscribe));
  }

  void _unsubscribe(Channel channel) {
    _channels.remove(channel.name);
    _connection?.send(<String, dynamic>{
      'event': 'pusher:unsubscribe',
      'data': <String, dynamic>{'channel': channel.name},
    });
  }

  void _send(Map<String, dynamic> message) => _connection?.send(message);

  void _onFrame(ReverbFrame frame) {
    final name = frame.channel;
    if (name == null) return;
    _channels[name]?.dispatch(frame.event, frame.data);
  }

  void _setState(ReverbState next) {
    if (_state == next) return;
    _state = next;
    if (!_states.isClosed) _states.add(next);
  }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `dart format . && flutter analyze && flutter test`
Expected: format clean, `No issues found!`, all tests PASS

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: Reverb facade with channel registry and subscribe flow"
```

---

### Task 6: Reconnect with re-authorization

The correctness-critical task. The auth signature is bound to the socket id, so a reconnect must re-authorize every private channel rather than replay the old subscribe frames. `onReconnected` must not fire until every channel is live again, or an application's REST reconcile races a half-restored socket.

**Files:**
- Modify: `lib/src/reverb.dart` (add the retry loop, `onReconnected`, and drop handling)
- Test: `test/reconnect_test.dart`

**Interfaces:**
- Consumes: everything from Task 5.
- Produces: `void Reverb.onReconnected(void Function() callback)` and automatic reconnection behaviour.

- [ ] **Step 1: Write the failing tests**

`test/reconnect_test.dart`:

```dart
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `flutter test test/reconnect_test.dart`
Expected: FAIL — `The method 'onReconnected' isn't defined for the class 'Reverb'`, plus reconnect assertions failing

- [ ] **Step 3: Rewrite the connection lifecycle in `lib/src/reverb.dart`**

Add these fields next to the existing ones:

```dart
  final List<void Function()> _reconnectedCallbacks = <void Function()>[];

  bool _shouldRun = false;
  int _attempt = 0;
```

Add the public registration method next to `connect`:

```dart
  /// Registers [callback] to run after a dropped socket is fully restored.
  ///
  /// Reverb does not replay events missed while disconnected, so this is where
  /// an application refetches whatever it may have missed. It fires only once
  /// every previously-live channel has resubscribed, so a REST reconcile
  /// cannot race a half-restored socket. It does not fire on first connect.
  void onReconnected(void Function() callback) =>
      _reconnectedCallbacks.add(callback);
```

Replace `connect` and `disconnect` with:

```dart
  /// Opens the socket, retrying with backoff until it succeeds or fails
  /// fatally, and subscribes to any channels created beforehand.
  Future<void> connect() async {
    if (_shouldRun && _state == ReverbState.connected) return;
    _shouldRun = true;
    _attempt = 0;
    await _open();
  }

  /// Closes the socket and stops reconnecting. Channels and listeners are kept
  /// so that a later [connect] restores them.
  Future<void> disconnect() async {
    _shouldRun = false;
    final connection = _connection;
    _connection = null;
    await connection?.close();
    _setState(ReverbState.disconnected);
  }

  Future<void> _open() async {
    while (_shouldRun) {
      _setState(
        _attempt == 0 ? ReverbState.connecting : ReverbState.reconnecting,
      );

      final connection = Connection(
        url: _url,
        socketFactory: _socketFactory,
        onLog: onLog,
      );
      _connection = connection;
      connection.frames.listen(_onFrame);

      try {
        await connection.open();
      } on ReverbFatalError catch (error, stackTrace) {
        _shouldRun = false;
        _connection = null;
        _setState(ReverbState.failed);
        onError?.call(error, stackTrace);
        return;
      } on Object catch (error, stackTrace) {
        onError?.call(error, stackTrace);
        await Future<void>.delayed(backoffDelay(_attempt++, _random));
        continue;
      }

      final wasReconnect = _attempt > 0;
      _attempt = 0;
      _setState(ReverbState.connected);

      // Watch for a later drop before awaiting resubscription, so a socket
      // that dies mid-resubscribe still schedules a retry.
      unawaited(connection.closed.then((_) => _onDropped(connection)));

      await _subscribeAll();
      if (wasReconnect) {
        for (final void Function() callback
            in List<void Function()>.of(_reconnectedCallbacks)) {
          callback();
        }
      }
      return;
    }
  }

  void _onDropped(Connection connection) {
    // Ignore a drop reported by a socket we already replaced or closed.
    if (!identical(_connection, connection) || !_shouldRun) return;

    _connection = null;
    _setState(ReverbState.reconnecting);
    unawaited(
      Future<void>.delayed(backoffDelay(_attempt++, _random)).then((_) {
        if (_shouldRun) unawaited(_open());
      }),
    );
  }
```

Note that `_attempt` is incremented before the delay in `_onDropped`, so the first retry after a drop uses the one-second step and each subsequent failure lengthens it.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `dart format . && flutter analyze && flutter test`
Expected: format clean, `No issues found!`, all tests PASS

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: reconnect with backoff and full re-authorization"
```

---

### Task 7: App lifecycle handling

**Files:**
- Modify: `lib/src/reverb.dart`
- Test: `test/lifecycle_test.dart`

**Interfaces:**
- Consumes: Task 6's `connect`/`disconnect`.
- Produces: `Reverb` mixes in `WidgetsBindingObserver`; constructor gains `bool handleAppLifecycle = true`.

- [ ] **Step 1: Write the failing tests**

`test/lifecycle_test.dart`:

```dart
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `flutter test test/lifecycle_test.dart`
Expected: FAIL — `The method 'didChangeAppLifecycleState' isn't defined for the class 'Reverb'`

- [ ] **Step 3: Add lifecycle handling to `lib/src/reverb.dart`**

Add the import:

```dart
import 'package:flutter/widgets.dart';
```

Change the class declaration:

```dart
class Reverb with WidgetsBindingObserver {
```

Add the constructor parameter `this.handleAppLifecycle = true` alongside `onError`, and the field plus its doc:

```dart
  /// Whether to disconnect on background and reconnect on foreground.
  ///
  /// Left on, this keeps iOS from holding a socket the OS will silently kill
  /// and avoids burning battery on a connection nobody is watching. Turn it
  /// off if the host application manages the socket itself.
  final bool handleAppLifecycle;

  bool _pausedByLifecycle = false;
  bool _observing = false;
```

Register the observer at the top of `connect`, immediately after the early return:

```dart
    if (handleAppLifecycle && !_observing) {
      _observing = true;
      WidgetsBinding.instance.addObserver(this);
    }
```

Add the observer callback:

```dart
  @override
  void didChangeAppLifecycleState(AppLifecycleState state) {
    if (!handleAppLifecycle) return;

    switch (state) {
      case AppLifecycleState.paused:
      case AppLifecycleState.detached:
        if (_state == ReverbState.disconnected) return;
        _pausedByLifecycle = true;
        unawaited(disconnect());
      case AppLifecycleState.resumed:
        if (!_pausedByLifecycle) return;
        _pausedByLifecycle = false;
        unawaited(connect());
      case AppLifecycleState.inactive:
      case AppLifecycleState.hidden:
        break;
    }
  }
```

Remove the observer in `dispose`:

```dart
  void dispose() {
    if (_observing) {
      _observing = false;
      WidgetsBinding.instance.removeObserver(this);
    }
    unawaited(disconnect());
    unawaited(_states.close());
  }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `dart format . && flutter analyze && flutter test`
Expected: format clean, `No issues found!`, all tests PASS

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: disconnect on background and reconnect on foreground"
```

---

### Task 8: Public surface, example app and publish readiness

**Files:**
- Create: `lib/flutter_reverb.dart`
- Create: `README.md`
- Create: `example/pubspec.yaml`
- Create: `example/lib/main.dart`
- Create: `.github/workflows/ci.yaml`
- Modify: `CHANGELOG.md`
- Test: `test/exports_test.dart`

**Interfaces:**
- Consumes: every public type from Tasks 1–7.
- Produces: `package:flutter_reverb/flutter_reverb.dart` exporting `Reverb`, `ReverbState`, `Channel`, `PrivateChannel`, `PresenceChannel`, `PresenceMember`, `Subscription`, `ReverbEventCallback`, `ReverbAuth`, `Authorizer`, `ReverbAuthException`, `ReverbFatalError`, `ReverbFrame`.

- [ ] **Step 1: Write the failing test**

`test/exports_test.dart`:

```dart
import 'package:flutter_reverb/flutter_reverb.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  test('the public entry point exposes the documented API', () {
    // Compiles only if every public type is exported from the entry point.
    expect(ReverbState.values, contains(ReverbState.connected));
    expect(const ReverbAuth(auth: 'a').auth, 'a');
    expect(const PresenceMember(id: '1', info: <String, dynamic>{}).id, '1');
    expect(const ReverbFatalError(4001, 'x').code, 4001);
    expect(
      const ReverbAuthException('private-a', 403, 'no').statusCode,
      403,
    );
    expect(Reverb.clientVersion, isNotEmpty);
  });
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `flutter test test/exports_test.dart`
Expected: FAIL — `Target of URI doesn't exist: 'package:flutter_reverb/flutter_reverb.dart'`

- [ ] **Step 3: Write the entry point**

`lib/flutter_reverb.dart`:

```dart
/// A Laravel Reverb realtime client for Flutter.
///
/// Speaks the Pusher protocol in pure Dart, with a Laravel Echo-style API,
/// automatic reconnection, presence channels and client events.
library flutter_reverb;

export 'src/auth.dart'
    show Authorizer, ReverbAuth, ReverbAuthException, httpAuthorizer;
export 'src/channel.dart'
    show
        Channel,
        PresenceChannel,
        PresenceMember,
        PrivateChannel,
        ReverbEventCallback,
        Subscription;
export 'src/connection.dart' show ReverbFatalError;
export 'src/protocol.dart' show ReverbFrame;
export 'src/reverb.dart' show Reverb, ReverbState;
```

- [ ] **Step 4: Write the example app**

`example/pubspec.yaml`:

```yaml
name: flutter_reverb_example
description: Minimal flutter_reverb example.
version: 1.0.0
publish_to: none

environment:
  sdk: ^3.5.0

dependencies:
  flutter:
    sdk: flutter
  flutter_reverb:
    path: ../

dev_dependencies:
  flutter_test:
    sdk: flutter
```

`example/lib/main.dart`:

```dart
import 'package:flutter/material.dart';
import 'package:flutter_reverb/flutter_reverb.dart';

/// Runs the example app.
void main() => runApp(const ExampleApp());

/// Minimal example: connect, listen to a public channel, show what arrives.
class ExampleApp extends StatefulWidget {
  /// Creates the example app.
  const ExampleApp({super.key});

  @override
  State<ExampleApp> createState() => _ExampleAppState();
}

class _ExampleAppState extends State<ExampleApp> {
  final Reverb _reverb = Reverb(
    host: 'localhost',
    port: 8080,
    appKey: 'local-key',
    useTls: false,
  );

  final List<String> _events = <String>[];
  Subscription? _subscription;
  ReverbState _state = ReverbState.disconnected;

  @override
  void initState() {
    super.initState();
    _reverb.states.listen((ReverbState state) {
      if (mounted) setState(() => _state = state);
    });
    _subscription = _reverb.channel('orders').listen(
          'OrderCreated',
          (Map<String, dynamic> data) {
            if (mounted) setState(() => _events.insert(0, data.toString()));
          },
        );
    _reverb.onReconnected(() => debugPrint('reconnected: refetch here'));
    _reverb.connect();
  }

  @override
  void dispose() {
    _subscription?.cancel();
    _reverb.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      home: Scaffold(
        appBar: AppBar(title: Text('flutter_reverb — ${_state.name}')),
        body: ListView.builder(
          itemCount: _events.length,
          itemBuilder: (BuildContext context, int index) =>
              ListTile(title: Text(_events[index])),
        ),
      ),
    );
  }
}
```

- [ ] **Step 5: Write the README**

`README.md` must contain, in this order:

1. A one-paragraph description and the pub.dev badge placeholder.
2. Install: the `flutter pub add flutter_reverb` line.
3. Laravel setup: a note that this targets a self-hosted Reverb server and needs `BROADCAST_CONNECTION=reverb`, plus the `REVERB_APP_KEY`, `REVERB_HOST`, `REVERB_PORT` values mapping onto the constructor arguments.
4. Quick start — the exact code below:

````markdown
```dart
final reverb = Reverb(
  host: 'api.example.com',
  port: 443,
  appKey: 'your-reverb-app-key',
  useTls: true,
  authEndpoint: 'https://api.example.com/broadcasting/auth',
  authHeaders: () async => {'Authorization': 'Bearer $token'},
);

await reverb.connect();
```
````

5. Four recipes, each a short code block: public channel, private channel with a chained `listen` and `cancel` in `dispose`, presence with `members(here:, joining:, leaving:)`, and whisper.
6. An "Event names" section explaining that `listen('OrderCreated')` matches `App\Events\OrderCreated` and `listen('.order.created')` matches a `broadcastAs()` name.
7. A "Reconnection" section explaining that Reverb does not replay missed events and showing `reverb.onReconnected(() => refetch())`.
8. A "Custom authorizer" section showing the `authorizer:` override for apps with their own HTTP client.
9. A "Migrating from pusher_channels_flutter" table mapping: `init` + `connect` to the constructor plus `connect()`; `subscribe(channelName: 'private-users.1')` to `private('users.1')`; `onAuthorizer` to `authEndpoint`/`authHeaders`; manual ref-counting to `Subscription.cancel()`; `trigger` to `whisper`.

- [ ] **Step 6: Write CI**

`.github/workflows/ci.yaml`:

```yaml
name: ci

on:
  push:
    branches: [main]
  pull_request:

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: subosito/flutter-action@v2
        with:
          channel: stable
      - run: flutter pub get
      - run: dart format --set-exit-if-changed .
      - run: flutter analyze
      - run: flutter test
```

- [ ] **Step 7: Verify everything, including publish readiness**

Run:

```bash
dart format . && flutter analyze && flutter test && flutter pub publish --dry-run
```

Expected: format clean, `No issues found!`, all tests PASS, and the dry run reports `Package has 0 warnings`. Fix any warnings it reports (missing `example/`, undocumented API, description length) before committing.

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "feat: public entry point, example app, README and CI"
```

---

## Self-Review

**Spec coverage**

| Spec section | Task |
|---|---|
| Pure Dart transport over `web_socket_channel` | 2 |
| Echo-style chaining API | 4 |
| Built-in HTTP auth plus authorizer override | 3 |
| `listen()` returns a chainable, cancelable handle | 4 |
| Ref-counted channel teardown | 4, 5 |
| Channel name prefixes | 5 |
| Event-name namespace resolution | 1, 4 |
| Endpoint and query parameters | 1 |
| Handshake and `socket_id` | 2 |
| Double-encoded `data` | 1 |
| Subscribe frames with `auth` / `channel_data` | 5 |
| Auth bound to `socket_id`, re-auth on reconnect | 6 |
| Ping / pong keepalive | 2 |
| Presence internals | 4 |
| Client events with `client-` prefix | 4 |
| `pusher:error` and fatal codes | 1, 2, 6 |
| Queued subscription before handshake | 5 |
| Backoff with jitter | 1, 6 |
| `onReconnected` after all channels live | 6 |
| App lifecycle | 7 |
| Nothing throws into app code; `onError`, `onLog` | 2, 5 |
| Test list | 1–7 |
| Example, README, CHANGELOG, LICENSE, CI | 1, 8 |

No spec requirement is unassigned.

**Type consistency** — checked across tasks: `SocketFactory` returns `StreamChannel<dynamic>` in Tasks 2, 5, 6, 7; `Authorizer` has the same `(String channelName, String socketId)` signature in Tasks 3, 5, 6; `onEmpty` is `void Function(Channel)` in Tasks 4 and 5; `Reverb.clientVersion` is defined in Task 5 and asserted in Task 8; `backoffDelay(int, math.Random)` is defined in Task 1 and called in Task 6.

**Known ordering note** — Task 5 writes a simple `connect()` that Task 6 replaces with the retry loop. This is deliberate: Task 5 stays reviewable on its own, and Task 6's tests are the ones that pin the reconnect behaviour.
