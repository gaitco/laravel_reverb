# laravel_reverb 0.5.0 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split `reverb.dart` into focused part files, then add connection metrics and a `ReverbFake` for host-application tests.

**Architecture:** Dart cannot split a class across files, so `Reverb` becomes `class Reverb extends _ReverbBase with _ReverbHealth, _ReverbChannels, _ReverbConnect`, where the mixins live in `part` files of the same library. `_ReverbBase` owns every field and declares the handful of methods that cross mixin boundaries as abstract, which makes the coupling explicit instead of implicit. No behaviour changes and no public API changes in Tasks 1-3 — the diff is code movement a reviewer can read line by line. Tasks 4 and 5 then land two additive features in the small files the split produced.

**Tech Stack:** Dart / Flutter, `flutter_test`, `stream_channel`, `web_socket_channel`. No new dependencies.

**Spec:** `docs/superpowers/specs/2026-08-06-roadmap-0.4-0.5-design.md`

## Global Constraints

- Dart SDK `^3.5.0`, Flutter `>=3.24.0`. Do not raise either.
- **No new dependencies.** Not in `dependencies`, not in `dev_dependencies`. In particular, do not add `package:clock` — Task 4 uses an injected clock seam instead, matching the `socketFactory` / `random` / `httpClientFactory` seams this package already has.
- **Additive only.** An app written against 0.4.0 that changes nothing must behave identically. No parameter loses its default, no exported name changes, no public member moves out of the public API.
- **Tasks 1-3 change no behaviour whatsoever.** Every existing test must pass without being edited. If a task tempts you to change a test, stop — the move is wrong.
- Every public declaration carries a dartdoc comment. Match the surrounding density — this package documents *why*, not *what*. **Doc comments move with the code they document, verbatim.**
- `dart format --set-exit-if-changed .`, `flutter analyze` and `flutter test` must all pass before every commit. These are exactly what CI runs (`.github/workflows/ci.yaml`).
- Run all commands from the package root, `/Users/abdullah/code/packages/flutter-reverb`.
- **`part` files cannot declare their own imports.** Every import stays in `lib/src/reverb.dart`, and every part file begins with exactly `part of 'reverb.dart';`.

## File Structure

| File | Responsibility | Change |
|---|---|---|
| `lib/src/reverb.dart` | Library head: imports, `ReverbState`, `part` directives, `class Reverb` (constructor, statics, `metrics`, `dispose`) | Modify: shrinks from 787 lines to ~200 |
| `lib/src/reverb_base.dart` | `abstract class _ReverbBase` — every field, and nothing else | Create (part) |
| `lib/src/reverb_health.dart` | `mixin _ReverbHealth` — channel liveness and presence-roster reset | Create (part) |
| `lib/src/reverb_channels.dart` | `mixin _ReverbChannels` — the channel registry, authorization and subscribe/unsubscribe | Create (part) |
| `lib/src/reverb_connect.dart` | `mixin _ReverbConnect` — connect/disconnect, the backoff loop, app lifecycle | Create (part) |
| `lib/src/metrics.dart` | `class ReverbMetrics` — the connection-quality snapshot | Create |
| `lib/src/connection.dart` | Socket, handshake, keepalive | Modify: records ping/pong latency and last-frame time |
| `lib/src/testing/in_memory_socket.dart` | An in-memory `StreamChannel` pair plus the canned server frames | Create |
| `lib/testing.dart` | Public testing entry point: `ReverbFake` | Create |
| `lib/laravel_reverb.dart` | Public entry point | Modify: export `ReverbMetrics` |
| `test/support/fake_socket.dart` | Existing test helper | Modify (Task 5): delegates to the new in-memory socket, same API |
| `test/metrics_test.dart` | Metrics unit and integration tests | Create |
| `test/testing_test.dart` | Tests for `ReverbFake` itself | Create |
| `README.md`, `CHANGELOG.md`, `pubspec.yaml` | Docs and release | Modify (Task 6) |

---

### Task 1: Scaffold the part files and move the health mixin

The smallest of the three moves, done first because it proves the whole pattern end to end — base class, part directives, mixin application, cross-mixin abstract declarations — against four short methods instead of a 240-line registry.

**Files:**
- Create: `lib/src/reverb_base.dart`, `lib/src/reverb_health.dart`
- Modify: `lib/src/reverb.dart`

**Interfaces:**
- Consumes: nothing.
- Produces: `abstract class _ReverbBase with WidgetsBindingObserver`, holding every field currently on `Reverb` and declaring the cross-mixin abstract methods. `mixin _ReverbHealth on _ReverbBase`. `class Reverb extends _ReverbBase with _ReverbHealth`. Tasks 2 and 3 add mixins to that `with` clause.

- [ ] **Step 1: Record the baseline**

Run: `flutter test`
Expected: PASS. Note the exact test count — every later step in Tasks 1-3 must report the same number. Nothing in this task adds or removes a test.

- [ ] **Step 2: Create the base class**

Create `lib/src/reverb_base.dart`, beginning with exactly:

```dart
part of 'reverb.dart';
```

Then declare `abstract class _ReverbBase with WidgetsBindingObserver`, and **move into it, verbatim with their doc comments**, every field currently declared on `Reverb` — that is, everything between the constructor and the first getter (`String? get socketId`), plus the two `late final` fields:

Public, constructor-set: `onError`, `onLog`, `handleAppLifecycle`, `pingInterval`, `watchdogTimeout`.
Private, constructor-set: `_url`, `_namespace`, `_socketFactory`, `_random`, `_authorizer` (`late final`), `_ownedHttpClient` (`late final`).
Private, inline-initialized: `_pausedByLifecycle`, `_observing`, `_channels`, `_generations`, `_live`, `_channelHealthController`, `_states`, `_reconnectedCallbacks`, `_connection`, `_state`, `_shouldRun`, `_everConnected`, `_attempt`, `_generation`, `_clientEpoch`.

Leave `clientVersion` and `_maxAuthAttempts` on `Reverb` — they are `static const`, so they belong with the class the API documents, not with instance state.

Give `_ReverbBase` a constructor taking the constructor-set values:

```dart
  _ReverbBase({
    required Uri url,
    required String namespace,
    required SocketFactory socketFactory,
    required math.Random random,
    required this.onError,
    required this.onLog,
    required this.handleAppLifecycle,
    required this.pingInterval,
    required this.watchdogTimeout,
  })  : _url = url,
        _namespace = namespace,
        _socketFactory = socketFactory,
        _random = random;
```

`_authorizer` and `_ownedHttpClient` stay `late final` and are still assigned from `Reverb`'s constructor body — a `late final` field is assignable once from anywhere in the same library, and part files are the same library.

- [ ] **Step 3: Do not declare anything abstract**

`_ReverbBase` holds fields only. Cross-mixin calls are resolved by **chaining the mixins' `on` clauses** rather than by forward-declaring private abstract members in the base:

```dart
mixin _ReverbHealth   on _ReverbBase
mixin _ReverbChannels on _ReverbBase, _ReverbHealth
mixin _ReverbConnect  on _ReverbBase, _ReverbHealth, _ReverbChannels

class Reverb extends _ReverbBase
    with _ReverbHealth, _ReverbChannels, _ReverbConnect
```

Each mixin's `on` clause names exactly what it calls, so the dependency direction is stated once, in one place, and enforced by the compiler. The application order in `Reverb`'s `with` clause satisfies every constraint.

A private abstract declaration in the base would work too, but the analyzer reports each one as `unused_element` — calls resolve to the concrete override, never the stub — and suppressing that would mean an `// ignore:` comment per declaration. The `on` chain needs none.

This step is therefore a no-op by design. It exists to say what NOT to add.

- [ ] **Step 4: Create the health mixin**

Create `lib/src/reverb_health.dart`, beginning with exactly `part of 'reverb.dart';`, declaring `mixin _ReverbHealth on _ReverbBase`, and **move into it verbatim, with their doc comments**, these three methods from `Reverb`:

- `_setChannelHealth`
- `_markAllChannelsDown`
- `_resetPresenceRosters`

Each needs `@override`, since `_ReverbBase` now declares them abstract.

Also move the `channelHealth` getter (`Stream<ChannelHealth> get channelHealth => _channelHealthController.stream;`) and the `isSubscribed` method with their doc comments — both read only health state.

- [ ] **Step 5: Rewire `Reverb`**

In `lib/src/reverb.dart`:

Add the part directives immediately after the imports and the `export`, before `enum ReverbState`:

```dart
part 'reverb_base.dart';
part 'reverb_channels.dart';
part 'reverb_connect.dart';
part 'reverb_health.dart';
```

Tasks 2 and 3 create `reverb_channels.dart` and `reverb_connect.dart`. Declaring all four parts now means one edit rather than three, but **the analyzer will fail until those files exist** — so for this task, create both as one-line stubs containing only `part of 'reverb.dart';`. Tasks 2 and 3 fill them in.

Change the class declaration to:

```dart
class Reverb extends _ReverbBase with _ReverbHealth {
```

Change the constructor to forward to `super` instead of initializing the moved fields itself. The initializer list becomes:

```dart
  })  : super(
          url: buildSocketUrl(
            host: host,
            port: port ?? (useTls ? 443 : 80),
            appKey: appKey,
            useTls: useTls,
            clientVersion: clientVersion,
            path: path,
          ),
          namespace: namespace,
          socketFactory: socketFactory ?? WebSocketChannel.connect,
          random: random ?? math.Random(),
          onError: onError,
          onLog: onLog,
          handleAppLifecycle: handleAppLifecycle,
          pingInterval: pingInterval,
          watchdogTimeout: watchdogTimeout,
        ) {
```

Note the consequence: `onError`, `onLog`, `handleAppLifecycle`, `pingInterval` and `watchdogTimeout` can no longer be declared as `this.onError` style parameters, because the fields now live on the base. Change those five to ordinary typed parameters in the parameter list, keeping their defaults exactly (`handleAppLifecycle = true`; the other four default to null). Keep every doc comment on the constructor unchanged.

The constructor body (the `watchdogTimeout` `ArgumentError` check and the authorizer/HTTP-client setup) stays as it is.

- [ ] **Step 6: Run the full suite**

Run: `flutter test`
Expected: PASS, with exactly the count from Step 1. No test file was edited.

If anything fails, the move changed behaviour — find what moved wrong rather than adjusting a test.

- [ ] **Step 7: Verify and commit**

Run: `dart format --set-exit-if-changed . && flutter analyze && flutter test`
Expected: all three pass.

```bash
git add lib/src/reverb.dart lib/src/reverb_base.dart lib/src/reverb_health.dart lib/src/reverb_channels.dart lib/src/reverb_connect.dart
git commit -m "refactor: extract _ReverbBase and the health mixin into part files

Dart cannot split a class across files, so Reverb becomes a subclass of
a base holding its fields, with behaviour moving into mixins in part
files. Health goes first as the smallest of the three. No behaviour
change: every existing test passes untouched."
```

---

### Task 2: Move the channel registry

The largest of the three moves: the registry, authorization, and the subscribe/unsubscribe paths, including the two epoch counters whose interaction is the subtlest code in the package.

**Files:**
- Modify: `lib/src/reverb_channels.dart` (the stub from Task 1), `lib/src/reverb.dart`

**Interfaces:**
- Consumes: `_ReverbBase` and its abstract declarations from Task 1.
- Produces: `mixin _ReverbChannels on _ReverbBase`, implementing `_subscribeAll`. `class Reverb extends _ReverbBase with _ReverbHealth, _ReverbChannels`.

- [ ] **Step 1: Move the members**

Into `lib/src/reverb_channels.dart`, declaring `mixin _ReverbChannels on _ReverbBase, _ReverbHealth`, **move verbatim with their doc comments** these members from `Reverb`:

- `channel(String name)`
- `private(String name)`
- `presence(String name)`
- `_register<T extends Channel>`
- `_requireAuthorizer`
- `_resubscribe`
- `_subscribe`
- `_subscribeAll`
- `_unsubscribe`
- `_sendFor`
- `_onFrame` — frame dispatch belongs here: it routes to `_channels` and calls `_setChannelHealth`, and putting it in this mixin is what lets `_ReverbConnect` reach it through the `on` chain without a forward declaration

The `on _ReverbHealth` clause is what makes `_setChannelHealth` resolve. No `@override` annotations are needed on anything — nothing is being overridden.

`_maxAuthAttempts` stays on `Reverb` as a `static const`; `_subscribe` references it as `Reverb._maxAuthAttempts`. (A mixin cannot see a subclass's statics unqualified.)

Change nothing else. In particular: do not rename `_generation`, `_generations` or `_clientEpoch`, do not "simplify" any of the `current()` closures, and do not touch the comments explaining them. Those three counters are near-identically named on purpose and every comment about them is load-bearing.

- [ ] **Step 2: Add the mixin to `Reverb`**

```dart
class Reverb extends _ReverbBase with _ReverbHealth, _ReverbChannels {
```

- [ ] **Step 3: Run the full suite**

Run: `flutter test`
Expected: PASS, same count as Task 1 Step 1, with no test file edited.

Pay attention to `test/reverb_test.dart` and `test/reconnect_test.dart` in particular — they cover the epoch behaviour this step moved.

- [ ] **Step 4: Verify and commit**

Run: `dart format --set-exit-if-changed . && flutter analyze && flutter test`
Expected: all three pass.

```bash
git add lib/src/reverb.dart lib/src/reverb_channels.dart
git commit -m "refactor: move the channel registry into _ReverbChannels

Registry, authorization and subscribe/unsubscribe, including the
generation and client-epoch guards, move verbatim into a mixin. No
behaviour change: every existing test passes untouched."
```

---

### Task 3: Move the connect loop and app lifecycle

**Files:**
- Modify: `lib/src/reverb_connect.dart` (the stub from Task 1), `lib/src/reverb.dart`

**Interfaces:**
- Consumes: `_ReverbBase` from Task 1; `_subscribeAll` from Task 2.
- Produces: `mixin _ReverbConnect on _ReverbBase, _ReverbHealth, _ReverbChannels`. `class Reverb extends _ReverbBase with _ReverbHealth, _ReverbChannels, _ReverbConnect`. After this task `lib/src/reverb.dart` holds only the library head, `ReverbState`, the part directives, and `Reverb`'s constructor, statics and `dispose`.

- [ ] **Step 1: Move the members**

Into `lib/src/reverb_connect.dart`, declaring `mixin _ReverbConnect on _ReverbBase, _ReverbHealth, _ReverbChannels`, **move verbatim with their doc comments** these members from `Reverb`:

- `connect()`
- `disconnect({bool forget = false})`
- `_open()`
- `_onDropped(Connection connection)`
- `didChangeAppLifecycleState` — keep its `@override` (it overrides `WidgetsBindingObserver`, which `_ReverbBase` mixes in)
- `_setState`
- `onReconnected` and the `state` / `states` / `socketId` getters, with their doc comments — all read connection state

Leave `dispose()` on `Reverb`: it spans all three mixins (it removes the observer, disconnects, closes both stream controllers and the owned HTTP client), so the facade is where it belongs.

- [ ] **Step 2: Add the mixin to `Reverb`**

```dart
class Reverb extends _ReverbBase with _ReverbHealth, _ReverbChannels, _ReverbConnect {
```

- [ ] **Step 3: Run the full suite**

Run: `flutter test`
Expected: PASS, same count as Task 1 Step 1, with no test file edited.

`test/lifecycle_test.dart` and `test/reconnect_test.dart` are the ones that matter most here.

- [ ] **Step 4: Confirm the split actually achieved its goal**

Run: `find lib -name '*.dart' | xargs wc -l | sort -n`
Expected: no file over 300 lines. If `reverb_channels.dart` exceeds it, report that in your task report rather than splitting further on your own — the plan's intent is these four files, not five.

- [ ] **Step 5: Verify and commit**

Run: `dart format --set-exit-if-changed . && flutter analyze && flutter test`
Expected: all three pass.

```bash
git add lib/src/reverb.dart lib/src/reverb_connect.dart
git commit -m "refactor: move the connect loop and lifecycle into _ReverbConnect

Completes the split. reverb.dart now holds the constructor, the public
getters, frame dispatch and dispose; everything else lives in three
mixins in part files. No behaviour change: every existing test passes
untouched."
```

---

### Task 4: Connection metrics

An app can currently render a connected/disconnected dot and nothing more. This exposes what the keepalive machinery already knows — round-trip latency, how stale the socket is, and how many times it has come back.

The clock is injected rather than read from `DateTime.now()` directly, so a test can drive latency deterministically. That matches the `socketFactory` / `random` / `httpClientFactory` seams the constructor already documents, and avoids adding `package:clock` as a dependency.

**Files:**
- Create: `lib/src/metrics.dart`, `test/metrics_test.dart`
- Modify: `lib/src/connection.dart`, `lib/src/reverb_base.dart`, `lib/src/reverb_connect.dart`, `lib/src/reverb.dart`, `lib/laravel_reverb.dart`

**Interfaces:**
- Consumes: the mixin structure from Tasks 1-3.
- Produces:
  - `class ReverbMetrics` with `final Duration? lastLatency`, `final int reconnectCount`, `final Duration? sinceLastFrame`, `final DateTime? connectedSince`.
  - `ReverbMetrics get metrics` on `Reverb`.
  - `Connection` gains `Duration? get lastLatency`, `DateTime? get lastFrameAt`, and a `DateTime Function() now` constructor parameter.
  - `Reverb`'s constructor gains `@visibleForTesting DateTime Function()? now`.

- [ ] **Step 1: Write the failing metrics-value test**

Create `test/metrics_test.dart`:

```dart
import 'package:flutter_test/flutter_test.dart';
import 'package:laravel_reverb/src/metrics.dart';

void main() {
  test('a fresh client reports no latency and no reconnects', () {
    const metrics = ReverbMetrics();

    expect(metrics.lastLatency, isNull);
    expect(metrics.reconnectCount, 0);
    expect(metrics.sinceLastFrame, isNull);
    expect(metrics.connectedSince, isNull);
  });
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `flutter test test/metrics_test.dart`
Expected: FAIL — `Target of URI doesn't exist: 'package:laravel_reverb/src/metrics.dart'`.

- [ ] **Step 3: Create the metrics value**

Create `lib/src/metrics.dart`:

```dart
/// A point-in-time snapshot of connection quality.
///
/// Read [Reverb.metrics] when you need it — nothing is streamed, because
/// latency changes on every ping and a widget that rebuilt on each one would
/// be doing so to redraw a value nobody watched change. There is no history:
/// each read reports the most recent round trip, not a series.
class ReverbMetrics {
  /// Creates a snapshot. Applications read [Reverb.metrics] instead.
  const ReverbMetrics({
    this.lastLatency,
    this.reconnectCount = 0,
    this.sinceLastFrame,
    this.connectedSince,
  });

  /// The most recent `pusher:ping` to `pusher:pong` round trip, or null when
  /// no ping has been answered on the current socket.
  final Duration? lastLatency;

  /// How many times a dropped socket has been restored since construction.
  ///
  /// The first connect is not a reconnect, so this stays 0 until a drop is
  /// recovered from.
  final int reconnectCount;

  /// How long since any frame arrived, or null while disconnected.
  ///
  /// A value climbing past the server's activity timeout is what a half-open
  /// socket looks like from the client's side.
  final Duration? sinceLastFrame;

  /// When the current socket completed its handshake, or null while
  /// disconnected.
  final DateTime? connectedSince;
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `flutter test test/metrics_test.dart`
Expected: PASS.

- [ ] **Step 5: Write the failing latency test**

Add to `test/metrics_test.dart` — this drives a real ping/pong through `Connection` with a controllable clock:

```dart
import 'package:laravel_reverb/src/connection.dart';

import 'support/fake_socket.dart';

// ...inside main():

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
```

`sendPingForTest()` is a `@visibleForTesting` wrapper around the private `_sendPing()`, needed because the real ping fires on a timer this test does not run. Add it next to `_sendPing` in `connection.dart`:

```dart
  /// Sends a `pusher:ping` immediately, for tests that need to drive a round
  /// trip without waiting out [pingInterval].
  @visibleForTesting
  void sendPingForTest() => _sendPing();
```

- [ ] **Step 6: Run it to verify it fails**

Run: `flutter test test/metrics_test.dart --plain-name 'round trip'`
Expected: FAIL — `No named parameter with the name 'now'`.

- [ ] **Step 7: Record latency and frame time in `Connection`**

In `lib/src/connection.dart`:

Add a constructor parameter `DateTime Function()? now` and store it as `final DateTime Function() _now;`, defaulting to `DateTime.now`:

```dart
    DateTime Function()? now,
  }) : _now = now ?? DateTime.now;
```

Add the fields and getters:

```dart
  final DateTime Function() _now;

  DateTime? _pingSentAt;
  Duration? _lastLatency;
  DateTime? _lastFrameAt;

  /// The most recent ping-to-pong round trip on this socket, or null when no
  /// ping has been answered yet.
  Duration? get lastLatency => _lastLatency;

  /// When the last inbound frame arrived, or null if none has.
  ///
  /// This is what the watchdog watches; exposing it lets an application see
  /// how stale a quiet socket has become before the watchdog acts.
  DateTime? get lastFrameAt => _lastFrameAt;
```

In `_sendPing()`, record the send time immediately before the `send`:

```dart
    _pingSentAt = _now();
```

In `_onMessage`'s `case 'pusher:pong':`, compute the round trip before the existing body runs:

```dart
      case 'pusher:pong':
        final sentAt = _pingSentAt;
        if (sentAt != null) {
          _lastLatency = _now().difference(sentAt);
          _pingSentAt = null;
        }
```

In `_onActivity()`, record the frame time as the first statement:

```dart
    _lastFrameAt = _now();
```

- [ ] **Step 8: Run the latency test to verify it passes**

Run: `flutter test test/metrics_test.dart`
Expected: PASS, all tests in the file.

- [ ] **Step 9: Write the failing client-level test**

Add to `test/metrics_test.dart`:

```dart
import 'package:laravel_reverb/laravel_reverb.dart';
import 'package:laravel_reverb/src/reverb.dart';

// ...inside main(), and add TestWidgetsFlutterBinding.ensureInitialized()
// as the first line of main() if it is not already there:

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
    await sockets[0].serverClose();
    await Future<void>.delayed(const Duration(seconds: 2));
    sockets[1].emitJson(handshakeFrame());
    await Future<void>.delayed(Duration.zero);

    expect(reverb.metrics.reconnectCount, 1);

    reverb.dispose();
  });
```

Note: this test waits out a real backoff delay (the first retry is ~1s plus jitter), which is why it allows 2 seconds. Do not add a `random` seam to shorten it — `backoffDelay`'s jitter is already covered in `protocol_test.dart`, and a real delay here proves the reconnect path end to end.

- [ ] **Step 10: Run it to verify it fails**

Run: `flutter test test/metrics_test.dart --plain-name 'restored socket'`
Expected: FAIL — `No named parameter with the name 'now'` on `Reverb`.

- [ ] **Step 11: Thread the clock and expose `metrics`**

In `lib/src/reverb.dart`, add to the constructor parameter list, next to the other seams:

```dart
    @visibleForTesting DateTime Function()? now,
```

Extend the seam paragraph in the constructor's doc comment to name it: `now` lets a test drive latency and staleness without real time passing.

Pass it to `super` as `now: now ?? DateTime.now`.

In `lib/src/reverb_base.dart`, add the field and the two pieces of state the connect loop maintains:

```dart
  final DateTime Function() _now;

  /// How many drops have been recovered from. See [ReverbMetrics.reconnectCount].
  int _reconnectCount = 0;

  /// When the current socket completed its handshake, cleared when it drops.
  DateTime? _connectedSince;
```

taking `required DateTime Function() now` in `_ReverbBase`'s constructor and assigning `_now = now`.

In `lib/src/reverb_connect.dart`:
- In `_open()`, where `_setState(ReverbState.connected)` runs, add `_connectedSince = _now();` and, immediately after it, `if (wasReconnect) _reconnectCount++;` — `wasReconnect` is already computed on the line above.
- In `_onDropped`, and in `disconnect`, set `_connectedSince = null;` alongside the existing `_markAllChannelsDown()` calls.
- Pass the clock into `Connection`: add `now: _now,` to the `Connection(...)` construction.

In `lib/src/reverb.dart`, add the getter next to the other public getters:

```dart
  /// A snapshot of connection quality, read on demand.
  ///
  /// Nothing streams these values: latency changes on every ping, so a
  /// consumer that rebuilt on each change would redraw far more often than
  /// anything it displays actually changes. Read this when you paint.
  ReverbMetrics get metrics {
    final lastFrameAt = _connection?.lastFrameAt;

    return ReverbMetrics(
      lastLatency: _connection?.lastLatency,
      reconnectCount: _reconnectCount,
      sinceLastFrame:
          lastFrameAt == null ? null : _now().difference(lastFrameAt),
      connectedSince: _connectedSince,
    );
  }
```

Add `import 'metrics.dart';` to `lib/src/reverb.dart`'s imports, and export the type from `lib/laravel_reverb.dart`:

```dart
export 'src/metrics.dart' show ReverbMetrics;
```

- [ ] **Step 12: Run the full suite**

Run: `flutter test`
Expected: PASS — the pre-existing tests plus the three new ones.

- [ ] **Step 13: Document it in the README**

Add a `## Connection quality` section immediately after the existing `## Channel health` section:

````markdown
## Connection quality

`reverb.metrics` is a snapshot, read whenever you need it:

```dart
final metrics = reverb.metrics;

metrics.lastLatency;     // Duration?  last ping/pong round trip
metrics.reconnectCount;  // int        drops recovered from
metrics.sinceLastFrame;  // Duration?  how stale the socket is
metrics.connectedSince;  // DateTime?  null while disconnected
```

Nothing streams these. Latency changes on every ping, so a widget rebuilding
on each change would redraw far more often than anything it displays actually
changes — read `metrics` when you paint instead.

`sinceLastFrame` is the one worth showing: climbing past the server's activity
timeout is what a half-open socket looks like from the client's side, and it is
what `watchdogTimeout` acts on. See [Keepalive](#keepalive).
````

- [ ] **Step 14: Verify and commit**

Run: `dart format --set-exit-if-changed . && flutter analyze && flutter test`
Expected: all three pass.

```bash
git add lib/src/metrics.dart lib/src/connection.dart lib/src/reverb.dart lib/src/reverb_base.dart lib/src/reverb_connect.dart lib/laravel_reverb.dart test/metrics_test.dart README.md
git commit -m "feat: connection metrics via reverb.metrics

Exposes ping/pong latency, reconnect count, socket staleness and
connect time as an on-demand snapshot. The clock is an injected seam,
matching socketFactory and random, so tests drive latency without real
time passing and without a new dependency."
```

---

### Task 5: `ReverbFake` for host-application tests

A host application currently cannot test its own listeners without a running Reverb server. This package's own tests have solved that since 0.1.0 with a fake socket behind the `socketFactory` seam — but that seam is `@visibleForTesting` and its type is not exported, so applications cannot reach it. This task packages the same approach as public API.

The fake drives a **real** `Reverb` over an in-memory socket. It does not reimplement client behaviour, so a test written against it exercises the actual subscribe, dispatch and teardown paths.

**Files:**
- Create: `lib/src/testing/in_memory_socket.dart`, `lib/testing.dart`, `test/testing_test.dart`
- Modify: `test/support/fake_socket.dart`

**Interfaces:**
- Consumes: `Reverb` and its `socketFactory` seam.
- Produces, from `package:laravel_reverb/testing.dart`:
  - `class ReverbFake` with `Reverb get reverb`, `Future<void> connect()`, `void emit(String channel, String event, [Map<String, dynamic> data = const {}])`, `Future<void> drop()`, `List<Map<String, dynamic>> get sent`, `void dispose()`.

- [ ] **Step 1: Write the failing test**

Create `test/testing_test.dart`:

```dart
import 'package:flutter_test/flutter_test.dart';
import 'package:laravel_reverb/testing.dart';

void main() {
  TestWidgetsFlutterBinding.ensureInitialized();

  test('delivers an emitted event to a listener on a public channel', () async {
    final fake = ReverbFake();
    await fake.connect();

    Map<String, dynamic>? received;
    fake.reverb.channel('orders').listen(
          'OrderCreated',
          (Map<String, dynamic> data) => received = data,
        );
    await Future<void>.delayed(Duration.zero);

    fake.emit('orders', r'App\Events\OrderCreated', <String, dynamic>{'id': 7});
    await Future<void>.delayed(Duration.zero);

    expect(received, <String, dynamic>{'id': 7});
    fake.dispose();
  });

  test('records what the client sent', () async {
    final fake = ReverbFake();
    await fake.connect();

    fake.reverb.channel('orders').listen('OrderCreated', (_) {});
    await Future<void>.delayed(Duration.zero);

    expect(
      fake.sent.map((Map<String, dynamic> f) => f['event']),
      contains('pusher:subscribe'),
    );
    fake.dispose();
  });

  test('a private channel authorizes without a server', () async {
    final fake = ReverbFake();
    await fake.connect();

    Map<String, dynamic>? received;
    fake.reverb.private('users.1').listen(
          'MessageSent',
          (Map<String, dynamic> data) => received = data,
        );
    await Future<void>.delayed(Duration.zero);

    fake.emit(
      'private-users.1',
      r'App\Events\MessageSent',
      <String, dynamic>{'body': 'hi'},
    );
    await Future<void>.delayed(Duration.zero);

    expect(received, <String, dynamic>{'body': 'hi'});
    fake.dispose();
  });
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `flutter test test/testing_test.dart`
Expected: FAIL — `Target of URI doesn't exist: 'package:laravel_reverb/testing.dart'`.

- [ ] **Step 3: Extract the in-memory socket**

Create `lib/src/testing/in_memory_socket.dart`. Move into it, verbatim, the implementation currently in `test/support/fake_socket.dart` — the `FakeSocket` class (renamed `InMemorySocket`), the `factoryFor` helper, and `handshakeFrame`. Its imports are `dart:convert`, `package:stream_channel/stream_channel.dart`, and `../connection.dart` for `SocketFactory`.

Keep every doc comment. Keep the member names (`channel`, `sentJson`, `emitJson`, `emitRaw`, `emitError`, `serverClose`, `closed`) exactly as they are — Step 5 depends on that.

- [ ] **Step 4: Write `ReverbFake`**

Create `lib/testing.dart`:

```dart
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
```

- [ ] **Step 5: Point the package's own test helper at the shared implementation**

Rewrite `test/support/fake_socket.dart` so it delegates rather than duplicating:

```dart
import 'package:laravel_reverb/src/testing/in_memory_socket.dart';

export 'package:laravel_reverb/src/testing/in_memory_socket.dart'
    show factoryFor, handshakeFrame;

/// The package's own tests knew this as `FakeSocket` before the
/// implementation moved to `lib/` for [ReverbFake] to share. Kept as an alias
/// so those tests read as they always have.
typedef FakeSocket = InMemorySocket;
```

This is why Step 3 insisted on keeping the member names: no test file changes.

- [ ] **Step 6: Run the full suite**

Run: `flutter test`
Expected: PASS — every pre-existing test, unedited, plus the three new ones.

If a pre-existing test fails here, the extraction in Step 3 changed something. Compare against `git show HEAD:test/support/fake_socket.dart` rather than adjusting the test.

- [ ] **Step 7: Prove it from outside the package**

The tests above live inside this package, where `src/` is reachable even though they do not import it. The spec asks for proof that a *consumer* can do this with exported API only, so the example app gets the same test. `example/pubspec.yaml` already has `flutter_test` as a dev dependency and depends on the package by path, so nothing needs adding.

Create `example/test/reverb_fake_test.dart`:

```dart
import 'package:flutter_test/flutter_test.dart';
import 'package:laravel_reverb/testing.dart';

void main() {
  TestWidgetsFlutterBinding.ensureInitialized();

  test('a listener fires for an emitted event', () async {
    final fake = ReverbFake();
    await fake.connect();

    Map<String, dynamic>? received;
    fake.reverb.channel('orders').listen(
          'OrderCreated',
          (Map<String, dynamic> data) => received = data,
        );
    await Future<void>.delayed(Duration.zero);

    fake.emit('orders', r'App\Events\OrderCreated', <String, dynamic>{'id': 7});
    await Future<void>.delayed(Duration.zero);

    expect(received, <String, dynamic>{'id': 7});
    fake.dispose();
  });
}
```

Run it from the example directory:

```bash
cd example && flutter pub get && flutter test && cd ..
```

Expected: PASS. If it fails to resolve `package:laravel_reverb/testing.dart`, the new library is not reachable from outside the package — check that `lib/testing.dart` is at the package root's `lib/`, not under `lib/src/`.

- [ ] **Step 8: Document it in the README**

Add a `## Testing your app` section immediately before the existing `## Migrating from pusher_channels_flutter` section:

````markdown
## Testing your app

`package:laravel_reverb/testing.dart` gives you a real client on a fake
socket, so your listeners can be tested without a running Reverb server:

```dart
import 'package:laravel_reverb/testing.dart';

test('the orders screen shows a new order', () async {
  final fake = ReverbFake();
  await fake.connect();

  Map<String, dynamic>? received;
  fake.reverb.channel('orders').listen('OrderCreated', (d) => received = d);
  await Future<void>.delayed(Duration.zero);

  fake.emit('orders', r'App\Events\OrderCreated', {'id': 7});
  await Future<void>.delayed(Duration.zero);

  expect(received, {'id': 7});
  fake.dispose();
});
```

The client is real — subscribe, dispatch and teardown run the same code they
run in production, so the test proves what a server would prove. `emit` takes
wire names (`private-users.1`, `App\Events\OrderCreated`), `sent` is every
frame your code sent, and `drop()` kills the socket so you can test whatever
your `onReconnected` does.

Private and presence channels authorize against a canned signature, so no auth
endpoint is needed.
````

- [ ] **Step 9: Verify and commit**

Run: `dart format --set-exit-if-changed . && flutter analyze && flutter test`
Expected: all three pass.

```bash
git add lib/testing.dart lib/src/testing/in_memory_socket.dart test/testing_test.dart test/support/fake_socket.dart example/test/reverb_fake_test.dart README.md
git commit -m "feat: ReverbFake for testing host applications

A real Reverb client on an in-memory socket, exported from
package:laravel_reverb/testing.dart. The package's own fake socket moves
to lib/ so both share one implementation; the test helper keeps its old
names so no existing test changes."
```

---

### Task 6: 0.5.0 release prep

**Files:**
- Modify: `lib/src/reverb.dart` (`clientVersion`), `pubspec.yaml`, `CHANGELOG.md`

**Interfaces:**
- Consumes: everything above.
- Produces: nothing — this is the last task.

- [ ] **Step 1: Bump both versions**

In `pubspec.yaml`, set `version: 0.5.0`.
In `lib/src/reverb.dart`, set `static const String clientVersion = '0.5.0';`.

`test/version_test.dart` (added in 0.4.0) fails if only one is changed.

- [ ] **Step 2: Run the suite to confirm the bump is consistent**

Run: `flutter test`
Expected: PASS, including `version_test.dart` against the new `0.5.0`.

- [ ] **Step 3: Write the CHANGELOG entry**

Add at the top of `CHANGELOG.md`, above the `## 0.4.0` heading:

```markdown
## 0.5.0

Observability and testability, on a `reverb.dart` that no longer does
everything itself. All additive — a 0.4.0 app that changes nothing behaves
identically.

- **`ReverbFake`, for testing your app.** `package:laravel_reverb/testing.dart`
  hands you a real client on an in-memory socket: `emit` an event as the server
  would, assert your listener fired, inspect everything the client `sent`, and
  `drop()` the socket to exercise your `onReconnected`. Subscribe, dispatch and
  teardown all run the production code paths — only the socket is fake. Private
  and presence channels authorize against a canned signature, so no auth
  endpoint is needed.
- **`reverb.metrics`.** A snapshot of connection quality: the last ping/pong
  round trip, how many drops have been recovered from, how stale the socket is,
  and when the current one connected. Read it when you paint — nothing is
  streamed, because latency changes on every ping.
- **`reverb.dart` split into part files.** The connect loop, channel registry
  and health tracking now live in three mixins in their own files, with a base
  class holding the fields and naming the cross-boundary calls explicitly. No
  public API changed and no test changed; this is groundwork that made the two
  features above small edits to focused files instead of more weight on an
  787-line one.
- **New test seam:** the `Reverb` constructor takes an optional `now`, joining
  `socketFactory`, `random` and `httpClientFactory`. It lets a test drive
  latency and staleness without real time passing.
```

- [ ] **Step 4: Verify everything and commit**

Run: `dart format --set-exit-if-changed . && flutter analyze && flutter test`
Expected: all three pass.

```bash
git add lib/src/reverb.dart pubspec.yaml CHANGELOG.md
git commit -m "chore: release 0.5.0

ReverbFake and connection metrics, on a reverb.dart split into part
files. Additive throughout: no public API changed."
```

- [ ] **Step 5: Dry-run the publish**

Run: `flutter pub publish --dry-run`
Expected: no errors or warnings. Fix anything it reports before publishing.

Do **not** run the real `flutter pub publish` — that is Abdullah's call, not the implementer's.

---

## Notes for the implementer

- **Tasks 1-3 are code movement, not redesign.** If a moved method needs an edit beyond adding `@override` or qualifying `Reverb._maxAuthAttempts`, stop and report it — it means the seam is in the wrong place, and the plan would rather be corrected than worked around.
- **Do not touch the three counters.** `_generation` (connect loop), `_generations` (per channel name), `_clientEpoch` (per client) are near-identically named on purpose, and every comment about them is load-bearing. Moving them is fine; renaming, merging or "clarifying" them is not.
- **Task 5 Step 5 is the payoff for Step 3's discipline.** Keeping `InMemorySocket`'s member names identical to the old `FakeSocket` is what lets 130-odd existing tests keep passing untouched.
- The example app's **source** (`example/lib/main.dart`) is not in scope for any task. If a change would force an edit there, that is a breaking change and the plan is wrong. Task 5 Step 7 adds a file under `example/test/`, which is additive and does not touch the app itself.

## Deviations from the spec

Both are deliberate and flagged so a reviewer judges them rather than discovering them:

- **The metrics test uses an injected clock, not `fake_async`.** The spec's done-when says "a `fake_async` test drives latency through a simulated ping/pong exchange". `fake_async` fakes timers, not `DateTime.now()`, so driving latency through it would mean adding `package:clock` — a new dependency, against the global constraints. An injected `now` seam achieves the same determinism, needs no dependency, and matches the `socketFactory` / `random` / `httpClientFactory` seams the constructor already documents. The test is deterministic either way; only the mechanism differs.
- **`ReverbMetrics` carries `connectedSince`, which the spec did not name.** The spec lists latency, reconnect count and time-since-last-frame. `connectedSince` costs one nullable field, comes from state the connect loop already sets, and is what a "connected for 4m" indicator needs. If a reviewer judges it unrequested scope, delete the field and its assignments in Task 4 Steps 3 and 11 — nothing else depends on it.
