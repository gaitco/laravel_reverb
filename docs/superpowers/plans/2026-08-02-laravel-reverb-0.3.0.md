# laravel_reverb 0.3.0 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the four gaps that make `laravel_reverb` a downgrade for `domono/mobile`'s hand-rolled client: slow dead-socket detection, no per-channel health signal, delta-only presence, and no logout-safe disconnect.

**Architecture:** Two independent keepalive mechanisms in `Connection` (a periodic ping and an inbound-frame watchdog), each overriding one half of today's server-driven behaviour only when configured. A `_live` set plus a `ChannelHealth` stream in `Reverb` makes the intended-vs-acknowledged distinction observable. `PresenceChannel` gains an internal roster map. A client-level epoch makes handles held across a logout permanently inert.

**Tech Stack:** Dart/Flutter, `web_socket_channel`, `stream_channel`, `http`. Tests use `flutter_test` plus `fake_async`.

## Global Constraints

- Version `0.3.0`. **Additive only** — a 0.2.0 app that upgrades and changes nothing must behave identically. Every default reproduces current behaviour exactly.
- Do not add any dependency. Do not change any existing public signature except `disconnect()`, which gains an optional named parameter with a default.
- Every public member in `lib/` gets a `///` doc comment.
- Nothing throws into application code at runtime. The only new exception is `ArgumentError` from the `Reverb` constructor for an invalid keepalive pair — a programming error, thrown at the call site.
- `pubspec.yaml` `version` and `Reverb.clientVersion` are hand-synced; both must read `0.3.0` by the end.
- Two distinct epochs already exist or will exist and must never be conflated: `_generations` (per **channel** name, added in 0.2.0, prevents a stale in-flight subscribe) and the new client epoch (per **Reverb instance**, prevents post-logout revival). Name them distinctly.
- After every task: `dart format .`, `flutter analyze` (root and `example/`), `flutter test` — all clean. The suite is at 102 passing tests; all must keep passing.
- Never `await` a `StreamSubscription.cancel()` inside a `fakeAsync` zone — it never resolves without an `onCancel` handler, and the test hangs.
- Commit after every task with the message given in that task's final step.

---

### Task 1: Configurable keepalive

**Files:**
- Modify: `lib/src/connection.dart`
- Modify: `lib/src/reverb.dart` (constructor parameters, validation, pass-through)
- Test: `test/connection_test.dart`, `test/reverb_test.dart`

**Interfaces:**
- Consumes: existing `Connection({required Uri url, required SocketFactory socketFactory, void Function(String)? onLog})`.
- Produces: `Connection({..., Duration? pingInterval, Duration? watchdogTimeout})` and `Reverb({..., Duration? pingInterval, Duration? watchdogTimeout})`.

- [ ] **Step 1: Write the failing tests**

Add to `test/connection_test.dart`:

```dart
  test('pings on the configured interval regardless of server activity', () {
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
        socket.emitJson(<String, dynamic>{'event': 'pusher:pong', 'data': '{}'});
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
        socket.sentJson.where((Map<String, dynamic> f) =>
            f['event'] == 'pusher:ping'),
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
```

Add to `test/reverb_test.dart`:

```dart
  test('rejects a watchdog that cannot outlive the ping interval', () {
    expect(
      () => Reverb(
        host: 'localhost',
        appKey: 'key',
        pingInterval: const Duration(seconds: 15),
        watchdogTimeout: const Duration(seconds: 15),
      ),
      throwsArgumentError,
    );
    expect(
      () => Reverb(
        host: 'localhost',
        appKey: 'key',
        pingInterval: const Duration(seconds: 20),
        watchdogTimeout: const Duration(seconds: 15),
      ),
      throwsArgumentError,
    );
  });

  test('accepts a watchdog longer than the ping interval', () {
    expect(
      () => Reverb(
        host: 'localhost',
        appKey: 'key',
        pingInterval: const Duration(seconds: 10),
        watchdogTimeout: const Duration(seconds: 15),
      ),
      returnsNormally,
    );
  });
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `flutter test test/connection_test.dart test/reverb_test.dart`
Expected: FAIL — `No named parameter with the name 'pingInterval'`

- [ ] **Step 3: Add the two mechanisms to `Connection`**

Add the constructor parameters and fields:

```dart
  /// Creates a connection to [url]. Call [open] to actually connect.
  Connection({
    required this.url,
    required this.socketFactory,
    this.onLog,
    this.pingInterval,
    this.watchdogTimeout,
  });

  /// How often to send `pusher:ping` regardless of server activity.
  ///
  /// Null (the default) keeps the server-driven behaviour: ping only after
  /// the handshake's `activity_timeout` of silence.
  final Duration? pingInterval;

  /// How long the socket may go without ANY inbound frame before it is
  /// treated as dead and closed.
  ///
  /// Null (the default) keeps the server-driven behaviour: a missed pong
  /// after a ping is what declares the socket dead.
  final Duration? watchdogTimeout;
```

Add two timer fields alongside `_idleTimer` and `_pongTimer`:

```dart
  Timer? _pingTimer;
  Timer? _watchdogTimer;
```

Replace the body of `_restartIdleTimer` and its call sites with an activity handler. `_onMessage` currently calls `_restartIdleTimer()`; change that call to `_onActivity()`. `_onEstablished` also calls `_restartIdleTimer()`; change it to `_onActivity()` followed by `_startPingLoop()`.

```dart
  /// Called on every inbound frame, and once when the handshake lands.
  ///
  /// Death detection is whichever of the two mechanisms is configured: a
  /// watchdog on inbound silence, or the legacy pong deadline armed by
  /// [_restartIdleTimer]. They are independent, so an app can take fast
  /// detection without also taking a faster ping, or vice versa.
  void _onActivity() {
    _pongTimer?.cancel();
    _pongTimer = null;

    final watchdog = watchdogTimeout;
    if (watchdog != null) {
      _watchdogTimer?.cancel();
      _watchdogTimer = Timer(watchdog, () {
        onLog?.call(
          'reverb: no inbound frame in ${watchdog.inSeconds}s, closing socket',
        );
        unawaited(close());
      });
    }

    // The periodic ping owns ping scheduling when configured; otherwise fall
    // back to pinging only after activity_timeout of silence.
    if (pingInterval == null) _restartIdleTimer();
  }

  void _startPingLoop() {
    final interval = pingInterval;
    if (interval == null) return;
    _pingTimer?.cancel();
    _pingTimer = Timer.periodic(interval, (_) => _sendPing());
  }

  void _sendPing() {
    send(<String, dynamic>{
      'event': 'pusher:ping',
      'data': <String, dynamic>{},
    });
  }

  /// Legacy path: ping after [_activityTimeout] of silence, then treat a
  /// missing pong within the same window as a dead socket. Only the pong
  /// deadline is skipped when a watchdog is configured — the watchdog is
  /// already covering death detection.
  void _restartIdleTimer() {
    _idleTimer?.cancel();
    _idleTimer = Timer(_activityTimeout, () {
      _sendPing();
      if (watchdogTimeout != null) return;
      _pongTimer = Timer(_activityTimeout, () {
        onLog?.call('reverb: ping timed out, closing socket');
        unawaited(close());
      });
    });
  }
```

Extend `_stopTimers` to cancel the two new timers:

```dart
  void _stopTimers() {
    _idleTimer?.cancel();
    _idleTimer = null;
    _pongTimer?.cancel();
    _pongTimer = null;
    _pingTimer?.cancel();
    _pingTimer = null;
    _watchdogTimer?.cancel();
    _watchdogTimer = null;
  }
```

- [ ] **Step 4: Add the parameters and validation to `Reverb`**

Add to the constructor parameter list, after `handleAppLifecycle`:

```dart
    this.pingInterval,
    this.watchdogTimeout,
```

Add the fields with doc comments:

```dart
  /// How often to send `pusher:ping` regardless of server activity.
  ///
  /// Null (the default) pings only after the server's advertised
  /// `activity_timeout` of silence. Set it — with [watchdogTimeout] — when an
  /// app needs a half-open socket noticed in seconds rather than a minute.
  final Duration? pingInterval;

  /// How long the socket may go without any inbound frame before it is closed
  /// and the reconnect path takes over.
  ///
  /// Must be longer than [pingInterval] when both are set, or the watchdog
  /// would fire between pings and reconnect forever.
  final Duration? watchdogTimeout;
```

Add validation as the first statement of the constructor body (the body already begins with the authorizer branch — put this above it):

```dart
    final ping = pingInterval;
    final watchdog = watchdogTimeout;
    if (ping != null && watchdog != null && watchdog <= ping) {
      throw ArgumentError(
        'watchdogTimeout ($watchdog) must be longer than pingInterval '
        '($ping), otherwise the watchdog fires between pings and the client '
        'reconnects forever.',
      );
    }
```

Pass both through at every `Connection(...)` construction site in `_open()`:

```dart
      final connection = Connection(
        url: _url,
        socketFactory: _socketFactory,
        onLog: onLog,
        pingInterval: pingInterval,
        watchdogTimeout: watchdogTimeout,
      );
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `dart format . && flutter analyze && flutter test`
Expected: format clean, `No issues found!`, all tests PASS

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat: configurable ping interval and inbound-frame watchdog"
```

---

### Task 2: Per-channel health and the live-channel set

**Files:**
- Create: `lib/src/channel_health.dart`
- Modify: `lib/src/reverb.dart`
- Modify: `lib/laravel_reverb.dart`
- Test: `test/reverb_test.dart`, `test/exports_test.dart`

**Interfaces:**
- Consumes: `Reverb._onFrame`, `_channels`, `_unsubscribe`, `_onDropped`, `dispose` from Task 1's file state.
- Produces: `class ChannelHealth { const ChannelHealth({required String channel, required bool healthy}); final String channel; final bool healthy; }`, `Stream<ChannelHealth> get channelHealth`, `bool isSubscribed(String wireName)`.

- [ ] **Step 1: Write the failing tests**

Add to `test/reverb_test.dart`:

```dart
  test('reports a channel healthy once the server acknowledges it', () async {
    final socket = FakeSocket();
    final reverb = reverbFor(socket);
    final health = <ChannelHealth>[];
    reverb.channelHealth.listen(health.add);

    final connected = reverb.connect();
    socket.emitJson(handshakeFrame());
    await connected;

    reverb.channel('orders').listen('OrderCreated', (_) {});
    await settle();
    expect(reverb.isSubscribed('orders'), isFalse);

    socket.emitJson(<String, dynamic>{
      'event': 'pusher_internal:subscription_succeeded',
      'channel': 'orders',
      'data': '{}',
    });
    await settle();

    expect(reverb.isSubscribed('orders'), isTrue);
    expect(health.single.channel, 'orders');
    expect(health.single.healthy, isTrue);
  });

  test('reports a channel unhealthy on subscription_error', () async {
    final socket = FakeSocket();
    final reverb = reverbFor(socket);
    final health = <ChannelHealth>[];
    reverb.channelHealth.listen(health.add);

    final connected = reverb.connect();
    socket.emitJson(handshakeFrame());
    await connected;

    reverb.channel('orders').listen('OrderCreated', (_) {});
    await settle();
    socket.emitJson(<String, dynamic>{
      'event': 'pusher_internal:subscription_succeeded',
      'channel': 'orders',
      'data': '{}',
    });
    await settle();

    socket.emitJson(<String, dynamic>{
      'event': 'pusher:subscription_error',
      'channel': 'orders',
      'data': '{"type":"AuthError","status":403}',
    });
    await settle();

    expect(reverb.isSubscribed('orders'), isFalse);
    expect(health.last.healthy, isFalse);
    expect(reverb.state, ReverbState.connected);
  });

  test('marks every live channel unhealthy when the socket drops', () async {
    final socket = FakeSocket();
    final reverb = reverbFor(socket);
    final health = <ChannelHealth>[];

    final connected = reverb.connect();
    socket.emitJson(handshakeFrame());
    await connected;

    reverb.channel('orders').listen('A', (_) {});
    reverb.channel('stock').listen('B', (_) {});
    await settle();
    for (final String name in <String>['orders', 'stock']) {
      socket.emitJson(<String, dynamic>{
        'event': 'pusher_internal:subscription_succeeded',
        'channel': name,
        'data': '{}',
      });
    }
    await settle();

    reverb.channelHealth.listen(health.add);
    await socket.serverClose();
    await settle();

    expect(
      health.map((ChannelHealth h) => h.channel).toSet(),
      <String>{'orders', 'stock'},
    );
    expect(health.every((ChannelHealth h) => !h.healthy), isTrue);
    expect(reverb.isSubscribed('orders'), isFalse);
  });
```

Add to `test/exports_test.dart`, inside the existing test body:

```dart
    expect(
      const ChannelHealth(channel: 'orders', healthy: true).healthy,
      isTrue,
    );
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `flutter test test/reverb_test.dart test/exports_test.dart`
Expected: FAIL — `Undefined name 'ChannelHealth'`

- [ ] **Step 3: Create the type**

`lib/src/channel_health.dart`:

```dart
/// A per-channel up/down notification.
///
/// Deliberately separate from `ReverbState`, which only describes the socket.
/// A connected socket whose channel authorization was rejected is
/// `ReverbState.connected` and unhealthy here — that combination is what an
/// app should gate degraded-mode polling on.
class ChannelHealth {
  /// Creates a health notification.
  const ChannelHealth({required this.channel, required this.healthy});

  /// The wire channel name, including any `private-` or `presence-` prefix.
  ///
  /// This matches `Channel.name` rather than the bare name passed to
  /// `Reverb.private`, because only the wire name is unambiguous across
  /// public, private and presence channels.
  final String channel;

  /// Whether the server currently acknowledges this subscription.
  final bool healthy;

  @override
  String toString() => 'ChannelHealth($channel, healthy: $healthy)';
}
```

- [ ] **Step 4: Wire it into `Reverb`**

Add fields next to `_channels`:

```dart
  /// Channels the server has acknowledged with a subscription-succeeded frame.
  ///
  /// `_channels` is what we *intend* to be subscribed to and drives the
  /// reconnect pass; this is what is actually live right now. A channel whose
  /// authorization failed stays in `_channels` — so the next reconnect retries
  /// it — while being absent here.
  final Set<String> _live = <String>{};

  final StreamController<ChannelHealth> _channelHealthController =
      StreamController<ChannelHealth>.broadcast();
```

Add the public surface next to `states`:

```dart
  /// Per-channel up/down notifications.
  Stream<ChannelHealth> get channelHealth => _channelHealthController.stream;

  /// Whether the server currently acknowledges [wireName].
  ///
  /// Pass the wire name, including any prefix — `'private-users.1'`, not
  /// `'users.1'`.
  bool isSubscribed(String wireName) => _live.contains(wireName);
```

Add the emitter:

```dart
  void _setChannelHealth(String wireName, {required bool healthy}) {
    final changed = healthy ? _live.add(wireName) : _live.remove(wireName);
    if (!changed) return;
    if (_channelHealthController.isClosed) return;
    _channelHealthController
        .add(ChannelHealth(channel: wireName, healthy: healthy));
  }

  void _markAllChannelsDown() {
    for (final String name in _live.toList()) {
      _setChannelHealth(name, healthy: false);
    }
  }
```

In `_onFrame`, before the existing dispatch, handle the two protocol frames. The method already intercepts `pusher:error` and `pusher:subscription_error`; add the health calls there and add a case for the success frame:

```dart
    if (frame.event == 'pusher_internal:subscription_succeeded' &&
        frame.channel != null) {
      _setChannelHealth(frame.channel!, healthy: true);
      // Fall through: presence channels also listen for this event.
    }
```

and inside the existing `pusher:subscription_error` branch, before returning:

```dart
      if (frame.channel != null) {
        _setChannelHealth(frame.channel!, healthy: false);
      }
```

In `_unsubscribe`, after removing from `_channels`:

```dart
    _setChannelHealth(channel.name, healthy: false);
```

In `_onDropped` and in `disconnect()`, call `_markAllChannelsDown()` before changing state.

In the authorizer-retry exhaustion path (where the retry gives up after `_maxAuthAttempts`), add:

```dart
      _setChannelHealth(channel.name, healthy: false);
```

In `dispose()`, close the controller alongside `_states`:

```dart
    unawaited(_channelHealthController.close());
```

- [ ] **Step 5: Export it**

In `lib/laravel_reverb.dart`, add:

```dart
export 'src/channel_health.dart' show ChannelHealth;
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `dart format . && flutter analyze && flutter test`
Expected: format clean, `No issues found!`, all tests PASS

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "feat: per-channel health stream and live-channel set"
```

---

### Task 3: Presence roster snapshots

**Files:**
- Modify: `lib/src/channel.dart`
- Modify: `lib/src/reverb.dart` (clear rosters on socket drop)
- Test: `test/channel_test.dart`, `test/reverb_test.dart`

**Interfaces:**
- Consumes: `PresenceChannel.members({here, joining, leaving})`, `PresenceMember`, `_parseMembers`, `_parseMember` from `lib/src/channel.dart`.
- Produces: `PresenceChannel.members({roster, here, joining, leaving})`, `Map<String, PresenceMember> get currentMembers`, `void resetPresence()`.

- [ ] **Step 1: Write the failing tests**

Add to `test/channel_test.dart`:

```dart
  test('roster fires with the full set on seed and on every change', () {
    final harness = Harness();
    final channel = harness.presence('presence-room.5');
    final snapshots = <Map<String, PresenceMember>>[];

    channel.members(roster: snapshots.add);

    channel.dispatch('pusher_internal:subscription_succeeded', <String, dynamic>{
      'presence': <String, dynamic>{
        'ids': <String>['1', '2'],
        'hash': <String, dynamic>{
          '1': <String, dynamic>{'name': 'Ann'},
          '2': <String, dynamic>{'name': 'Bo'},
        },
      },
    });
    channel.dispatch('pusher_internal:member_added', <String, dynamic>{
      'user_id': '3',
      'user_info': <String, dynamic>{'name': 'Cy'},
    });
    channel.dispatch(
      'pusher_internal:member_removed',
      <String, dynamic>{'user_id': '1'},
    );

    expect(snapshots.map((Map<String, PresenceMember> m) => m.keys.toSet()), <Set<String>>[
      <String>{'1', '2'},
      <String>{'1', '2', '3'},
      <String>{'2', '3'},
    ]);
    expect(snapshots.last['3']!.info, <String, dynamic>{'name': 'Cy'});
  });

  test('currentMembers matches the latest roster and is unmodifiable', () {
    final harness = Harness();
    final channel = harness.presence('presence-room.5');

    expect(channel.currentMembers, isEmpty);

    channel.members(roster: (_) {});
    channel.dispatch('pusher_internal:subscription_succeeded', <String, dynamic>{
      'presence': <String, dynamic>{
        'ids': <String>['1'],
        'hash': <String, dynamic>{
          '1': <String, dynamic>{'name': 'Ann'},
        },
      },
    });

    expect(channel.currentMembers.keys, <String>['1']);
    expect(
      () => channel.currentMembers['2'] =
          const PresenceMember(id: '2', info: <String, dynamic>{}),
      throwsUnsupportedError,
    );
  });

  test('resetPresence clears the roster', () {
    final harness = Harness();
    final channel = harness.presence('presence-room.5');

    channel.members(roster: (_) {});
    channel.dispatch('pusher_internal:subscription_succeeded', <String, dynamic>{
      'presence': <String, dynamic>{
        'ids': <String>['1'],
        'hash': <String, dynamic>{'1': <String, dynamic>{}},
      },
    });
    expect(channel.currentMembers, isNotEmpty);

    channel.resetPresence();
    expect(channel.currentMembers, isEmpty);
  });

  test('a roster handler alone holds the channel subscribed', () {
    final harness = Harness();
    final channel = harness.presence('presence-room.5');

    final sub = channel.members(roster: (_) {});
    expect(harness.emptied, isEmpty);

    sub.cancel();
    expect(harness.emptied, <Channel>[channel]);
  });

  test('roster and the delta callbacks coexist', () {
    final harness = Harness();
    final channel = harness.presence('presence-room.5');
    var rosters = 0;
    PresenceMember? joined;

    channel.members(
      roster: (_) => rosters++,
      joining: (PresenceMember m) => joined = m,
    );
    channel.dispatch('pusher_internal:member_added', <String, dynamic>{
      'user_id': '3',
      'user_info': <String, dynamic>{'name': 'Cy'},
    });

    expect(rosters, 1);
    expect(joined!.id, '3');
  });
```

Add to `test/reverb_test.dart`:

```dart
  test('presence rosters clear when the socket drops', () async {
    final socket = FakeSocket();
    final reverb = reverbFor(socket);

    final connected = reverb.connect();
    socket.emitJson(handshakeFrame());
    await connected;

    final channel = reverb.presence('room.5')..members(roster: (_) {});
    await settle();
    socket.emitJson(<String, dynamic>{
      'event': 'pusher_internal:subscription_succeeded',
      'channel': 'presence-room.5',
      'data': '{"presence":{"ids":["1"],"hash":{"1":{"name":"Ann"}}}}',
    });
    await settle();
    expect(channel.currentMembers, isNotEmpty);

    await socket.serverClose();
    await settle();

    expect(channel.currentMembers, isEmpty);
  });
```

`reverb.presence('room.5')` returns a `PresenceChannel`, so `channel.currentMembers` resolves without a cast.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `flutter test test/channel_test.dart test/reverb_test.dart`
Expected: FAIL — `No named parameter with the name 'roster'`

- [ ] **Step 3: Add the roster to `PresenceChannel`**

Add the field and getter:

```dart
  final Map<String, PresenceMember> _members = <String, PresenceMember>{};

  /// The channel's current members, keyed by member id.
  ///
  /// Empty until the subscription is acknowledged, and cleared whenever the
  /// socket drops — membership does not survive a socket. The map is an
  /// unmodifiable view; mutate nothing through it.
  Map<String, PresenceMember> get currentMembers =>
      Map<String, PresenceMember>.unmodifiable(_members);

  /// Clears the roster. Called by `Reverb` when the socket drops; the
  /// resubscribe re-seeds it from the server.
  void resetPresence() => _members.clear();
```

Extend `members()` with the new callback. Register it against all three wire events so a snapshot is emitted on seed and on both deltas, and keep the existing three registrations exactly as they are:

```dart
  /// Registers membership callbacks and returns one cancelable handle.
  ///
  /// [roster] receives the channel's full current member set on subscribe and
  /// after every join or leave, so a caller never has to track membership
  /// itself. [here], [joining] and [leaving] are the delta form and still
  /// work; use whichever suits — a join/leave toast wants the delta, a seat
  /// map wants the roster.
  Subscription members({
    void Function(Map<String, PresenceMember> members)? roster,
    void Function(List<PresenceMember> members)? here,
    void Function(PresenceMember member)? joining,
    void Function(PresenceMember member)? leaving,
  }) {
    final subscription = Subscription._(this);

    if (roster != null) {
      subscription._register(
        _addListener(
          'pusher_internal:subscription_succeeded',
          (Map<String, dynamic> data) => roster(currentMembers),
        ),
      );
      subscription._register(
        _addListener(
          'pusher_internal:member_added',
          (Map<String, dynamic> data) => roster(currentMembers),
        ),
      );
      subscription._register(
        _addListener(
          'pusher_internal:member_removed',
          (Map<String, dynamic> data) => roster(currentMembers),
        ),
      );
    }

    // ... existing here / joining / leaving registrations, unchanged ...

    return subscription;
  }
```

The roster map must already be updated by the time any callback runs, so update `_members` in `dispatch` rather than in a listener. Override `dispatch` on `PresenceChannel`:

```dart
  @override
  void dispatch(String wireEvent, Map<String, dynamic> data) {
    // Update membership BEFORE notifying listeners, so a roster callback and
    // a joining callback observe the same state for one event.
    switch (wireEvent) {
      case 'pusher_internal:subscription_succeeded':
        _members
          ..clear()
          ..addEntries(_parseMembers(data).map(
            (PresenceMember m) => MapEntry<String, PresenceMember>(m.id, m),
          ));
      case 'pusher_internal:member_added':
        final member = _parseMember(data);
        _members[member.id] = member;
      case 'pusher_internal:member_removed':
        _members.remove(_parseMember(data).id);
      default:
        break;
    }
    super.dispatch(wireEvent, data);
  }
```

- [ ] **Step 4: Clear rosters on socket drop in `Reverb`**

Add a helper next to `_markAllChannelsDown`:

```dart
  void _resetPresenceRosters() {
    for (final Channel channel in _channels.values) {
      if (channel is PresenceChannel) channel.resetPresence();
    }
  }
```

Call it from the same two places that call `_markAllChannelsDown()` — `_onDropped` and `disconnect()`.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `dart format . && flutter analyze && flutter test`
Expected: format clean, `No issues found!`, all tests PASS

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat: presence roster snapshots alongside the delta callbacks"
```

---

### Task 4: `disconnect(forget:)` and the client epoch

**Files:**
- Modify: `lib/src/reverb.dart`
- Modify: `lib/src/channel.dart` (carry the epoch a channel was created under)
- Test: `test/reverb_test.dart`

**Interfaces:**
- Consumes: `_channels`, `_live`, `_resubscribe`, `_register`, `disconnect()` from Tasks 1-3.
- Produces: `Future<void> disconnect({bool forget = false})`, `Channel.clientEpoch`.

- [ ] **Step 1: Write the failing tests**

Add to `test/reverb_test.dart`:

```dart
  test('disconnect(forget: true) drops every channel', () async {
    final socket = FakeSocket();
    final reverb = reverbFor(socket);

    final connected = reverb.connect();
    socket.emitJson(handshakeFrame());
    await connected;

    reverb.channel('orders').listen('OrderCreated', (_) {});
    await settle();

    await reverb.disconnect(forget: true);
    await settle();

    expect(reverb.isSubscribed('orders'), isFalse);

    final second = FakeSocket();
    final revived = Reverb(
      host: 'localhost',
      port: 8080,
      appKey: 'key',
      useTls: false,
      socketFactory: factoryFor(second),
    );
    await revived.disconnect();

    // Reconnecting the original client must not resubscribe the old channel.
    final again = reverb.connect();
    socket.emitJson(handshakeFrame(socketId: 'second'));
    await again;
    await settle();

    expect(
      socket.sentJson
          .where((Map<String, dynamic> f) => f['event'] == 'pusher:subscribe')
          .length,
      1,
    );
  });

  test('a handle held across disconnect(forget: true) cannot revive', () async {
    final socket = FakeSocket();
    final reverb = reverbFor(socket);

    final connected = reverb.connect();
    socket.emitJson(handshakeFrame());
    await connected;

    final stale = reverb.channel('orders');
    final sub = stale.listen('OrderCreated', (_) {});
    await settle();

    await reverb.disconnect(forget: true);
    sub.cancel();
    await settle();

    final before = socket.sentJson.length;
    stale.listen('OrderCreated', (_) {});
    await settle();

    expect(socket.sentJson.length, before);
    expect(reverb.isSubscribed('orders'), isFalse);
  });

  test('plain disconnect still restores channels on reconnect', () async {
    final socket = FakeSocket();
    final reverb = reverbFor(socket);

    final connected = reverb.connect();
    socket.emitJson(handshakeFrame());
    await connected;

    reverb.channel('orders').listen('OrderCreated', (_) {});
    await settle();

    await reverb.disconnect();
    final again = reverb.connect();
    socket.emitJson(handshakeFrame(socketId: 'second'));
    await again;
    await settle();

    expect(
      socket.sentJson
          .where((Map<String, dynamic> f) => f['event'] == 'pusher:subscribe')
          .length,
      2,
    );
  });
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `flutter test test/reverb_test.dart`
Expected: FAIL — `No named parameter with the name 'forget'`

- [ ] **Step 3: Carry the epoch on `Channel`**

Add a constructor parameter and field to `Channel`, and thread it through `PrivateChannel` and `PresenceChannel` with `super.clientEpoch`:

```dart
  /// The `Reverb` client epoch this channel was created under.
  ///
  /// `Reverb.disconnect(forget: true)` bumps that epoch, which makes every
  /// handle created before the logout permanently inert — re-listening on one
  /// cannot resubscribe it under the next user's session.
  final int clientEpoch;
```

- [ ] **Step 4: Add the epoch and the flag to `Reverb`**

Add the field:

```dart
  /// Bumped by `disconnect(forget: true)`. Distinct from `_generations`,
  /// which is per channel name and guards a stale in-flight subscribe; this
  /// one is per client and guards post-logout revival.
  int _clientEpoch = 0;
```

Pass it at every `Channel`/`PrivateChannel`/`PresenceChannel` construction site in `channel()`, `private()` and `presence()`: `clientEpoch: _clientEpoch,`.

Reject stale handles at the top of `_resubscribe`:

```dart
    if (channel.clientEpoch != _clientEpoch) return;
```

Replace `disconnect`:

```dart
  /// Closes the socket and stops reconnecting.
  ///
  /// By default channels and listeners are kept, so a later [connect]
  /// restores them — this is what the app-lifecycle pause path wants.
  ///
  /// Pass `forget: true` on logout or a session wipe. That also drops every
  /// channel, cancels pending authorization retries, and makes every handle
  /// created before this call permanently inert, so a screen still holding a
  /// reference to the previous user's channel cannot resubscribe it under the
  /// next user's session. Callers get fresh handles from [channel],
  /// [private] or [presence] after the next login.
  Future<void> disconnect({bool forget = false}) async {
    _shouldRun = false;
    _generation++;
    _resetPresenceRosters();
    _markAllChannelsDown();

    if (forget) {
      _clientEpoch++;
      _channels.clear();
      _generations.clear();
    }

    final connection = _connection;
    _connection = null;
    await connection?.close();
    if (!_shouldRun) _setState(ReverbState.disconnected);
  }
```

The existing `_generation++` line already cancels in-flight connection work; bumping `_clientEpoch` and clearing `_generations` is what additionally strands pending per-channel auth retries, because their `current()` check compares against a `_generations` entry that no longer exists.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `dart format . && flutter analyze && flutter test`
Expected: format clean, `No issues found!`, all tests PASS

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat: disconnect(forget:) for logout, with a client epoch"
```

---

### Task 5: Release 0.3.0

**Files:**
- Modify: `pubspec.yaml`, `lib/src/reverb.dart` (`clientVersion`), `CHANGELOG.md`, `README.md`, `docs/roadmap-0.2.0.md`

**Interfaces:**
- Consumes: everything from Tasks 1-4.
- Produces: a publishable 0.3.0.

- [ ] **Step 1: Bump both versions**

`pubspec.yaml`: `version: 0.3.0`. `lib/src/reverb.dart`: `static const String clientVersion = '0.3.0';`. They are hand-synced and carry comments saying so.

- [ ] **Step 2: Write the changelog**

Prepend to `CHANGELOG.md`:

```markdown
## 0.3.0

Hardening drawn from a production Reverb client running in a realtime game.
All additive — a 0.2.0 app that changes nothing behaves identically.

- **Configurable keepalive.** `pingInterval` and `watchdogTimeout` let an app
  notice a half-open socket in seconds instead of the ~60s a server-driven
  `activity_timeout` allows. Both default to null, which is exactly the 0.2.0
  behaviour. `watchdogTimeout` must exceed `pingInterval`, or the constructor
  throws `ArgumentError`.
- **Per-channel health.** `channelHealth` is a `Stream<ChannelHealth>` of
  per-channel up/down transitions, and `isSubscribed(wireName)` reports the
  current value. A connected socket whose channel authorization was rejected
  is now observable, which is what degraded-mode polling should gate on.
- **Presence roster snapshots.** `members(roster: ...)` receives the full
  member set on subscribe and after every join or leave, and
  `channel.currentMembers` exposes it synchronously. The existing `here`,
  `joining` and `leaving` callbacks are unchanged.
- **`disconnect(forget: true)`** drops every channel and makes handles created
  before the call inert, so a screen holding the previous user's channel
  cannot resubscribe it under the next user's session. Plain `disconnect()` is
  unchanged.
```

- [ ] **Step 3: Document the new API in the README**

Add a `## Keepalive` section after `## Reconnection` covering `pingInterval`/`watchdogTimeout`, when to set them (realtime apps needing sub-15s detection), the requirement that the watchdog exceed the ping interval, and that the defaults match server-driven behaviour:

````markdown
```dart
final reverb = Reverb(
  host: 'api.example.com',
  appKey: 'your-reverb-app-key',
  pingInterval: Duration(seconds: 10),
  watchdogTimeout: Duration(seconds: 15),
);
```
````

Add a `## Channel health` section showing gating a fallback on it:

````markdown
```dart
reverb.channelHealth.listen((health) {
  if (!health.healthy) startPollingFallbackFor(health.channel);
});
```
````

Extend the existing presence recipe with the `roster` callback and `currentMembers`, and extend the `## Errors, state and teardown` bullets with `disconnect(forget: true)`.

- [ ] **Step 4: Retire the items this release delivered**

`docs/roadmap-0.2.0.md` lists deferred findings. Remove the two entries this release resolves — the presence end-to-end test gap and the disconnect-state-write test gap are both now covered — and leave the rest.

- [ ] **Step 5: Verify and commit**

Run:

```bash
dart format . && flutter analyze && flutter test && flutter pub publish --dry-run
```

Expected: format clean, `No issues found!`, all tests PASS, `Package has 0 warnings` once the tree is committed. Do NOT run a real publish — that is the maintainer's action.

```bash
git add -A
git commit -m "chore: release 0.3.0"
```

---

## Self-Review

**Spec coverage**

| Spec section | Task |
|---|---|
| §1 Configurable keepalive, defaults unchanged, ArgumentError validation | 1 |
| §2 `ChannelHealth`, `channelHealth`, `isSubscribed`, wire names | 2 |
| §3 `roster` callback, `currentMembers`, clear on drop, ref-count | 3 |
| §4 Intended vs live channel sets | 2 (`_live` is the split) |
| §5 `disconnect(forget:)`, client epoch, revival prevention | 4 |
| Testing list | 1-4 |
| Version sync, CHANGELOG, README | 5 |

No spec requirement is unassigned. §4 has no task of its own because `_live` — the data structure the spec describes — is what Task 2 introduces and what `channelHealth` is derived from; splitting them would produce a task with no observable deliverable.

**Type consistency** — `ChannelHealth({channel, healthy})` is defined in Task 2 and used in Tasks 2 and 5. `Channel.clientEpoch` is added in Task 4 Step 3 and read in Step 4. `resetPresence()` is defined in Task 3 Step 3 and called in Step 4 and again in Task 4's `disconnect`. `_markAllChannelsDown()` is defined in Task 2 and called in Tasks 2, 3 and 4. `pingInterval`/`watchdogTimeout` keep the same names on `Connection` and `Reverb`.

**Ordering note** — Task 4's `disconnect` body calls `_resetPresenceRosters()` and `_markAllChannelsDown()`, both introduced in Tasks 2 and 3. Tasks must run in order.
