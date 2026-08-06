# laravel_reverb 0.6.0 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move each field from `_ReverbBase` to the mixin that mutates it, give `ReverbFake` a raw-frame escape hatch and a real error before `connect()`, and close three recorded coverage gaps.

**Architecture:** 0.5.0 moved every method into a mixin but left all 29 fields on `_ReverbBase`, so any mixin can still mutate any field. Dart mixins can declare instance fields, so Tasks 1-3 push each field down to its owner, leaving the base holding only constructor-injected values plus `_connection` — the one field two mixins genuinely share. Tasks 4-5 are additive API and tests; Task 6 ships it.

**Tech Stack:** Dart / Flutter, `flutter_test`, `stream_channel`. No new dependencies.

**Spec:** `docs/superpowers/specs/2026-08-06-roadmap-0.6.0-design.md`

## Global Constraints

- Dart SDK `^3.5.0`, Flutter `>=3.24.0`. Do not raise either.
- **No new dependencies.** Not in `dependencies`, not in `dev_dependencies`.
- **Additive only.** An app written against 0.5.0 that changes nothing must behave identically. No exported name changes; `emitFrame` is the only public addition.
- **Tasks 1-3 change no behaviour whatsoever.** Every existing test must pass without being edited. The suite is **140** in the package and **1** in the example app; both counts hold through Task 3.
- Doc comments move with the code they document, **verbatim**. Do not reword, shorten or improve any comment you move.
- **No `// ignore:` comments.** `flutter analyze` must be clean without them.
- Every public declaration carries a dartdoc documenting *why*, not *what*.
- `part` files cannot declare imports. Every import stays in `lib/src/reverb.dart`.
- `dart format --set-exit-if-changed .`, `flutter analyze` and `flutter test` must all pass before every commit, plus `cd example && flutter test`.
- Run all commands from the package root, `/Users/abdullah/code/packages/flutter-reverb`.

## The target partition

Tasks 1-3 implement exactly this. Nothing else moves.

| Owner | Fields |
|---|---|
| `_ReverbHealth` | `_live`, `_channelHealthController` |
| `_ReverbChannels` | `_channels`, `_generations`, `_clientEpoch` |
| `_ReverbConnect` | `_state`, `_shouldRun`, `_everConnected`, `_attempt`, `_generation`, `_states`, `_reconnectedCallbacks`, `_pausedByLifecycle`, `_observing`, `_reconnectCount`, `_connectedSince` |
| `_ReverbBase` (stays) | `onError`, `onLog`, `handleAppLifecycle`, `pingInterval`, `watchdogTimeout`, `_url`, `_namespace`, `_socketFactory`, `_random`, `_now`, `_authorizer`, `_ownedHttpClient`, `_connection` |

29 fields today: 16 move, 13 stay.

`_connection` stays on the base deliberately. `_ReverbChannels._subscribe` and `_sendFor` need it to send; `_ReverbConnect._open` and `_onDropped` need it to manage the socket. Moving it into either would make the `on` chain circular.

## File Structure

| File | Change |
|---|---|
| `lib/src/reverb_base.dart` | Modify: shrinks from 29 fields to 13 |
| `lib/src/reverb_health.dart` | Modify: gains 2 fields, loses `_resetPresenceRosters` |
| `lib/src/reverb_channels.dart` | Modify: gains 3 fields, `_resetPresenceRosters`, and a new `_forgetAllChannels` |
| `lib/src/reverb_connect.dart` | Modify: gains 11 fields; `disconnect` calls `_forgetAllChannels` |
| `lib/testing.dart` | Modify: `emitFrame`, and a `StateError` before `connect()` |
| `test/testing_test.dart` | Modify: roster seeding, pre-connect error |
| `test/protocol_test.dart`, `test/reverb_test.dart` | Modify: two coverage gaps |
| `test/exports_test.dart` | Modify: detect an accidental extra export |
| `README.md`, `CHANGELOG.md`, `pubspec.yaml`, `lib/src/reverb.dart` | Modify (Tasks 4, 6) |

---

### Task 1: Health owns its own state

Smallest of the three field moves, first because it proves the pattern. It also unpicks the one misfiled method: `_resetPresenceRosters` walks `_channels` and resets presence rosters, which is a channels concern wearing a health label. Moving it out is what lets `_ReverbHealth` depend on nothing but the base.

**Files:**
- Modify: `lib/src/reverb_base.dart`, `lib/src/reverb_health.dart`, `lib/src/reverb_channels.dart`

**Interfaces:**
- Consumes: nothing.
- Produces: `_ReverbHealth` declares `_live` and `_channelHealthController`. `_resetPresenceRosters` now lives on `_ReverbChannels`. `_ReverbHealth`'s `on` clause stays `on _ReverbBase`.

- [ ] **Step 1: Record the baseline**

Run: `flutter test`
Expected: PASS at **140**. Every step in Tasks 1-3 must report the same number.

- [ ] **Step 2: Move the two fields**

Cut these from `lib/src/reverb_base.dart` (lines 97 and 99-101) and paste them into `mixin _ReverbHealth`, above `channelHealth`, with their doc comments verbatim:

```dart
  final Set<String> _live = <String>{};

  final StreamController<ChannelHealth> _channelHealthController =
      StreamController<ChannelHealth>.broadcast();
```

The `_live` declaration in the base carries a doc comment explaining that `_channels` is what we intend to be subscribed to while `_live` is what is actually live. Move it verbatim — it is the clearest statement of that distinction anywhere in the codebase.

- [ ] **Step 3: Move `_resetPresenceRosters` to `_ReverbChannels`**

Cut it from `lib/src/reverb_health.dart:32-39` and paste it into `mixin _ReverbChannels`, with its doc comment verbatim. Place it next to `_unsubscribe`, which is the other method that manipulates registry contents.

`_ReverbConnect` calls it and reaches it through `on ..., _ReverbChannels`, so no `on` clause changes.

- [ ] **Step 4: Verify `_ReverbHealth` is now self-contained**

Run: `grep -vE "^\s*///" lib/src/reverb_health.dart | grep -n "_channels\|_generation\|_clientEpoch\|_state\|_connection"`
Expected: **no output.** `_ReverbHealth`'s *code* should reference only `_live`, `_channelHealthController`, and the `ChannelHealth` type.

The `grep -vE` strips doc comments first, and that is deliberate: `_live`'s doc comment names `_channels` when explaining the difference between what we intend to be subscribed to and what is actually live. That prose is the reason to keep the comment, not a dependency — a grep that did not strip comments would flag it and be wrong.

If anything matches, that member also belongs elsewhere — stop and report it rather than moving something the plan did not name.

- [ ] **Step 5: Run the full suite**

Run: `flutter test`
Expected: PASS at 140, no test file edited.

- [ ] **Step 6: Verify and commit**

Run: `dart format --set-exit-if-changed . && flutter analyze && flutter test`
Expected: all three pass.

```bash
git add lib/src/reverb_base.dart lib/src/reverb_health.dart lib/src/reverb_channels.dart
git commit -m "refactor: _ReverbHealth owns _live and its controller

Also moves _resetPresenceRosters to _ReverbChannels, where the registry
it walks actually lives. _ReverbHealth now touches nothing but its own
two fields. No behaviour change."
```

---

### Task 2: Channels owns the registry and the epochs

Moves the three fields whose interaction is the subtlest code in the package, and replaces `disconnect`'s three-line reach into them with one call.

**Files:**
- Modify: `lib/src/reverb_base.dart`, `lib/src/reverb_channels.dart`, `lib/src/reverb_connect.dart`

**Interfaces:**
- Consumes: Task 1's structure.
- Produces: `_ReverbChannels` declares `_channels`, `_generations`, `_clientEpoch`, and a new `void _forgetAllChannels()`.

- [ ] **Step 1: Move the three fields**

Cut from `lib/src/reverb_base.dart` and paste into `mixin _ReverbChannels`, above `channel()`, **with their doc comments verbatim**:

- `_channels` (line 76)
- `_generations` (lines 78-89, whose doc comment explains why identity alone cannot tell a stale subscribe apart from a fresh one)
- `_clientEpoch` (lines 118-123, whose doc comment distinguishes it from `_generations`)

Those two doc comments are the only place the distinction between the three counters is written down. They must arrive character for character.

`_generation` — singular, the connect-loop counter — stays on the base for now and moves in Task 3. Do not touch it. Confusing it with `_generations` is the single most likely error in this task.

- [ ] **Step 2: Add `_forgetAllChannels`**

In `mixin _ReverbChannels`, next to `_unsubscribe`:

```dart
  /// Drops every channel and resets per-name generations, for
  /// `disconnect(forget: true)`.
  ///
  /// Bumping [_clientEpoch] is what makes every handle created before the
  /// call inert: [_resubscribe] refuses a channel whose epoch has moved on,
  /// and [_sendFor] turns its whispers into no-ops.
  void _forgetAllChannels() {
    _clientEpoch++;
    _channels.clear();
    // Resets the per-channel-name generation to 0 for every name. This is
    // not what stops a pending authorizer retry — _subscribe re-checks
    // `(_generations[name] ?? 0) == generation`, and a retry that captured
    // 0 still matches 0 after this clear. What actually strands it is
    // _channels.clear() just above: current()'s identity check
    // (`identical(_channels[channel.name], channel)`) fails once the
    // channel is gone from the registry, on top of the existing
    // socket-id guard. This clear exists so a channel name freed by
    // forget starts its next life at generation 0 instead of some
    // arbitrary leftover count.
    _generations.clear();
  }
```

The block comment is moved verbatim from `disconnect`, not rewritten.

- [ ] **Step 3: Call it from `disconnect`**

In `lib/src/reverb_connect.dart`, inside `disconnect`'s `if (forget) {` block, replace these three statements —

```dart
      _clientEpoch++;
      _channels.clear();
      // ...the long generations comment...
      _generations.clear();
```

— with:

```dart
      _forgetAllChannels();
```

Leave the rest of the block exactly as it is: `_reconnectedCallbacks.clear()` and `_everConnected = false` are connect-owned state with their own comment, which stays put.

**Do not move the `_resetPresenceRosters()` call**, which sits above the `if (forget)` block and runs unconditionally. Its preceding comment explains that it must run before the registry is cleared — that ordering is still load-bearing, and now depends on `_forgetAllChannels` being called after it.

- [ ] **Step 4: Run the full suite**

Run: `flutter test`
Expected: PASS at 140, no test file edited.

`test/reverb_test.dart` and `test/reconnect_test.dart` carry the epoch coverage; a failure there means a counter moved wrong.

- [ ] **Step 5: Verify and commit**

Run: `dart format --set-exit-if-changed . && flutter analyze && flutter test`
Expected: all three pass.

```bash
git add lib/src/reverb_base.dart lib/src/reverb_channels.dart lib/src/reverb_connect.dart
git commit -m "refactor: _ReverbChannels owns the registry and both epochs

_channels, _generations and _clientEpoch move to the mixin that mutates
them, and disconnect(forget:) calls _forgetAllChannels instead of
reaching into all three. No behaviour change."
```

---

### Task 3: Connect owns the loop's state

Completes the partition.

**Files:**
- Modify: `lib/src/reverb_base.dart`, `lib/src/reverb_connect.dart`

**Interfaces:**
- Consumes: Tasks 1-2.
- Produces: `_ReverbBase` holds 13 fields — the 12 constructor-injected values plus `_connection`.

- [ ] **Step 1: Move the eleven fields**

Cut from `lib/src/reverb_base.dart` and paste into `mixin _ReverbConnect`, above the `socketId` getter, **with their doc comments verbatim**:

`_reconnectCount`, `_connectedSince`, `_pausedByLifecycle`, `_observing`, `_states`, `_reconnectedCallbacks`, `_state`, `_shouldRun`, `_everConnected`, `_attempt`, `_generation`.

`_generation`'s doc comment — explaining that a fast `disconnect()`-then-`connect()` would otherwise race two live sockets into existence — is the reason that counter exists. Move it character for character.

**Leave `_connection` on the base.** It is the one shared field, and the partition depends on it staying put.

- [ ] **Step 2: Confirm the base is down to 13 fields**

**List them, do not count them with a regex.** Read `lib/src/reverb_base.dart` and write out every field the class still declares, then compare that list against the target partition table above.

Expected, exactly these 13 and nothing else:

`onError`, `onLog`, `handleAppLifecycle`, `pingInterval`, `watchdogTimeout`, `_url`, `_namespace`, `_socketFactory`, `_random`, `_now`, `_authorizer`, `_ownedHttpClient`, `_connection`

A regex is the wrong tool here and an earlier draft of this plan got it wrong twice: a character class for the type will miss the function-typed fields (`onError`, `onLog`, `_now` are all `Function` types containing parentheses), and a looser pattern picks up the constructor's initializer-list entries as if they were declarations. Enumerating thirteen names by eye is faster than debugging the pattern, and it checks identity rather than just quantity — a field left behind and a field moved to the wrong mixin produce the same count.

- [ ] **Step 3: Confirm `Reverb` still compiles against the moved state**

`Reverb.metrics` reads `_reconnectCount` and `_connectedSince`, and `dispose()` closes `_states` — all now on `_ReverbConnect`, which `Reverb` mixes in, so they resolve. No edit should be needed in `lib/src/reverb.dart`. If one is, report it: it means the partition is wrong, not that `reverb.dart` needs patching.

- [ ] **Step 4: Run the full suite and the example app**

Run: `flutter test && cd example && flutter test && cd ..`
Expected: 140 and 1, no test file edited.

- [ ] **Step 5: Verify and commit**

Run: `dart format --set-exit-if-changed . && flutter analyze && flutter test`
Expected: all three pass.

```bash
git add lib/src/reverb_base.dart lib/src/reverb_connect.dart
git commit -m "refactor: _ReverbConnect owns the connect loop's state

Completes the partition: _ReverbBase now holds only the twelve
constructor-injected values and _connection, the one field two mixins
genuinely share. No behaviour change."
```

---

### Task 4: `ReverbFake` gains a raw-frame escape hatch and a real error

**Files:**
- Modify: `lib/testing.dart`, `test/testing_test.dart`, `README.md`

**Interfaces:**
- Consumes: nothing from Tasks 1-3.
- Produces: `void emitFrame(Map<String, dynamic> frame)` on `ReverbFake`. `emit` and `drop` throw `StateError` when called before `connect()`.

- [ ] **Step 1: Write the failing tests**

Add to `test/testing_test.dart`:

```dart
  test('seeds a presence roster through emitFrame', () async {
    final fake = ReverbFake();
    await fake.connect();

    List<PresenceMember>? present;
    fake.reverb.presence('room.1').members(
          here: (List<PresenceMember> members) => present = members,
        );
    await Future<void>.delayed(Duration.zero);

    fake.emitFrame(<String, dynamic>{
      'event': 'pusher_internal:subscription_succeeded',
      'channel': 'presence-room.1',
      'data': <String, dynamic>{
        'presence': <String, dynamic>{
          'ids': <String>['1', '2'],
          'hash': <String, dynamic>{
            '1': <String, dynamic>{'name': 'Ada'},
            '2': <String, dynamic>{'name': 'Linus'},
          },
        },
      },
    });
    await Future<void>.delayed(Duration.zero);

    expect(present!.map((PresenceMember m) => m.id), <String>['1', '2']);
    expect(present!.first.info['name'], 'Ada');
    fake.dispose();
  });

  test('emit before connect explains itself', () {
    final fake = ReverbFake();

    expect(
      () => fake.emit('orders', 'X'),
      throwsA(
        isA<StateError>().having(
          (StateError e) => e.message,
          'message',
          contains('connect()'),
        ),
      ),
    );
    fake.dispose();
  });
```

`PresenceMember` comes from `package:laravel_reverb/laravel_reverb.dart`; add that import if the file does not already have it.

- [ ] **Step 2: Run them to verify they fail**

Run: `flutter test test/testing_test.dart`
Expected: FAIL — `emitFrame` is not defined, and `emit` throws a null-check error whose message does not contain `connect()`.

- [ ] **Step 3: Implement both**

In `lib/testing.dart`, add a private accessor that produces the real error, and use it in `emit` and `drop`:

```dart
  InMemorySocket get _liveSocket {
    final socket = _socket;
    if (socket == null) {
      throw StateError(
        'ReverbFake: call connect() before emit(), emitFrame() or drop() — '
        'there is no socket until the client has connected.',
      );
    }
    return socket;
  }
```

Name it `_liveSocket`, not `_live` — `_live` is already the `Set<String>` of live channel names on `_ReverbHealth`, and a second meaning for that word in the same package is exactly the confusion this release is trying to reduce.

Replace `_socket!` in `emit` and `drop` with `_liveSocket`. Then add `emitFrame` immediately after `emit`:

```dart
  /// Delivers [frame] exactly as written, for what [emit] cannot express.
  ///
  /// [emit] always builds an application event with a channel and a data
  /// payload, which is the common case. Two things need more than that:
  /// seeding a presence roster, which arrives as a
  /// `pusher_internal:subscription_succeeded` frame carrying `presence.ids`
  /// and `presence.hash`; and connection-level frames such as `pusher:error`,
  /// which carry no channel at all. Pass the frame the server would send.
  void emitFrame(Map<String, dynamic> frame) => _liveSocket.emitJson(frame);
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `flutter test test/testing_test.dart`
Expected: PASS, all tests in the file.

- [ ] **Step 5: Document roster seeding in the README**

In the `## Testing your app` section, after the existing example and before the closing paragraph about private and presence channels, add:

````markdown
Presence rosters are seeded with `emitFrame`, which sends a frame verbatim —
`emit` always builds an application event, and a roster arrives as a protocol
frame instead:

```dart
fake.reverb.presence('room.1').members(here: (members) => print(members));

fake.emitFrame({
  'event': 'pusher_internal:subscription_succeeded',
  'channel': 'presence-room.1',
  'data': {
    'presence': {
      'ids': ['1', '2'],
      'hash': {
        '1': {'name': 'Ada'},
        '2': {'name': 'Linus'},
      },
    },
  },
});
```

`emitFrame` is also how you deliver a channel-less frame such as
`pusher:error`.
````

- [ ] **Step 6: Verify and commit**

Run: `dart format --set-exit-if-changed . && flutter analyze && flutter test && cd example && flutter test && cd ..`
Expected: all pass.

```bash
git add lib/testing.dart test/testing_test.dart README.md
git commit -m "feat: ReverbFake.emitFrame, and a real error before connect

emit builds an application event, so presence rosters and channel-less
frames were unreachable. emitFrame sends a frame verbatim. Calling emit
or drop before connect() now names the mistake instead of throwing a
null-check error from inside the fake."
```

---

### Task 5: Close three recorded coverage gaps

Each was found during an earlier release's review, judged correct by inspection, and deferred as untested. This closes all three.

**Files:**
- Modify: `test/protocol_test.dart`, `test/reverb_test.dart`, `test/exports_test.dart`

**Interfaces:**
- Consumes: nothing.
- Produces: nothing.

- [ ] **Step 1: Assert the all-slashes path**

`buildSocketUrl`'s slash normalization strips leading and trailing slashes, so a `path` of `'/'` empties to `''` and falls back to `/app/KEY`. Traced by hand during the 0.4.0 review; never asserted.

Add to the existing `group('buildSocketUrl', ...)` in `test/protocol_test.dart`:

```dart
    test('treats a path of only slashes as no path at all', () {
      final url = buildSocketUrl(
        host: 'api.example.com',
        port: 443,
        appKey: 'abc',
        useTls: true,
        clientVersion: '0.6.0',
        path: '///',
      );

      expect(url.path, '/app/abc');
    });
```

- [ ] **Step 2: Cover cache miss on the prefixed channel types**

The end-to-end `pusher:cache_miss` test covers a plain `cache-` channel. `private-cache-` and `presence-cache-` traverse identical code — nothing on that path inspects the prefix — but the README names all three.

Add to `test/reverb_test.dart`, next to the existing cache-miss test:

```dart
  test('delivers pusher:cache_miss on private- and presence- cache channels',
      () async {
    final socket = FakeSocket();
    final reverb = reverbFor(socket);

    final connected = reverb.connect();
    socket.emitJson(handshakeFrame());
    await connected;

    final seen = <String>[];
    reverb
        .private('cache-orders')
        .listen('pusher:cache_miss', (_) => seen.add('private-cache-orders'));
    reverb
        .presence('cache-room')
        .listen('pusher:cache_miss', (_) => seen.add('presence-cache-room'));
    await settle();

    for (final String name in <String>[
      'private-cache-orders',
      'presence-cache-room',
    ]) {
      socket.emitJson(<String, dynamic>{
        'event': 'pusher:cache_miss',
        'channel': name,
      });
    }
    await settle();

    expect(seen, <String>['private-cache-orders', 'presence-cache-room']);
    reverb.dispose();
  });
```

Note the channel names: `private('cache-orders')` produces the wire name `private-cache-orders`, which is the real Reverb prefix order.

- [ ] **Step 3: Make `exports_test.dart` notice an extra export**

Open since the 0.2.0 roadmap. The file is a hand-maintained compile check: it fails when something stops being exported and stays silent when something starts being exported that should not be.

Dart has no runtime reflection here, so read the entry point and compare its `show` clauses against an expected set — the same file-reading approach `test/version_test.dart` already uses.

Add to `test/exports_test.dart` (it needs `import 'dart:io';`):

```dart
  test('the entry point exports exactly the documented API', () {
    // The rest of this file fails when something stops being exported. This
    // one fails when something *starts* being exported — an accidental
    // addition is otherwise invisible until it is someone's breaking change
    // to remove.
    const expected = <String>{
      'Authorizer',
      'Channel',
      'ChannelHealth',
      'PresenceChannel',
      'PresenceMember',
      'PrivateChannel',
      'Reverb',
      'ReverbAuth',
      'ReverbAuthException',
      'ReverbConnectionClosed',
      'ReverbEventCallback',
      'ReverbException',
      'ReverbFatalError',
      'ReverbMetrics',
      'ReverbProtocolError',
      'ReverbState',
      'ReverbSubscriptionError',
      'Subscription',
    };

    final source = File('lib/laravel_reverb.dart').readAsStringSync();
    final actual = RegExp(r'show\s+([^;]+);')
        .allMatches(source)
        .expand((RegExpMatch m) => m.group(1)!.split(','))
        .map((String name) => name.trim())
        .where((String name) => name.isNotEmpty)
        .toSet();

    expect(actual, expected);
  });
```

The set above was verified against `lib/laravel_reverb.dart` as it stands — 18 names, matching exactly. If your run disagrees, something changed after this plan was written: the file is the truth and the list is what needs correcting. Say so in your report rather than editing the export list to match the test.

One detail the regex depends on: `dart format` wraps the longest `show` clause across several lines, so `show` and its names are not on one line. `[^;]` matches newlines, so reading the file as a single string handles that — do not switch to a line-by-line scan.

- [ ] **Step 4: Run the suite**

Run: `flutter test`
Expected: PASS. The count rises from 142 to 145 — Task 4 added two tests, so the 140 baseline that Tasks 1-3 held is no longer current.

- [ ] **Step 5: Verify and commit**

Run: `dart format --set-exit-if-changed . && flutter analyze && flutter test`
Expected: all three pass.

```bash
git add test/protocol_test.dart test/reverb_test.dart test/exports_test.dart
git commit -m "test: close three gaps recorded in earlier reviews

An all-slashes path, cache misses on private- and presence- cache
channels, and an exports check that notices an accidental addition
rather than only an accidental removal."
```

---

### Task 6: 0.6.0 release prep

**Files:**
- Modify: `lib/src/reverb.dart` (`clientVersion`), `pubspec.yaml`, `CHANGELOG.md`

**Interfaces:**
- Consumes: everything above.
- Produces: nothing.

- [ ] **Step 1: Bump both versions**

In `pubspec.yaml`, set `version: 0.6.0`.
In `lib/src/reverb.dart`, set `static const String clientVersion = '0.6.0';`.

`test/version_test.dart` fails if only one changes.

- [ ] **Step 2: Run the suite**

Run: `flutter test`
Expected: PASS at 145, including `version_test.dart` against the new `0.6.0`.

- [ ] **Step 3: Write the CHANGELOG entry**

Add at the top of `CHANGELOG.md`, above `## 0.5.0`:

```markdown
## 0.6.0

Cleanup. No new capability, and nothing an existing app has to change.

- **`ReverbFake.emitFrame`.** `emit` builds an application event, which left
  two things unreachable from a test: seeding a presence roster, which arrives
  as a `pusher_internal:subscription_succeeded` frame, and connection-level
  frames such as `pusher:error`, which carry no channel. `emitFrame` sends a
  frame verbatim. The README shows roster seeding.
- **`ReverbFake` explains itself before `connect()`.** Calling `emit`,
  `emitFrame` or `drop` on a fake that has not connected threw a null-check
  error from inside the fake; it now throws a `StateError` naming the mistake.
- **Each mixin owns its own state.** 0.5.0 split `Reverb` into three mixins but
  left all 29 fields on the shared base, so any mixin could still mutate any
  field. The registry and both epoch counters now live with the channel code,
  liveness with the health code, and the connect loop's state with the connect
  loop — leaving only constructor-injected values and the socket itself shared.
  Internal throughout: no public API changed and no test changed.
- Test coverage for an all-slashes `path`, cache misses on `private-cache-` and
  `presence-cache-` channels, and an exports check that catches an accidental
  addition rather than only an accidental removal.
```

- [ ] **Step 4: Verify everything and commit**

Run: `dart format --set-exit-if-changed . && flutter analyze && flutter test && cd example && flutter test && cd ..`
Expected: all pass.

```bash
git add lib/src/reverb.dart pubspec.yaml CHANGELOG.md
git commit -m "chore: release 0.6.0

Field ownership moves into the mixins, ReverbFake gains emitFrame and a
real pre-connect error, and three recorded coverage gaps close."
```

- [ ] **Step 5: Dry-run the publish**

Run: `flutter pub publish --dry-run`
Expected: 0 errors, 0 warnings.

Do **not** run the real `flutter pub publish` — that is Abdullah's call, not the implementer's.

---

## Notes for the implementer

- **Tasks 1-3 are field movement, not redesign.** If a moved field needs an edit beyond changing which file it sits in, stop and report it — it means the partition is wrong, and the plan would rather be corrected than worked around.
- **`_generation` and `_generations` are different fields with different owners.** Singular is the connect-loop counter and moves in Task 3; plural is per-channel-name and moves in Task 2. Confusing them is the most likely error in this plan, and the tests that would catch it are in `test/reconnect_test.dart`.
- **Do not touch `_connection`.** It stays on the base by design; the partition depends on it.
- The 0.6.0 spec records one open item that is deliberately *not* a task: the README's Android cleartext claim is unverified and needs a `flutter run` on an API 28+ emulator to settle. Leave the hedged wording alone.
