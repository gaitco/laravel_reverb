# laravel_reverb 0.4.0 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship the Reverb-native gaps this package still has — custom server path and cache channels — plus the docs and positioning that make the package honest about its scope.

**Architecture:** Both features are small, surgical edits to `lib/src/protocol.dart`. The path change adds a parameter to `buildSocketUrl` and threads it from the `Reverb` constructor. Cache channels need no new API at all: Reverb already replays a cache hit as an ordinary event, so the only change is teaching `resolveEventName` to pass `pusher:`-prefixed names through literally, which routes `pusher:cache_miss` to the existing dispatch path. Tasks 3 and 4 are documentation and release prep.

**Tech Stack:** Dart / Flutter, `flutter_test`, `web_socket_channel`, `stream_channel`. No new dependencies.

**Spec:** `docs/superpowers/specs/2026-08-06-roadmap-0.4-0.5-design.md`

## Global Constraints

- Dart SDK `^3.5.0`, Flutter `>=3.24.0`. Do not raise either.
- **No new dependencies.** Not in `dependencies`, not in `dev_dependencies`.
- **Additive only.** An app written against 0.3.0 that changes nothing must behave identically. No parameter loses its default, no exported name changes.
- Every public declaration carries a dartdoc comment. Match the surrounding density — this package documents *why*, not *what*.
- `dart format --set-exit-if-changed .`, `flutter analyze` and `flutter test` must all pass before every commit. These are exactly what CI runs (`.github/workflows/ci.yaml`).
- Run all commands from the package root, `/Users/abdullah/code/packages/flutter-reverb`.

## File Structure

| File | Responsibility | Change |
|---|---|---|
| `lib/src/protocol.dart` | Wire-level helpers: frame parsing, URL building, event-name resolution | Modify: `buildSocketUrl` gains `path`; `resolveEventName` gains a `pusher:` passthrough |
| `lib/src/reverb.dart` | Client facade | Modify: constructor gains `path`, forwards it; `clientVersion` bumped |
| `test/protocol_test.dart` | Unit tests for the above | Modify: new cases in the `buildSocketUrl` and `resolveEventName` groups |
| `test/reverb_test.dart` | End-to-end tests through a fake socket | Modify: path wiring test, cache-miss delivery test |
| `test/version_test.dart` | Guards `clientVersion` against `pubspec.yaml` drift | Create |
| `README.md` | User-facing docs | Modify: path env mapping, cache-channel recipe, local-development section, scope rewrite |
| `CHANGELOG.md` | Release notes | Modify: 0.4.0 entry |
| `pubspec.yaml` | Package manifest | Modify: version 0.4.0 |

---

### Task 1: Custom server path

Reverb exposes `REVERB_SERVER_PATH` (`config/reverb.php`), letting the server sit at a subpath behind a reverse proxy. This package hardcodes `/app/$appKey`, so those deployments cannot connect at all. This is the only genuine defect on the README's "not supported" list.

**Files:**
- Modify: `lib/src/protocol.dart:56-75` (`buildSocketUrl`)
- Modify: `lib/src/reverb.dart:53-79` (constructor parameter list and `_url` initializer)
- Test: `test/protocol_test.dart` (the `buildSocketUrl` group, currently at line 52)
- Test: `test/reverb_test.dart`
- Modify: `README.md` (the "Laravel setup" section, lines 50-64)

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `buildSocketUrl({required String host, required int port, required String appKey, required bool useTls, required String clientVersion, String path = ''})` returning `Uri`. `Reverb`'s constructor gains `String path = ''`. Task 4 references neither.

- [ ] **Step 1: Write the failing unit tests**

Add these three tests inside the existing `group('buildSocketUrl', ...)` in `test/protocol_test.dart`, after the two tests already there:

```dart
    test('prefixes a custom server path onto the app path', () {
      final url = buildSocketUrl(
        host: 'api.example.com',
        port: 443,
        appKey: 'abc',
        useTls: true,
        clientVersion: '0.4.0',
        path: '/ws',
      );

      expect(url.path, '/ws/app/abc');
    });

    test('normalizes a path given with or without slashes', () {
      Uri urlFor(String path) => buildSocketUrl(
            host: 'api.example.com',
            port: 443,
            appKey: 'abc',
            useTls: true,
            clientVersion: '0.4.0',
            path: path,
          );

      expect(urlFor('ws').path, '/ws/app/abc');
      expect(urlFor('/ws').path, '/ws/app/abc');
      expect(urlFor('/ws/').path, '/ws/app/abc');
      expect(urlFor('/realtime/ws/').path, '/realtime/ws/app/abc');
    });

    test('leaves the app path alone when no path is given', () {
      final url = buildSocketUrl(
        host: 'api.example.com',
        port: 443,
        appKey: 'abc',
        useTls: true,
        clientVersion: '0.4.0',
        path: '',
      );

      expect(url.path, '/app/abc');
    });
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `flutter test test/protocol_test.dart`
Expected: FAIL — `No named parameter with the name 'path'`.

- [ ] **Step 3: Implement the path parameter**

In `lib/src/protocol.dart`, replace `buildSocketUrl` with:

```dart
/// Builds the Reverb socket URL for [appKey].
///
/// [path] mirrors Reverb's `REVERB_SERVER_PATH`: a server hosted behind a
/// reverse proxy at `/ws` serves the app endpoint at `/ws/app/KEY`. Leading
/// and trailing slashes are optional, so `ws`, `/ws` and `/ws/` are the same
/// path.
Uri buildSocketUrl({
  required String host,
  required int port,
  required String appKey,
  required bool useTls,
  required String clientVersion,
  String path = '',
}) {
  final prefix = path.replaceAll(RegExp(r'^/+|/+$'), '');

  return Uri(
    scheme: useTls ? 'wss' : 'ws',
    host: host,
    port: port,
    path: prefix.isEmpty ? '/app/$appKey' : '/$prefix/app/$appKey',
    queryParameters: <String, String>{
      'protocol': '7',
      'client': 'flutter',
      'version': clientVersion,
    },
  );
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `flutter test test/protocol_test.dart`
Expected: PASS, all tests in the file.

- [ ] **Step 5: Write the failing constructor-wiring test**

The unit tests above prove the URL builder. This proves `Reverb` actually forwards the parameter — the `socketFactory` seam receives the real URL. Add to `test/reverb_test.dart`, inside `main()`, after the first test:

```dart
  test('connects to the custom server path when one is configured', () async {
    final socket = FakeSocket();
    Uri? dialled;

    final reverb = Reverb(
      host: 'localhost',
      port: 8080,
      appKey: 'key',
      useTls: false,
      path: '/ws',
      socketFactory: (Uri url) {
        dialled = url;
        return socket.channel;
      },
    );

    final connected = reverb.connect();
    socket.emitJson(handshakeFrame());
    await connected;

    expect(dialled!.path, '/ws/app/key');
    reverb.dispose();
  });
```

- [ ] **Step 6: Run it to verify it fails**

Run: `flutter test test/reverb_test.dart --plain-name 'custom server path'`
Expected: FAIL — `No named parameter with the name 'path'`.

- [ ] **Step 7: Add the constructor parameter**

In `lib/src/reverb.dart`, add `String path = ''` to the constructor parameter list immediately after `bool useTls = true,` (line 57), and pass it through in the `_url` initializer:

```dart
        _url = buildSocketUrl(
          host: host,
          port: port ?? (useTls ? 443 : 80),
          appKey: appKey,
          useTls: useTls,
          clientVersion: clientVersion,
          path: path,
        ) {
```

Then extend the constructor's dartdoc (the block starting at line 40) with a paragraph placed after the existing "Provide either [authorizer] or [authEndpoint]" paragraph:

```dart
  /// [path] mirrors Reverb's `REVERB_SERVER_PATH` for a server behind a
  /// reverse proxy on a subpath: `path: '/ws'` dials `/ws/app/KEY` instead of
  /// `/app/KEY`. Leave it empty for a Reverb server at the root, which is the
  /// default deployment.
```

- [ ] **Step 8: Run the full suite**

Run: `flutter test`
Expected: PASS. Every pre-existing test still passes — `path` defaults to `''`, which is the 0.3.0 URL exactly.

- [ ] **Step 9: Document the env-var mapping in the README**

In `README.md`, in the "Laravel setup" section, add `REVERB_SERVER_PATH=` to the `.env` block and extend the sentence that maps env vars onto constructor arguments so it reads:

```markdown
These map directly onto the `Reverb` constructor: `REVERB_APP_KEY` to
`appKey`, `REVERB_HOST` to `host`, `REVERB_PORT` to `port`, and
`REVERB_SERVER_PATH` to `path` — set that last one only if your Reverb server
sits behind a reverse proxy on a subpath, such as `/ws`.
```

- [ ] **Step 10: Verify formatting and analysis, then commit**

Run: `dart format --set-exit-if-changed . && flutter analyze && flutter test`
Expected: all three pass.

```bash
git add lib/src/protocol.dart lib/src/reverb.dart test/protocol_test.dart test/reverb_test.dart README.md
git commit -m "feat: custom Reverb server path

Mirrors REVERB_SERVER_PATH, so a Reverb server behind a reverse proxy on
a subpath is reachable. Defaults to empty, which builds the 0.3.0 URL."
```

---

### Task 2: Cache channels

Reverb's `cache-`, `private-cache-` and `presence-cache-` channels replay the last broadcast event to a new subscriber. A cache **hit** already works today: the server sends the ordinary event frame, which the existing dispatch path delivers. A cache **miss** sends `pusher:cache_miss`, which reaches `_onFrame` (`lib/src/reverb.dart:744`), finds no listener registered under that name, and is dropped — so an app cannot distinguish "nothing has been broadcast yet" from "still waiting".

The fix is one condition in `resolveEventName`. A bare name is namespaced (`CacheMiss` → `App\Events\CacheMiss`), so `pusher:cache_miss` is currently mangled into `App\Events\pusher:cache_miss` and never matches. Passing `pusher:`-prefixed names through literally makes `listen('pusher:cache_miss', ...)` work with no new API.

**Files:**
- Modify: `lib/src/protocol.dart:88-97` (`resolveEventName`)
- Test: `test/protocol_test.dart` (the `resolveEventName` group, currently at line 108)
- Test: `test/reverb_test.dart`
- Modify: `README.md` (new recipe in the "Recipes" section, and the "Event names" section)

**Interfaces:**
- Consumes: nothing from Task 1.
- Produces: `resolveEventName(String name, String namespace)` keeps its signature; names starting with `pusher:` are now returned unchanged.

- [ ] **Step 1: Write the failing unit tests**

Add to the existing `group('resolveEventName', ...)` in `test/protocol_test.dart`:

```dart
    test('passes a pusher protocol event through literally', () {
      expect(
        resolveEventName('pusher:cache_miss', r'App\Events'),
        'pusher:cache_miss',
      );
    });

    test('still namespaces a bare name that merely mentions cache', () {
      expect(
        resolveEventName('CacheMiss', r'App\Events'),
        r'App\Events\CacheMiss',
      );
    });
```

- [ ] **Step 2: Run them to verify the first fails**

Run: `flutter test test/protocol_test.dart --plain-name 'pusher protocol event'`
Expected: FAIL — expected `pusher:cache_miss`, actual `App\Events\pusher:cache_miss`.

- [ ] **Step 3: Implement the passthrough**

In `lib/src/protocol.dart`, replace `resolveEventName` with:

```dart
/// Resolves an Echo-style event name to the name that appears on the wire.
///
/// A bare name is namespaced (`OrderCreated` becomes `App\Events\OrderCreated`).
/// A leading `.` marks a literal `broadcastAs()` name, and a leading `\` marks a
/// fully qualified class name; both are returned with the marker stripped.
///
/// A `pusher:` prefix marks a protocol event rather than an application one —
/// `pusher:cache_miss` is the reachable example — and is returned untouched,
/// since namespacing it could never match anything on the wire.
String resolveEventName(String name, String namespace) {
  if (name.startsWith('pusher:')) return name;
  if (name.startsWith('.') || name.startsWith(r'\')) return name.substring(1);
  if (namespace.isEmpty) return name;
  return '$namespace\\$name';
}
```

- [ ] **Step 4: Run the unit tests to verify they pass**

Run: `flutter test test/protocol_test.dart`
Expected: PASS, all tests in the file.

- [ ] **Step 5: Write the failing end-to-end test**

This proves the frame actually survives the trip through `Connection` and `Reverb._onFrame` to a channel listener — the unit test only proves name resolution. Add to `test/reverb_test.dart`, inside `main()`:

```dart
  test('delivers pusher:cache_miss to a listener on a cache channel', () async {
    final socket = FakeSocket();
    final reverb = reverbFor(socket);

    final connected = reverb.connect();
    socket.emitJson(handshakeFrame());
    await connected;

    Map<String, dynamic>? missed;
    reverb.channel('cache-orders').listen(
          'pusher:cache_miss',
          (Map<String, dynamic> data) => missed = data,
        );
    await settle();

    socket.emitJson(<String, dynamic>{
      'event': 'pusher:cache_miss',
      'channel': 'cache-orders',
      'data': '{}',
    });
    await settle();

    expect(missed, isNotNull);
    reverb.dispose();
  });
```

- [ ] **Step 6: Run it to verify it passes**

Run: `flutter test test/reverb_test.dart --plain-name 'cache_miss'`
Expected: PASS. It passes because Step 3 already landed — the point of this test is to lock the whole path down, since nothing else covers a protocol event reaching a channel listener.

If it FAILS, stop: the dispatch path at `lib/src/reverb.dart:742-744` is not routing the frame, and the cause needs diagnosing before continuing.

- [ ] **Step 7: Document cache channels in the README**

Add this recipe to `README.md` at the end of the "Recipes" section, after "Whisper (client events)":

````markdown
### Cache channel

Reverb's `cache-`, `private-cache-` and `presence-cache-` channels replay the
last event broadcast on them to each new subscriber, so a screen that opens
after the fact still gets current state without a REST round-trip.

A cache hit arrives as an ordinary event — nothing special to write:

```dart
reverb.channel('cache-scoreboard').listen(
  'ScoreUpdated',
  (Map<String, dynamic> data) => setState(() => score = data['score'] as int),
);
```

A cache **miss** — nothing has been broadcast on that channel yet — arrives as
the protocol event `pusher:cache_miss`. Listen for it when "no value yet" needs
different handling from "still connecting":

```dart
reverb.channel('cache-scoreboard').listen(
  'pusher:cache_miss',
  (_) => setState(() => score = 0),
);
```
````

Then extend the "Event names" section with a closing paragraph:

```markdown
Names beginning with `pusher:` are protocol events, not application events, and
are never namespaced — `listen('pusher:cache_miss')` matches that wire event
exactly.
```

- [ ] **Step 8: Verify and commit**

Run: `dart format --set-exit-if-changed . && flutter analyze && flutter test`
Expected: all three pass.

```bash
git add lib/src/protocol.dart test/protocol_test.dart test/reverb_test.dart README.md
git commit -m "feat: listen for pusher protocol events, unblocking cache channels

A pusher:-prefixed name now resolves literally instead of being
namespaced, so pusher:cache_miss reaches a listener. Cache hits already
worked: Reverb replays them as ordinary events."
```

---

### Task 3: Local development documentation

Where first-time users bounce. Documentation only — no code changes, so the deliverable is a README section a developer can follow end to end from `php artisan reverb:start` to a connected app on both simulators.

**Files:**
- Modify: `README.md` (new section placed immediately after "Laravel setup")

**Interfaces:**
- Consumes: the `path` parameter from Task 1 is *not* used here; nothing else.
- Produces: nothing consumed by later tasks.

- [ ] **Step 1: Add the "Local development" section**

Insert into `README.md` between the "Laravel setup" and "How it fits together" sections:

````markdown
## Local development

A Reverb server started with `php artisan reverb:start` listens on
`0.0.0.0:8080` over plain HTTP. Three things bite in that setup, all of them
platform quirks rather than package behaviour:

**The host differs per platform.** `localhost` on a device means the device,
not your Mac:

| Target | `host` |
|---|---|
| iOS simulator | `localhost` |
| Android emulator | `10.0.2.2` |
| Physical device | Your machine's LAN IP, e.g. `192.168.1.20` |

```dart
final reverb = Reverb(
  host: Platform.isAndroid ? '10.0.2.2' : 'localhost',
  port: 8080,
  appKey: 'your-reverb-app-key',
  useTls: false,
  authEndpoint: 'http://10.0.2.2:8000/broadcasting/auth',
);
```

**Android blocks cleartext.** On API 28 and above, `ws://` is refused before
the socket is ever opened, which surfaces as a connection failure with no
server-side log. Allow it for debug builds only, in
`android/app/src/debug/AndroidManifest.xml`:

```xml
<manifest xmlns:android="http://schemas.android.com/apk/res/android">
    <application android:usesCleartextTraffic="true" />
</manifest>
```

Putting this in the debug manifest keeps release builds cleartext-free.

**Self-signed TLS is not worth it locally.** If you terminate TLS in front of
Reverb with a self-signed certificate, Dart rejects it, and there is no hook
in this package to override that — `useTls: false` over the local network is
the simpler answer. Use real certificates in staging and production, where
`useTls: true` and `port: 443` are the defaults.

### It connects on iOS but not Android

In order, the three usual causes:

1. `host` is `localhost` — an Android emulator needs `10.0.2.2`.
2. Cleartext is blocked — add the debug manifest above.
3. `authEndpoint` still points at `localhost` — it needs the same
   platform-specific host as the socket.

Pass `onLog: print` and `onError: (e, _) => print(e)` to the constructor while
debugging; between them, every connection attempt and every failure is
visible.
````

- [ ] **Step 2: Verify each claim against the repo before committing**

This is documentation about behaviour, so check it rather than trusting it:

- Confirm `useTls: false` still defaults the port to 80 and `true` to 443 — `lib/src/reverb.dart:75`.
- Confirm `onLog` and `onError` are both constructor parameters — `lib/src/reverb.dart:62-63`.
- Confirm the package exposes no TLS/certificate override, so the self-signed claim is accurate: `grep -rn "badCertificate\|SecurityContext\|HttpClient" lib/` returns nothing.

Fix the text if any check disagrees.

- [ ] **Step 3: Verify and commit**

Run: `dart format --set-exit-if-changed . && flutter analyze && flutter test`
Expected: all three pass (nothing changed but Markdown).

```bash
git add README.md
git commit -m "docs: local development setup for emulator, cleartext and TLS"
```

---

### Task 4: Scope rewrite and 0.4.0 release prep

The README's comparison section reads as a shortfall list. Two of its three items are Pusher-hosted features that Reverb itself does not implement — verified against `laravel/reverb`, which contains zero occurrences of "encrypt" — and the third ships in Task 1. Rewriting it as a scope statement is the point of the release.

This task also adds a guard for a documented paper-cut: `Reverb.clientVersion` is hand-synced with `pubspec.yaml`, and it ships in the socket URL that Task 1 just changed. **Note:** the spec left this open as non-blocking housekeeping; it is folded in here deliberately because the release step is exactly where the two versions drift.

**Files:**
- Modify: `README.md` (the "Is this the right package for you?" section, lines 28-42)
- Create: `test/version_test.dart`
- Modify: `lib/src/reverb.dart:111` (`clientVersion`)
- Modify: `pubspec.yaml` (`version:`)
- Modify: `CHANGELOG.md`

**Interfaces:**
- Consumes: the `path` parameter from Task 1 (referenced in the CHANGELOG entry and removed from the README's unsupported list). The cache-channel support from Task 2 (referenced in the CHANGELOG entry).
- Produces: nothing — this is the last task.

- [ ] **Step 1: Write the failing version-sync test**

Create `test/version_test.dart`:

```dart
import 'dart:io';

import 'package:flutter_test/flutter_test.dart';
import 'package:laravel_reverb/src/reverb.dart';

void main() {
  test('clientVersion matches the version in pubspec.yaml', () {
    // clientVersion ships in the socket URL as `version=`, so a stale constant
    // silently misreports this client to every server it connects to. Nothing
    // but this test keeps the two in step.
    final pubspec = File('pubspec.yaml').readAsStringSync();
    final declared = RegExp(r'^version:\s*(\S+)', multiLine: true)
        .firstMatch(pubspec)!
        .group(1)!;

    expect(Reverb.clientVersion, declared);
  });
}
```

- [ ] **Step 2: Run it to verify it passes right now**

Run: `flutter test test/version_test.dart`
Expected: PASS — both are `0.3.0` today. This test is a guard, not a red-green cycle: it must pass before the bump and keep passing after it. If it FAILS now, the two are already out of sync and that is the bug to fix first.

- [ ] **Step 3: Bump both versions**

In `pubspec.yaml`, set `version: 0.4.0`.
In `lib/src/reverb.dart:111`, set `static const String clientVersion = '0.4.0';`.

Also update the comment above `version:` in `pubspec.yaml`, which claims nothing enforces the match:

```yaml
# Kept in sync with Reverb.clientVersion in lib/src/reverb.dart —
# test/version_test.dart fails if the two drift.
```

And the matching dartdoc on `clientVersion` in `lib/src/reverb.dart:107-110`:

```dart
  /// The package version reported to the server in the socket URL.
  ///
  /// Kept in sync with `pubspec.yaml`'s `version:`; `test/version_test.dart`
  /// fails if the two drift, so bump both together on every release.
```

- [ ] **Step 4: Run the suite to verify the bump is consistent**

Run: `flutter test`
Expected: PASS, including `version_test.dart` against the new `0.4.0`.

- [ ] **Step 5: Rewrite the scope section in the README**

Replace the whole "Is this the right package for you?" section (lines 28-42) with:

```markdown
## Is this the right package for you?

This is a client for **self-hosted Laravel Reverb**, and its scope is
deliberately that. It does not support Pusher's hosted service, clusters or
API-key configuration, and it does not implement encrypted channels
(`private-encrypted-`) — Reverb has no server-side support for them either, so
they are a Pusher-hosted feature rather than something missing here.

If you are on Pusher's hosted service,
[`pusher_reverb_flutter`](https://pub.dev/packages/pusher_reverb_flutter) is a
mature, actively maintained alternative and the better choice.

Pick this one if you run Reverb yourself and want ref-counted channel teardown,
app lifecycle handling, or Echo-compatible event names. Both are MIT and speak
the same protocol, so switching either direction is a mechanical change.
```

Custom WebSocket paths are gone from the list because Task 1 shipped them.

- [ ] **Step 6: Write the CHANGELOG entry**

Add at the top of `CHANGELOG.md`, above the `## 0.3.0` heading:

```markdown
## 0.4.0

Reverb-native gaps, closed. All additive — a 0.3.0 app that changes nothing
behaves identically.

- **Custom server path.** `path` mirrors Reverb's `REVERB_SERVER_PATH`, so a
  server behind a reverse proxy at `/ws` is reachable: the client dials
  `/ws/app/KEY` instead of `/app/KEY`. Leading and trailing slashes are
  optional. Defaults to empty, which is the 0.3.0 URL exactly.
- **Cache channels.** `cache-`, `private-cache-` and `presence-cache-` channels
  now work end to end. A cache hit always arrived as an ordinary event; the
  miss did not arrive at all, because `pusher:cache_miss` was namespaced into
  an event name no server sends. Any `pusher:`-prefixed name now resolves
  literally, so `listen('pusher:cache_miss', ...)` reaches a listener.
- **Documented local development** — emulator hosts, Android cleartext, and
  why self-signed TLS is not worth it locally.
- **Scope, stated plainly.** Encrypted channels and Pusher clusters are out of
  scope because Reverb does not implement them, not because they are pending.
  The README says so instead of listing them as shortfalls.
```

- [ ] **Step 7: Verify everything and commit**

Run: `dart format --set-exit-if-changed . && flutter analyze && flutter test`
Expected: all three pass.

```bash
git add README.md CHANGELOG.md pubspec.yaml lib/src/reverb.dart test/version_test.dart
git commit -m "chore: release 0.4.0

Custom server path and cache channels, plus a scope statement that stops
listing Pusher-hosted features as shortfalls. Adds a test tying
clientVersion to pubspec.yaml so the socket URL cannot ship a stale
version."
```

- [ ] **Step 8: Dry-run the publish**

Run: `flutter pub publish --dry-run`
Expected: no errors or warnings. Fix anything it reports before publishing.

Do **not** run the real `flutter pub publish` — that is Abdullah's call, not the implementer's.

---

## Notes for the implementer

- **Do not add a `path` to the auth endpoint.** `authEndpoint` is a full URL the host supplies; `path` only affects the WebSocket URL.
- **Task 2's Step 6 is a passing test, not a red-green cycle.** That is intentional and called out in the step. Every other test in this plan fails first.
- **Task 4 exceeds the spec by one item** (the version-sync test). It is flagged in the task. If Abdullah wants strict spec fidelity, drop Step 1 and Step 2 and revert the two comment edits in Step 3; nothing else depends on them.
