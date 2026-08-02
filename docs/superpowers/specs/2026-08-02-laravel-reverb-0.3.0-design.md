# laravel_reverb 0.3.0 — Design

**Date:** 2026-08-02
**Status:** Awaiting approval
**Version:** 0.3.0 — additive only, no breaking changes to the 0.2.0 API

## Problem

`domono/mobile` runs a 596-line hand-rolled Reverb client across 25 call sites, including
WebRTC voice signalling and presence-driven seat rosters. Reading it against
`laravel_reverb` 0.2.0 showed the package would be a **downgrade** for that app in four
specific ways. This release closes those gaps so migrating domono is an upgrade, and so
`bidi` and `dahab` get the same hardening.

Two things in domono's client also independently validate 0.2.0's design: it abandoned
`pusher_channels_flutter` because the Dart wrapper never forwards `host` on Android (every
connection silently dialled `ws-mt1.pusher.com`), and it hit the same stale-auth-after-socket-
recycle bug — 4009 "Connection is unauthorized" spinning a reconnect loop — that 0.1.0's
socket-id guard exists to prevent.

## Scope

Four features plus one method signature change. Everything is additive: a 0.2.0 app that
upgrades and changes nothing keeps its current behaviour exactly.

---

## 1. Configurable keepalive

**Today:** `Connection` pings only after `activity_timeout` of silence, then waits another
`activity_timeout` for a pong before declaring the socket dead. Reverb advertises 30s, so
worst-case detection is **~60s**. For a turn-based game that is 60s of an opponent's move
frozen behind a half-open socket.

**Change:** two optional constructor parameters, threaded through to `Connection`.

```dart
Reverb(
  pingInterval: Duration(seconds: 10),     // send pusher:ping unconditionally
  watchdogTimeout: Duration(seconds: 15),  // no inbound frame in this window = dead
)
```

- **Defaults are unchanged behaviour.** Both null → today's server-driven idle/pong logic.
  Nobody pays for detection speed they did not ask for.
- When `pingInterval` is set, a periodic timer sends `pusher:ping` regardless of activity.
- When `watchdogTimeout` is set, **any** inbound frame (event, server ping, pong) defers it.
  On expiry the socket is closed, which routes into the existing reconnect path — it is a
  drop, not a new state.
- **Validation:** `watchdogTimeout` must exceed `pingInterval`, otherwise the watchdog is
  guaranteed to fire between pings and the client reconnects forever. This is a programming
  error, so it throws `ArgumentError` from the constructor rather than reporting at runtime.
- Setting one without the other is allowed; each independently overrides its half.

## 2. Per-channel health

**Today:** a `subscription_error` reaches `onError` and stops there. Nothing a UI can
subscribe to distinguishes "socket is up but this channel's auth was rejected" from
"everything is fine", so an app cannot fall back to polling for just the dead channel.

**Change:**

```dart
class ChannelHealth {
  final String channel;   // wire name, matching Channel.name
  final bool healthy;
}

Stream<ChannelHealth> get channelHealth;
bool isSubscribed(String wireName);
```

Emits `healthy: true` on `pusher_internal:subscription_succeeded`, and `healthy: false` on
`pusher:subscription_error`, on authorizer failure once retries are exhausted, and for every
live channel when the socket drops. It is deliberately separate from `ReverbState`: a
connected socket with a rejected channel is `ReverbState.connected` and channel-unhealthy,
and that combination is exactly what consumers need to gate degraded-mode polling on.

The stream carries the **wire** name (`private-users.1`), matching `Channel.name`, because
that is the only name that is unambiguous across public/private/presence.

## 3. Presence roster snapshots

**Today:** `members(here:, joining:, leaving:)` emits deltas, so every consumer rebuilds the
roster itself. domono's voice code wants "who is in this channel right now" and would get
worse on our API.

**Change:** an additional `roster` callback and a synchronous getter, alongside the existing
three.

```dart
final sub = channel.members(
  roster: (Map<String, PresenceMember> all) => ...,  // full set, on subscribe and every change
  here: (List<PresenceMember> initial) => ...,       // unchanged
  joining: (PresenceMember m) => ...,                // unchanged
  leaving: (PresenceMember m) => ...,                // unchanged
);

channel.currentMembers;  // unmodifiable Map<String, PresenceMember>, empty when not subscribed
```

`PresenceChannel` maintains the map internally: seeded from `subscription_succeeded`, updated
on `member_added`/`member_removed`, and **cleared when the socket drops** — membership does
not survive a socket, and is re-seeded by the resubscribe. `roster` fires on seed and on every
mutation. Each `roster` handler counts toward the channel's ref-count like any other listener.

## 4. Intended vs live channels

**Today:** one registry, `_channels`. A channel whose authorizer failed all its retries sits
in it looking identical to a healthy one.

**Change:** keep `_channels` as the set of channels we *intend* to be subscribed to, and add a
`_live` set of those the server has acknowledged. `isSubscribed` reads `_live`; `channelHealth`
is driven by transitions in and out of it; the reconnect path continues to re-subscribe
everything in `_channels`, which is what lets an auth-failed channel recover on the next
reconnect. Mostly this makes an existing distinction observable rather than changing behaviour.

## 5. `disconnect({bool forget = false})`

**Today:** `disconnect()` keeps channels so a later `connect()` restores them, which is what
the app-lifecycle pause path needs.

**Change:** an optional `forget` flag for the logout case. `disconnect(forget: true)` also
clears `_channels` and `_live`, cancels pending auth retries, and — importantly — **prevents
stale handles from reviving**.

That last point is the subtle one. 0.2.0 added revival: re-listening on a handle whose last
listener was cancelled puts it back in the registry and resubscribes. After a logout that is a
data-leak risk in reverse — a screen still holding a handle to `private-users.7` could
resubscribe it under the *next* user's session. So `forget: true` bumps a client-level epoch
that handles captured before it cannot pass, making every pre-logout handle permanently inert.
Callers get fresh handles from `reverb.private(...)` after the next login, which is what the
auth-watching providers already do.

`disconnect()` with no argument is byte-for-byte the current behaviour.

## Non-goals

- Encrypted channels (`private-encrypted-`) — still out of scope; `pusher_reverb_flutter`
  covers that case and the README says so.
- Automatic degraded-mode polling. `channelHealth` is the signal; what an app does with it is
  the app's business.
- Changing the default keepalive. Conservative by default is deliberate.

## Testing

Every item needs a test that fails against 0.2.0:

- ping fires on the configured interval independent of server activity; watchdog fires on
  silence and closes the socket; **any** inbound frame defers the watchdog; `watchdogTimeout <=
  pingInterval` throws `ArgumentError`; defaults reproduce 0.2.0 timing exactly.
- `channelHealth` emits true on subscription success, false on `subscription_error`, false for
  every live channel on socket drop, and false after auth retries are exhausted; `isSubscribed`
  tracks it.
- `roster` fires on seed and on both member deltas with the full set; `currentMembers` matches;
  membership clears on socket drop and re-seeds on resubscribe; roster handlers hold the
  ref-count open.
- `disconnect(forget: true)` clears the registry, cancels pending retries, and a handle held
  across it cannot revive; plain `disconnect()` still restores on reconnect.

The keepalive tests need `fakeAsync`. Note the standing hazard: never `await` a
`StreamSubscription.cancel()` inside a `fakeAsync` zone — it never resolves without an
`onCancel` handler.

## Risks

- **The watchdog is a new way to kill a live socket.** A too-tight `watchdogTimeout` turns a
  healthy connection into a reconnect loop. Mitigated by the constructor validation, by
  defaulting off, and by documenting the relationship to `pingInterval`.
- **The revival epoch interacts with 0.2.0's per-channel epoch.** These are different
  mechanisms at different scopes and must not be conflated: the per-channel epoch prevents a
  stale in-flight subscribe, the client epoch prevents post-logout revival. Name them
  distinctly.
- **Roster handlers affect ref-counting**, so a presence channel with only a `roster` callback
  must stay subscribed — easy to get wrong given `members()` already registers up to three.
