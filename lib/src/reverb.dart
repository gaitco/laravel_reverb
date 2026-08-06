import 'dart:async';
import 'dart:math' as math;

import 'package:flutter/widgets.dart';
import 'package:http/http.dart' as http;
import 'package:web_socket_channel/web_socket_channel.dart';

import 'auth.dart';
import 'channel.dart';
import 'channel_health.dart';
import 'connection.dart';
import 'protocol.dart';

export 'channel_health.dart' show ChannelHealth;

part 'reverb_base.dart';
part 'reverb_channels.dart';
part 'reverb_connect.dart';
part 'reverb_health.dart';

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
class Reverb extends _ReverbBase with _ReverbHealth, _ReverbChannels {
  /// Creates a client. Call [connect] to open the socket.
  ///
  /// Provide either [authorizer] or [authEndpoint] to use private and presence
  /// channels; public channels need neither.
  ///
  /// [path] mirrors Reverb's `REVERB_SERVER_PATH` for a server behind a
  /// reverse proxy on a subpath: `path: '/ws'` dials `/ws/app/KEY` instead of
  /// `/app/KEY`. Leave it empty for a Reverb server at the root, which is the
  /// default deployment.
  ///
  /// [socketFactory], [random] and [httpClientFactory] are test seams, not
  /// for application use: they let tests substitute a fake socket, a seeded/
  /// deterministic source of backoff jitter, and a trackable HTTP client.
  /// `socketFactory`'s type, [SocketFactory], is internal and not exported,
  /// so it cannot be named outside this package — pass a matching function
  /// literal instead. [httpClientFactory] is only consulted when
  /// [authEndpoint] is set and [authorizer] is not, since that is the only
  /// case where this class creates an `http.Client` of its own. All three
  /// parameters are marked `@visibleForTesting`.
  Reverb({
    required String host,
    required String appKey,
    int? port,
    bool useTls = true,
    String path = '',
    String? authEndpoint,
    Future<Map<String, String>> Function()? authHeaders,
    Authorizer? authorizer,
    super.namespace = r'App\Events',
    super.onError,
    super.onLog,
    super.handleAppLifecycle = true,
    super.pingInterval,
    super.watchdogTimeout,
    @visibleForTesting SocketFactory? socketFactory,
    @visibleForTesting math.Random? random,
    @visibleForTesting http.Client Function()? httpClientFactory,
  }) : super(
          url: buildSocketUrl(
            host: host,
            port: port ?? (useTls ? 443 : 80),
            appKey: appKey,
            useTls: useTls,
            clientVersion: clientVersion,
            path: path,
          ),
          socketFactory: socketFactory ?? WebSocketChannel.connect,
          random: random ?? math.Random(),
        ) {
    final ping = pingInterval;
    final watchdog = watchdogTimeout;
    if (ping != null && watchdog != null && watchdog <= ping) {
      throw ArgumentError(
        'watchdogTimeout ($watchdog) must be longer than pingInterval '
        '($ping), otherwise the watchdog fires between pings and the client '
        'reconnects forever.',
      );
    }

    if (authorizer != null || authEndpoint == null) {
      // Either the host owns its own client (a full authorizer override) or
      // there is no HTTP authorization at all (public channels only) — this
      // instance has no client of its own to close in dispose().
      _authorizer = authorizer;
      _ownedHttpClient = null;
    } else {
      final client = (httpClientFactory ?? http.Client.new)();
      _ownedHttpClient = client;
      _authorizer = httpAuthorizer(
        endpoint: authEndpoint,
        headers: authHeaders,
        client: client,
      );
    }
  }

  /// The package version reported to the server in the socket URL.
  ///
  /// Kept in sync with `pubspec.yaml`'s `version:`; `test/version_test.dart`
  /// fails if the two drift, so bump both together on every release.
  static const String clientVersion = '0.4.0';

  /// The maximum number of subscribe attempts for a private or presence
  /// channel, including the first, before a failing [Authorizer] is left
  /// unsubscribed rather than retried immediately again. Not a permanent
  /// giving-up: see [_subscribe]'s doc comment for what restarts it.
  static const int _maxAuthAttempts = 3;

  /// The socket id assigned by the server, or null while disconnected.
  String? get socketId => _connection?.socketId;

  /// The current connection state.
  ReverbState get state => _state;

  /// Connection state changes.
  Stream<ReverbState> get states => _states.stream;

  /// Registers [callback] to run after a dropped socket is fully restored.
  ///
  /// Reverb does not replay events missed while disconnected, so this is where
  /// an application refetches whatever it may have missed. It fires only once
  /// every previously-live channel has resubscribed, so a REST reconcile
  /// cannot race a half-restored socket. It does not fire on first connect —
  /// but an explicit [disconnect] followed by [connect] does fire it, once
  /// that reconnect's channels are resubscribed, since from the socket's
  /// perspective that is the same kind of restore as an unplanned drop. The
  /// exception is `disconnect(forget: true)`: that clears every registered
  /// callback (including this one) and resets the first-connect tracking, so
  /// the following [connect] looks like a first connect and does not fire
  /// the callbacks it just discarded.
  void onReconnected(void Function() callback) =>
      _reconnectedCallbacks.add(callback);

  /// Opens the socket, retrying with backoff until it succeeds or fails
  /// fatally, and subscribes to any channels created beforehand.
  ///
  /// A no-op while a connect or reconnect attempt is already in flight —
  /// including mid-handshake or waiting out a backoff delay — so a
  /// double-tapped connect button can never open two sockets at once.
  Future<void> connect() async {
    if (_shouldRun) return;

    if (handleAppLifecycle && !_observing) {
      _observing = true;
      WidgetsBinding.instance.addObserver(this);
    }

    _shouldRun = true;
    _attempt = 0;
    await _open();
  }

  /// Closes the socket and stops reconnecting.
  ///
  /// By default channels and listeners are kept, so a later [connect]
  /// restores them — this is what the app-lifecycle pause path wants.
  ///
  /// Pass `forget: true` on logout or a session wipe. That also drops every
  /// channel, cancels pending authorization retries, discards every
  /// [onReconnected] callback, and makes every handle created before this
  /// call permanently inert — it cannot resubscribe, and any send it
  /// attempts (including [PrivateChannel.whisper]) is silently dropped
  /// rather than going out over the next user's socket. Callers get fresh
  /// handles from [channel], [private] or [presence], and re-register
  /// [onReconnected] callbacks, after the next login.
  Future<void> disconnect({bool forget = false}) async {
    _shouldRun = false;
    _generation++;
    _markAllChannelsDown();
    // Must run before _channels.clear() below: it walks _channels to reset
    // each PresenceChannel's roster, so clearing the registry first would
    // leave a stale handle's roster holding the previous user's membership.
    _resetPresenceRosters();

    if (forget) {
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
      // onReconnected callbacks close over whichever session registered
      // them; without clearing them here, the next user's first connect
      // would run the previous user's reconcile logic. Resetting
      // _everConnected makes that connect look like a first connect (per
      // its own doc comment) rather than a reconnect, which is also what
      // suppresses onReconnected from firing at all until a later drop.
      _reconnectedCallbacks.clear();
      _everConnected = false;
    }

    final connection = _connection;
    _connection = null;
    await connection?.close();
    // A concurrent connect() may have already restarted the client while the
    // close above was in flight; only report disconnected if nothing else
    // has since claimed the state, otherwise a fast pause/resume would show
    // up on the stream as connecting -> disconnected -> connected.
    if (!_shouldRun) _setState(ReverbState.disconnected);
  }

  Future<void> _open() async {
    // Captured once per _open() call so every suspension point below —
    // across the backoff delay and the handshake await — can tell whether a
    // later disconnect() has superseded this attempt before acting on stale
    // state. See _generation's doc comment for why this exists.
    final generation = _generation;
    bool current() => _shouldRun && generation == _generation;

    while (current()) {
      _setState(
        _attempt == 0 ? ReverbState.connecting : ReverbState.reconnecting,
      );

      final connection = Connection(
        url: _url,
        socketFactory: _socketFactory,
        onLog: onLog,
        pingInterval: pingInterval,
        watchdogTimeout: watchdogTimeout,
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
        // A disconnect() mid-handshake now fails this await (Connection's
        // close() completes the handshake with an error), which used to look
        // exactly like an ordinary drop. If we are no longer current, that is
        // exactly what happened: report nothing and do not back off, there is
        // nothing to retry.
        if (!current()) return;
        onError?.call(error, stackTrace);
        await Future<void>.delayed(backoffDelay(_attempt++, _random));
        if (!current()) return;
        continue;
      }

      // The handshake succeeded, but disconnect() may have run while it was
      // in flight. Close this socket rather than adopting it: _connection no
      // longer points at it, and nothing else will.
      if (!current()) {
        unawaited(connection.close());
        return;
      }

      final wasReconnect = _everConnected;
      _attempt = 0;
      _everConnected = true;
      _setState(ReverbState.connected);

      // Watch for a later drop before awaiting resubscription, so a socket
      // that dies mid-resubscribe still schedules a retry.
      unawaited(connection.closed.then((_) => _onDropped(connection)));

      await _subscribeAll();
      // Only fire onReconnected if this connection is still the one in play.
      // A drop while _subscribeAll was still awaiting an authorizer leaves
      // _connection cleared (or pointing elsewhere), in which case every
      // _subscribe above returned early without actually resubscribing —
      // firing here would tell the app to reconcile against a dead socket.
      if (wasReconnect && identical(_connection, connection)) {
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

    // A fatal pusher:error arriving after the handshake already completed
    // has no completer left to throw through, so Connection surfaces it here
    // instead of through connection.open()'s error path. Without this check
    // it would look like an ordinary drop and retry forever against a server
    // that will never accept the connection again.
    final fatalError = connection.fatalError;
    if (fatalError != null) {
      _shouldRun = false;
      _markAllChannelsDown();
      _resetPresenceRosters();
      _setState(ReverbState.failed);
      onError?.call(fatalError, null);
      return;
    }

    _markAllChannelsDown();
    _resetPresenceRosters();
    _setState(ReverbState.reconnecting);
    final generation = _generation;
    unawaited(
      Future<void>.delayed(backoffDelay(_attempt++, _random)).then((_) {
        if (_shouldRun && generation == _generation) unawaited(_open());
      }),
    );
  }

  /// Reacts to the app moving to the background or foreground.
  ///
  /// Disconnects on [AppLifecycleState.paused] or [AppLifecycleState.detached]
  /// and reconnects on [AppLifecycleState.resumed], but only if this instance
  /// was the one that disconnected it — a client that was never connected, or
  /// that the host application disconnected before this handler paused it,
  /// stays that way. [disconnect] cannot tell why it was called, so calling it
  /// explicitly while already paused by this handler still leaves the next
  /// resume free to reconnect.
  @override
  void didChangeAppLifecycleState(AppLifecycleState state) {
    if (!handleAppLifecycle) return;

    switch (state) {
      case AppLifecycleState.paused:
      case AppLifecycleState.detached:
        // failed is permanent (fatalError paths deliberately leave _shouldRun
        // false so nothing retries); treat it like disconnected so a
        // background/foreground cycle can never resurrect a dead connection.
        if (_state == ReverbState.disconnected ||
            _state == ReverbState.failed) {
          return;
        }
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

  /// Releases the state stream. Call from the host application's teardown.
  void dispose() {
    if (_observing) {
      _observing = false;
      WidgetsBinding.instance.removeObserver(this);
    }
    unawaited(disconnect());
    unawaited(_states.close());
    unawaited(_channelHealthController.close());
    _ownedHttpClient?.close();
  }

  void _setState(ReverbState next) {
    if (_state == next) return;
    _state = next;
    if (!_states.isClosed) _states.add(next);
  }
}
