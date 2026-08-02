import 'dart:async';
import 'dart:math' as math;

import 'package:flutter/widgets.dart';
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
class Reverb with WidgetsBindingObserver {
  /// Creates a client. Call [connect] to open the socket.
  ///
  /// Provide either [authorizer] or [authEndpoint] to use private and presence
  /// channels; public channels need neither.
  ///
  /// [socketFactory] and [random] are test seams, not for application use:
  /// they let tests substitute a fake socket and a seeded/deterministic
  /// source of backoff jitter. `socketFactory`'s type, [SocketFactory], is
  /// internal and not exported, so it cannot be named outside this package —
  /// pass a matching function literal instead. Both parameters are marked
  /// `@visibleForTesting`.
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
    this.handleAppLifecycle = true,
    @visibleForTesting SocketFactory? socketFactory,
    @visibleForTesting math.Random? random,
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
  ///
  /// Kept in sync with `pubspec.yaml`'s `version:` by hand — nothing enforces
  /// the two matching, so bump both together on every release.
  static const String clientVersion = '0.1.0';

  /// Reports runtime failures that the package handled without throwing.
  final void Function(Object error, StackTrace? stackTrace)? onError;

  /// Optional log sink, so the host application controls logging.
  final void Function(String message)? onLog;

  /// Whether to disconnect on background and reconnect on foreground.
  ///
  /// Left on, this keeps iOS from holding a socket the OS will silently kill
  /// and avoids burning battery on a connection nobody is watching. Turn it
  /// off if the host application manages the socket itself.
  final bool handleAppLifecycle;

  bool _pausedByLifecycle = false;
  bool _observing = false;

  final Uri _url;
  final String _namespace;
  final SocketFactory _socketFactory;
  final Authorizer? _authorizer;
  final math.Random _random;

  final Map<String, Channel> _channels = <String, Channel>{};
  final StreamController<ReverbState> _states =
      StreamController<ReverbState>.broadcast();
  final List<void Function()> _reconnectedCallbacks = <void Function()>[];

  Connection? _connection;
  ReverbState _state = ReverbState.disconnected;
  bool _shouldRun = false;
  bool _everConnected = false;
  int _attempt = 0;

  /// Bumped on every [disconnect], so a connect loop suspended mid-backoff
  /// or mid-handshake can tell it has been superseded and must not resume —
  /// otherwise a fast disconnect()-then-connect() (a lifecycle pause/resume
  /// under a second, or a logout/login) races a second loop into existence,
  /// leaving two live sockets both wired to [_onFrame] and every event
  /// delivered twice.
  int _generation = 0;

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
  /// perspective that is the same kind of restore as an unplanned drop.
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

  /// Closes the socket and stops reconnecting. Channels and listeners are kept
  /// so that a later [connect] restores them.
  Future<void> disconnect() async {
    _shouldRun = false;
    _generation++;
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
      _setState(ReverbState.failed);
      onError?.call(fatalError, null);
      return;
    }

    _setState(ReverbState.reconnecting);
    final generation = _generation;
    unawaited(
      Future<void>.delayed(backoffDelay(_attempt++, _random)).then((_) {
        if (_shouldRun && generation == _generation) unawaited(_open());
      }),
    );
  }

  /// Returns the public channel named [name], creating it on first use.
  ///
  /// The handle returned is only live while it has at least one listener:
  /// once the last one is cancelled, [_unsubscribe] drops the channel from
  /// the registry and sends `pusher:unsubscribe`. A handle held past that
  /// point is a dead object — calling `listen` on it again bumps its
  /// listener count from 0 to 1 without ever re-sending `pusher:subscribe`,
  /// so it silently receives nothing. Call [channel] (or [private]/
  /// [presence]) again to get a fresh, live handle instead of reusing one
  /// whose last listener was cancelled.
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
  ///
  /// See [channel] for why a handle is dead once its last listener is
  /// cancelled. Private and presence channels have a second failure mode:
  /// if [Authorizer] throws for this channel, the failure is reported
  /// through [onError] and nothing retries — the channel stays subscribed to
  /// nothing for the rest of the session. To recover, cancel every listener
  /// on it (so it is removed from the registry) and call [private] again to
  /// force a fresh authorization attempt.
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
  ///
  /// See [private] for the authorizer-failure caveat, which applies here
  /// too.
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

    // The caller may have cancelled its last listener while the authorizer
    // await above was in flight, which unregisters the channel and sends
    // pusher:unsubscribe immediately. Without this check, resuming here would
    // still send pusher:subscribe and orphan a server-side subscription that
    // nothing in _channels tracks anymore. Identity, not just presence by
    // name, so a channel re-registered under the same name in the meantime
    // doesn't let this stale subscribe through either.
    if (!identical(_channels[channel.name], channel)) return;

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
    switch (frame.event) {
      case 'pusher:subscription_error':
        // Otherwise dropped silently: nothing is ever listening for this
        // event name on the channel, since it is a server rejection, not an
        // application event. The most common cause is a broadcasting auth
        // endpoint signed with the wrong app secret.
        onError?.call(
          ReverbSubscriptionError(frame.channel ?? '', frame.data),
          null,
        );
        return;
      case 'pusher:error':
        // A non-fatal protocol error forwarded here by Connection (fatal
        // ones surface through connect()'s error path instead).
        onError?.call(
          ReverbProtocolError(
            frame.data['code'] is int ? frame.data['code'] as int : null,
            frame.data['message']?.toString() ?? 'unknown error',
          ),
          null,
        );
        return;
    }

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
