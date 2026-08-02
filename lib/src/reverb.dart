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
  /// cannot race a half-restored socket. It does not fire on first connect.
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

      final wasReconnect = _everConnected;
      _attempt = 0;
      _everConnected = true;
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
    unawaited(
      Future<void>.delayed(backoffDelay(_attempt++, _random)).then((_) {
        if (_shouldRun) unawaited(_open());
      }),
    );
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

  /// Reacts to the app moving to the background or foreground.
  ///
  /// Disconnects on [AppLifecycleState.paused] or [AppLifecycleState.detached]
  /// and reconnects on [AppLifecycleState.resumed], but only if this instance
  /// was the one that disconnected it — a client that was never connected, or
  /// was explicitly disconnected by the host application, stays that way.
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
