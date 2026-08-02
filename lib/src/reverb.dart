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
  // Stored for Task 6's reconnect backoff; this single-attempt connect()
  // does not retry, so nothing reads it yet.
  // ignore: unused_field
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
