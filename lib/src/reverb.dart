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
    String? authEndpoint,
    Future<Map<String, String>> Function()? authHeaders,
    Authorizer? authorizer,
    String namespace = r'App\Events',
    this.onError,
    this.onLog,
    this.handleAppLifecycle = true,
    this.pingInterval,
    this.watchdogTimeout,
    @visibleForTesting SocketFactory? socketFactory,
    @visibleForTesting math.Random? random,
    @visibleForTesting http.Client Function()? httpClientFactory,
  })  : _namespace = namespace,
        _socketFactory = socketFactory ?? WebSocketChannel.connect,
        _random = random ?? math.Random(),
        _url = buildSocketUrl(
          host: host,
          port: port ?? (useTls ? 443 : 80),
          appKey: appKey,
          useTls: useTls,
          clientVersion: clientVersion,
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
  /// Kept in sync with `pubspec.yaml`'s `version:` by hand — nothing enforces
  /// the two matching, so bump both together on every release.
  static const String clientVersion = '0.2.0';

  /// The maximum number of subscribe attempts for a private or presence
  /// channel, including the first, before a failing [Authorizer] is left
  /// unsubscribed rather than retried immediately again. Not a permanent
  /// giving-up: see [_subscribe]'s doc comment for what restarts it.
  static const int _maxAuthAttempts = 3;

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

  /// How often to send `pusher:ping` regardless of server activity.
  ///
  /// Null (the default) pings only after the server's advertised
  /// `activity_timeout` of silence, with a missed pong as death detection.
  /// Set alone, pings run on this schedule instead, but a missed pong after
  /// each ping still closes the socket — death detection is always on,
  /// whatever the configuration. Set together with [watchdogTimeout], the
  /// watchdog replaces the pong deadline: it resets on any inbound frame,
  /// letting an app notice a half-open socket in seconds rather than a
  /// minute.
  final Duration? pingInterval;

  /// How long the socket may go without any inbound frame before it is closed
  /// and the reconnect path takes over.
  ///
  /// Must be longer than [pingInterval] when both are set, or the watchdog
  /// would fire between pings and reconnect forever.
  final Duration? watchdogTimeout;

  bool _pausedByLifecycle = false;
  bool _observing = false;

  final Uri _url;
  final String _namespace;
  final SocketFactory _socketFactory;
  late final Authorizer? _authorizer;
  final math.Random _random;

  /// The `http.Client` this instance created for the default HTTP
  /// authorizer, or null when there is none (a caller-supplied [authorizer]
  /// owns its own client, if any, and public-only usage needs no client at
  /// all). Closed in [dispose] — a client the caller passed in is theirs to
  /// close, not this class's.
  late final http.Client? _ownedHttpClient;

  final Map<String, Channel> _channels = <String, Channel>{};

  /// Bumped per channel name every time [_unsubscribe] actually evicts that
  /// name's occupant.
  ///
  /// Needed because a revived handle is the *same object* the registry held
  /// before: `identical(_channels[channel.name], channel)` is true again the
  /// moment [_resubscribe] puts it back, so identity alone cannot tell a
  /// subscribe attempt from before the eviction apart from one started
  /// after it. [_subscribe] captures the generation for [channel.name] at
  /// entry and re-checks it before ever sending, so a stale attempt left
  /// over from before an unsubscribe+resubscribe cycle is recognized as
  /// stale even though it is, by identity, the channel currently registered.
  final Map<String, int> _generations = <String, int>{};

  /// Channels the server has acknowledged with a subscription-succeeded frame.
  ///
  /// `_channels` is what we *intend* to be subscribed to and drives the
  /// reconnect pass; this is what is actually live right now. A channel whose
  /// authorization failed stays in `_channels` — so the next reconnect retries
  /// it — while being absent here.
  final Set<String> _live = <String>{};

  final StreamController<ChannelHealth> _channelHealthController =
      StreamController<ChannelHealth>.broadcast();

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

  /// Per-channel up/down notifications.
  Stream<ChannelHealth> get channelHealth => _channelHealthController.stream;

  /// Whether the server currently acknowledges [wireName].
  ///
  /// Pass the wire name, including any prefix — `'private-users.1'`, not
  /// `'users.1'`.
  bool isSubscribed(String wireName) => _live.contains(wireName);

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
    _markAllChannelsDown();
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
      _setState(ReverbState.failed);
      onError?.call(fatalError, null);
      return;
    }

    _markAllChannelsDown();
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
  /// The handle returned stays usable for as long as you hold it, even
  /// across gaps with no listeners: once the last listener is cancelled,
  /// [_unsubscribe] drops the channel from the registry and sends
  /// `pusher:unsubscribe`, but calling `listen` on the same handle again
  /// puts it straight back — [_resubscribe] re-registers it and resends
  /// `pusher:subscribe` — *provided nothing else has since claimed [name]*.
  /// Only one handle can be live under a given name at a time: if you called
  /// [channel] again in the meantime and it is still in use, this handle's
  /// revival is refused rather than evicting the one that's live, and this
  /// handle stays inert until that occupant is itself dropped. Stick to one
  /// pattern per name — keep reusing a single handle, or always ask
  /// [channel] for a fresh one — rather than mixing the two.
  Channel channel(String name) => _register(
        name,
        () => Channel(
          name: name,
          namespace: _namespace,
          send: _send,
          onEmpty: _unsubscribe,
          onFirst: _resubscribe,
        ),
      );

  /// Returns the private channel for the bare [name], adding the prefix.
  ///
  /// See [channel] for how re-listening on a handle behaves. Re-listening
  /// also re-authorizes: a resurrected private or presence channel calls
  /// [Authorizer] again against the current socket id, exactly as a fresh
  /// [private] call would. If the authorizer fails, [_subscribe] retries it
  /// a bounded number of times with backoff before leaving the channel
  /// unsubscribed until the next reconnect or a fresh listen; every
  /// failure, including the last, is reported through [onError].
  PrivateChannel private(String name) {
    _requireAuthorizer('private');
    return _register(
      'private-$name',
      () => PrivateChannel(
        name: 'private-$name',
        namespace: _namespace,
        send: _send,
        onEmpty: _unsubscribe,
        onFirst: _resubscribe,
      ),
    );
  }

  /// Returns the presence channel for the bare [name], adding the prefix.
  ///
  /// See [private] for how re-listening and authorizer retries behave, which
  /// apply here too.
  PresenceChannel presence(String name) {
    _requireAuthorizer('presence');
    return _register(
      'presence-$name',
      () => PresenceChannel(
        name: 'presence-$name',
        namespace: _namespace,
        send: _send,
        onEmpty: _unsubscribe,
        onFirst: _resubscribe,
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
    unawaited(_channelHealthController.close());
    _ownedHttpClient?.close();
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
      'laravel_reverb: $kind channels need authorization. Pass either '
      'authEndpoint or authorizer to the Reverb constructor.',
    );
  }

  /// Puts a dead channel handle back to work.
  ///
  /// [Channel] cannot tell a fresh handle from one whose last listener was
  /// already cancelled — both go from zero listeners to one, so [Channel]
  /// calls this either way. If [channel]'s name is already occupied — by
  /// [channel] itself (the ordinary case, since [_register] puts a brand new
  /// channel there before the caller's first `listen` ever runs) or by some
  /// *other* channel object (a second handle for the same name that is still
  /// live) — there is nothing to do: claiming an occupied name would either
  /// be a no-op or, worse, evict a live channel out from under its own
  /// listeners. Only an unoccupied name — dropped by [_unsubscribe], or never
  /// registered at all — gets [channel] put back, and always *before*
  /// calling [_subscribe], so it is already registered by the time
  /// [_subscribe]'s own checks run.
  void _resubscribe(Channel channel) {
    if (_channels[channel.name] != null) return;
    _channels[channel.name] = channel;
    unawaited(_subscribe(channel));
  }

  /// Subscribes [channel], authorizing first when the channel is private.
  ///
  /// Silently returns when there is no live socket: channels created before
  /// connecting are flushed by [_subscribeAll] once the handshake lands, so
  /// callers never see a "not connected yet" error.
  ///
  /// If the authorizer throws, this retries with backoff up to
  /// [_maxAuthAttempts] times (counting [attempt], which starts at 0) before
  /// leaving this channel unsubscribed. That is not permanent: the next
  /// reconnect calls [_subscribeAll], which restarts every channel at
  /// attempt 0, and cancelling every listener (which unsubscribes it) then
  /// listening again forces the same restart immediately. Every failure,
  /// including the last, is reported through [onError] so nothing is silent.
  Future<void> _subscribe(Channel channel, [int attempt = 0]) async {
    final connection = _connection;
    final socketId = connection?.socketId;
    if (connection == null || socketId == null) return;

    // Captured once, at entry: see _generations' doc comment for why
    // identity alone can't tell a stale attempt apart from a fresh one once
    // a channel has been unsubscribed and revived as the same object.
    final generation = _generations[channel.name] ?? 0;
    bool current() =>
        identical(_channels[channel.name], channel) &&
        (_generations[channel.name] ?? 0) == generation;

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
        if (attempt + 1 >= _maxAuthAttempts) {
          _setChannelHealth(channel.name, healthy: false);
          return;
        }

        await Future<void>.delayed(backoffDelay(attempt, _random));

        // The socket may have been replaced (an ordinary reconnect, or a
        // disconnect()) while this retry was backing off. A fresh
        // _subscribeAll already re-authorized every channel against the new
        // socket id in that case, so resuming this chain would send a stale
        // subscribe bound to a socket id nobody accepts anymore.
        if (_connection?.socketId != socketId) return;
        // The caller may likewise have cancelled the channel's last listener
        // and relisted on the same handle while this was backing off. That
        // is still `current()` by identity (the same object is back in the
        // registry) but not by generation, so this still catches it.
        if (!current()) return;

        unawaited(_subscribe(channel, attempt + 1));
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
    // nothing in _channels tracks anymore. Checking current() rather than
    // just identity also catches the same-object-revived case: a fresh
    // listen() on the same handle in the gap has already started its own
    // _subscribe, and this older attempt must not send a second one.
    if (!current()) return;

    connection.send(<String, dynamic>{
      'event': 'pusher:subscribe',
      'data': payload,
    });
  }

  Future<void> _subscribeAll() async {
    await Future.wait(_channels.values.map(_subscribe));
  }

  void _unsubscribe(Channel channel) {
    // Only the channel that actually occupies its name may vacate it. A
    // stale handle whose subscribe never got this far — because a different,
    // still-live channel already holds this name — must not evict the
    // occupant it never displaced.
    if (!identical(_channels[channel.name], channel)) return;
    _channels.remove(channel.name);
    _generations[channel.name] = (_generations[channel.name] ?? 0) + 1;
    _setChannelHealth(channel.name, healthy: false);
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
        if (frame.channel != null) {
          _setChannelHealth(frame.channel!, healthy: false);
        }
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

    if (frame.event == 'pusher_internal:subscription_succeeded' &&
        frame.channel != null) {
      _setChannelHealth(frame.channel!, healthy: true);
      // Fall through: presence channels also listen for this event.
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

  /// Updates [wireName]'s liveness and emits on [channelHealth], but only on
  /// an actual transition — reporting `healthy: false` twice for the same
  /// channel would be noise a consumer had to dedupe itself.
  void _setChannelHealth(String wireName, {required bool healthy}) {
    final changed = healthy ? _live.add(wireName) : _live.remove(wireName);
    if (!changed) return;
    if (_channelHealthController.isClosed) return;
    _channelHealthController
        .add(ChannelHealth(channel: wireName, healthy: healthy));
  }

  /// Marks every currently-live channel unhealthy, e.g. when the socket
  /// drops entirely.
  void _markAllChannelsDown() {
    for (final String name in _live.toList()) {
      _setChannelHealth(name, healthy: false);
    }
  }
}
