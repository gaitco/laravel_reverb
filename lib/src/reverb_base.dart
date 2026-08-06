part of 'reverb.dart';

abstract class _ReverbBase with WidgetsBindingObserver {
  _ReverbBase({
    required Uri url,
    required String namespace,
    required SocketFactory socketFactory,
    required math.Random random,
    required DateTime Function() now,
    required this.onError,
    required this.onLog,
    required this.handleAppLifecycle,
    required this.pingInterval,
    required this.watchdogTimeout,
  })  : _url = url,
        _namespace = namespace,
        _socketFactory = socketFactory,
        _random = random,
        _now = now;

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

  final Uri _url;
  final String _namespace;
  final SocketFactory _socketFactory;
  final math.Random _random;
  final DateTime Function() _now;

  late final Authorizer? _authorizer;

  /// The `http.Client` this instance created for the default HTTP
  /// authorizer, or null when there is none (a caller-supplied [authorizer]
  /// owns its own client, if any, and public-only usage needs no client at
  /// all). Closed in [dispose] — a client the caller passed in is theirs to
  /// close, not this class's.
  late final http.Client? _ownedHttpClient;

  Connection? _connection;
}
