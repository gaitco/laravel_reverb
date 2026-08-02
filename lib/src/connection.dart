import 'dart:async';
import 'dart:convert';

import 'package:stream_channel/stream_channel.dart';

import 'exceptions.dart';
import 'protocol.dart';

export 'exceptions.dart'
    show ReverbConnectionClosed, ReverbFatalError, ReverbProtocolError;
export 'protocol.dart' show ReverbFrame;

/// Opens a socket to [url].
///
/// Typed as [StreamChannel] rather than `WebSocketChannel` so that tests can
/// substitute a controllable channel without implementing a WebSocket.
typedef SocketFactory = StreamChannel<dynamic> Function(Uri url);

/// One socket to a Reverb server, speaking the Pusher wire protocol.
///
/// A [Connection] handles the handshake and keepalive only. It knows nothing
/// about channels, authorization or reconnection; those belong to `Reverb`.
class Connection {
  /// Creates a connection to [url]. Call [open] to actually connect.
  Connection({
    required this.url,
    required this.socketFactory,
    this.onLog,
    this.pingInterval,
    this.watchdogTimeout,
  });

  /// The socket URL, including the protocol query parameters.
  final Uri url;

  /// Creates the underlying socket.
  final SocketFactory socketFactory;

  /// Optional log sink, so the host application controls logging.
  final void Function(String message)? onLog;

  /// How often to send `pusher:ping` regardless of server activity.
  ///
  /// Null (the default) keeps the server-driven behaviour: ping only after
  /// the handshake's `activity_timeout` of silence, with the legacy pong
  /// deadline as death detection.
  ///
  /// Set alone, pings run on this schedule instead, but death detection is
  /// still on: each ping arms the same pong deadline the legacy path uses,
  /// so a socket that never replies is closed rather than pinged forever.
  ///
  /// Set together with [watchdogTimeout], the watchdog replaces the pong
  /// deadline as death detection — it resets on any inbound frame, not just
  /// a reply to our own ping.
  final Duration? pingInterval;

  /// How long the socket may go without ANY inbound frame before it is
  /// treated as dead and closed.
  ///
  /// Null (the default) keeps the server-driven behaviour: a missed pong
  /// after a ping is what declares the socket dead.
  final Duration? watchdogTimeout;

  final StreamController<ReverbFrame> _frames =
      StreamController<ReverbFrame>.broadcast();
  final Completer<void> _closed = Completer<void>();

  StreamChannel<dynamic>? _socket;
  StreamSubscription<dynamic>? _subscription;
  Completer<String>? _handshake;
  Timer? _idleTimer;
  Timer? _pongTimer;
  Timer? _pingTimer;
  Timer? _watchdogTimer;
  Duration _activityTimeout = const Duration(seconds: 120);
  String? _socketId;
  ReverbFatalError? _fatalError;

  /// The socket id assigned by the server, or null before the handshake.
  String? get socketId => _socketId;

  /// The fatal error that ended this connection, if any.
  ///
  /// Set when a 4000-4099 `pusher:error` arrives after the handshake already
  /// completed, since by then there is no completer left to throw it through.
  /// Callers should check this once [closed] completes, to tell a fatal
  /// server rejection apart from an ordinary drop that is safe to retry.
  ReverbFatalError? get fatalError => _fatalError;

  /// Frames this connection does not fully own itself: application-level
  /// events, plus non-fatal `pusher:error` and `pusher:subscription_error`
  /// (fatal errors surface through [open]'s error path instead, and never
  /// reach this stream). Handshake and keepalive frames
  /// (`pusher:connection_established`, `pusher:ping`, `pusher:pong`) are
  /// consumed internally and never appear here. `Reverb` filters the two
  /// protocol-error events out before dispatching anything to a `Channel`.
  Stream<ReverbFrame> get frames => _frames.stream;

  /// Completes when this connection is finished, from either side.
  Future<void> get closed => _closed.future;

  /// Connects and completes with the server-assigned socket id.
  ///
  /// Throws [ReverbFatalError] if the server rejects the connection outright,
  /// or [ReverbConnectionClosed] if the socket dies before the handshake.
  Future<String> open() {
    final handshake = Completer<String>();
    // Attach a no-op listener now so a handshake failure delivered before the
    // caller awaits `open()` (e.g. the socket closes on the same microtask
    // turn as `serverClose()`) is never reported as an unhandled error by the
    // zone; the caller's own await still receives the error normally.
    handshake.future.then((_) {}, onError: (_) {});
    _handshake = handshake;

    final socket = socketFactory(url);
    _socket = socket;
    _subscription = socket.stream.listen(
      _onMessage,
      onError: _onSocketError,
      onDone: _onSocketDone,
      cancelOnError: true,
    );

    return handshake.future;
  }

  /// Sends [message] as an encoded frame. No-op once the socket is gone.
  void send(Map<String, dynamic> message) {
    final socket = _socket;
    if (socket == null) return;
    socket.sink.add(jsonEncode(message));
  }

  /// Closes the socket from this side.
  Future<void> close() async {
    // Without this, a close() that lands mid-handshake (e.g. the app is
    // backgrounded while `open()` is still awaiting the server) leaves the
    // handshake completer pending forever, so the caller's `await open()` —
    // and everything chained onto it, including `Reverb.connect()` — never
    // resumes.
    _failHandshake(const ReverbConnectionClosed());
    _stopTimers();
    final subscription = _subscription;
    _subscription = null;
    // Not awaited: cancellation takes effect as soon as this call returns
    // (no further events reach `_onMessage`), and its cleanup future — for a
    // subscription with no `onCancel` handler — never resolves inside a
    // `fakeAsync` zone, which would otherwise hang every test that closes a
    // connection under `fakeAsync`.
    unawaited(subscription?.cancel());
    final socket = _socket;
    _socket = null;
    if (socket != null) await socket.sink.close();
    _finish();
  }

  void _onMessage(dynamic raw) {
    _onActivity();

    if (raw is! String) return;
    final frame = ReverbFrame.parse(raw);
    if (frame == null) {
      onLog?.call('reverb: dropped unparseable frame');
      return;
    }

    switch (frame.event) {
      case 'pusher:connection_established':
        _onEstablished(frame);
      case 'pusher:ping':
        send(<String, dynamic>{
          'event': 'pusher:pong',
          'data': <String, dynamic>{},
        });
      case 'pusher:pong':
        break;
      case 'pusher:error':
        _onProtocolError(frame);
      default:
        _frames.add(frame);
    }
  }

  void _onEstablished(ReverbFrame frame) {
    final id = frame.data['socket_id'];
    if (id is! String) {
      _failHandshake(const ReverbConnectionClosed());
      unawaited(close());
      return;
    }

    _socketId = id;
    final timeout = frame.data['activity_timeout'];
    if (timeout is int) _activityTimeout = Duration(seconds: timeout);

    _onActivity();
    _startPingLoop();
    onLog?.call('reverb: connected as $id');
    _handshake?.complete(id);
    _handshake = null;
  }

  void _onProtocolError(ReverbFrame frame) {
    final code = frame.data['code'];
    final message = frame.data['message']?.toString() ?? 'unknown error';
    onLog?.call('reverb: server error $code $message');

    if (code is int && isFatalErrorCode(code)) {
      final error = ReverbFatalError(code, message);
      _fatalError = error;
      _failHandshake(error);
      unawaited(close());
      return;
    }

    // Non-fatal: the connection survives, but silently dropping this left
    // the application with no way to learn a subscription's auth was wrong,
    // a payload was malformed, etc. Forward it as a frame so `Reverb` can
    // route it to `onError`; it carries no channel, so it never reaches a
    // `Channel.dispatch`.
    if (!_frames.isClosed) _frames.add(frame);
  }

  void _onSocketError(Object error, StackTrace stackTrace) {
    onLog?.call('reverb: socket error $error');
    _failHandshake(error);
    _stopTimers();
    _finish();
  }

  void _onSocketDone() {
    _failHandshake(const ReverbConnectionClosed());
    _stopTimers();
    _finish();
  }

  void _failHandshake(Object error) {
    final handshake = _handshake;
    if (handshake == null || handshake.isCompleted) return;
    _handshake = null;
    handshake.completeError(error);
  }

  /// Called on every inbound frame, and once when the handshake lands.
  ///
  /// Death detection is whichever of the two mechanisms is configured: a
  /// watchdog on inbound silence, or the legacy pong deadline armed by
  /// [_armPongDeadline] after every ping. They are independent, so an app
  /// can take fast detection without also taking a faster ping, or vice
  /// versa — but one of the two is always live, whatever the configuration.
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
    _pingTimer = Timer.periodic(interval, (_) {
      _sendPing();
      _armPongDeadline();
    });
  }

  void _sendPing() {
    send(<String, dynamic>{
      'event': 'pusher:ping',
      'data': <String, dynamic>{},
    });
  }

  /// Arms the legacy pong deadline after a ping just went out, unless a
  /// watchdog is configured (it already covers death detection, and running
  /// both would race two detectors against one dead socket) or a deadline
  /// from an earlier, still-unanswered ping is already ticking.
  ///
  /// That last guard matters once [pingInterval] drives scheduling: pings
  /// then fire on a fixed cadence independent of replies, so without it
  /// every new ping would cancel and replace the previous deadline before it
  /// could ever fire — chasing it forever and silently disabling death
  /// detection for a socket that never replies to anything.
  void _armPongDeadline() {
    if (watchdogTimeout != null || _pongTimer != null) return;
    _pongTimer = Timer(_activityTimeout, () {
      onLog?.call('reverb: ping timed out, closing socket');
      unawaited(close());
    });
  }

  /// Legacy path: ping after [_activityTimeout] of silence, then treat a
  /// missing pong within the same window as a dead socket.
  void _restartIdleTimer() {
    _idleTimer?.cancel();
    _idleTimer = Timer(_activityTimeout, () {
      _sendPing();
      _armPongDeadline();
    });
  }

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

  void _finish() {
    if (!_closed.isCompleted) _closed.complete();
    if (!_frames.isClosed) unawaited(_frames.close());
  }
}
