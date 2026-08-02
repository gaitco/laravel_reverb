import 'dart:async';
import 'dart:convert';

import 'package:stream_channel/stream_channel.dart';

import 'protocol.dart';

export 'protocol.dart' show ReverbFrame;

/// Opens a socket to [url].
///
/// Typed as [StreamChannel] rather than `WebSocketChannel` so that tests can
/// substitute a controllable channel without implementing a WebSocket.
typedef SocketFactory = StreamChannel<dynamic> Function(Uri url);

/// An error that must not be retried, such as an unknown application key.
class ReverbFatalError implements Exception {
  /// Creates a fatal error.
  const ReverbFatalError(this.code, this.message);

  /// The `pusher:error` code.
  final int code;

  /// The human readable message from the server.
  final String message;

  @override
  String toString() => 'ReverbFatalError($code): $message';
}

/// Raised when the socket closes before the handshake completes.
class ReverbConnectionClosed implements Exception {
  /// Creates the exception.
  const ReverbConnectionClosed();

  @override
  String toString() => 'ReverbConnectionClosed: socket closed before handshake';
}

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
  });

  /// The socket URL, including the protocol query parameters.
  final Uri url;

  /// Creates the underlying socket.
  final SocketFactory socketFactory;

  /// Optional log sink, so the host application controls logging.
  final void Function(String message)? onLog;

  final StreamController<ReverbFrame> _frames =
      StreamController<ReverbFrame>.broadcast();
  final Completer<void> _closed = Completer<void>();

  StreamChannel<dynamic>? _socket;
  StreamSubscription<dynamic>? _subscription;
  Completer<String>? _handshake;
  Timer? _idleTimer;
  Timer? _pongTimer;
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

  /// Application-level frames. Handshake and keepalive frames are consumed
  /// internally and never appear here.
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
    _restartIdleTimer();
    _pongTimer?.cancel();
    _pongTimer = null;

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

    _restartIdleTimer();
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
    }
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

  /// Sends a ping after [_activityTimeout] of silence, then treats a missing
  /// pong within the same window as a dead socket. Reverb never notices a
  /// half-open TCP connection on its own, so without this a backgrounded
  /// device can sit on a socket that will never deliver another event.
  void _restartIdleTimer() {
    _idleTimer?.cancel();
    _idleTimer = Timer(_activityTimeout, () {
      send(<String, dynamic>{
        'event': 'pusher:ping',
        'data': <String, dynamic>{},
      });
      _pongTimer = Timer(_activityTimeout, () {
        onLog?.call('reverb: ping timed out, closing socket');
        unawaited(close());
      });
    });
  }

  void _stopTimers() {
    _idleTimer?.cancel();
    _idleTimer = null;
    _pongTimer?.cancel();
    _pongTimer = null;
  }

  void _finish() {
    if (!_closed.isCompleted) _closed.complete();
    if (!_frames.isClosed) unawaited(_frames.close());
  }
}
