/// Common base for every runtime failure `laravel_reverb` reports through
/// the package's `onError` callback.
///
/// Sealed so host applications can `switch` over the concrete type
/// exhaustively instead of writing an `is` chain, and so a new failure mode
/// added later is a compile error at every switch that isn't updated for it.
///
/// Declared alongside its five subtypes in this one file because Dart only
/// allows a sealed class to be extended within its own library — the
/// concrete types otherwise live conceptually close to where they are
/// thrown (`Connection`, `Channel`, the HTTP authorizer).
sealed class ReverbException implements Exception {
  const ReverbException();
}

/// An error that must not be retried, such as an unknown application key.
final class ReverbFatalError extends ReverbException {
  /// Creates a fatal error.
  const ReverbFatalError(this.code, this.message);

  /// The `pusher:error` code.
  final int code;

  /// The human readable message from the server.
  final String message;

  @override
  String toString() => 'ReverbFatalError($code): $message';
}

/// A non-fatal `pusher:error` from the server: worth surfacing to the host
/// application, but not grounds to stop reconnecting.
final class ReverbProtocolError extends ReverbException {
  /// Creates the error.
  const ReverbProtocolError(this.code, this.message);

  /// The `pusher:error` code, or null when the server sent none.
  final int? code;

  /// The human readable message from the server.
  final String message;

  @override
  String toString() => 'ReverbProtocolError($code): $message';
}

/// Raised when the socket closes before the handshake completes.
final class ReverbConnectionClosed extends ReverbException {
  /// Creates the exception.
  const ReverbConnectionClosed();

  @override
  String toString() => 'ReverbConnectionClosed: socket closed before handshake';
}

/// Raised when the broadcasting auth endpoint refuses or returns nonsense.
final class ReverbAuthException extends ReverbException {
  /// Creates the exception.
  const ReverbAuthException(this.channelName, this.statusCode, this.body);

  /// The channel that failed to authorize.
  final String channelName;

  /// The HTTP status code. 200 when the response body was unusable; 0 when no
  /// HTTP response was received (transport failure: DNS, connection, timeout, etc).
  final int statusCode;

  /// The raw response body, for diagnostics. On transport failure, contains
  /// the underlying error message.
  final String body;

  @override
  String toString() => 'ReverbAuthException($channelName, $statusCode): $body';
}

/// Raised when the server rejects a channel subscription.
///
/// The most common cause is a `/broadcasting/auth` endpoint signing with the
/// wrong app secret: auth returns 200, `pusher:subscribe` is sent, and the
/// server answers with `pusher:subscription_error` instead of ever
/// dispatching an event on that channel.
final class ReverbSubscriptionError extends ReverbException {
  /// Creates the error.
  const ReverbSubscriptionError(this.channelName, this.reason);

  /// The channel the server refused to subscribe.
  final String channelName;

  /// The server's decoded `pusher:subscription_error` payload.
  final Map<String, dynamic> reason;

  @override
  String toString() => 'ReverbSubscriptionError($channelName): $reason';
}
