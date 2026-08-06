/// A point-in-time snapshot of connection quality.
///
/// Read [Reverb.metrics] when you need it — nothing is streamed, because
/// latency changes on every ping and a widget that rebuilt on each one would
/// be doing so to redraw a value nobody watched change. There is no history:
/// each read reports the most recent round trip, not a series.
class ReverbMetrics {
  /// Creates a snapshot. Applications read [Reverb.metrics] instead.
  const ReverbMetrics({
    this.lastLatency,
    this.reconnectCount = 0,
    this.sinceLastFrame,
    this.connectedSince,
  });

  /// The most recent `pusher:ping` to `pusher:pong` round trip, or null when
  /// no ping has been answered on the current socket.
  final Duration? lastLatency;

  /// How many times a dropped socket has been restored since construction.
  ///
  /// The first connect is not a reconnect, so this stays 0 until a drop is
  /// recovered from.
  final int reconnectCount;

  /// How long since any frame arrived, or null while disconnected.
  ///
  /// A value climbing past the server's activity timeout is what a half-open
  /// socket looks like from the client's side.
  final Duration? sinceLastFrame;

  /// When the current socket completed its handshake, or null while
  /// disconnected.
  final DateTime? connectedSince;

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is ReverbMetrics &&
          other.lastLatency == lastLatency &&
          other.reconnectCount == reconnectCount &&
          other.sinceLastFrame == sinceLastFrame &&
          other.connectedSince == connectedSince;

  @override
  int get hashCode =>
      Object.hash(lastLatency, reconnectCount, sinceLastFrame, connectedSince);

  @override
  String toString() => 'ReverbMetrics(lastLatency: $lastLatency, '
      'reconnectCount: $reconnectCount, sinceLastFrame: $sinceLastFrame, '
      'connectedSince: $connectedSince)';
}
