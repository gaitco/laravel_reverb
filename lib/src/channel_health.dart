/// A per-channel up/down notification.
///
/// Deliberately separate from `ReverbState`, which only describes the socket.
/// A connected socket whose channel authorization was rejected is
/// `ReverbState.connected` and unhealthy here — that combination is what an
/// app should gate degraded-mode polling on.
class ChannelHealth {
  /// Creates a health notification.
  const ChannelHealth({required this.channel, required this.healthy});

  /// The wire channel name, including any `private-` or `presence-` prefix.
  ///
  /// This matches `Channel.name` rather than the bare name passed to
  /// `Reverb.private`, because only the wire name is unambiguous across
  /// public, private and presence channels.
  final String channel;

  /// Whether the server currently acknowledges this subscription.
  final bool healthy;

  @override
  String toString() => 'ChannelHealth($channel, healthy: $healthy)';
}
