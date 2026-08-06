part of 'reverb.dart';

mixin _ReverbHealth on _ReverbBase {
  /// Per-channel up/down notifications.
  Stream<ChannelHealth> get channelHealth => _channelHealthController.stream;

  /// Whether the server currently acknowledges [wireName].
  ///
  /// Pass the wire name, including any prefix — `'private-users.1'`, not
  /// `'users.1'`.
  bool isSubscribed(String wireName) => _live.contains(wireName);

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

  /// Clears every presence channel's roster, e.g. when the socket drops
  /// entirely — membership does not survive a socket, and the resubscribe
  /// re-seeds it from the server.
  void _resetPresenceRosters() {
    for (final Channel channel in _channels.values) {
      if (channel is PresenceChannel) channel.resetPresence();
    }
  }
}
