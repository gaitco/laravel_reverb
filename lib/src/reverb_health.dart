part of 'reverb.dart';

mixin _ReverbHealth {
  /// Channels the server has acknowledged with a subscription-succeeded frame.
  ///
  /// `_channels` is what we *intend* to be subscribed to and drives the
  /// reconnect pass; this is what is actually live right now. A channel whose
  /// authorization failed stays in `_channels` — so the next reconnect retries
  /// it — while being absent here.
  final Set<String> _live = <String>{};

  final StreamController<ChannelHealth> _channelHealthController =
      StreamController<ChannelHealth>.broadcast();

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
}
