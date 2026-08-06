part of 'reverb.dart';

mixin _ReverbChannels on _ReverbBase, _ReverbHealth {
  /// The maximum number of subscribe attempts for a private or presence
  /// channel, including the first, before a failing [Authorizer] is left
  /// unsubscribed rather than retried immediately again. Not a permanent
  /// giving-up: see [_subscribe]'s doc comment for what restarts it.
  static const int _maxAuthAttempts = 3;

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

  /// Bumped by `disconnect(forget: true)`. Distinct from `_generations`,
  /// which is per channel name and guards a stale in-flight subscribe; this
  /// one is per client and guards post-logout revival.
  int _clientEpoch = 0;

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
          send: _sendFor(_clientEpoch),
          onEmpty: _unsubscribe,
          onFirst: _resubscribe,
          clientEpoch: _clientEpoch,
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
        send: _sendFor(_clientEpoch),
        onEmpty: _unsubscribe,
        onFirst: _resubscribe,
        clientEpoch: _clientEpoch,
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
        send: _sendFor(_clientEpoch),
        onEmpty: _unsubscribe,
        onFirst: _resubscribe,
        clientEpoch: _clientEpoch,
      ),
    );
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
    if (channel.clientEpoch != _clientEpoch) return;
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

  /// Drops every channel and resets per-name generations, for
  /// `disconnect(forget: true)`.
  ///
  /// Bumping [_clientEpoch] is what makes every handle created before the
  /// call inert: [_resubscribe] refuses a channel whose epoch has moved on,
  /// and [_sendFor] turns its whispers into no-ops.
  ///
  /// Callers must reset presence rosters first — this clears the registry
  /// that [_resetPresenceRosters] walks.
  void _forgetAllChannels() {
    _clientEpoch++;
    _channels.clear();
    // Resets the per-channel-name generation to 0 for every name. This is
    // not what stops a pending authorizer retry — _subscribe re-checks
    // `(_generations[name] ?? 0) == generation`, and a retry that captured
    // 0 still matches 0 after this clear. What actually strands it is
    // _channels.clear() just above: current()'s identity check
    // (`identical(_channels[channel.name], channel)`) fails once the
    // channel is gone from the registry, on top of the existing
    // socket-id guard. This clear exists so a channel name freed by
    // forget starts its next life at generation 0 instead of some
    // arbitrary leftover count.
    _generations.clear();
  }

  /// Clears every presence channel's roster, e.g. when the socket drops
  /// entirely — membership does not survive a socket, and the resubscribe
  /// re-seeds it from the server.
  void _resetPresenceRosters() {
    for (final Channel channel in _channels.values) {
      if (channel is PresenceChannel) channel.resetPresence();
    }
  }

  /// Returns the `send` function a channel uses for
  /// [PrivateChannel.whisper], bound to [epoch].
  ///
  /// Subscribing and unsubscribing always go through [_subscribe] and
  /// [_unsubscribe], which the `clientEpoch` check in [_resubscribe] already
  /// guards. [PrivateChannel.whisper] is different: it calls
  /// [Channel._sendMessage] directly, with no listen/cancel and so no trip
  /// through [_resubscribe] in between, so a stale handle held across
  /// `disconnect(forget: true)` could otherwise whisper straight onto the
  /// next user's socket. Binding this send to the epoch the channel was
  /// created under closes that gap: once [_clientEpoch] has moved on, a
  /// whisper through this channel is a silent no-op instead of reaching the
  /// new socket.
  void Function(Map<String, dynamic> message) _sendFor(int epoch) =>
      (Map<String, dynamic> message) {
        if (epoch == _clientEpoch) _connection?.send(message);
      };

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
}
