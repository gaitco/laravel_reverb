import 'protocol.dart';

export 'exceptions.dart' show ReverbSubscriptionError;

/// Receives the decoded payload of a broadcast event.
typedef ReverbEventCallback = void Function(Map<String, dynamic> data);

/// A cancelable handle over one or more listeners on a single channel.
///
/// [listen] returns the same handle, so a chain of calls produces one handle
/// whose [cancel] removes every listener in that chain.
class Subscription {
  Subscription._(this._channel);

  final Channel _channel;
  final List<void Function()> _removers = <void Function()>[];

  /// Adds an event listener to the same channel and returns this handle.
  Subscription listen(String event, ReverbEventCallback callback) {
    _removers.add(_channel._addListener(_channel._wireEvent(event), callback));
    return this;
  }

  /// Adds a client event (whisper) listener and returns this handle.
  Subscription listenForWhisper(String event, ReverbEventCallback callback) {
    _removers.add(_channel._addListener('client-$event', callback));
    return this;
  }

  void _register(void Function() remover) => _removers.add(remover);

  /// Removes every listener registered through this handle.
  ///
  /// The channel itself is only torn down once its last listener is gone, so
  /// cancelling one screen's subscription never disconnects another's.
  void cancel() {
    final removers = List<void Function()>.of(_removers);
    _removers.clear();
    for (final void Function() remove in removers) {
      remove();
    }
  }
}

/// A public channel: events in, no authorization, no client events.
///
/// Ref-counted by listener: once the last [Subscription] returned by
/// [listen] is cancelled, the owning `Reverb` drops this channel from its
/// registry and unsubscribes on the wire. Calling [listen] again on the same
/// handle resends `pusher:subscribe` and puts it back in the registry, so a
/// handle you have held onto keeps working.
///
/// Exactly one handle is live per channel name. If something else claimed the
/// name while yours was unregistered — because another part of the app asked
/// `Reverb` for the same channel — yours does not evict it; it stays inert
/// and will only reclaim the name on a later 0-to-1 listener transition, once
/// the occupant has released it. Pick one pattern per channel name: either
/// hold a handle, or ask `Reverb` for it each time. Mixing both is what
/// produces two handles for one name.
class Channel {
  /// Creates a channel. Applications get channels from `Reverb`, not directly.
  Channel({
    required this.name,
    required String namespace,
    required void Function(Map<String, dynamic> message) send,
    required void Function(Channel channel) onEmpty,
    required void Function(Channel channel) onFirst,
  })  : _namespace = namespace,
        _send = send,
        _onEmpty = onEmpty,
        _onFirst = onFirst;

  /// The wire channel name, including any `private-` or `presence-` prefix.
  final String name;

  final String _namespace;
  final void Function(Map<String, dynamic> message) _send;
  final void Function(Channel channel) _onEmpty;
  final void Function(Channel channel) _onFirst;
  final Map<String, List<ReverbEventCallback>> _listeners =
      <String, List<ReverbEventCallback>>{};

  int _handlerCount = 0;

  /// Listens for [event] and returns a chainable, cancelable handle.
  Subscription listen(String event, ReverbEventCallback callback) =>
      Subscription._(this).listen(event, callback);

  /// Routes an incoming wire event to its listeners.
  void dispatch(String wireEvent, Map<String, dynamic> data) {
    final callbacks = _listeners[wireEvent];
    if (callbacks == null) return;
    for (final ReverbEventCallback callback
        in List<ReverbEventCallback>.of(callbacks)) {
      callback(data);
    }
  }

  String _wireEvent(String event) => resolveEventName(event, _namespace);

  void _sendMessage(Map<String, dynamic> message) => _send(message);

  /// Registers [callback] and returns an idempotent remover.
  ///
  /// The remover guards against being called twice so that a double `cancel()`
  /// cannot drop the ref count below the number of live listeners and tear
  /// down a channel someone else is still using.
  ///
  /// Symmetrically, going from zero listeners to one calls [_onFirst] so the
  /// owner can put this channel back to work — the mirror image of
  /// [_onEmpty], since a handle whose last listener was cancelled is
  /// otherwise indistinguishable from a channel nobody has listened on yet.
  void Function() _addListener(String wireEvent, ReverbEventCallback callback) {
    (_listeners[wireEvent] ??= <ReverbEventCallback>[]).add(callback);
    final wasEmpty = _handlerCount == 0;
    _handlerCount++;
    if (wasEmpty) _onFirst(this);

    var removed = false;
    return () {
      if (removed) return;
      removed = true;

      _listeners[wireEvent]?.remove(callback);
      _handlerCount--;
      if (_handlerCount == 0) _onEmpty(this);
    };
  }
}

/// A private channel. Requires authorization and permits client events.
class PrivateChannel extends Channel {
  /// Creates a private channel.
  PrivateChannel({
    required super.name,
    required super.namespace,
    required super.send,
    required super.onEmpty,
    required super.onFirst,
  });

  /// Sends a client event directly to other subscribers.
  ///
  /// Client events never reach the application server, so they suit ephemeral
  /// signals such as typing indicators. The `client-` prefix is protocol
  /// mandated and added here.
  void whisper(String event, Map<String, dynamic> data) {
    _sendMessage(<String, dynamic>{
      'event': 'client-$event',
      'channel': name,
      'data': data,
    });
  }

  /// Listens for a client event (whisper) and returns a chainable, cancelable
  /// handle.
  Subscription listenForWhisper(String event, ReverbEventCallback callback) =>
      Subscription._(this).listenForWhisper(event, callback);
}

/// A subscriber of a presence channel.
class PresenceMember {
  /// Creates a member.
  const PresenceMember({required this.id, required this.info});

  /// The member id, from Laravel's presence channel authorization.
  final String id;

  /// Arbitrary member info returned alongside the id.
  final Map<String, dynamic> info;
}

/// A presence channel: a private channel that also tracks who is subscribed.
class PresenceChannel extends PrivateChannel {
  /// Creates a presence channel.
  PresenceChannel({
    required super.name,
    required super.namespace,
    required super.send,
    required super.onEmpty,
    required super.onFirst,
  });

  final Map<String, PresenceMember> _members = <String, PresenceMember>{};

  /// The channel's current members, keyed by member id.
  ///
  /// Empty until the subscription is acknowledged, and cleared whenever the
  /// socket drops — membership does not survive a socket. The map is an
  /// unmodifiable view; mutate nothing through it.
  Map<String, PresenceMember> get currentMembers =>
      Map<String, PresenceMember>.unmodifiable(_members);

  /// Clears the roster. Called by `Reverb` when the socket drops; the
  /// resubscribe re-seeds it from the server.
  void resetPresence() => _members.clear();

  /// Registers membership callbacks and returns one cancelable handle.
  ///
  /// A single entry point rather than three chainable methods, because the
  /// three callbacks have different signatures and are almost always wanted
  /// together.
  ///
  /// [roster] receives the channel's full current member set on subscribe and
  /// after every join or leave, so a caller never has to track membership
  /// itself. [here], [joining] and [leaving] are the delta form and still
  /// work; use whichever suits — a join/leave toast wants the delta, a seat
  /// map wants the roster.
  Subscription members({
    void Function(Map<String, PresenceMember> members)? roster,
    void Function(List<PresenceMember> members)? here,
    void Function(PresenceMember member)? joining,
    void Function(PresenceMember member)? leaving,
  }) {
    final subscription = Subscription._(this);

    if (roster != null) {
      subscription._register(
        _addListener(
          'pusher_internal:subscription_succeeded',
          (Map<String, dynamic> data) => roster(currentMembers),
        ),
      );
      subscription._register(
        _addListener(
          'pusher_internal:member_added',
          (Map<String, dynamic> data) => roster(currentMembers),
        ),
      );
      subscription._register(
        _addListener(
          'pusher_internal:member_removed',
          (Map<String, dynamic> data) => roster(currentMembers),
        ),
      );
    }

    if (here != null) {
      subscription._register(
        _addListener(
          'pusher_internal:subscription_succeeded',
          (Map<String, dynamic> data) => here(_parseMembers(data)),
        ),
      );
    }
    if (joining != null) {
      subscription._register(
        _addListener(
          'pusher_internal:member_added',
          (Map<String, dynamic> data) => joining(_parseMember(data)),
        ),
      );
    }
    if (leaving != null) {
      subscription._register(
        _addListener(
          'pusher_internal:member_removed',
          (Map<String, dynamic> data) => leaving(_parseMember(data)),
        ),
      );
    }

    return subscription;
  }

  /// Updates the roster from presence wire events, then dispatches as usual.
  ///
  /// Membership is applied here, before any listener runs, so a [roster]
  /// callback and a [joining]/[leaving] callback registered for the same
  /// event always observe the same state.
  @override
  void dispatch(String wireEvent, Map<String, dynamic> data) {
    // Update membership BEFORE notifying listeners, so a roster callback and
    // a joining callback observe the same state for one event.
    switch (wireEvent) {
      case 'pusher_internal:subscription_succeeded':
        _members
          ..clear()
          ..addEntries(_parseMembers(data).map(
            (PresenceMember m) => MapEntry<String, PresenceMember>(m.id, m),
          ));
      case 'pusher_internal:member_added':
        final member = _parseMember(data);
        _members[member.id] = member;
      case 'pusher_internal:member_removed':
        _members.remove(_parseMember(data).id);
      default:
        break;
    }
    super.dispatch(wireEvent, data);
  }

  List<PresenceMember> _parseMembers(Map<String, dynamic> data) {
    final presence = data['presence'];
    if (presence is! Map) return const <PresenceMember>[];

    final ids = presence['ids'];
    final hash = presence['hash'];
    if (ids is! List) return const <PresenceMember>[];

    return ids.map((dynamic id) {
      final key = id.toString();
      final info = hash is Map ? hash[key] : null;
      return PresenceMember(
        id: key,
        info: info is Map
            ? info.cast<String, dynamic>()
            : const <String, dynamic>{},
      );
    }).toList();
  }

  PresenceMember _parseMember(Map<String, dynamic> data) {
    final info = data['user_info'];
    return PresenceMember(
      id: data['user_id'].toString(),
      info: info is Map
          ? info.cast<String, dynamic>()
          : const <String, dynamic>{},
    );
  }
}
