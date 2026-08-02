import 'protocol.dart';

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
class Channel {
  /// Creates a channel. Applications get channels from `Reverb`, not directly.
  Channel({
    required this.name,
    required String namespace,
    required void Function(Map<String, dynamic> message) send,
    required void Function(Channel channel) onEmpty,
  })  : _namespace = namespace,
        _send = send,
        _onEmpty = onEmpty;

  /// The wire channel name, including any `private-` or `presence-` prefix.
  final String name;

  final String _namespace;
  final void Function(Map<String, dynamic> message) _send;
  final void Function(Channel channel) _onEmpty;
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
  void Function() _addListener(String wireEvent, ReverbEventCallback callback) {
    (_listeners[wireEvent] ??= <ReverbEventCallback>[]).add(callback);
    _handlerCount++;

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
  });

  /// Registers membership callbacks and returns one cancelable handle.
  ///
  /// A single entry point rather than three chainable methods, because the
  /// three callbacks have different signatures and are almost always wanted
  /// together.
  Subscription members({
    void Function(List<PresenceMember> members)? here,
    void Function(PresenceMember member)? joining,
    void Function(PresenceMember member)? leaving,
  }) {
    final subscription = Subscription._(this);

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
