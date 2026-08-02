import 'package:laravel_reverb/src/channel.dart';
import 'package:flutter_test/flutter_test.dart';

class Harness {
  final List<Map<String, dynamic>> sent = <Map<String, dynamic>>[];
  final List<Channel> emptied = <Channel>[];
  final List<Channel> firsted = <Channel>[];

  Channel public(String name) => Channel(
        name: name,
        namespace: r'App\Events',
        send: sent.add,
        onEmpty: emptied.add,
        onFirst: firsted.add,
      );

  PrivateChannel private(String name) => PrivateChannel(
        name: name,
        namespace: r'App\Events',
        send: sent.add,
        onEmpty: emptied.add,
        onFirst: firsted.add,
      );

  PresenceChannel presence(String name) => PresenceChannel(
        name: name,
        namespace: r'App\Events',
        send: sent.add,
        onEmpty: emptied.add,
        onFirst: firsted.add,
      );
}

void main() {
  test('dispatches a namespaced event to its listener', () {
    final harness = Harness();
    final channel = harness.public('orders');
    Map<String, dynamic>? received;

    channel.listen('OrderCreated', (Map<String, dynamic> data) {
      received = data;
    });
    channel.dispatch(r'App\Events\OrderCreated', <String, dynamic>{'id': 7});

    expect(received, <String, dynamic>{'id': 7});
  });

  test('dispatches a broadcastAs event registered with a leading dot', () {
    final harness = Harness();
    final channel = harness.public('orders');
    var calls = 0;

    channel.listen('.order.created', (_) => calls++);
    channel.dispatch('order.created', <String, dynamic>{});

    expect(calls, 1);
  });

  test('ignores events with no listener', () {
    final harness = Harness();
    final channel = harness.public('orders');

    expect(
      () => channel.dispatch('Nothing', <String, dynamic>{}),
      returnsNormally,
    );
  });

  test('chained listen calls share one cancelable handle', () {
    final harness = Harness();
    final channel = harness.public('orders');
    var created = 0;
    var edited = 0;

    final sub = channel
        .listen('OrderCreated', (_) => created++)
        .listen('OrderEdited', (_) => edited++);

    channel.dispatch(r'App\Events\OrderCreated', <String, dynamic>{});
    channel.dispatch(r'App\Events\OrderEdited', <String, dynamic>{});
    expect(<int>[created, edited], <int>[1, 1]);

    sub.cancel();
    channel.dispatch(r'App\Events\OrderCreated', <String, dynamic>{});
    channel.dispatch(r'App\Events\OrderEdited', <String, dynamic>{});
    expect(<int>[created, edited], <int>[1, 1]);
  });

  test('channel survives while another listener remains', () {
    final harness = Harness();
    final channel = harness.public('orders');

    final first = channel.listen('OrderCreated', (_) {});
    final second = channel.listen('OrderCreated', (_) {});

    first.cancel();
    expect(harness.emptied, isEmpty);

    second.cancel();
    expect(harness.emptied, <Channel>[channel]);
  });

  test('cancelling twice does not double-decrement the ref count', () {
    final harness = Harness();
    final channel = harness.public('orders');

    final first = channel.listen('OrderCreated', (_) {});
    final second = channel.listen('OrderCreated', (_) {});

    first.cancel();
    first.cancel();

    expect(harness.emptied, isEmpty);
    second.cancel();
    expect(harness.emptied, <Channel>[channel]);
  });

  test('whisper sends a client prefixed event', () {
    final harness = Harness();
    final channel = harness.private('private-room.1');

    channel.whisper('typing', <String, dynamic>{'user': 7});

    expect(harness.sent.single, <String, dynamic>{
      'event': 'client-typing',
      'channel': 'private-room.1',
      'data': <String, dynamic>{'user': 7},
    });
  });

  test('listenForWhisper receives client events', () {
    final harness = Harness();
    final channel = harness.private('private-room.1');
    Map<String, dynamic>? received;

    channel.listen('X', (_) {}).listenForWhisper(
          'typing',
          (Map<String, dynamic> data) => received = data,
        );
    channel.dispatch('client-typing', <String, dynamic>{'user': 7});

    expect(received, <String, dynamic>{'user': 7});
  });

  test(
      'PrivateChannel.listenForWhisper receives a dispatched client event '
      'with no prior listen call', () {
    final harness = Harness();
    final channel = harness.private('private-room.1');
    Map<String, dynamic>? received;

    channel.listenForWhisper(
      'typing',
      (Map<String, dynamic> data) => received = data,
    );
    channel.dispatch('client-typing', <String, dynamic>{'user': 7});

    expect(received, <String, dynamic>{'user': 7});
  });

  test(
    'listenForWhisper chains with listen and one cancel removes both',
    () {
      final harness = Harness();
      final channel = harness.private('private-room.1');
      var whispered = 0;
      var broadcast = 0;

      final sub = channel
          .listenForWhisper('typing', (_) => whispered++)
          .listen('OrderCreated', (_) => broadcast++);

      channel.dispatch('client-typing', <String, dynamic>{});
      channel.dispatch(r'App\Events\OrderCreated', <String, dynamic>{});
      expect(<int>[whispered, broadcast], <int>[1, 1]);
      expect(harness.emptied, isEmpty);

      sub.cancel();
      expect(harness.emptied, <Channel>[channel]);

      channel.dispatch('client-typing', <String, dynamic>{});
      channel.dispatch(r'App\Events\OrderCreated', <String, dynamic>{});
      expect(<int>[whispered, broadcast], <int>[1, 1]);
    },
  );

  test('PresenceChannel inherits listenForWhisper', () {
    final harness = Harness();
    final channel = harness.presence('presence-room.5');
    Map<String, dynamic>? received;

    channel.listenForWhisper(
      'typing',
      (Map<String, dynamic> data) => received = data,
    );
    channel.dispatch('client-typing', <String, dynamic>{'user': 7});

    expect(received, <String, dynamic>{'user': 7});
  });

  test('presence reports the initial member list', () {
    final harness = Harness();
    final channel = harness.presence('presence-room.5');
    List<PresenceMember>? members;

    channel.members(here: (List<PresenceMember> m) => members = m);
    channel
        .dispatch('pusher_internal:subscription_succeeded', <String, dynamic>{
      'presence': <String, dynamic>{
        'ids': <String>['1', '2'],
        'hash': <String, dynamic>{
          '1': <String, dynamic>{'name': 'Ann'},
          '2': <String, dynamic>{'name': 'Bo'},
        },
      },
    });

    expect(members!.map((PresenceMember m) => m.id), <String>['1', '2']);
    expect(members!.first.info, <String, dynamic>{'name': 'Ann'});
  });

  test('presence reports joining and leaving members', () {
    final harness = Harness();
    final channel = harness.presence('presence-room.5');
    PresenceMember? joined;
    PresenceMember? left;

    channel.members(
      joining: (PresenceMember m) => joined = m,
      leaving: (PresenceMember m) => left = m,
    );

    channel.dispatch('pusher_internal:member_added', <String, dynamic>{
      'user_id': '3',
      'user_info': <String, dynamic>{'name': 'Cy'},
    });
    channel.dispatch(
      'pusher_internal:member_removed',
      <String, dynamic>{'user_id': '3'},
    );

    expect(joined!.id, '3');
    expect(joined!.info, <String, dynamic>{'name': 'Cy'});
    expect(left!.id, '3');
    expect(left!.info, isEmpty);
  });

  test('presence members handle counts toward the ref count', () {
    final harness = Harness();
    final channel = harness.presence('presence-room.5');

    final sub = channel.members(here: (_) {});
    expect(harness.emptied, isEmpty);

    sub.cancel();
    expect(harness.emptied, <Channel>[channel]);
  });

  test('onFirst fires when the first listener is added, not on later ones', () {
    final harness = Harness();
    final channel = harness.public('orders');

    channel.listen('OrderCreated', (_) {});
    expect(harness.firsted, <Channel>[channel]);

    channel.listen('OrderEdited', (_) {});
    expect(harness.firsted, <Channel>[channel]);
  });

  test('onFirst fires again once a channel that emptied gets a new listener',
      () {
    final harness = Harness();
    final channel = harness.public('orders');

    final sub = channel.listen('OrderCreated', (_) {});
    expect(harness.firsted, <Channel>[channel]);

    sub.cancel();
    expect(harness.emptied, <Channel>[channel]);

    channel.listen('OrderCreated', (_) {});
    expect(harness.firsted, <Channel>[channel, channel]);
  });

  test('roster fires with the full set on seed and on every change', () {
    final harness = Harness();
    final channel = harness.presence('presence-room.5');
    final snapshots = <Map<String, PresenceMember>>[];

    channel.members(roster: snapshots.add);

    channel
        .dispatch('pusher_internal:subscription_succeeded', <String, dynamic>{
      'presence': <String, dynamic>{
        'ids': <String>['1', '2'],
        'hash': <String, dynamic>{
          '1': <String, dynamic>{'name': 'Ann'},
          '2': <String, dynamic>{'name': 'Bo'},
        },
      },
    });
    channel.dispatch('pusher_internal:member_added', <String, dynamic>{
      'user_id': '3',
      'user_info': <String, dynamic>{'name': 'Cy'},
    });
    channel.dispatch(
      'pusher_internal:member_removed',
      <String, dynamic>{'user_id': '1'},
    );

    expect(
        snapshots.map((Map<String, PresenceMember> m) => m.keys.toSet()),
        <Set<String>>[
          <String>{'1', '2'},
          <String>{'1', '2', '3'},
          <String>{'2', '3'},
        ]);
    expect(snapshots.last['3']!.info, <String, dynamic>{'name': 'Cy'});
  });

  test('currentMembers matches the latest roster and is unmodifiable', () {
    final harness = Harness();
    final channel = harness.presence('presence-room.5');

    expect(channel.currentMembers, isEmpty);

    channel.members(roster: (_) {});
    channel
        .dispatch('pusher_internal:subscription_succeeded', <String, dynamic>{
      'presence': <String, dynamic>{
        'ids': <String>['1'],
        'hash': <String, dynamic>{
          '1': <String, dynamic>{'name': 'Ann'},
        },
      },
    });

    expect(channel.currentMembers.keys, <String>['1']);
    expect(
      () => channel.currentMembers['2'] =
          const PresenceMember(id: '2', info: <String, dynamic>{}),
      throwsUnsupportedError,
    );
  });

  test('resetPresence clears the roster', () {
    final harness = Harness();
    final channel = harness.presence('presence-room.5');

    channel.members(roster: (_) {});
    channel
        .dispatch('pusher_internal:subscription_succeeded', <String, dynamic>{
      'presence': <String, dynamic>{
        'ids': <String>['1'],
        'hash': <String, dynamic>{'1': <String, dynamic>{}},
      },
    });
    expect(channel.currentMembers, isNotEmpty);

    channel.resetPresence();
    expect(channel.currentMembers, isEmpty);
  });

  test('a roster handler alone holds the channel subscribed', () {
    final harness = Harness();
    final channel = harness.presence('presence-room.5');

    final sub = channel.members(roster: (_) {});
    expect(harness.emptied, isEmpty);

    sub.cancel();
    expect(harness.emptied, <Channel>[channel]);
  });

  test('roster and the delta callbacks coexist', () {
    final harness = Harness();
    final channel = harness.presence('presence-room.5');
    var rosters = 0;
    PresenceMember? joined;

    channel.members(
      roster: (_) => rosters++,
      joining: (PresenceMember m) => joined = m,
    );
    channel.dispatch('pusher_internal:member_added', <String, dynamic>{
      'user_id': '3',
      'user_info': <String, dynamic>{'name': 'Cy'},
    });

    expect(rosters, 1);
    expect(joined!.id, '3');
  });
}
