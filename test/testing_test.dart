import 'package:flutter_test/flutter_test.dart';
import 'package:laravel_reverb/laravel_reverb.dart';
import 'package:laravel_reverb/testing.dart';

void main() {
  TestWidgetsFlutterBinding.ensureInitialized();

  test('delivers an emitted event to a listener on a public channel', () async {
    final fake = ReverbFake();
    await fake.connect();

    Map<String, dynamic>? received;
    fake.reverb.channel('orders').listen(
          'OrderCreated',
          (Map<String, dynamic> data) => received = data,
        );
    await Future<void>.delayed(Duration.zero);

    fake.emit('orders', r'App\Events\OrderCreated', <String, dynamic>{'id': 7});
    await Future<void>.delayed(Duration.zero);

    expect(received, <String, dynamic>{'id': 7});
    fake.dispose();
  });

  test('records what the client sent', () async {
    final fake = ReverbFake();
    await fake.connect();

    fake.reverb.channel('orders').listen('OrderCreated', (_) {});
    await Future<void>.delayed(Duration.zero);

    expect(
      fake.sent.map((Map<String, dynamic> f) => f['event']),
      contains('pusher:subscribe'),
    );
    fake.dispose();
  });

  test('a private channel authorizes without a server', () async {
    final fake = ReverbFake();
    await fake.connect();

    Map<String, dynamic>? received;
    fake.reverb.private('users.1').listen(
          'MessageSent',
          (Map<String, dynamic> data) => received = data,
        );
    await Future<void>.delayed(Duration.zero);

    fake.emit(
      'private-users.1',
      r'App\Events\MessageSent',
      <String, dynamic>{'body': 'hi'},
    );
    await Future<void>.delayed(Duration.zero);

    expect(received, <String, dynamic>{'body': 'hi'});
    fake.dispose();
  });

  test('drop() reconnects and fires onReconnected', () async {
    final fake = ReverbFake();
    await fake.connect();

    var reconnected = false;
    fake.reverb.onReconnected(() => reconnected = true);

    await fake.drop();

    // The first backoff attempt is ~1000-1250ms; poll with a generous
    // deadline rather than sleeping a fixed amount (see metrics_test.dart's
    // reconnect test for why a fixed sleep is flaky).
    final deadline = DateTime.now().add(const Duration(seconds: 5));
    while (fake.reverb.metrics.reconnectCount == 0 &&
        DateTime.now().isBefore(deadline)) {
      await Future<void>.delayed(const Duration(milliseconds: 20));
    }

    expect(fake.reverb.metrics.reconnectCount, 1);
    expect(reconnected, isTrue);
    fake.dispose();
  });

  test('seeds a presence roster through emitFrame', () async {
    final fake = ReverbFake();
    await fake.connect();

    List<PresenceMember>? present;
    fake.reverb.presence('room.1').members(
          here: (List<PresenceMember> members) => present = members,
        );
    await Future<void>.delayed(Duration.zero);

    fake.emitFrame(<String, dynamic>{
      'event': 'pusher_internal:subscription_succeeded',
      'channel': 'presence-room.1',
      'data': <String, dynamic>{
        'presence': <String, dynamic>{
          'ids': <String>['1', '2'],
          'hash': <String, dynamic>{
            '1': <String, dynamic>{'name': 'Ada'},
            '2': <String, dynamic>{'name': 'Linus'},
          },
        },
      },
    });
    await Future<void>.delayed(Duration.zero);

    expect(present!.map((PresenceMember m) => m.id), <String>['1', '2']);
    expect(present!.first.info['name'], 'Ada');
    fake.dispose();
  });

  test('delivers a channel-less frame such as pusher:error through emitFrame',
      () async {
    Object? caught;
    final fake = ReverbFake(onError: (Object error, StackTrace? _) {
      caught = error;
    });
    await fake.connect();

    fake.emitFrame(<String, dynamic>{
      'event': 'pusher:error',
      'data': <String, dynamic>{'code': 4200, 'message': 'boom'},
    });
    await Future<void>.delayed(Duration.zero);

    expect(caught, isA<ReverbProtocolError>());
    expect((caught! as ReverbProtocolError).message, 'boom');
    fake.dispose();
  });

  test('emit before connect explains itself', () {
    final fake = ReverbFake();

    expect(
      () => fake.emit('orders', 'X'),
      throwsA(
        isA<StateError>().having(
          (StateError e) => e.message,
          'message',
          contains('connect()'),
        ),
      ),
    );
    fake.dispose();
  });
}
