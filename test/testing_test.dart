import 'package:flutter_test/flutter_test.dart';
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
}
