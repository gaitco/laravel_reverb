import 'package:flutter_test/flutter_test.dart';
import 'package:laravel_reverb/testing.dart';

void main() {
  TestWidgetsFlutterBinding.ensureInitialized();

  test('a listener fires for an emitted event', () async {
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
}
