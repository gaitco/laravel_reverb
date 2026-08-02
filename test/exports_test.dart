import 'package:laravel_reverb/laravel_reverb.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  test('the public entry point exposes the documented API', () {
    // Compiles only if every public type is exported from the entry point.
    expect(ReverbState.values, contains(ReverbState.connected));
    expect(const ReverbAuth(auth: 'a').auth, 'a');
    expect(const PresenceMember(id: '1', info: <String, dynamic>{}).id, '1');
    expect(const ReverbFatalError(4001, 'x').code, 4001);
    expect(
      const ReverbAuthException('private-a', 403, 'no').statusCode,
      403,
    );
    // The error every ordinary drop delivers to onError — must be
    // type-testable by hosts, unlike the internal-only ReverbFrame.
    expect(const ReverbConnectionClosed(), isA<Exception>());
    expect(const ReverbProtocolError(4200, 'x').message, 'x');
    expect(
      const ReverbSubscriptionError('private-a', <String, dynamic>{})
          .channelName,
      'private-a',
    );
    expect(Reverb.clientVersion, isNotEmpty);
  });
}
