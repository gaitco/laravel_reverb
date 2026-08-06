import 'dart:io';

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
    expect(
      const ChannelHealth(channel: 'orders', healthy: true).healthy,
      isTrue,
    );
    expect(const ReverbMetrics().reconnectCount, 0);
  });

  test('every error type shares the sealed ReverbException base', () {
    // Lets hosts switch exhaustively over onError's argument instead of an
    // is-chain; a new failure type added later without extending this base
    // would be a regression this test catches.
    expect(const ReverbFatalError(4001, 'x'), isA<ReverbException>());
    expect(const ReverbProtocolError(4200, 'x'), isA<ReverbException>());
    expect(const ReverbConnectionClosed(), isA<ReverbException>());
    expect(
      const ReverbAuthException('private-a', 403, 'no'),
      isA<ReverbException>(),
    );
    expect(
      const ReverbSubscriptionError('private-a', <String, dynamic>{}),
      isA<ReverbException>(),
    );
  });

  test('the entry point exports exactly the documented API', () {
    // The rest of this file fails when something stops being exported. This
    // one fails when something starts being exported that isn't listed below
    // — but only for names added to an existing `show` clause. An export
    // written without one publishes every public name in that file with no
    // change to the shown-name set, so a bare export is checked separately,
    // first.
    const expected = <String>{
      'Authorizer',
      'Channel',
      'ChannelHealth',
      'PresenceChannel',
      'PresenceMember',
      'PrivateChannel',
      'Reverb',
      'ReverbAuth',
      'ReverbAuthException',
      'ReverbConnectionClosed',
      'ReverbEventCallback',
      'ReverbException',
      'ReverbFatalError',
      'ReverbMetrics',
      'ReverbProtocolError',
      'ReverbState',
      'ReverbSubscriptionError',
      'Subscription',
    };

    final source = File('lib/laravel_reverb.dart').readAsStringSync();

    final bare = RegExp(r'^export\s+[^;]*;', multiLine: true)
        .allMatches(source)
        .map((RegExpMatch m) => m.group(0)!)
        .where((String d) => !d.contains('show'));
    expect(bare, isEmpty,
        reason: 'Every export must name what it exports, or the set check '
            'below cannot see what it added.');

    final actual = RegExp(r'show\s+([^;]+);')
        .allMatches(source)
        .expand((RegExpMatch m) => m.group(1)!.split(','))
        .map((String name) => name.trim())
        .where((String name) => name.isNotEmpty)
        .toSet();

    expect(actual, expected,
        reason: 'lib/laravel_reverb.dart is the truth. If you meant to add or '
            'remove a public name, update the expected set here to match.');
  });
}
