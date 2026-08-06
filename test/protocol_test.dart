import 'dart:math' as math;

import 'package:laravel_reverb/src/protocol.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('decodeData', () {
    test('decodes the double-encoded string Pusher actually sends', () {
      expect(decodeData('{"id":5,"name":"a"}'), {'id': 5, 'name': 'a'});
    });

    test('accepts a payload that is already a map', () {
      expect(decodeData(<String, dynamic>{'id': 5}), {'id': 5});
    });

    test('returns an empty map for a null payload', () {
      expect(decodeData(null), isEmpty);
    });

    test('wraps a non-object payload under the data key', () {
      expect(decodeData('"hello"'), {'data': 'hello'});
      expect(decodeData('not json'), {'data': 'not json'});
    });
  });

  group('ReverbFrame.parse', () {
    test('parses event, channel and double-encoded data', () {
      final frame = ReverbFrame.parse(
        '{"event":"App\\\\Events\\\\OrderCreated",'
        '"channel":"private-users.1","data":"{\\"id\\":7}"}',
      );

      expect(frame!.event, r'App\Events\OrderCreated');
      expect(frame.channel, 'private-users.1');
      expect(frame.data, {'id': 7});
    });

    test('leaves channel null for connection-level frames', () {
      final frame = ReverbFrame.parse('{"event":"pusher:ping","data":"{}"}');

      expect(frame!.event, 'pusher:ping');
      expect(frame.channel, isNull);
      expect(frame.data, isEmpty);
    });

    test('returns null for text that is not a frame', () {
      expect(ReverbFrame.parse('garbage'), isNull);
      expect(ReverbFrame.parse('{"no":"event"}'), isNull);
    });
  });

  group('buildSocketUrl', () {
    test('builds a wss url with the protocol query parameters', () {
      final url = buildSocketUrl(
        host: 'api.example.com',
        port: 443,
        appKey: 'abc',
        useTls: true,
        clientVersion: '0.1.0',
      );

      expect(url.scheme, 'wss');
      expect(url.path, '/app/abc');
      expect(url.queryParameters['protocol'], '7');
      expect(url.queryParameters['client'], 'flutter');
      expect(url.queryParameters['version'], '0.1.0');
    });

    test('uses ws when tls is disabled', () {
      final url = buildSocketUrl(
        host: 'localhost',
        port: 8080,
        appKey: 'abc',
        useTls: false,
        clientVersion: '0.1.0',
      );

      expect(url.scheme, 'ws');
      expect(url.port, 8080);
    });

    test('prefixes a custom server path onto the app path', () {
      final url = buildSocketUrl(
        host: 'api.example.com',
        port: 443,
        appKey: 'abc',
        useTls: true,
        clientVersion: '0.4.0',
        path: '/ws',
      );

      expect(url.path, '/ws/app/abc');
    });

    test('normalizes a path given with or without slashes', () {
      Uri urlFor(String path) => buildSocketUrl(
            host: 'api.example.com',
            port: 443,
            appKey: 'abc',
            useTls: true,
            clientVersion: '0.4.0',
            path: path,
          );

      expect(urlFor('ws').path, '/ws/app/abc');
      expect(urlFor('/ws').path, '/ws/app/abc');
      expect(urlFor('/ws/').path, '/ws/app/abc');
      expect(urlFor('/realtime/ws/').path, '/realtime/ws/app/abc');
    });

    test('leaves the app path alone when no path is given', () {
      final url = buildSocketUrl(
        host: 'api.example.com',
        port: 443,
        appKey: 'abc',
        useTls: true,
        clientVersion: '0.4.0',
        path: '',
      );

      expect(url.path, '/app/abc');
    });
  });

  group('backoffDelay', () {
    test('doubles per attempt and caps at 30 seconds', () {
      final zero = _ZeroRandom();

      expect(backoffDelay(0, zero).inMilliseconds, 1000);
      expect(backoffDelay(1, zero).inMilliseconds, 2000);
      expect(backoffDelay(2, zero).inMilliseconds, 4000);
      expect(backoffDelay(3, zero).inMilliseconds, 8000);
      expect(backoffDelay(4, zero).inMilliseconds, 16000);
      expect(backoffDelay(5, zero).inMilliseconds, 30000);
      expect(backoffDelay(99, zero).inMilliseconds, 30000);
    });

    test('adds at most 25 percent jitter', () {
      final random = math.Random(1);

      for (var attempt = 0; attempt < 8; attempt++) {
        final base = backoffDelay(attempt, _ZeroRandom()).inMilliseconds;
        final jittered = backoffDelay(attempt, random).inMilliseconds;

        expect(jittered, greaterThanOrEqualTo(base));
        expect(jittered, lessThanOrEqualTo((base * 1.25).round()));
      }
    });
  });

  group('resolveEventName', () {
    test('namespaces a bare event name', () {
      expect(
        resolveEventName('OrderCreated', r'App\Events'),
        r'App\Events\OrderCreated',
      );
    });

    test('treats a leading dot as a literal broadcastAs name', () {
      expect(
          resolveEventName('.order.created', r'App\Events'), 'order.created');
    });

    test('treats a leading backslash as a fully qualified name', () {
      expect(
        resolveEventName(r'\Domain\Events\Paid', r'App\Events'),
        r'Domain\Events\Paid',
      );
    });

    test('leaves the name alone when the namespace is empty', () {
      expect(resolveEventName('OrderCreated', ''), 'OrderCreated');
    });

    test('passes a pusher protocol event through literally', () {
      expect(
        resolveEventName('pusher:cache_miss', r'App\Events'),
        'pusher:cache_miss',
      );
    });

    test('still namespaces a bare name that merely mentions cache', () {
      expect(
        resolveEventName('CacheMiss', r'App\Events'),
        r'App\Events\CacheMiss',
      );
    });
  });

  group('isFatalErrorCode', () {
    test('treats 4000 series as fatal and others as retryable', () {
      expect(isFatalErrorCode(4001), isTrue);
      expect(isFatalErrorCode(4099), isTrue);
      expect(isFatalErrorCode(4100), isFalse);
      expect(isFatalErrorCode(4200), isFalse);
    });
  });
}

class _ZeroRandom implements math.Random {
  @override
  bool nextBool() => false;
  @override
  double nextDouble() => 0;
  @override
  int nextInt(int max) => 0;
}
