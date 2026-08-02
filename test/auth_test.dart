import 'dart:convert';

import 'package:flutter_reverb/src/auth.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:http/http.dart' as http;
import 'package:http/testing.dart';

void main() {
  test('posts socket id and channel name and returns the signature', () async {
    late http.Request captured;
    final client = MockClient((http.Request request) async {
      captured = request;
      return http.Response(
        jsonEncode(<String, dynamic>{'auth': 'key:sig'}),
        200,
      );
    });

    final authorizer = httpAuthorizer(
      endpoint: 'https://api.test/broadcasting/auth',
      client: client,
    );
    final result = await authorizer('private-users.1', '123.456');

    final body = jsonDecode(captured.body) as Map<String, dynamic>;
    expect(body['socket_id'], '123.456');
    expect(body['channel_name'], 'private-users.1');
    expect(result.auth, 'key:sig');
    expect(result.channelData, isNull);
  });

  test('returns channel_data for presence channels', () async {
    final client = MockClient((http.Request request) async {
      return http.Response(
        jsonEncode(<String, dynamic>{
          'auth': 'key:sig',
          'channel_data': '{"user_id":"7"}',
        }),
        200,
      );
    });

    final authorizer = httpAuthorizer(
      endpoint: 'https://api.test/broadcasting/auth',
      client: client,
    );
    final result = await authorizer('presence-room.5', '123.456');

    expect(result.channelData, '{"user_id":"7"}');
  });

  test('applies headers from the callback on every call', () async {
    var calls = 0;
    final seen = <String?>[];
    final client = MockClient((http.Request request) async {
      seen.add(request.headers['authorization']);
      return http.Response(jsonEncode(<String, dynamic>{'auth': 'a'}), 200);
    });

    final authorizer = httpAuthorizer(
      endpoint: 'https://api.test/broadcasting/auth',
      headers: () async =>
          <String, String>{'Authorization': 'Bearer ${++calls}'},
      client: client,
    );

    await authorizer('private-a', '1.1');
    await authorizer('private-b', '1.1');

    expect(seen, <String>['Bearer 1', 'Bearer 2']);
  });

  test('throws ReverbAuthException on a non-200 response', () async {
    final client = MockClient((http.Request request) async {
      return http.Response('Forbidden', 403);
    });

    final authorizer = httpAuthorizer(
      endpoint: 'https://api.test/broadcasting/auth',
      client: client,
    );

    await expectLater(
      authorizer('private-users.1', '123.456'),
      throwsA(
        isA<ReverbAuthException>()
            .having((ReverbAuthException e) => e.statusCode, 'statusCode', 403)
            .having((ReverbAuthException e) => e.channelName, 'channelName',
                'private-users.1'),
      ),
    );
  });

  test('throws ReverbAuthException when the response has no auth field',
      () async {
    final client = MockClient((http.Request request) async {
      return http.Response(jsonEncode(<String, dynamic>{'nope': 1}), 200);
    });

    final authorizer = httpAuthorizer(
      endpoint: 'https://api.test/broadcasting/auth',
      client: client,
    );

    await expectLater(
      authorizer('private-users.1', '123.456'),
      throwsA(isA<ReverbAuthException>()),
    );
  });
}
