import 'dart:convert';

import 'package:http/http.dart' as http;

/// The signature returned by Laravel's broadcasting auth endpoint.
class ReverbAuth {
  /// Creates an auth result.
  const ReverbAuth({required this.auth, this.channelData});

  /// The `auth` signature, in `appKey:signature` form.
  final String auth;

  /// The `channel_data` blob, present only for presence channels.
  final String? channelData;
}

/// Produces the auth signature for [channelName] bound to [socketId].
///
/// Supply your own to reuse an existing HTTP client, interceptors, token
/// refresh or certificate pinning; the package then makes no requests itself.
typedef Authorizer = Future<ReverbAuth> Function(
  String channelName,
  String socketId,
);

/// Raised when the broadcasting auth endpoint refuses or returns nonsense.
class ReverbAuthException implements Exception {
  /// Creates the exception.
  const ReverbAuthException(this.channelName, this.statusCode, this.body);

  /// The channel that failed to authorize.
  final String channelName;

  /// The HTTP status code. 200 when the response body was unusable; 0 when no
  /// HTTP response was received (transport failure: DNS, connection, timeout, etc).
  final int statusCode;

  /// The raw response body, for diagnostics. On transport failure, contains
  /// the underlying error message.
  final String body;

  @override
  String toString() => 'ReverbAuthException($channelName, $statusCode): $body';
}

/// The default authorizer: POSTs JSON to Laravel's `/broadcasting/auth`.
///
/// [headers] is called per request rather than captured once, so a token that
/// is refreshed between subscriptions is picked up without recreating the
/// client.
Authorizer httpAuthorizer({
  required String endpoint,
  Future<Map<String, String>> Function()? headers,
  http.Client? client,
}) {
  final httpClient = client ?? http.Client();

  return (String channelName, String socketId) async {
    late http.Response response;
    try {
      response = await httpClient.post(
        Uri.parse(endpoint),
        headers: <String, String>{
          'Content-Type': 'application/json',
          'Accept': 'application/json',
          ...?await headers?.call(),
        },
        body: jsonEncode(<String, dynamic>{
          'socket_id': socketId,
          'channel_name': channelName,
        }),
      );
    } on Exception catch (e) {
      throw ReverbAuthException(channelName, 0, e.toString());
    }

    if (response.statusCode != 200) {
      throw ReverbAuthException(
        channelName,
        response.statusCode,
        response.body,
      );
    }

    Object? decoded;
    try {
      decoded = jsonDecode(response.body);
    } on FormatException {
      throw ReverbAuthException(channelName, 200, response.body);
    }

    if (decoded is! Map || decoded['auth'] is! String) {
      throw ReverbAuthException(channelName, 200, response.body);
    }

    final channelData = decoded['channel_data'];
    return ReverbAuth(
      auth: decoded['auth'] as String,
      channelData: channelData is String ? channelData : null,
    );
  };
}
