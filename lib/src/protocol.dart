import 'dart:convert';
import 'dart:math' as math;

/// A single decoded frame received from a Reverb server.
class ReverbFrame {
  /// Creates a frame.
  const ReverbFrame({
    required this.event,
    required this.data,
    this.channel,
  });

  /// The wire event name, such as `pusher:ping` or `App\Events\OrderCreated`.
  final String event;

  /// The wire channel name, or null for connection-level frames.
  final String? channel;

  /// The decoded payload. Non-object payloads are wrapped under a `data` key.
  final Map<String, dynamic> data;

  /// Parses a raw text frame, returning null if it is not a usable frame.
  static ReverbFrame? parse(String raw) {
    final decoded = _tryDecode(raw);
    if (decoded is! Map) return null;

    final event = decoded['event'];
    if (event is! String) return null;

    final channel = decoded['channel'];
    return ReverbFrame(
      event: event,
      channel: channel is String ? channel : null,
      data: decodeData(decoded['data']),
    );
  }
}

/// Decodes Pusher's `data` field, which arrives as a JSON string containing
/// JSON rather than as a nested object.
///
/// Already-decoded maps pass through unchanged. Anything that is neither a map
/// nor a JSON object string is wrapped as `{'data': value}` so callers always
/// receive a map.
Map<String, dynamic> decodeData(Object? raw) {
  if (raw == null) return const <String, dynamic>{};
  if (raw is Map) return raw.cast<String, dynamic>();
  if (raw is String) {
    final decoded = _tryDecode(raw);
    if (decoded is Map) return decoded.cast<String, dynamic>();
    return <String, dynamic>{'data': decoded ?? raw};
  }
  return <String, dynamic>{'data': raw};
}

/// Builds the Reverb socket URL for [appKey].
///
/// [path] mirrors Reverb's `REVERB_SERVER_PATH`: a server hosted behind a
/// reverse proxy at `/ws` serves the app endpoint at `/ws/app/KEY`. Leading
/// and trailing slashes are optional, so `ws`, `/ws` and `/ws/` are the same
/// path.
Uri buildSocketUrl({
  required String host,
  required int port,
  required String appKey,
  required bool useTls,
  required String clientVersion,
  String path = '',
}) {
  final prefix = path.replaceAll(RegExp(r'^/+|/+$'), '');

  return Uri(
    scheme: useTls ? 'wss' : 'ws',
    host: host,
    port: port,
    path: prefix.isEmpty ? '/app/$appKey' : '/$prefix/app/$appKey',
    queryParameters: <String, String>{
      'protocol': '7',
      'client': 'flutter',
      'version': clientVersion,
    },
  );
}

/// The delay before reconnect attempt [attempt] (zero-based).
///
/// Doubles from one second, caps at thirty, and adds up to 25 percent jitter so
/// that clients dropped by the same server outage do not reconnect in lockstep.
Duration backoffDelay(int attempt, math.Random random) {
  final seconds = math.min(1 << attempt.clamp(0, 5), 30);
  return Duration(
    milliseconds: seconds * 1000 + random.nextInt(seconds * 250 + 1),
  );
}

/// Resolves an Echo-style event name to the name that appears on the wire.
///
/// A bare name is namespaced (`OrderCreated` becomes `App\Events\OrderCreated`).
/// A leading `.` marks a literal `broadcastAs()` name, and a leading `\` marks a
/// fully qualified class name; both are returned with the marker stripped.
String resolveEventName(String name, String namespace) {
  if (name.startsWith('.') || name.startsWith(r'\')) return name.substring(1);
  if (namespace.isEmpty) return name;
  return '$namespace\\$name';
}

/// Whether a `pusher:error` [code] means the client must stop reconnecting.
///
/// The 4000 series covers unrecoverable conditions such as an unknown app key
/// or unsupported protocol version; retrying those in a loop only hammers the
/// server. Higher codes are transient and are retried with backoff.
bool isFatalErrorCode(int code) => code >= 4000 && code < 4100;

Object? _tryDecode(String raw) {
  try {
    return jsonDecode(raw);
  } on FormatException {
    return null;
  }
}
