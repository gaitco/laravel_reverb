import 'dart:async';
import 'dart:math' as math;

import 'package:flutter/widgets.dart';
import 'package:http/http.dart' as http;
import 'package:web_socket_channel/web_socket_channel.dart';

import 'auth.dart';
import 'channel.dart';
import 'channel_health.dart';
import 'connection.dart';
import 'metrics.dart';
import 'protocol.dart';

export 'channel_health.dart' show ChannelHealth;

part 'reverb_base.dart';
part 'reverb_channels.dart';
part 'reverb_connect.dart';
part 'reverb_health.dart';

/// The connection lifecycle of a [Reverb] client.
enum ReverbState {
  /// Not connected and not trying to be.
  disconnected,

  /// The first connection attempt is in flight.
  connecting,

  /// Connected, with the handshake complete.
  connected,

  /// The socket dropped and a retry is in flight.
  reconnecting,

  /// Stopped permanently, for example on an unknown application key.
  failed,
}

/// A Laravel Reverb client.
///
/// Owns the channel registry, authorization and the connection lifecycle.
/// Create one per application and keep it alive.
class Reverb extends _ReverbBase
    with _ReverbHealth, _ReverbChannels, _ReverbConnect {
  /// Creates a client. Call [connect] to open the socket.
  ///
  /// Provide either [authorizer] or [authEndpoint] to use private and presence
  /// channels; public channels need neither.
  ///
  /// [path] mirrors Reverb's `REVERB_SERVER_PATH` for a server behind a
  /// reverse proxy on a subpath: `path: '/ws'` dials `/ws/app/KEY` instead of
  /// `/app/KEY`. Leave it empty for a Reverb server at the root, which is the
  /// default deployment.
  ///
  /// [socketFactory], [random], [httpClientFactory] and [now] are test
  /// seams, not for application use: they let tests substitute a fake
  /// socket, a seeded/deterministic source of backoff jitter, a trackable
  /// HTTP client, and a controllable clock. `socketFactory`'s type,
  /// [SocketFactory], is internal and not exported, so it cannot be named
  /// outside this package — pass a matching function literal instead.
  /// [httpClientFactory] is only consulted when [authEndpoint] is set and
  /// [authorizer] is not, since that is the only case where this class
  /// creates an `http.Client` of its own. `now` lets a test drive latency
  /// and staleness without real time passing. All four parameters are
  /// marked `@visibleForTesting`.
  Reverb({
    required String host,
    required String appKey,
    int? port,
    bool useTls = true,
    String path = '',
    String? authEndpoint,
    Future<Map<String, String>> Function()? authHeaders,
    Authorizer? authorizer,
    super.namespace = r'App\Events',
    super.onError,
    super.onLog,
    super.handleAppLifecycle = true,
    super.pingInterval,
    super.watchdogTimeout,
    @visibleForTesting SocketFactory? socketFactory,
    @visibleForTesting math.Random? random,
    @visibleForTesting http.Client Function()? httpClientFactory,
    @visibleForTesting DateTime Function()? now,
  }) : super(
          url: buildSocketUrl(
            host: host,
            port: port ?? (useTls ? 443 : 80),
            appKey: appKey,
            useTls: useTls,
            clientVersion: clientVersion,
            path: path,
          ),
          socketFactory: socketFactory ?? WebSocketChannel.connect,
          random: random ?? math.Random(),
          now: now ?? DateTime.now,
        ) {
    final ping = pingInterval;
    final watchdog = watchdogTimeout;
    if (ping != null && watchdog != null && watchdog <= ping) {
      throw ArgumentError(
        'watchdogTimeout ($watchdog) must be longer than pingInterval '
        '($ping), otherwise the watchdog fires between pings and the client '
        'reconnects forever.',
      );
    }

    if (authorizer != null || authEndpoint == null) {
      // Either the host owns its own client (a full authorizer override) or
      // there is no HTTP authorization at all (public channels only) — this
      // instance has no client of its own to close in dispose().
      _authorizer = authorizer;
      _ownedHttpClient = null;
    } else {
      final client = (httpClientFactory ?? http.Client.new)();
      _ownedHttpClient = client;
      _authorizer = httpAuthorizer(
        endpoint: authEndpoint,
        headers: authHeaders,
        client: client,
      );
    }
  }

  /// The package version reported to the server in the socket URL.
  ///
  /// Kept in sync with `pubspec.yaml`'s `version:`; `test/version_test.dart`
  /// fails if the two drift, so bump both together on every release.
  static const String clientVersion = '0.5.0';

  /// The maximum number of subscribe attempts for a private or presence
  /// channel, including the first, before a failing [Authorizer] is left
  /// unsubscribed rather than retried immediately again. Not a permanent
  /// giving-up: see [_subscribe]'s doc comment for what restarts it.
  static const int _maxAuthAttempts = 3;

  /// A snapshot of connection quality, read on demand.
  ///
  /// Nothing streams these values: latency changes on every ping, so a
  /// consumer that rebuilt on each change would redraw far more often than
  /// anything it displays actually changes. Read this when you paint.
  ReverbMetrics get metrics {
    final lastFrameAt = _connection?.lastFrameAt;

    return ReverbMetrics(
      lastLatency: _connection?.lastLatency,
      reconnectCount: _reconnectCount,
      sinceLastFrame:
          lastFrameAt == null ? null : _now().difference(lastFrameAt),
      connectedSince: _connectedSince,
    );
  }

  /// Releases the state stream. Call from the host application's teardown.
  void dispose() {
    if (_observing) {
      _observing = false;
      WidgetsBinding.instance.removeObserver(this);
    }
    unawaited(disconnect());
    unawaited(_states.close());
    unawaited(_channelHealthController.close());
    _ownedHttpClient?.close();
  }
}

/// Builds a [Reverb] wired to [socketFactory], for `package:laravel_reverb/testing.dart`.
///
/// [Reverb]'s `socketFactory` parameter is `@visibleForTesting`, which the
/// analyzer enforces at every call site outside a `test/` directory —
/// including `lib/testing.dart`, which ships. Call sites inside this library
/// are exempt, so routing construction through here keeps the annotation
/// doing its real job: an application that tries to swap the socket on a
/// production `Reverb(...)` is still flagged by its own analyzer.
Reverb buildTestReverb({
  required String host,
  required int port,
  required String appKey,
  required bool useTls,
  required String namespace,
  required bool handleAppLifecycle,
  required Authorizer authorizer,
  required SocketFactory socketFactory,
}) =>
    Reverb(
      host: host,
      port: port,
      appKey: appKey,
      useTls: useTls,
      namespace: namespace,
      handleAppLifecycle: handleAppLifecycle,
      authorizer: authorizer,
      socketFactory: socketFactory,
    );
