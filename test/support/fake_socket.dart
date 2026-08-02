import 'dart:convert';

import 'package:flutter_reverb/src/connection.dart';
import 'package:stream_channel/stream_channel.dart';

/// A controllable stand-in for a WebSocket, used to drive [Connection] in tests.
///
/// The package under test talks to [channel]; the test drives the other end.
class FakeSocket {
  /// Creates a connected fake socket.
  FakeSocket() {
    _controller = StreamChannelController<dynamic>(sync: true);
    _controller.foreign.stream.listen(
      _sent.add,
      onDone: () => closed = true,
    );
  }

  late final StreamChannelController<dynamic> _controller;
  final List<dynamic> _sent = <dynamic>[];

  /// Whether the package closed its side of the socket.
  bool closed = false;

  /// The end of the socket handed to the package under test.
  StreamChannel<dynamic> get channel => _controller.local;

  /// Everything the package has sent, decoded from JSON.
  List<Map<String, dynamic>> get sentJson => _sent
      .map((dynamic e) => jsonDecode(e as String) as Map<String, dynamic>)
      .toList();

  /// Delivers [frame] to the package as a server message.
  void emitJson(Map<String, dynamic> frame) => emitRaw(jsonEncode(frame));

  /// Delivers raw text to the package as a server message.
  void emitRaw(String raw) => _controller.foreign.sink.add(raw);

  /// Delivers [error] to the package as a stream error.
  void emitError(Object error) => _controller.foreign.sink.addError(error);

  /// Closes the socket from the server side.
  Future<void> serverClose() => _controller.foreign.sink.close();
}

/// A [SocketFactory] that always returns [socket].
SocketFactory factoryFor(FakeSocket socket) => (Uri _) => socket.channel;

/// The handshake frame a Reverb server sends immediately after connecting.
Map<String, dynamic> handshakeFrame({
  String socketId = '123.456',
  int activityTimeout = 30,
}) {
  return <String, dynamic>{
    'event': 'pusher:connection_established',
    'data': jsonEncode(<String, dynamic>{
      'socket_id': socketId,
      'activity_timeout': activityTimeout,
    }),
  };
}
