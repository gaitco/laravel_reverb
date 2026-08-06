import 'package:laravel_reverb/src/testing/in_memory_socket.dart';

export 'package:laravel_reverb/src/testing/in_memory_socket.dart'
    show factoryFor, handshakeFrame;

/// The package's own tests knew this as `FakeSocket` before the
/// implementation moved to `lib/` for [ReverbFake] to share. Kept as an alias
/// so those tests read as they always have.
typedef FakeSocket = InMemorySocket;
