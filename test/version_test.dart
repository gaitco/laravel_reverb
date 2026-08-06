import 'dart:io';

import 'package:flutter_test/flutter_test.dart';
import 'package:laravel_reverb/src/reverb.dart';

void main() {
  test('clientVersion matches the version in pubspec.yaml', () {
    // clientVersion ships in the socket URL as `version=`, so a stale constant
    // silently misreports this client to every server it connects to. Nothing
    // but this test keeps the two in step.
    final pubspec = File('pubspec.yaml').readAsStringSync();
    final declared = RegExp(r'^version:\s*(\S+)', multiLine: true)
        .firstMatch(pubspec)!
        .group(1)!;

    expect(Reverb.clientVersion, declared);
  });
}
