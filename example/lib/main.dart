import 'package:flutter/material.dart';
import 'package:laravel_reverb/laravel_reverb.dart';

/// Runs the example app.
void main() => runApp(const ExampleApp());

/// Minimal example: connect, listen to a public channel, show what arrives.
class ExampleApp extends StatefulWidget {
  /// Creates the example app.
  const ExampleApp({super.key});

  @override
  State<ExampleApp> createState() => _ExampleAppState();
}

class _ExampleAppState extends State<ExampleApp> {
  final Reverb _reverb = Reverb(
    host: 'localhost',
    port: 8080,
    appKey: 'local-key',
    useTls: false,
  );

  final List<String> _events = <String>[];
  Subscription? _subscription;
  ReverbState _state = ReverbState.disconnected;

  @override
  void initState() {
    super.initState();
    _reverb.states.listen((ReverbState state) {
      if (mounted) setState(() => _state = state);
    });
    _subscription = _reverb.channel('orders').listen(
      'OrderCreated',
      (Map<String, dynamic> data) {
        if (mounted) setState(() => _events.insert(0, data.toString()));
      },
    );
    _reverb.onReconnected(() => debugPrint('reconnected: refetch here'));
    _reverb.connect();
  }

  @override
  void dispose() {
    _subscription?.cancel();
    _reverb.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      home: Scaffold(
        appBar: AppBar(title: Text('laravel_reverb — ${_state.name}')),
        body: ListView.builder(
          itemCount: _events.length,
          itemBuilder: (BuildContext context, int index) =>
              ListTile(title: Text(_events[index])),
        ),
      ),
    );
  }
}
