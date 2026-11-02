import 'dart:convert';
import 'package:flutter/foundation.dart';
import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:http/http.dart' as http;
import '../config/api_config.dart';
import 'update_notice.dart';

class NotificationCoordinator with WidgetsBindingObserver {
  NotificationCoordinator._();
  static final instance = NotificationCoordinator._();
  static const channel = MethodChannel('philly_scan/notifications');
  final message = ValueNotifier<String>('Update announcements are off.');
  bool _started = false, _busy = false, _again = false;
  void start() {
    if (_started || kIsWeb || defaultTargetPlatform != TargetPlatform.iOS)
      return;
    _started = true;
    WidgetsBinding.instance.addObserver(this);
    channel.setMethodCallHandler((call) async {
      if (call.method == 'tokenReady') await sync();
      if (call.method == 'registrationFailed')
        message.value =
            'Apple registration unavailable. Try again on a signed iPhone build.';
    });
    sync();
  }

  @override
  void didChangeAppLifecycleState(AppLifecycleState state) {
    if (state == AppLifecycleState.resumed) sync();
  }

  Future<void> sync() async {
    if (_busy) {
      _again = true;
      return;
    }
    _busy = true;
    final client = http.Client();
    try {
      final status =
          await channel.invokeMapMethod<String, dynamic>('status') ?? {};
      final data = await channel.invokeMapMethod<String, dynamic>(
        'registration',
      );
      if (data == null) throw const FormatException();
      final uri = Uri.parse(
        '${ApiConfig.baseUrl}/api/notifications/devices/${data['id']}',
      );
      final headers = {
        'Content-Type': 'application/json',
        'X-Installation-Secret': '${data['secret']}',
      };
      if (status['enabled'] != true || status['authorized'] != true) {
        final r = await client
            .delete(uri, headers: headers)
            .timeout(const Duration(seconds: 8));
        message.value = r.statusCode == 200
            ? 'Update announcements are off.'
            : 'Off on this device. Server removal will retry when connected.';
      } else if ('${data['token'] ?? ''}'.isEmpty) {
        await channel.invokeMethod<void>('refresh');
        message.value = 'Waiting for Apple device registration…';
      } else {
        final r = await client
            .put(
              uri,
              headers: headers,
              body: jsonEncode({
                'token': data['token'],
                'environment': data['environment'],
                'version': currentMarketingVersion,
              }),
            )
            .timeout(const Duration(seconds: 8));
        message.value = r.statusCode == 200
            ? 'Update announcements are enabled.'
            : 'Permission granted. Server registration is not connected yet.';
      }
    } on MissingPluginException {
      message.value = 'Requires the iOS build.';
    } catch (_) {
      message.value =
          'Notification connection unavailable. Retry when connected.';
    } finally {
      client.close();
      _busy = false;
      if (_again) {
        _again = false;
        sync();
      }
    }
  }

  Future<void> enable() async {
    try {
      final allowed = await channel.invokeMethod<bool>('enable') ?? false;
      if (!allowed) {
        message.value =
            'Notifications are not allowed. You can enable them in iPhone Settings.';
        return;
      }
      await sync();
    } catch (_) {
      message.value = 'Could not request notifications on this device.';
    }
  }

  Future<void> disable() async {
    try {
      await channel.invokeMethod<void>('disable');
      await sync();
    } catch (_) {
      message.value = 'Could not change notifications. Please retry.';
    }
  }
}

class NotificationSettings extends StatelessWidget {
  const NotificationSettings({super.key});
  @override
  Widget build(BuildContext context) {
    final n = NotificationCoordinator.instance;
    return Card(
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            const Text(
              'Update announcements',
              style: TextStyle(fontWeight: FontWeight.bold),
            ),
            const SizedBox(height: 8),
            const Text(
              'Get a notification when a new Philly Scan release is ready.',
            ),
            ValueListenableBuilder<String>(
              valueListenable: n.message,
              builder: (_, value, __) => Text(value),
            ),
            Wrap(
              spacing: 8,
              children: [
                TextButton(onPressed: n.enable, child: const Text('Enable')),
                TextButton(onPressed: n.disable, child: const Text('Turn off')),
                TextButton(
                  onPressed: n.sync,
                  child: const Text('Retry connection'),
                ),
                TextButton(
                  onPressed: () async {
                    try {
                      await NotificationCoordinator.channel.invokeMethod<void>(
                        'openSettings',
                      );
                    } catch (_) {}
                  },
                  child: const Text('iPhone Settings'),
                ),
              ],
            ),
          ],
        ),
      ),
    );
  }
}
