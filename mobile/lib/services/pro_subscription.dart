import 'dart:async';
import 'package:flutter/foundation.dart';
import 'package:flutter/services.dart';
import 'package:flutter/widgets.dart';

class ProSubscription extends ChangeNotifier with WidgetsBindingObserver {
  static final instance = ProSubscription();
  static const channel = MethodChannel('philly_scan/pro');
  DateTime? _expires;
  Timer? _expiryTimer;
  bool _started = false;
  bool busy = false;
  String? price, message;
  bool get active => _expires?.isAfter(DateTime.now()) ?? false;

  void start() {
    if (_started) return;
    _started = true;
    WidgetsBinding.instance.addObserver(this);
    channel.setMethodCallHandler((call) async {
      if (call.method == 'changed') _accept(call.arguments);
    });
    unawaited(refresh());
  }

  void _accept(dynamic value) {
    _expiryTimer?.cancel();
    final data = value is Map ? value : const {};
    final end = data['expiresAt'];
    _expires = data['active'] == true && end is num
        ? DateTime.fromMillisecondsSinceEpoch(end.toInt())
        : null;
    if (active) {
      _expiryTimer = Timer(_expires!.difference(DateTime.now()), () {
        _expires = null;
        notifyListeners();
        unawaited(refresh());
      });
    }
    notifyListeners();
  }

  Future<void> refresh() async {
    try {
      _accept(await channel.invokeMethod<dynamic>('status'));
    } catch (_) {
      _accept(null);
    }
  }

  Future<void> loadProduct() async {
    try {
      final data = await channel.invokeMapMethod<String, dynamic>('product');
      price = data?['price'] as String?;
      message = price == null ? 'Pro is currently unavailable.' : null;
    } catch (_) {
      price = null;
      message =
          'Pro is currently unavailable from the App Store. Please try again.';
    }
    notifyListeners();
  }

  Future<void> redeem() async {
    if (busy) return;
    busy = true;
    message = null;
    notifyListeners();
    try {
      _accept(await channel.invokeMethod<dynamic>('redeem'));
      message = active
          ? 'Pro is active.'
          : 'Complete redemption with Apple. If your code was accepted, try Restore Purchases.';
    } catch (_) {
      message = 'Could not open Apple code redemption. Please try again.';
    } finally {
      busy = false;
      notifyListeners();
    }
  }

  Future<void> transact({bool restore = false}) async {
    if (busy || (!restore && price == null)) return;
    busy = true;
    message = null;
    notifyListeners();
    try {
      final data = await channel.invokeMapMethod<String, dynamic>(
        restore ? 'restore' : 'purchase',
      );
      if (data?['outcome'] == 'pending') {
        message =
            'Waiting for Apple approval. Pro unlocks once the purchase completes.';
      } else if (data?['outcome'] == 'cancelled') {
        message = 'Purchase cancelled. You have not been charged.';
      } else if (data?['outcome'] != null) {
        message = 'Purchase not completed. Please try again.';
      } else {
        _accept(data);
        message = active
            ? 'Pro is active.'
            : restore
            ? 'No active Pro subscription found for this Apple Account.'
            : 'Purchase received. Try Restore Purchases if Pro has not unlocked.';
      }
    } catch (_) {
      message =
          'Could not complete the request. Please try again or restore purchases.';
    } finally {
      busy = false;
      notifyListeners();
    }
  }

  @override
  void dispose() {
    _expiryTimer?.cancel();
    if (_started) WidgetsBinding.instance.removeObserver(this);
    super.dispose();
  }

  @override
  void didChangeAppLifecycleState(AppLifecycleState state) {
    if (state == AppLifecycleState.resumed) unawaited(refresh());
  }
}
