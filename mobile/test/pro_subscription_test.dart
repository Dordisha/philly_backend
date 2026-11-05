import 'package:flutter/services.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:philly_scan_app/services/pro_subscription.dart';

void main() {
  TestWidgetsFlutterBinding.ensureInitialized();
  final messenger =
      TestDefaultBinaryMessengerBinding.instance.defaultBinaryMessenger;
  late ProSubscription store;
  dynamic status;
  dynamic outcome;
  setUp(() {
    store = ProSubscription();
    status = {'active': false};
    outcome = {'outcome': 'cancelled'};
    messenger.setMockMethodCallHandler(ProSubscription.channel, (call) async {
      if (call.method == 'product') return {'price': r'$2.99'};
      if (call.method == 'purchase') return outcome;
      return status;
    });
  });
  tearDown(() async {
    status = {'active': false};
    await store.refresh();
    store.dispose();
    messenger.setMockMethodCallHandler(ProSubscription.channel, null);
  });
  test('active flag without future expiration cannot unlock', () async {
    status = {'active': true};
    await store.refresh();
    expect(store.active, false);
    status = {
      'active': true,
      'expiresAt': DateTime.now()
          .subtract(const Duration(seconds: 1))
          .millisecondsSinceEpoch,
    };
    await store.refresh();
    expect(store.active, false);
  });
  test(
    'restore grants current subscription and revocation removes it',
    () async {
      status = {
        'active': true,
        'expiresAt': DateTime.now()
            .add(const Duration(hours: 1))
            .millisecondsSinceEpoch,
      };
      await store.transact(restore: true);
      expect(store.active, true);
      status = {'active': false};
      await store.refresh();
      expect(store.active, false);
    },
  );
  test('cancelled and pending purchases never unlock Pro', () async {
    await store.loadProduct();
    await store.transact();
    expect(store.active, false);
    outcome = {'outcome': 'pending'};
    await store.transact();
    expect(store.active, false);
    expect(store.message, contains('approval'));
  });
  test('bridge failure removes stale entitlement', () async {
    status = {
      'active': true,
      'expiresAt': DateTime.now()
          .add(const Duration(hours: 1))
          .millisecondsSinceEpoch,
    };
    await store.refresh();
    messenger.setMockMethodCallHandler(
      ProSubscription.channel,
      (_) async => throw PlatformException(code: 'offline'),
    );
    await store.refresh();
    expect(store.active, false);
  });
}
