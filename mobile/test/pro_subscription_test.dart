import 'dart:async';
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
  test(
    'closing redemption sheet without entitlement does not unlock',
    () async {
      await store.redeem();
      expect(store.active, false);
    },
  );
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
  test('older status cannot restore access after a newer denial', () async {
    final oldReply = Completer<dynamic>();
    var calls = 0;
    messenger.setMockMethodCallHandler(ProSubscription.channel, (_) async {
      if (calls++ == 0) return oldReply.future;
      return {'active': false};
    });
    final oldRequest = store.refresh();
    await Future<void>.delayed(Duration.zero);
    await store.refresh();
    oldReply.complete({
      'active': true,
      'expiresAt': DateTime.now().add(const Duration(hours: 1)).millisecondsSinceEpoch,
    });
    await oldRequest;
    expect(store.active, false);
  });

  test('older bridge failure cannot erase newer verified access', () async {
    final oldReply = Completer<dynamic>();
    var calls = 0;
    messenger.setMockMethodCallHandler(ProSubscription.channel, (_) async {
      if (calls++ == 0) return oldReply.future;
      return {
        'active': true,
        'expiresAt': DateTime.now().add(const Duration(hours: 1)).millisecondsSinceEpoch,
      };
    });
    final oldRequest = store.refresh();
    await Future<void>.delayed(Duration.zero);
    await store.refresh();
    oldReply.completeError(PlatformException(code: 'offline'));
    await oldRequest;
    expect(store.active, true);
  });

  test('delayed restore cannot overwrite a newer revoked status', () async {
    final restoreReply = Completer<dynamic>();
    messenger.setMockMethodCallHandler(ProSubscription.channel, (call) async {
      if (call.method == 'restore') return restoreReply.future;
      return {'active': false};
    });
    final restoring = store.transact(restore: true);
    await Future<void>.delayed(Duration.zero);
    await store.refresh();
    restoreReply.complete({
      'active': true,
      'expiresAt': DateTime.now().add(const Duration(hours: 1)).millisecondsSinceEpoch,
    });
    await restoring;
    expect(store.active, false);
  });

  for (final operation in ['refresh', 'restore', 'redeem']) {
    test('malformed $operation expiration removes existing access', () async {
      for (final invalid in [double.nan, double.infinity, double.negativeInfinity,
        1e20, -1e20, 'tomorrow', null]) {
        status = {
          'active': true,
          'expiresAt': DateTime.now().add(const Duration(hours: 1)).millisecondsSinceEpoch,
        };
        await store.refresh();
        expect(store.active, true);
        status = {'active': true, 'expiresAt': invalid};
        if (operation == 'restore') {
          await store.transact(restore: true);
        } else if (operation == 'redeem') {
          await store.redeem();
        } else {
          await store.refresh();
        }
        expect(store.active, false, reason: '$operation accepted $invalid');
        expect(store.busy, false);
      }
    });
  }

}
