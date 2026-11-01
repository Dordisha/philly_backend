import 'package:flutter/foundation.dart';

/// UI samples only. Never grants a server entitlement or enables purchases.
abstract final class ProPreviewConfig {
  static const enabled = kDebugMode &&
      bool.fromEnvironment('PRO_PREVIEW', defaultValue: false);
}
