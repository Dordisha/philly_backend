import 'package:flutter/foundation.dart';

/// Debug builds open live-data testing by default. No server entitlement or purchase is granted.
abstract final class ProPreviewConfig {
  static const enabled = kDebugMode &&
      bool.fromEnvironment('PRO_PREVIEW', defaultValue: true);
}
