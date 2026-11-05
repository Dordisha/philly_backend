import 'dart:convert';
import 'package:crypto/crypto.dart';
import 'package:flutter/foundation.dart';
import 'api_config.dart';

/// Internal testing only; never grants a production subscription.
abstract final class ProPreviewConfig {
  static const _debug =
      kDebugMode && bool.fromEnvironment('PRO_PREVIEW', defaultValue: false);
  static const _internal = bool.fromEnvironment('INTERNAL_TESTING');
  static const _digest = String.fromEnvironment('TEST_ACCESS_SHA256');
  static bool _unlocked = false;
  static bool get internalTesting =>
      _internal &&
      ApiConfig.environment == 'test' &&
      ApiConfig.baseUrl == ApiConfig.testBaseUrl;
  static bool get enabled =>
      (_debug && ApiConfig.environment == 'test') ||
      (internalTesting && _unlocked);
  static bool unlock(String code) {
    if (!internalTesting || _digest.length != 64) return false;
    if (sha256.convert(utf8.encode(code.trim())).toString() != _digest)
      return false;
    _unlocked = true;
    return true;
  }
}
