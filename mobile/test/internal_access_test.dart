import 'package:flutter_test/flutter_test.dart';
import 'package:philly_scan_app/config/pro_preview_config.dart';

void main() {
  test('internal code cannot activate normal builds or accept wrong input', () {
    expect(ProPreviewConfig.unlock('wrong-code'), isFalse);
    const code = String.fromEnvironment('TEST_INPUT_CODE');
    if (ProPreviewConfig.internalTesting && code.isNotEmpty) {
      expect(ProPreviewConfig.enabled, isFalse);
      expect(ProPreviewConfig.unlock(code), isTrue);
      expect(ProPreviewConfig.enabled, isTrue);
    } else {
      expect(ProPreviewConfig.unlock(code), isFalse);
    }
  });
}
