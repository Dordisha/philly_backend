/// One host for lookup and enforcement. Release builds can target staging too.
class ApiConfig {
  static const environment = String.fromEnvironment(
    'APP_ENV',
    defaultValue: 'test',
  );
  static const _override = String.fromEnvironment('API_BASE_URL');
  static const testBaseUrl = 'https://philly-scan-repair-test.onrender.com';
  static const prodBaseUrl = 'https://philly-backend-main.onrender.com';

  static String get baseUrl {
    if (environment != 'test' && environment != 'prod') {
      throw StateError('APP_ENV must be test or prod');
    }
    final value = _override.isNotEmpty
        ? _override
        : environment == 'prod'
        ? prodBaseUrl
        : testBaseUrl;
    final uri = Uri.tryParse(value);
    if (uri == null ||
        uri.scheme != 'https' ||
        uri.host.isEmpty ||
        uri.userInfo.isNotEmpty ||
        uri.hasQuery ||
        uri.hasFragment ||
        (uri.path.isNotEmpty && uri.path != '/')) {
      throw StateError('API_BASE_URL must be an HTTPS origin');
    }
    return value.replaceFirst(RegExp(r'/$'), '');
  }
}
