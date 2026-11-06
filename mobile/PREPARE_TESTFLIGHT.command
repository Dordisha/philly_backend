#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")"
if [[ ! -f release-settings.json ]]; then
  printf 'First fill in Privacy Policy URL in release-settings.json (see START-HERE.md).\n'
  exit 1
fi
python3 - <<'CHECK'
import json, urllib.parse
s=json.load(open('release-settings.json'))
u=urllib.parse.urlparse(s.get('PRIVACY_POLICY_URL',''))
assert u.scheme=='https' and u.netloc and not u.username, 'A published HTTPS privacy-policy URL is required.'
assert s.get('PRO_PREVIEW') is False and s.get('INTERNAL_TESTING') is False, 'Testing bypasses must be disabled.'
assert s.get('APP_ENV') == 'prod', 'The release candidate must use production.'
CHECK
flutter pub get
flutter build ios --release --config-only --build-name=1.1.0 --build-number="${BUILD_NUMBER:-16}" --dart-define-from-file=release-settings.json
open ios/Runner.xcworkspace
printf '\nIn Xcode: confirm bundle ID and signing team, select Any iOS Device, Product > Archive. Upload using App Store Connect (not TestFlight Internal Only if this build is intended for later release). Test purchases on an iPhone.\n'
