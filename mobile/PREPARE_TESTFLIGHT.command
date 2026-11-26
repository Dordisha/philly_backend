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
build_number="${BUILD_NUMBER:-$(sed -n 's/^version: .*+//p' pubspec.yaml)}"
export CI=true
export FLUTTER_SUPPRESS_ANALYTICS=true
flutter pub get
flutter analyze
flutter test --dart-define=PRO_PREVIEW=false
flutter build ios --release --config-only --build-name=1.1.0 --build-number="$build_number" --dart-define-from-file=release-settings.json
open ios/Runner.xcworkspace
printf '\nPrepared developer workspace. In Xcode: select Any iOS Device, Product > Archive. HOLD upload/submission until production compatibility, privacy, native acceptance and final owner approval are recorded in tracker #18. No subscriptions are sold in this build.\n'
