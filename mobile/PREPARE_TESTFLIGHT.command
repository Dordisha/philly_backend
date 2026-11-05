#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")"
flutter pub get
flutter build ios --release --config-only --build-name=1.1.0 --build-number="${BUILD_NUMBER:-14}" --dart-define-from-file=internal-testing.json
open ios/Runner.xcworkspace
printf '\nIn Xcode: select your signing team and Any iOS Device, then Product > Archive. Distribute using TestFlight Internal Only.\n'
