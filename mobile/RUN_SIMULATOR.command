#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")"
flutter pub get
open -a Simulator
flutter run --debug --dart-define=APP_ENV=test --dart-define=PRO_PREVIEW=true "$@"
