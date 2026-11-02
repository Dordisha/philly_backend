#!/bin/bash
# Prepare the owner's downloaded Flutter source without requiring Terminal input.
set -Eeuo pipefail
export PATH="/opt/homebrew/bin:/opt/homebrew/opt/ruby/bin:/usr/local/bin:$HOME/.rbenv/shims:$HOME/.asdf/shims:$HOME/.pub-cache/bin:$PATH"
export CI=true COCOAPODS_DISABLE_STATS=true
LOG_DIR="$HOME/Library/Logs/PhillyScan"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/XcodeDependencyRepair.log"
exec > "$LOG_FILE" 2>&1
trap 'printf "\nSetup stopped at line %s.\n" "$LINENO"' ERR
printf 'Philly Scan 1.1.0-preview.2 — local Xcode dependency repair\n'

PROJECT_DIR=""
for candidate in "$HOME"/Downloads/Philly_Scan_1.1.0-preview.2* "$HOME"/Downloads/Philly_Scan_Xcode_Project* "$HOME"/Downloads/Philly_Scan_Xcode_R2*; do
  if [ -f "$candidate/pubspec.yaml" ] && [ -f "$candidate/ios/Podfile" ]; then
    if [ -z "$PROJECT_DIR" ] || [ "$candidate" -nt "$PROJECT_DIR" ]; then PROJECT_DIR="$candidate"; fi
  fi
done
if [ -z "$PROJECT_DIR" ]; then
  PROJECT_DIR="$(/usr/bin/osascript -e 'POSIX path of (choose folder with prompt "Select the extracted Philly Scan project folder containing pubspec.yaml and ios.")')"
fi
if [ ! -f "$PROJECT_DIR/pubspec.yaml" ] || [ ! -f "$PROJECT_DIR/ios/Podfile" ]; then
  printf 'The selected directory is not the Flutter project. No project files were changed.\n'
  exit 1
fi
printf 'Project: %s\n' "$PROJECT_DIR"

# Reuse the same shell configuration that already made flutter work on this Mac.
FLUTTER_BIN="$(command -v flutter || true)"
if [ -z "$FLUTTER_BIN" ]; then
  FLUTTER_BIN="$(/bin/zsh -lic 'whence -p flutter' 2>/dev/null | tail -n 1 || true)"
fi
if [ ! -x "$FLUTTER_BIN" ]; then
  FLUTTER_BIN=""
  for sdk in "$HOME/development/flutter" "$HOME/Developer/flutter" "$HOME/dev/flutter" "$HOME/flutter" "$HOME/Documents/flutter" /opt/homebrew/share/flutter; do
    if [ -x "$sdk/bin/flutter" ]; then FLUTTER_BIN="$sdk/bin/flutter"; break; fi
  done
fi
if [ -z "$FLUTTER_BIN" ]; then
  for config in "$HOME/philly_scan_app/ios/Flutter/Generated.xcconfig" "$PROJECT_DIR/ios/Flutter/Generated.xcconfig"; do
    if [ -f "$config" ]; then
      sdk="$(sed -n 's/^FLUTTER_ROOT=//p' "$config" | head -n 1 | tr -d '\r')"
      if [ -n "$sdk" ] && [ -x "$sdk/bin/flutter" ]; then FLUTTER_BIN="$sdk/bin/flutter"; break; fi
    fi
  done
fi
if [ -z "$FLUTTER_BIN" ] && [ -x /usr/bin/mdfind ]; then
  while IFS= read -r sdk; do
    if [ -x "$sdk/bin/flutter" ]; then FLUTTER_BIN="$sdk/bin/flutter"; break; fi
    if [ -f "$sdk" ] && [ -x "$sdk" ] && [ "$(basename "$(dirname "$sdk")")" = bin ]; then FLUTTER_BIN="$sdk"; break; fi
  done < <(/usr/bin/mdfind -onlyin "$HOME" 'kMDItemFSName == "flutter"' 2>/dev/null)
fi
if [ -z "$FLUTTER_BIN" ] || [ ! -x "$FLUTTER_BIN" ]; then
  printf 'Could not locate the existing Flutter SDK. This repair did not install or replace an SDK.\n'
  exit 1
fi
export PATH="$(dirname "$FLUTTER_BIN"):$PATH"
POD_BIN="$(command -v pod || true)"
if [ -z "$POD_BIN" ]; then POD_BIN="$(/bin/zsh -lic 'whence -p pod' 2>/dev/null | tail -n 1 || true)"; fi
if [ ! -x "$POD_BIN" ]; then
  POD_BIN=""
  for pod in "$HOME"/.gem/ruby/*/bin/pod /opt/homebrew/lib/ruby/gems/*/bin/pod /usr/local/lib/ruby/gems/*/bin/pod; do
    if [ -x "$pod" ]; then POD_BIN="$pod"; break; fi
  done
fi
if [ -z "$POD_BIN" ]; then
  printf 'CocoaPods is not installed or could not be found. No system software was installed by this repair.\n'
  exit 1
fi
export PATH="$(dirname "$POD_BIN"):$PATH"
printf 'Using existing Flutter: %s\nUsing existing CocoaPods: %s\n' "$FLUTTER_BIN" "$POD_BIN"
/usr/bin/xcrun --find xcodebuild
cd "$PROJECT_DIR"
"$FLUTTER_BIN" pub get
"$FLUTTER_BIN" build ios --config-only --simulator --debug --dart-define=APP_ENV=test --dart-define=PRO_PREVIEW=true
(cd ios && "$POD_BIN" install)
# Verify real generated dependency files; never manufacture empty file lists.
SUPPORT="ios/Pods/Target Support Files/Pods-Runner"
test -s ios/Flutter/Generated.xcconfig
test -s "$SUPPORT/Pods-Runner.debug.xcconfig"
test -f "$SUPPORT/Pods-Runner-frameworks-Debug-input-files.xcfilelist"
test -f "$SUPPORT/Pods-Runner-frameworks-Debug-output-files.xcfilelist"
grep -q 'PODS_ROOT' "$SUPPORT/Pods-Runner.debug.xcconfig"
printf '\nRequired CocoaPods configuration and framework file lists exist. Opening the canonical iOS workspace.\n'
/usr/bin/open -a Xcode "$PROJECT_DIR/ios/Runner.xcworkspace"
