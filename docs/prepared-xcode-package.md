# Prepared Xcode source: 1.1.0-preview.3 (build 6)

Owner asked for complete source/workspace files, not helpers or a simulator run.
Earlier archives omitted generated CocoaPods files; the renamed preview.1 also
incorrectly retained the obsolete launcher. Those were packaging errors.

The prepared preview.3 ZIP restores the original matching ios/Pods tree, support
xcconfigs, real Debug input/output xcfilelists, Flutter.podspec and generated plugin
registrant. Original Runner.xcodeproj and pubspec.lock match the current native
project and dependency lock exactly. No dependency versions changed.

Project paths in Generated.xcconfig are relative to SRCROOT. The SDK path remains
/opt/homebrew/share/flutter, taken directly from the owner's original upload.
The SDK/runtime is not bundled. All 47 hosted Dart packages are vendored from pub.dev,
with each archive checked against the SHA-256 in pubspec.lock. Dart package paths
resolve locally; plugin sources are included directly rather than linking into the
owner's cache. No launcher .app or setup prompt is included.

Reproducible packaging entry point: tools/package_prepared_xcode.py. It requires the
recovered original mobile folder from the owner archive, an existing package config
for the matching lock, and an output directory. It does not execute Mac builds.
Do not call ordinary generated-file-stripped repository ZIPs prepared Xcode packages.

Checks performed: native-project/lock equality, original SDK-path match, restored
support/filelist paths, plugin sources, vendored hashes, local package resolution,
no unexpected symlinks, and ZIP integrity. Xcode compilation, simulator availability
and runtime/device acceptance are still unverified. No production deploy or merge.
