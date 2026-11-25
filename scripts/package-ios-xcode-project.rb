#!/usr/bin/env ruby
# Build a portable native Xcode project around precompiled Flutter frameworks.
# This does not sign, export, upload, or change the app's Dart/native sources.
require 'fileutils'
require 'json'
require 'digest'
require 'xcodeproj'

mobile, frameworks_root, output = ARGV.map { |p| File.expand_path(p) }
abort 'usage: package-ios-xcode-project.rb MOBILE RELEASE_FRAMEWORKS OUTPUT' unless output
abort "Output already exists: #{output}" if File.exist?(output)
version = File.read(File.join(mobile, 'pubspec.yaml')).match(/^version:\s*(\S+)$/)[1]
name, build = version.split('+')
abort 'Unexpected version' unless name == '1.1.0' && build == '23'
team = 'YBKAAF3GF2'
FileUtils.mkdir_p(File.join(output, 'Frameworks'))
FileUtils.mkdir_p(File.join(output, 'Symbols'))
FileUtils.cp_r(File.join(mobile, 'ios/Runner'), File.join(output, 'Runner'))
# This command generates registrants outside the framework directory for apps.
%w[GeneratedPluginRegistrant.h GeneratedPluginRegistrant.m].each do |file|
  source = File.join(frameworks_root, '..', file)
  source = File.join(mobile, 'ios/Runner', file) unless File.file?(source)
  abort "Missing #{file}" unless File.file?(source)
  FileUtils.cp(source, File.join(output, 'Runner', file))
end

framework_names = []
Dir.glob(File.join(frameworks_root, '*.xcframework')).sort.each do |xcframework|
  metadata = Xcodeproj::Plist.read_from_path(File.join(xcframework, 'Info.plist'))
  variants = metadata.fetch('AvailableLibraries').select do |v|
    v['SupportedPlatform'] == 'ios' && !v.key?('SupportedPlatformVariant')
  end
  abort "Expected one iPhoneOS variant: #{xcframework}" unless variants.length == 1
  variant = variants.first
  abort 'Expected arm64 device framework' unless variant['SupportedArchitectures'] == ['arm64']
  root = File.join(xcframework, variant.fetch('LibraryIdentifier'))
  framework = File.join(root, variant.fetch('LibraryPath'))
  framework_names << File.basename(framework)
  FileUtils.cp_r(framework, File.join(output, 'Frameworks'))
  Dir.glob(File.join(root, '**/*.dSYM')).each do |symbols|
    FileUtils.cp_r(symbols, File.join(output, 'Symbols'))
  end
end
expected = %w[App.framework Flutter.framework integration_test.framework url_launcher_ios.framework]
abort "Unexpected frameworks: #{framework_names}" unless framework_names.sort == expected.sort

project = Xcodeproj::Project.new(File.join(output, 'PhillyScan.xcodeproj'))
project.root_object.attributes['LastUpgradeCheck'] = '2600'
target = project.new_target(:application, 'Runner', :ios, '13.0')
project.root_object.attributes['TargetAttributes'] = {
  target.uuid => { 'DevelopmentTeam' => team, 'ProvisioningStyle' => 'Automatic',
                   'SystemCapabilities' => { 'com.apple.Push' => { 'enabled' => 1 } } }
}
sources = project.main_group.new_group('Runner', 'Runner')
%w[AppDelegate.swift GeneratedPluginRegistrant.m].each do |file|
  target.source_build_phase.add_file_reference(sources.new_file(file))
end
%w[GeneratedPluginRegistrant.h Runner-Bridging-Header.h Runner.entitlements Info.plist].each do |file|
  sources.new_file(file)
end
%w[Assets.xcassets Base.lproj/Main.storyboard Base.lproj/LaunchScreen.storyboard PrivacyInfo.xcprivacy].each do |file|
  target.resources_build_phase.add_file_reference(sources.new_file(file))
end

framework_group = project.main_group.new_group('Frameworks', 'Frameworks')
embed = target.new_copy_files_build_phase('Embed Frameworks')
embed.dst_subfolder_spec = '10'
framework_names.each do |file|
  ref = framework_group.new_file(file)
  target.frameworks_build_phase.add_file_reference(ref)
  copy = embed.add_file_reference(ref)
  copy.settings = { 'ATTRIBUTES' => ['CodeSignOnCopy', 'RemoveHeadersOnCopy'] }
end
project.build_configurations.each do |config|
  config.build_settings['IPHONEOS_DEPLOYMENT_TARGET'] = '13.0'
  config.build_settings['CLANG_ENABLE_MODULES'] = 'YES'
end
target.build_configurations.each do |config|
  config.build_settings.merge!({
    'PRODUCT_BUNDLE_IDENTIFIER' => 'com.dorian.phillyscan',
    'DEVELOPMENT_TEAM' => team,
    'CODE_SIGN_STYLE' => 'Automatic',
    'CODE_SIGN_IDENTITY' => 'Apple Development',
    'CODE_SIGN_ENTITLEMENTS' => 'Runner/Runner.entitlements',
    'INFOPLIST_FILE' => 'Runner/Info.plist',
    'GENERATE_INFOPLIST_FILE' => 'NO',
    'FLUTTER_BUILD_NAME' => name,
    'FLUTTER_BUILD_NUMBER' => build,
    'MARKETING_VERSION' => name,
    'CURRENT_PROJECT_VERSION' => build,
    'APS_ENVIRONMENT' => 'production',
    'TARGETED_DEVICE_FAMILY' => '1',
    'SUPPORTED_PLATFORMS' => 'iphoneos',
    'SUPPORTS_MACCATALYST' => 'NO',
    'SUPPORTS_MAC_DESIGNED_FOR_IPHONE_IPAD' => 'NO',
    'SDKROOT' => 'iphoneos',
    'ARCHS' => 'arm64',
    'ASSETCATALOG_COMPILER_APPICON_NAME' => 'AppIcon',
    'SWIFT_VERSION' => '5.0',
    'SWIFT_OBJC_BRIDGING_HEADER' => 'Runner/Runner-Bridging-Header.h',
    'CLANG_ENABLE_MODULES' => 'YES',
    'CLANG_ENABLE_OBJC_ARC' => 'YES',
    'FRAMEWORK_SEARCH_PATHS' => ['$(inherited)', '$(PROJECT_DIR)/Frameworks'],
    'LD_RUNPATH_SEARCH_PATHS' => ['$(inherited)', '@executable_path/Frameworks'],
    'ALWAYS_EMBED_SWIFT_STANDARD_LIBRARIES' => 'YES',
    'DEBUG_INFORMATION_FORMAT' => 'dwarf-with-dsym',
    'ENABLE_USER_SCRIPT_SANDBOXING' => 'NO'
  })
end
symbols = target.new_shell_script_build_phase('Copy precompiled framework symbols')
symbols.shell_script = <<~'SH'
  set -eu
  mkdir -p "${DWARF_DSYM_FOLDER_PATH}"
  for symbols in "${PROJECT_DIR}"/Symbols/*.dSYM; do
    [ -d "$symbols" ] || continue
    /usr/bin/ditto "$symbols" "${DWARF_DSYM_FOLDER_PATH}/$(basename "$symbols")"
  done
SH
symbols.always_out_of_date = '1'
project.save
scheme = Xcodeproj::XCScheme.new
scheme.add_build_target(target)
scheme.set_launch_target(target)
scheme.launch_action.build_configuration = 'Release'
scheme.archive_action.build_configuration = 'Release'
scheme.archive_action.reveal_archive_in_organizer = true
scheme.save_as(project.path, 'PhillyScan', true)

File.write(File.join(output, 'START-HERE.txt'), <<~TEXT)
  PHILLY SCAN 1.1.0 (23) — XCODE SIGNING PROJECT

  1. Double-click PhillyScan.xcodeproj in this folder.
  2. Click the blue PhillyScan project in the left sidebar, then the Runner target.
  3. Open Signing & Capabilities. Keep automatic signing on and choose Dorian Dishani.
  4. At the top of Xcode choose Any iOS Device (arm64), then Product > Archive.
  5. The NEW archive appears in Organizer. Select it and click Validate App.

  Keep this whole folder together. Flutter, CocoaPods and Terminal are not required.
  This project includes the compiled release app. It is for iPhone archiving, not Simulator.
  Xcode signs with your Apple Developer account on your Mac; this download is not signed.

  Release remains on hold for production-backend compatibility, privacy/Store review,
  Apple signing/validation and final owner release approval. Do not submit this candidate yet.
TEXT
manifest = {
  'version' => version, 'team' => team, 'frameworks' => framework_names,
  'source_commit' => `git -C "#{mobile}" rev-parse HEAD`.strip,
  'packaging' => 'Precompiled Flutter iPhoneOS frameworks; native host rebuilt and signed in Xcode',
  'apple_signing_verified' => false, 'release_approved' => false
}
manifest['files_sha256'] = Dir.glob(File.join(output, '**/*')).select { |p| File.file?(p) }.to_h do |path|
  [path.delete_prefix(output + '/'), Digest::SHA256.file(path).hexdigest]
end
File.write(File.join(output, 'PACKAGE-EVIDENCE.json'), JSON.pretty_generate(manifest) + "\n")
puts JSON.pretty_generate(manifest.reject { |k, _| k == 'files_sha256' })
