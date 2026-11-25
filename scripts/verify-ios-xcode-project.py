"""Check a fresh portable-project archive without claiming Apple signing passed."""
import hashlib
import json
import pathlib
import re
import subprocess
import sys

root, archive, settings_file = map(pathlib.Path, sys.argv[1:])
manifest = json.loads((root / 'PACKAGE-EVIDENCE.json').read_text())
for relative, digest in manifest['files_sha256'].items():
    assert hashlib.sha256((root / relative).read_bytes()).hexdigest() == digest, relative
project = (root / 'PhillyScan.xcodeproj/project.pbxproj').read_text()
for obsolete in ('Generated.xcconfig', 'Pods/', 'PODS_ROOT', 'FLUTTER_ROOT', 'xcode_backend.sh'):
    assert obsolete not in project, obsolete
assert 'CodeSignOnCopy' in project
settings = next(x['buildSettings'] for x in json.loads(settings_file.read_text()) if x['target'] == 'Runner')
assert settings['DEVELOPMENT_TEAM'] == 'YBKAAF3GF2'
assert settings['CODE_SIGN_STYLE'] == 'Automatic'
assert settings['CODE_SIGNING_ALLOWED'] == 'YES', settings.get('CODE_SIGNING_ALLOWED')
assert settings['CODE_SIGN_ENTITLEMENTS'] == 'Runner/Runner.entitlements'
assert settings['FLUTTER_BUILD_NAME'] == '1.1.0' and settings['FLUTTER_BUILD_NUMBER'] == '23'
assert settings['APS_ENVIRONMENT'] == 'production'
app = archive / 'Products/Applications/Runner.app'
assert (app / 'Frameworks/App.framework/flutter_assets').is_dir()
assert list(app.rglob('Main.storyboardc'))
assert list(app.rglob('LaunchScreen.storyboardc'))
assert (app / 'Assets.car').is_file()

def uuids(binary):
    text = subprocess.check_output(['dwarfdump', '--uuid', str(binary)], text=True)
    return sorted(re.findall(r'UUID: ([A-Fa-f0-9-]+) \(([^)]+)\)', text))

symbol_matches = {}
for name in manifest['frameworks']:
    framework = app / 'Frameworks' / name
    binary_name = name.removesuffix('.framework')
    source = root / 'Frameworks' / name / binary_name
    binary = framework / binary_name
    assert uuids(source) == uuids(binary), f'Changed framework UUID: {name}'
    symbols = archive / 'dSYMs' / (name + '.dSYM') / 'Contents/Resources/DWARF' / binary_name
    assert symbols.is_file(), f'Missing symbols: {name}'
    assert uuids(binary) == uuids(symbols), f'Mismatched symbols: {name}'
    symbol_matches[name] = uuids(binary)

print(json.dumps({
    'version': manifest['version'], 'source_commit': manifest['source_commit'],
    'fresh_zip_hashes_verified': True, 'standalone_archive_succeeded': True,
    'flutter_or_pods_project_dependencies': False,
    'automatic_signing_team': settings['DEVELOPMENT_TEAM'],
    'signing_enabled_in_delivered_project': True,
    'framework_symbol_matches': symbol_matches,
    'apple_signing_or_validation_verified': False,
    'production_release_approved': False,
}, indent=2))
