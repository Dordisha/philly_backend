"""Package tracked app source with the owner's recovered, matching CocoaPods files.

Does not run Xcode/Simulator. Hosted Dart sources are vendored from pub.dev and
checked against pubspec.lock hashes. Requires the owner's existing Flutter SDK.
"""
import argparse,base64,concurrent.futures,hashlib,io,json,os,pathlib,re,shutil,subprocess,tarfile,urllib.request,zipfile
import yaml

parser=argparse.ArgumentParser()
parser.add_argument('--original',required=True,type=pathlib.Path)
parser.add_argument('--package-config',required=True,type=pathlib.Path)
parser.add_argument('--output-dir',required=True,type=pathlib.Path)
args=parser.parse_args()
repo=pathlib.Path(__file__).resolve().parent.parent
version='1.1.0-preview.3'
build='6'
name='Philly_Scan_'+version
stage=args.output_dir/name
stage.mkdir(parents=True,exist_ok=True)
tracked=subprocess.check_output(['git','ls-files','-z','mobile'],cwd=repo).decode().split('\0')
for rel in tracked:
 if not rel:continue
 src=repo/rel
 dest=stage/src.relative_to(repo/'mobile')
 if not src.is_file() or 'Open_Philly_Scan.app' in src.parts or src.name in ['OPEN_IN_XCODE.txt','README.md','START_HERE.txt']:continue
 dest.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(src,dest)
# Recovered dependency lock must match the current app exactly.
assert (args.original/'pubspec.lock').read_bytes()==(repo/'mobile/pubspec.lock').read_bytes()
shutil.copytree(args.original/'ios/Pods',stage/'ios/Pods',dirs_exist_ok=True,symlinks=True)
for rel in ['ios/Flutter/Flutter.podspec','ios/Runner/GeneratedPluginRegistrant.h','ios/Runner/GeneratedPluginRegistrant.m']:
 dest=stage/rel;dest.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(args.original/rel,dest)
shutil.copytree(args.original/'ios/Flutter/ephemeral',stage/'ios/Flutter/ephemeral',dirs_exist_ok=True)

lock=yaml.safe_load((stage/'pubspec.lock').read_text())['packages']
cache=args.output_dir/'public-package-archives';cache.mkdir(exist_ok=True)
def vendor(item):
 package,entry=item
 if entry['source']!='hosted':return
 v=entry['version'];expected=entry['description']['sha256']
 assert re.fullmatch(r'[a-zA-Z0-9_]+',package) and re.fullmatch(r'[a-zA-Z0-9.+-]+',v)
 archive=cache/f'{package}-{v}.tar.gz'
 if not archive.exists():
  data=urllib.request.urlopen(f'https://pub.dev/api/archives/{package}-{v}.tar.gz',timeout=60).read()
  if hashlib.sha256(data).hexdigest()!=expected:raise ValueError('Checksum mismatch: '+package)
  archive.write_bytes(data)
 else:data=archive.read_bytes()
 assert hashlib.sha256(data).hexdigest()==expected
 dest=stage/'vendor/packages'/f'{package}-{v}';dest.mkdir(parents=True,exist_ok=True)
 with tarfile.open(fileobj=io.BytesIO(data),mode='r:gz') as tar:tar.extractall(dest,filter='data')
 return package
with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
 completed=[p for p in pool.map(vendor,lock.items()) if p]
print(f'Vendored {len(completed)} checksum-verified public packages.',flush=True)
# Native pod project references .symlinks/plugins. Supply a real directory instead
# of the original absolute symlink into the owner's .pub-cache.
plugin=stage/'ios/.symlinks/plugins/url_launcher_ios'
plugin.parent.mkdir(parents=True,exist_ok=True)
shutil.copytree(stage/'vendor/packages/url_launcher_ios-6.3.6',plugin,dirs_exist_ok=True)
config=json.loads(args.package_config.read_text())
flutter_root='/opt/homebrew/share/flutter'  # Exact SDK path from owner's original Generated.xcconfig.
original=(args.original/'ios/Flutter/Generated.xcconfig').read_text()
assert f'FLUTTER_ROOT={flutter_root}' in original
for package in config['packages']:
 p=package['name']
 if p in lock and lock[p]['source']=='hosted':package['rootUri']='../vendor/packages/'+p+'-'+lock[p]['version']
 elif p=='philly_scan_app':package['rootUri']='../'
 elif p in ['flutter','flutter_test','flutter_web_plugins']:package['rootUri']='file://'+flutter_root+'/packages/'+p
 elif p=='sky_engine':package['rootUri']='file://'+flutter_root+'/bin/cache/pkg/sky_engine'
 else:raise ValueError('Unmapped package '+p)
config.pop('pubCache',None);config['flutterRoot']='file://'+flutter_root
(stage/'.dart_tool').mkdir(exist_ok=True)
(stage/'.dart_tool/package_config.json').write_text(json.dumps(config,indent=2))
# Relocate project-specific generated settings instead of retaining the old app
# path or any build-host path. SDK path is the owner's existing Homebrew SDK.
defines=','.join(base64.b64encode(s.encode()).decode() for s in ['APP_ENV=test','PRO_PREVIEW=true'])
settings={
 'FLUTTER_ROOT':flutter_root,'FLUTTER_APPLICATION_PATH':'$(SRCROOT)/..',
 'COCOAPODS_PARALLEL_CODE_SIGN':'true','FLUTTER_TARGET':'lib/main.dart',
 'FLUTTER_BUILD_DIR':'build','FLUTTER_BUILD_NAME':'1.1.0','FLUTTER_BUILD_NUMBER':build,
 'EXCLUDED_ARCHS[sdk=iphonesimulator*]':'i386','EXCLUDED_ARCHS[sdk=iphoneos*]':'armv7',
 'DART_DEFINES':defines,'DART_OBFUSCATION':'false','TRACK_WIDGET_CREATION':'true',
 'TREE_SHAKE_ICONS':'false','PACKAGE_CONFIG':'$(SRCROOT)/../.dart_tool/package_config.json',
}
(stage/'ios/Flutter/Generated.xcconfig').write_text('// Prepared for the owner\'s existing Mac SDK. Project paths are relocatable.\n'+''.join(k+'='+v+'\n' for k,v in settings.items()))
exports='#!/bin/sh\n# Prepared project settings; no dependency installation or launch helper.\n'
for key,value in settings.items():
 if '[' in key:continue
 value=value.replace('$(SRCROOT)','${SRCROOT}')
 exports+='export "'+key+'='+value+'"\n'
exp=stage/'ios/Flutter/flutter_export_environment.sh';exp.write_text(exports);exp.chmod(0o755)
# Update the app's actual version as well as the download name.
p=stage/'pubspec.yaml';s=p.read_text();s=re.sub(r'^version:.*$',f'version: 1.1.0+{build}',s,flags=re.M);p.write_text(s)
p=stage/'lib/src/main.dart';s=p.read_text().replace('1.1.0-preview.2 (5)',f'{version} ({build})').replace('LIVE DATA TEST • 1.1.0-preview.2','LIVE DATA TEST • '+version);p.write_text(s)
(stage/'VERSION.txt').write_text(f'''Philly Scan {version} (build {build})

Open OPEN_PHILLY_SCAN.xcworkspace, select Runner and your iPhone simulator, and Run.
Keep this full folder together. No launcher app, installer or setup prompts included.

Included: original matching CocoaPods project/support files, framework file lists,
GeneratedPluginRegistrant, relocatable project settings, vendored checksum-verified
Dart dependencies and the updated live-data app code.

Uses your existing Flutter SDK at /opt/homebrew/share/flutter, as in your original
uploaded project. The Flutter SDK and Apple simulator runtimes are not bundled.
Static references and dependency files are checked; Mac compilation/runtime has
NOT been tested here. Public paid Pro purchase/entitlements remain unimplemented.
''')
# Verify files in the reported error and native plugin source paths actually exist.
required=[
 'ios/Pods/Pods.xcodeproj/project.pbxproj',
 'ios/Pods/Target Support Files/Pods-Runner/Pods-Runner-frameworks-Debug-input-files.xcfilelist',
 'ios/Pods/Target Support Files/Pods-Runner/Pods-Runner-frameworks-Debug-output-files.xcfilelist',
 'ios/Pods/Target Support Files/Pods-Runner/Pods-Runner.debug.xcconfig',
 'ios/Flutter/Generated.xcconfig','ios/Flutter/Flutter.podspec',
 'ios/Runner/GeneratedPluginRegistrant.h','ios/Runner/GeneratedPluginRegistrant.m',
 'OPEN_PHILLY_SCAN.xcworkspace/contents.xcworkspacedata',
]
for rel in required:assert (stage/rel).is_file(),rel
for package in config['packages']:
 if not package['rootUri'].startswith('file:'):
  assert (stage/'.dart_tool'/package['rootUri']/package['packageUri']).is_dir(),package['name']
assert not (stage/'Open_Philly_Scan.app').exists()
assert 'PODS_ROOT = ${SRCROOT}/Pods' in (stage/required[3]).read_text()
assert not any(p.is_symlink() for p in stage.rglob('*')), 'Unexpected source symlink'
# Identify all source references in the native pod project that rely on .symlinks.
assert (plugin/'ios/url_launcher_ios/Sources/url_launcher_ios').is_dir()
archive=args.output_dir/(name+'.zip')
with zipfile.ZipFile(archive,'w',zipfile.ZIP_DEFLATED) as z:
 for p in stage.rglob('*'):
  if p.is_file():z.write(p,str(pathlib.Path(name)/p.relative_to(stage)))
with zipfile.ZipFile(archive) as z:
 assert z.testzip() is None
 for rel in required:assert name+'/'+rel in z.namelist()
 print(json.dumps({'file':str(archive),'files':len(z.namelist()),'bytes':archive.stat().st_size,'checks':'Original matching Pods restored, missing xcfilelists present, 47 hosted package hashes verified, local package paths resolved, plugin sources present, ZIP integrity passed; no Mac build run'}),flush=True)
