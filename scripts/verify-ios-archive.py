"""Verify a prepared unsigned device archive; this never signs or uploads it."""
import json
import pathlib
import plistlib
import subprocess
import sys

archive = pathlib.Path(sys.argv[1]).resolve()
with (archive / "Info.plist").open("rb") as f:
    metadata = plistlib.load(f)
relative_app = metadata["ApplicationProperties"]["ApplicationPath"]
app = (archive / "Products" / relative_app).resolve()
assert app.is_relative_to(archive / "Products"), "Invalid archive application path"
with (app / "Info.plist").open("rb") as f:
    info = plistlib.load(f)
assert info["CFBundleIdentifier"] == "com.dorian.phillyscan"
assert info["CFBundleShortVersionString"] == "1.1.0"
assert info["CFBundleVersion"] == "23"
for key in ("CFBundleIdentifier", "CFBundleShortVersionString", "CFBundleVersion"):
    assert metadata["ApplicationProperties"][key] == info[key], key
assert info["CFBundleSupportedPlatforms"] == ["iPhoneOS"]
assert info["PhillyAPNSEnvironment"] == "production"
with (app / "PrivacyInfo.xcprivacy").open("rb") as f:
    privacy = plistlib.load(f)
assert privacy["NSPrivacyTracking"] is False
assert any(api["NSPrivacyAccessedAPIType"] == "NSPrivacyAccessedAPICategoryUserDefaults"
           and "CA92.1" in api["NSPrivacyAccessedAPITypeReasons"]
           for api in privacy["NSPrivacyAccessedAPITypes"])
architectures = subprocess.check_output(
    ["lipo", "-archs", str(app / info["CFBundleExecutable"])], text=True).strip().split()
assert architectures == ["arm64"], architectures
frameworks = sorted(p.name for p in (app / "Frameworks").glob("*.framework"))
assert "Flutter.framework" in frameworks and "App.framework" in frameworks
assert (archive / "dSYMs" / "Runner.app.dSYM").is_dir(), "Missing native symbols"
signature = subprocess.run(["codesign", "-dv", str(app)], capture_output=True, text=True)
assert signature.returncode != 0, "Expected unsigned preparation archive"
assert "not signed" in signature.stderr.lower(), signature.stderr
print(json.dumps({
    "version": info["CFBundleShortVersionString"], "build": info["CFBundleVersion"],
    "bundle_id": info["CFBundleIdentifier"], "platform": info["CFBundleSupportedPlatforms"],
    "architectures": architectures, "frameworks": frameworks,
    "app_privacy_manifest": privacy, "native_symbols_present": True,
    "unsigned": True, "apns_environment": info["PhillyAPNSEnvironment"],
    "release_status": "HOLD: signing, export, production compatibility and final approval pending"
}, indent=2))
