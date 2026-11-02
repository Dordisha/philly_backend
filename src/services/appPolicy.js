export const STORE_URL = 'https://apps.apple.com/us/app/philly-scan/id6758272125';
export function versionParts(value) {
  return typeof value === 'string' && /^\d{1,4}(\.\d{1,4}){0,2}$/.test(value)
    ? value.split('.').map(Number) : null;
}
export function compareVersions(a, b) {
  const x = versionParts(a), y = versionParts(b);
  if (!x || !y) throw new Error('Invalid version');
  for (let i=0;i<3;i++) { const d=(x[i]||0)-(y[i]||0); if(d) return Math.sign(d); }
  return 0;
}
export function appPolicy(env = process.env) {
  const minimumVersion = env.IOS_MINIMUM_VERSION || '1.0.0';
  const releasedVersion = env.IOS_RELEASED_VERSION || '1.0.3';
  const valid = versionParts(minimumVersion) && versionParts(releasedVersion)
    && compareVersions(minimumVersion, releasedVersion) <= 0;
  return {
    schema: 1, platform:'ios', bundleId:'com.dorian.phillyscan',
    required: Boolean(valid && env.IOS_REQUIRE_UPDATE === 'true'),
    minimumVersion: valid ? minimumVersion : '1.0.0',
    releasedVersion: valid ? releasedVersion : '1.0.3',
    message: 'Please update Philly Scan to continue.', storeUrl: STORE_URL,
    notificationsEnabled: env.PUSH_REGISTRATION_ENABLED === 'true',
  };
}
