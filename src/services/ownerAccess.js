import { createHash } from 'node:crypto';

// Server-owned grant registry. Never accept a tier or identity from the client.
// Subscription entries must be written by a trusted purchase verifier, not the app.
export function resolveOwnerAccess(authorization, { registry = process.env.OWNER_ACCESS_GRANTS, now = Date.now() } = {}) {
  const token = /^Bearer ([A-Za-z0-9_-]{43,256})$/.exec(authorization || '')?.[1];
  if (!token) return false;
  const entries = JSON.parse(registry || '[]');
  if (!Array.isArray(entries)) throw new Error('Invalid access configuration');
  const hash = createHash('sha256').update(token).digest('hex');
  const grant = entries.find(g => g.token_sha256 === hash);
  if (!grant || !grant.subject || grant.revoked === true || !['pro', 'ai'].includes(grant.tier)) return false;
  if (!['complimentary', 'subscription'].includes(grant.source)) return false;
  if (!Number.isFinite(Date.parse(grant.expires_at)) || Date.parse(grant.expires_at) <= now) return false;
  if (grant.source === 'subscription' && (!grant.verified_transaction_id || !Number.isFinite(Date.parse(grant.verified_at)))) return false;
  return true;
}
