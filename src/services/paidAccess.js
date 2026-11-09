import { createHash } from 'node:crypto';

// This consumes trusted server records; it does not verify Apple transactions.
// Never populate these records from client-supplied tier/transaction claims.
export function resolvePaidAccess(authorization, requiredTier = 'pro', { registry = process.env.OWNER_ACCESS_GRANTS, now = Date.now() } = {}) {
  if (!['pro', 'ai'].includes(requiredTier)) return false;
  const token = /^Bearer ([A-Za-z0-9_-]{43,256})$/.exec(authorization || '')?.[1];
  if (!token) return false;
  const entries = JSON.parse(registry || '[]');
  if (!Array.isArray(entries)) throw new Error('Invalid access configuration');
  const hash = createHash('sha256').update(token).digest('hex');
  const grant = entries.find(g => g?.token_sha256 === hash);
  if (!grant || !grant.subject || grant.revoked === true || !['pro', 'ai'].includes(grant.tier)) return false;
  if (requiredTier === 'ai' && grant.tier !== 'ai') return false;
  if (!['complimentary', 'subscription'].includes(grant.source)) return false;
  if (!Number.isFinite(Date.parse(grant.expires_at)) || Date.parse(grant.expires_at) <= now) return false;
  if (grant.source === 'subscription' && (!grant.verified_transaction_id || !Number.isFinite(Date.parse(grant.verified_at)))) return false;
  return true;
}

export function requirePaidAccess(tier, authorize = resolvePaidAccess) {
  if (!['pro', 'ai'].includes(tier)) throw new TypeError('Unknown access tier');
  return async (req, res, next) => {
    // Neither a successful paid response nor a denial should be shared/cached.
    res.set('Cache-Control', 'no-store');
    try {
      if (await authorize(req.get('Authorization'), tier) !== true) {
        return res.status(403).json({
          ok: false,
          code: tier === 'ai' ? 'ai_required' : 'pro_required',
          message: tier === 'ai' ? 'AI Premium access required.' : 'Pro subscription required.',
        });
      }
    } catch {
      return res.status(503).json({ ok: false, message: 'Access verification unavailable. Please retry.' });
    }
    next();
  };
}
