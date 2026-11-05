import { Router } from 'express';
import { resolveOwnerAccess } from '../services/ownerAccess.js';
import { searchOwnerProperties } from '../lib/ownerSearch.js';

export function ownerRouter({ authorize = resolveOwnerAccess, search = searchOwnerProperties } = {}) {
  const router = Router();
  router.use(async (req, res, next) => {
    res.set('Cache-Control', 'no-store');
    try {
      if (await authorize(req.get('Authorization')) !== true) return res.status(403).json({ ok: false, code: 'pro_required', message: 'Pro subscription required.' });
      next();
    } catch {
      res.status(503).json({ ok: false, message: 'Access verification unavailable. Please retry.' });
    }
  });
  router.get('/', async (req, res) => {
    try {
      const result = await search(req.query.name, { limit: 25, after: req.query.after ?? '' });
      res.json({ ok: true, ...result });
    } catch (error) {
      res.status(error instanceof TypeError ? 400 : 503).json({ ok: false, message: error instanceof TypeError ? 'A valid recorded owner name and cursor are required.' : 'Owner records unavailable. Please retry.' });
    }
  });
  return router;
}
