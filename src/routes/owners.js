import { Router } from 'express';
import { searchOwnerProperties } from '../lib/ownerSearch.js';

export function ownerRouter({ search = searchOwnerProperties } = {}) {
  const router = Router();
  // Public property data is free; input validation and bounded paging remain.
  router.use((req, res, next) => {
    res.set('Cache-Control', 'no-store');
    next();
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
