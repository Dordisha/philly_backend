const express = require('express');
const router = express.Router();
const { getOpaDetails } = require('../services/opaScraper');

// Accepts route like /api/tax/363258000
router.get('/:account', async (req, res) => {
  const account = req.params.account;
  if (!account) return res.status(400).json({ error: 'Missing account number' });

  try {
    const data = await getOpaDetails(account);
    res.json(data);
  } catch (err) {
    console.error('Scraper error:', err);
    res.status(500).json({ error: 'Failed to fetch OPA data' });
  }
});

module.exports = router;
