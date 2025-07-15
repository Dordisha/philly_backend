const puppeteer = require('puppeteer');

async function fetchTaxBalance(opaNumber) {
  const url = `https://property.phila.gov/?p=${opaNumber}`;
  const browser = await puppeteer.launch({ headless: true });
  const page = await browser.newPage();
  await page.goto(url, { waitUntil: 'networkidle2' });

  await page.waitForSelector('.FGNVT');

  const balanceText = await page.evaluate(() => {
    const spans = Array.from(document.querySelectorAll('.FGNVT'));
    for (let span of spans) {
      const text = span.textContent.trim();
      if (/\$[\d,.]+/.test(text) || /balance/i.test(text)) {
        return text;
      }
    }
    return 'Balance not found';
  });

  await browser.close();
  return balanceText;
}

module.exports = { fetchTaxBalance };
