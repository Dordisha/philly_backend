const puppeteer = require('puppeteer');

async function fetchTaxBalance(opaNumber) {
  const url = `https://property.phila.gov/?p=${opaNumber}`;

  try {
    const browser = await puppeteer.launch({
      headless: true,
      args: ['--no-sandbox', '--disable-setuid-sandbox'],
    });

    const page = await browser.newPage();
    await page.goto(url, { waitUntil: 'networkidle2' });

    await page.waitForSelector('.FGNVT', { timeout: 10000 });

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
  } catch (error) {
    console.error('[PUPPETEER ERROR]', error); // 👈 add this
    throw error; // don't swallow the error — let your route handle it
  }
}

module.exports = { fetchTaxBalance };
