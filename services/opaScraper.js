const chromium = require('chrome-aws-lambda');
const puppeteer = require('puppeteer-core');
const { cleanDollar } = require('../utils/formatHelpers');

const getOpaDetails = async (opaAccountNumber) => {
  let browser = null;

  try {
    browser = await puppeteer.launch({
      args: chromium.args,
      executablePath: (await chromium.executablePath) || '/usr/bin/google-chrome-stable',
      headless: true, // Force headless true on server
    });

    const page = await browser.newPage();
    const url = `https://property.phila.gov/?p=${opaAccountNumber}`;
    await page.goto(url, { waitUntil: 'domcontentloaded' });
    await page.waitForSelector('.property', { timeout: 10000 });

    const result = await page.evaluate(() => {
      const text = document.body.innerText;

      // DEBUG: Return the full page text so you can inspect it in Render logs
      return { raw: text };
    });

    console.log('📄 Scraped page text:', result.raw); // This will appear in Render logs

    // You can comment out the return below after debugging
    return {
      owner: null,
      salePrice: null,
      assessedValue: null,
      saleDate: null,
      marketValue: null,
      rawDump: result.raw, // Include raw text in API response for debugging
    };
  } catch (error) {
    console.error('OPA scraper error:', error);
    return null;
  } finally {
    if (browser !== null) {
      await browser.close();
    }
  }
};

module.exports = { getOpaDetails };
