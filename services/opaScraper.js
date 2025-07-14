const chromium = require('chrome-aws-lambda');
const puppeteer = require('puppeteer-core');
const { cleanDollar } = require('../utils/formatHelpers');

const getOpaDetails = async (opaAccountNumber) => {
  let browser = null;

  try {
    browser = await puppeteer.launch({
      args: chromium.args,
      executablePath: await chromium.executablePath,
      headless: chromium.headless,
    });

    const page = await browser.newPage();
    const url = `https://property.phila.gov/?p=${opaAccountNumber}`;
    await page.goto(url, { waitUntil: 'domcontentloaded' });
    await page.waitForSelector('.property', { timeout: 10000 });

    const result = await page.evaluate(() => {
      const text = document.body.innerText;

      const extract = (label) => {
        const match = text.match(new RegExp(`${label}\\s+\\$?([\\d,]+)`));
        return match ? match[1] : null;
      };

      const extractDate = (label) => {
        const match = text.match(new RegExp(`${label}\\s+(\\d{2}/\\d{2}/\\d{4})`));
        return match ? match[1] : null;
      };

      const extractOwner = () => {
        const match = text.match(/Owner\s+([A-Z\s]+)/);
        return match ? match[1].trim() : null;
      };

      return {
        owner: extractOwner(),
        salePrice: extract('Sale Price'),
        assessedValue: extract('Assessed Value'),
        saleDate: extractDate('Sale Date'),
        marketValue: extract('2025\\s+\\$([\\d,]+)'),
      };
    });

    return {
      owner: result.owner,
      salePrice: cleanDollar(result.salePrice),
      assessedValue: cleanDollar(result.assessedValue),
      saleDate: result.saleDate,
      marketValue: cleanDollar(result.marketValue),
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
