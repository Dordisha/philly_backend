const puppeteer = require('puppeteer');
const chromium = require('chrome-aws-lambda');
const { cleanDollar } = require('../utils/formatHelpers');

const getOpaDetails = async (opaAccountNumber) => {
  let browser = null;

  try {
    const executablePath = await chromium.executablePath;

    browser = await puppeteer.launch({
      args: chromium.args,
      executablePath: executablePath || undefined,
      headless: true,
    });

    const page = await browser.newPage();
    const url = `https://property.phila.gov/?p=${opaAccountNumber}`;
    await page.goto(url, { waitUntil: 'networkidle2', timeout: 30000 });
    await page.waitForTimeout(3000);

    const text = await page.evaluate(() => document.body.innerText);

    const extract = (label, dollar = true) => {
      const regex = new RegExp(`${label}\\s+\\$?([\\d,]+)`);
      const match = text.match(regex);
      return match ? (dollar ? cleanDollar(match[1]) : match[1]) : null;
    };

    const extractDate = (label) => {
      const match = text.match(new RegExp(`${label}\\s+(\\d{2}/\\d{2}/\\d{4})`));
      return match ? match[1] : null;
    };

    const extractOwner = () => {
      const match = text.match(/Owner\s+OPA Account Number\s+([A-Z\s]+)/);
      return match ? match[1].trim() : null;
    };

    return {
      owner: extractOwner(),
      salePrice: extract('Sale Price'),
      assessedValue: extract('Assessed Value'),
      saleDate: extractDate('Sale Date'),
      marketValue: extract('2025'),
    };
  } catch (error) {
    console.error('OPA scraper error:', error);
    return { error: error.message };
  } finally {
    if (browser) await browser.close();
  }
};

module.exports = { getOpaDetails };
