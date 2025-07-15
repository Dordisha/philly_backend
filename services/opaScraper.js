const chromium = require('chrome-aws-lambda');
const puppeteer = require('puppeteer-core');
const { cleanDollar } = require('../utils/formatHelpers');

const getOpaDetails = async (opaAccountNumber) => {
  let browser = null;

  try {
    browser = await puppeteer.launch({
      args: chromium.args,
      executablePath: (await chromium.executablePath) || '/usr/bin/google-chrome-stable',
      headless: true,
    });

    const page = await browser.newPage();
    const url = `https://property.phila.gov/?p=${opaAccountNumber}`;
    await page.goto(url, { waitUntil: 'domcontentloaded' });
    await page.waitForSelector('.property', { timeout: 10000 });

    const rawText = await page.evaluate(() => document.body.innerText);
    console.log('📄 RAW PAGE TEXT:\n', rawText); // This helps us debug on Render logs

    return { debugText: rawText }; // TEMPORARY: Return the raw content
  } catch (error) {
    console.error('OPA scraper error:', error);
    return { error: error.message };
  } finally {
    if (browser !== null) {
      await browser.close();
    }
  }
};

module.exports = { getOpaDetails };
