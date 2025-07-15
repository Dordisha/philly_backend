const chromium = require('chrome-aws-lambda');
const puppeteer = require('puppeteer');
const { cleanDollar } = require('../utils/formatHelpers');

const getOpaDetails = async (opaAccountNumber) => {
  let browser = null;

  try {
    const executablePath = await chromium.executablePath;

    browser = await puppeteer.launch({
      args: chromium.args,
      executablePath: executablePath || undefined, // only use if available
      headless: true,
    });

    const page = await browser.newPage();
    const url = `https://property.phila.gov/?p=${opaAccountNumber}`;
    await page.goto(url, { waitUntil: 'networkidle2', timeout: 30000 });
    await page.waitForTimeout(3000); // give extra time for dynamic content

    const text = await page.evaluate(() => document.body.innerText);

    return {
      rawText: text, // temporarily return full dump
    };
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
