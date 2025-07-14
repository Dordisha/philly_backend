function cleanDollar(str) {
  if (!str) return 'N/A';
  return str.replace(/[$,]/g, '').trim();
}

module.exports = { cleanDollar };
