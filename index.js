const express = require('express');
const cors = require('cors');
const taxRoute = require('./routes/taxRoute');

const app = express();
const PORT = 3001;

console.log("🟢 index.js is running...");

app.use(cors());
app.use(express.json());
app.use('/api/tax', taxRoute);

app.listen(PORT, () => {
  console.log(`✅ Server is running on http://localhost:${PORT}`);
});
