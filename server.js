// TradeLens Server — Node.js + Express
// Deploy gratis en Render.com
// 
// SETUP:
// 1. Sube esta carpeta a GitHub
// 2. Crea un Web Service en render.com apuntando a tu repo
// 3. Build Command: npm install
// 4. Start Command: node server.js
// 5. Agrega variable de entorno: SECRET_TOKEN=tu_token_secreto

const express = require('express');
const cors = require('cors');
const app = express();

app.use(cors());
app.use(express.json({ limit: '5mb' }));

const SECRET_TOKEN = process.env.SECRET_TOKEN || 'MI_TOKEN_SECRETO';
let accountData = null;
let lastUpdate = null;

// ── RECIBE DATOS DEL EA ───────────────────────────────────────────────────────
app.post('/sync', (req, res) => {
  const token = req.headers['x-token'] || req.body?.token;
  if (token !== SECRET_TOKEN) {
    return res.status(401).json({ error: 'Token inválido' });
  }
  accountData = req.body;
  lastUpdate = new Date().toISOString();
  console.log(`✅ Sync recibido — ${new Date().toLocaleTimeString()} — Deals: ${req.body?.deals?.length || 0}`);
  res.json({ ok: true, timestamp: lastUpdate });
});

// ── SIRVE DATOS AL DASHBOARD ──────────────────────────────────────────────────
app.get('/data', (req, res) => {
  const token = req.query.token || req.headers['x-token'];
  if (token !== SECRET_TOKEN) {
    return res.status(401).json({ error: 'Token inválido' });
  }
  if (!accountData) {
    return res.status(404).json({ error: 'Sin datos aún. Asegúrate que el EA está corriendo.' });
  }
  res.json({ ...accountData, server_time: lastUpdate });
});

// ── STATUS ────────────────────────────────────────────────────────────────────
app.get('/status', (req, res) => {
  res.json({
    online: true,
    has_data: !!accountData,
    last_update: lastUpdate,
    deals_count: accountData?.deals?.length || 0,
    positions_count: accountData?.positions?.length || 0
  });
});

app.get('/', (req, res) => {
  res.send(`
    <html><body style="font-family:monospace;background:#0d1117;color:#00d4ff;padding:40px">
    <h1>🟢 TradeLens Server Online</h1>
    <p>Last sync: ${lastUpdate || 'Esperando EA...'}</p>
    <p>Deals: ${accountData?.deals?.length || 0}</p>
    <p>Positions: ${accountData?.positions?.length || 0}</p>
    </body></html>
  `);
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`🚀 TradeLens Server corriendo en puerto ${PORT}`));
