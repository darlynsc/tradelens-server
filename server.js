// TradeLens Server — Node.js + Express
// Deploy gratis en Render.com
// 
// SETUP:
// 1. Sube esta carpeta a GitHub
// 2. Crea un Web Service en render.com apuntando a tu repo
// 3. Build Command: npm install
// 4. Start Command: node server.js
// 5. Agrega variable de entorno: SECRET_TOKEN=tu_token_secreto
// 6. (JOURNAL EN LA NUBE) Agrega variable de entorno: DATABASE_URL=<cadena de conexión Postgres>
//    Sin DATABASE_URL el journal se guarda en un archivo local, que Render BORRA al reiniciar.

const express = require('express');
const cors = require('cors');
const fs = require('fs');
const path = require('path');
const app = express();

app.use(cors({ methods: ['GET','POST','PUT','OPTIONS'] }));
app.use(express.json({ limit: '5mb' }));

const SECRET_TOKEN = process.env.SECRET_TOKEN || 'MI_TOKEN_SECRETO';
let accountData = null;
let lastUpdate = null;
let lastDealCount = 0;
let lastBalance = 0;
let lastPositionCount = 0;

// ── SSE CLIENTS ───────────────────────────────────────────────────────────────
const sseClients = new Set();

function notifyClients(reason) {
  const msg = `data: ${JSON.stringify({
    deals_count: lastDealCount,
    balance: lastBalance,
    positions_count: lastPositionCount,
    timestamp: lastUpdate,
    reason: reason
  })}\n\n`;
  for (const client of sseClients) {
    try { client.write(msg); } catch(_) { sseClients.delete(client); }
  }
}

// ── RECIBE DATOS DEL EA ───────────────────────────────────────────────────────
app.post('/sync', (req, res) => {
  const token = req.headers['x-token'] || req.body?.token;
  if (token !== SECRET_TOKEN) {
    return res.status(401).json({ error: 'Token inválido' });
  }

  const newDealCount     = req.body?.deals?.length || 0;
  const newBalance       = req.body?.account?.balance || 0;
  const newPositionCount = req.body?.positions?.length || 0;

  accountData = req.body;
  lastUpdate  = new Date().toISOString();

  const dealChanged     = newDealCount     !== lastDealCount;
  const balanceChanged  = Math.abs(newBalance - lastBalance) > 0.01;
  const positionChanged = newPositionCount !== lastPositionCount;

  let reason = 'sync';
  if (balanceChanged)  reason = 'balance';
  if (dealChanged)     reason = 'deal';
  if (positionChanged) reason = newPositionCount < lastPositionCount ? 'cierre' : 'apertura';

  console.log(`✅ Sync — ${new Date().toLocaleTimeString()} — Deals: ${newDealCount} | Balance: ${newBalance} | Pos: ${newPositionCount} | Motivo: ${reason}`);

  lastDealCount     = newDealCount;
  lastBalance       = newBalance;
  lastPositionCount = newPositionCount;

  // Notificar al dashboard SIEMPRE que el EA envíe datos
  notifyClients(reason);

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

// ── SSE — PUSH AL DASHBOARD CUANDO HAY CAMBIO ────────────────────────────────
app.get('/events', (req, res) => {
  const token = req.query.token || req.headers['x-token'];
  if (token !== SECRET_TOKEN) {
    return res.status(401).json({ error: 'Token inválido' });
  }

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  // Enviar estado inicial
  res.write(`data: ${JSON.stringify({ deals_count: lastDealCount, balance: lastBalance, positions_count: lastPositionCount, timestamp: lastUpdate, reason: "init" })}\n\n`);

  sseClients.add(res);
  console.log(`📡 Dashboard conectado via SSE (${sseClients.size} clientes)`);

  // Keepalive cada 30s para que Render no cierre la conexión
  const keepalive = setInterval(() => {
    try { res.write(': ping\n\n'); } catch(_) {}
  }, 30000);

  req.on('close', () => {
    sseClients.delete(res);
    clearInterval(keepalive);
    console.log(`📡 Dashboard desconectado (${sseClients.size} clientes)`);
  });
});


// ── JOURNAL EN LA NUBE ────────────────────────────────────────────────────────
// Guarda las notas del journal (una por día) para que PC y teléfono compartan lo mismo.
// Con DATABASE_URL usa Postgres (persistente). Sin ella, usa un archivo (se pierde en Render gratis).
// Regla de conflicto: gana la versión con `saved` (fecha ISO) más reciente.
const DATABASE_URL = process.env.DATABASE_URL || '';
let journalStore;

if (DATABASE_URL) {
  const { Pool } = require('pg');
  const pool = new Pool({ connectionString: DATABASE_URL, ssl: { rejectUnauthorized: false } });
  const ready = pool.query(`CREATE TABLE IF NOT EXISTS tl_journal (
    day TEXT PRIMARY KEY, tag TEXT NOT NULL DEFAULT '', note TEXT NOT NULL DEFAULT '',
    img TEXT NOT NULL DEFAULT '', saved TEXT NOT NULL DEFAULT '')`)
    .then(() => console.log('🗄️  Journal: Postgres listo'))
    .catch(e => console.error('❌ Error preparando Postgres:', e.message));
  journalStore = {
    async all() {
      await ready;
      const { rows } = await pool.query('SELECT day, tag, note, img, saved FROM tl_journal');
      const out = {};
      rows.forEach(r => { out[r.day] = { tag: r.tag, note: r.note, img: r.img, saved: r.saved }; });
      return out;
    },
    async put(day, e) {
      await ready;
      await pool.query(
        `INSERT INTO tl_journal (day, tag, note, img, saved) VALUES ($1,$2,$3,$4,$5)
         ON CONFLICT (day) DO UPDATE SET tag=EXCLUDED.tag, note=EXCLUDED.note, img=EXCLUDED.img, saved=EXCLUDED.saved
         WHERE tl_journal.saved < EXCLUDED.saved`, [day, e.tag, e.note, e.img, e.saved]);
      const { rows } = await pool.query('SELECT tag, note, img, saved FROM tl_journal WHERE day=$1', [day]);
      return rows[0];
    }
  };
} else {
  const dir = process.env.DATA_DIR || path.join(__dirname, 'data');
  const file = path.join(dir, 'journal.json');
  let cache = {};
  try { cache = JSON.parse(fs.readFileSync(file, 'utf8')); } catch (_) {}
  console.warn('⚠️  Journal: sin DATABASE_URL — se guarda en archivo (NO persistente en Render gratis)');
  journalStore = {
    async all() { return cache; },
    async put(day, e) {
      if (!cache[day] || (cache[day].saved || '') < e.saved) {
        cache[day] = e;
        try { fs.mkdirSync(dir, { recursive: true }); fs.writeFileSync(file, JSON.stringify(cache)); } catch (_) {}
      }
      return cache[day];
    }
  };
}

function checkToken(req, res) {
  const token = req.query.token || req.headers['x-token'];
  if (token !== SECRET_TOKEN) { res.status(401).json({ error: 'Token inválido' }); return false; }
  return true;
}

// Avisa a los dashboards abiertos (PC y teléfono) que el journal cambió
function notifyJournal(day) {
  const msg = `event: journal\ndata: ${JSON.stringify({ day, ts: new Date().toISOString() })}\n\n`;
  for (const client of sseClients) {
    try { client.write(msg); } catch (_) { sseClients.delete(client); }
  }
}

app.get('/journal', async (req, res) => {
  if (!checkToken(req, res)) return;
  try { res.json({ entries: await journalStore.all() }); }
  catch (e) { console.error(e); res.status(500).json({ error: 'Error leyendo journal' }); }
});

app.put('/journal/:day', async (req, res) => {
  if (!checkToken(req, res)) return;
  const day = req.params.day;
  if (!/^\d{4}-\d{2}-\d{2}$/.test(day)) return res.status(400).json({ error: 'Fecha inválida' });
  const b = req.body || {};
  const entry = {
    tag:   String(b.tag   || '').slice(0, 20),
    note:  String(b.note  || '').slice(0, 20000),
    img:   String(b.img   || '').slice(0, 2000),
    saved: String(b.saved || new Date().toISOString()).slice(0, 40)
  };
  try {
    const current = await journalStore.put(day, entry);
    notifyJournal(day);
    res.json({ ok: true, entry: current });
  } catch (e) { console.error(e); res.status(500).json({ error: 'Error guardando journal' }); }
});

// ── STATUS ────────────────────────────────────────────────────────────────────
app.get('/status', (req, res) => {
  res.json({
    online: true,
    has_data: !!accountData,
    last_update: lastUpdate,
    deals_count: lastDealCount,
    positions_count: accountData?.positions?.length || 0,
    sse_clients: sseClients.size
  });
});

app.get('/', (req, res) => {
  res.send(`
    <html><body style="font-family:monospace;background:#0d1117;color:#00d4ff;padding:40px">
    <h1>🟢 TradeLens Server Online</h1>
    <p>Last sync: ${lastUpdate || 'Esperando EA...'}</p>
    <p>Deals: ${lastDealCount}</p>
    <p>Positions: ${accountData?.positions?.length || 0}</p>
    <p>Dashboard clients: ${sseClients.size}</p>
    </body></html>
  `);
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`🚀 TradeLens Server corriendo en puerto ${PORT}`));
