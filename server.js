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
//    Las capturas del journal también se guardan en esa base de datos (tabla tl_images):
//    sin DATABASE_URL las imágenes NO son persistentes.

const express = require('express');
const cors = require('cors');
const fs = require('fs');
const crypto = require('crypto');
const path = require('path');
const app = express();
app.set('trust proxy', 1);

app.use(cors({ methods: ['GET','POST','PUT','DELETE','OPTIONS'] }));
app.use(express.json({ limit: '12mb' }));

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
  if (token !== SECRET_TOKEN) { // /sync solo lo acepta con el token maestro (la EA)
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

  // Guardar respaldo en la nube (cambios al instante; si no hay cambios, máximo cada 60 s)
  persistSnapshot(reason);

  // Notificar al dashboard SIEMPRE que el EA envíe datos
  notifyClients(reason);

  res.json({ ok: true, timestamp: lastUpdate });
});

// ── SIRVE DATOS AL DASHBOARD ──────────────────────────────────────────────────
app.get('/data', (req, res) => {
  const token = req.query.token || req.headers['x-token'];
  if (!isAuthorized(token)) {
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
  if (!isAuthorized(token)) {
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
    .then(() => pool.query(`ALTER TABLE tl_journal ADD COLUMN IF NOT EXISTS imgs TEXT NOT NULL DEFAULT '[]'`))
    .then(() => pool.query(`CREATE TABLE IF NOT EXISTS tl_images (
      id TEXT PRIMARY KEY, day TEXT NOT NULL, ref TEXT NOT NULL DEFAULT '',
      mime TEXT NOT NULL, size INTEGER NOT NULL, data BYTEA NOT NULL, created TEXT NOT NULL DEFAULT '')`))
    .then(() => pool.query(`CREATE INDEX IF NOT EXISTS tl_images_day ON tl_images (day)`))
    .then(() => pool.query(`CREATE TABLE IF NOT EXISTS tl_kv (
      k TEXT PRIMARY KEY, v TEXT NOT NULL, updated TEXT NOT NULL DEFAULT '')`))
    .then(() => console.log('🗄️  Journal: Postgres listo'))
    .catch(e => console.error('❌ Error preparando Postgres:', e.message));
  journalStore = {
    async kvGet(k) {
      await ready;
      const { rows } = await pool.query('SELECT v FROM tl_kv WHERE k=$1', [k]);
      return rows[0] ? JSON.parse(rows[0].v) : null;
    },
    async kvSet(k, v) {
      await ready;
      await pool.query(
        `INSERT INTO tl_kv (k, v, updated) VALUES ($1,$2,$3)
         ON CONFLICT (k) DO UPDATE SET v=EXCLUDED.v, updated=EXCLUDED.updated`,
        [k, JSON.stringify(v), new Date().toISOString()]);
    },
    async all() {
      await ready;
      const { rows } = await pool.query('SELECT day, tag, note, img, imgs, saved FROM tl_journal');
      const refs = {};
      (await pool.query("SELECT id, ref FROM tl_images WHERE ref <> ''")).rows.forEach(r => { refs[r.id] = r.ref; });
      const out = {};
      rows.forEach(r => {
        const imgs = parseImgs(r.imgs), imgRefs = {};
        imgs.forEach(i => { if (refs[i]) imgRefs[i] = refs[i]; });
        out[r.day] = { tag: r.tag, note: r.note, img: r.img, imgs, imgRefs, saved: r.saved };
      });
      return out;
    },
    async put(day, e) {
      await ready;
      await pool.query(
        `INSERT INTO tl_journal (day, tag, note, img, imgs, saved) VALUES ($1,$2,$3,$4,$5,$6)
         ON CONFLICT (day) DO UPDATE SET tag=EXCLUDED.tag, note=EXCLUDED.note, img=EXCLUDED.img, imgs=EXCLUDED.imgs, saved=EXCLUDED.saved
         WHERE tl_journal.saved < EXCLUDED.saved`, [day, e.tag, e.note, e.img, JSON.stringify(e.imgs || []), e.saved]);
      const { rows } = await pool.query('SELECT tag, note, img, imgs, saved FROM tl_journal WHERE day=$1', [day]);
      return rows[0] ? { ...rows[0], imgs: parseImgs(rows[0].imgs) } : rows[0];
    },
    // ── Imágenes (bytea en Postgres: persistente, sin credenciales extra) ──
    async imgPut(id, day, ref, mime, buf) {
      await ready;
      await pool.query('INSERT INTO tl_images (id, day, ref, mime, size, data, created) VALUES ($1,$2,$3,$4,$5,$6,$7)',
        [id, day, ref, mime, buf.length, buf, new Date().toISOString()]);
    },
    async imgGet(id) {
      await ready;
      const { rows } = await pool.query('SELECT day, ref, mime, size, data FROM tl_images WHERE id=$1', [id]);
      return rows[0] || null;
    },
    async imgDel(id, day) {
      await ready;
      const r = await pool.query('DELETE FROM tl_images WHERE id=$1 AND day=$2', [id, day]);
      return r.rowCount > 0;
    },
    async imgCountDay(day) {
      await ready;
      const { rows } = await pool.query('SELECT COUNT(*)::int AS n FROM tl_images WHERE day=$1', [day]);
      return rows[0].n;
    },
    imgPersistent: true
  };
} else {
  const dir = process.env.DATA_DIR || path.join(__dirname, 'data');
  const file = path.join(dir, 'journal.json');
  let cache = {};
  try { cache = JSON.parse(fs.readFileSync(file, 'utf8')); } catch (_) {}
  console.warn('⚠️  Journal: sin DATABASE_URL — se guarda en archivo (NO persistente en Render gratis)');
  journalStore = {
    async kvGet(k) { try { return JSON.parse(fs.readFileSync(path.join(dir, 'kv_' + k + '.json'), 'utf8')); } catch (_) { return null; } },
    async kvSet(k, v) { try { fs.mkdirSync(dir, { recursive: true }); fs.writeFileSync(path.join(dir, 'kv_' + k + '.json'), JSON.stringify(v)); } catch (_) {} },
    async all() { return cache; },
    async put(day, e) {
      if (!cache[day] || (cache[day].saved || '') < e.saved) {
        cache[day] = e;
        try { fs.mkdirSync(dir, { recursive: true }); fs.writeFileSync(file, JSON.stringify(cache)); } catch (_) {}
      }
      return cache[day];
    },
    // Imágenes en disco local: NO persistente en Render sin disco. Solo para pruebas locales.
    async imgPut(id, day, ref, mime, buf) {
      const d = path.join(dir, 'images'); fs.mkdirSync(d, { recursive: true });
      fs.writeFileSync(path.join(d, id + '.bin'), buf);
      fs.writeFileSync(path.join(d, id + '.json'), JSON.stringify({ day, ref, mime, size: buf.length }));
    },
    async imgGet(id) {
      try {
        const d = path.join(dir, 'images');
        const meta = JSON.parse(fs.readFileSync(path.join(d, id + '.json'), 'utf8'));
        return { ...meta, data: fs.readFileSync(path.join(d, id + '.bin')) };
      } catch (_) { return null; }
    },
    async imgDel(id, day) {
      try {
        const d = path.join(dir, 'images');
        const meta = JSON.parse(fs.readFileSync(path.join(d, id + '.json'), 'utf8'));
        if (meta.day !== day) return false;
        fs.unlinkSync(path.join(d, id + '.bin')); fs.unlinkSync(path.join(d, id + '.json'));
        return true;
      } catch (_) { return false; }
    },
    async imgCountDay(day) {
      try {
        const d = path.join(dir, 'images');
        return fs.readdirSync(d).filter(f => f.endsWith('.json') && JSON.parse(fs.readFileSync(path.join(d, f), 'utf8')).day === day).length;
      } catch (_) { return 0; }
    },
    imgPersistent: false
  };
}
function parseImgs(s) {
  try { const a = typeof s === 'string' ? JSON.parse(s) : s; return Array.isArray(a) ? a.filter(x => /^[a-f0-9]{32}$/.test(x)).slice(0, 8) : []; }
  catch (_) { return []; }
}

function checkToken(req, res) {
  const token = req.query.token || req.headers['x-token'];
  if (!isAuthorized(token)) { res.status(401).json({ error: 'Token inválido' }); return false; }
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
    img:   String(b.img   || '').slice(0, 2000), // enlace antiguo (se conserva)
    imgs:  parseImgs(b.imgs),                    // ids de imágenes subidas
    saved: String(b.saved || new Date().toISOString()).slice(0, 40)
  };
  try {
    const current = await journalStore.put(day, entry);
    notifyJournal(day);
    res.json({ ok: true, entry: current });
  } catch (e) { console.error(e); res.status(500).json({ error: 'Error guardando journal' }); }
});

// ── IMÁGENES DEL JOURNAL ──────────────────────────────────────────────────────
// Subida directa (cuerpo binario). Se guardan en la base de datos (persistente con DATABASE_URL).
// Tipos permitidos verificados por firma del archivo (no por la extensión ni el Content-Type enviado).
const IMG_MAX_BYTES = 5 * 1024 * 1024;
const IMG_MAX_PER_DAY = 8;
function sniffImage(b) {
  if (b.length > 12 && b[0] === 0xFF && b[1] === 0xD8 && b[2] === 0xFF) return 'image/jpeg';
  if (b.length > 8 && b.slice(0, 8).equals(Buffer.from([0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A]))) return 'image/png';
  if (b.length > 12 && b.slice(0, 4).toString('latin1') === 'RIFF' && b.slice(8, 12).toString('latin1') === 'WEBP') return 'image/webp';
  if (b.length > 6 && (b.slice(0, 6).toString('latin1') === 'GIF87a' || b.slice(0, 6).toString('latin1') === 'GIF89a')) return 'image/gif';
  return null;
}
const rawImage = express.raw({ type: () => true, limit: IMG_MAX_BYTES + 1024 });

app.post('/journal/:day/images', (req, res, next) => {
  if (!checkToken(req, res)) return;
  next();
}, rawImage, async (req, res) => {
  const day = req.params.day;
  if (!/^\d{4}-\d{2}-\d{2}$/.test(day)) return res.status(400).json({ error: 'Fecha inválida' });
  const buf = req.body;
  if (!Buffer.isBuffer(buf) || !buf.length) return res.status(400).json({ error: 'No se recibió ninguna imagen.' });
  if (buf.length > IMG_MAX_BYTES) return res.status(413).json({ error: 'La imagen supera 5 MB.' });
  const mime = sniffImage(buf);
  if (!mime) return res.status(415).json({ error: 'Formato no permitido. Usa JPG, PNG, WEBP o GIF.' });
  const ref = String(req.query.trade || '').replace(/[^0-9A-Za-z_-]/g, '').slice(0, 30);
  try {
    if ((await journalStore.imgCountDay(day)) >= IMG_MAX_PER_DAY) return res.status(409).json({ error: `Máximo ${IMG_MAX_PER_DAY} imágenes por día.` });
    const id = crypto.randomBytes(16).toString('hex');
    await journalStore.imgPut(id, day, ref, mime, buf);
    res.json({ ok: true, id, mime, size: buf.length, ref, persistent: journalStore.imgPersistent });
  } catch (e) { console.error(e); res.status(500).json({ error: 'No se pudo guardar la imagen.' }); }
});

app.get('/images/:id', async (req, res) => {
  if (!checkToken(req, res)) return;
  const id = req.params.id;
  if (!/^[a-f0-9]{32}$/.test(id)) return res.status(400).json({ error: 'Id inválido' });
  try {
    const im = await journalStore.imgGet(id);
    if (!im) return res.status(404).json({ error: 'No encontrada' });
    res.setHeader('Content-Type', im.mime);
    res.setHeader('Content-Length', im.data.length);
    res.setHeader('X-Content-Type-Options', 'nosniff');
    res.setHeader('Content-Disposition', 'inline');
    res.setHeader('Cache-Control', 'private, max-age=31536000, immutable');
    res.end(im.data);
  } catch (e) { console.error(e); res.status(500).json({ error: 'Error leyendo imagen' }); }
});

// Solo borra la imagen si pertenece a ese día (no toca archivos de otras entradas)
app.delete('/journal/:day/images/:id', async (req, res) => {
  if (!checkToken(req, res)) return;
  const { day, id } = req.params;
  if (!/^\d{4}-\d{2}-\d{2}$/.test(day) || !/^[a-f0-9]{32}$/.test(id)) return res.status(400).json({ error: 'Parámetros inválidos' });
  try { res.json({ ok: true, deleted: await journalStore.imgDel(id, day) }); }
  catch (e) { console.error(e); res.status(500).json({ error: 'Error borrando imagen' }); }
});

app.get('/journal-config', (req, res) => {
  if (!checkToken(req, res)) return;
  res.json({ images: true, persistent: !!journalStore.imgPersistent, maxBytes: IMG_MAX_BYTES, maxPerDay: IMG_MAX_PER_DAY });
});


// ── RESPALDO DE LOS DATOS DE LA EA ────────────────────────────────────────────
// El último envío de la EA se guarda en la base de datos. Así el dashboard abre
// con esos datos aunque la EA / la PC estén apagadas o el servidor se haya reiniciado.
let lastSnapshotSave = 0;
function persistSnapshot(reason) {
  const now = Date.now();
  if (reason === 'sync' && now - lastSnapshotSave < 60000) return;
  lastSnapshotSave = now;
  journalStore.kvSet('snapshot', { data: accountData, lastUpdate })
    .catch(e => console.error('❌ No se pudo guardar el respaldo:', e.message));
}

// ── USUARIO Y CONTRASEÑA ──────────────────────────────────────────────────────
// Un solo usuario. Contraseña guardada con scrypt (nunca en texto plano).
// El SECRET_TOKEN de Render sirve como llave maestra: con él se crea o se restablece el usuario.
const SESSION_SECRET = process.env.SESSION_SECRET || SECRET_TOKEN;
let authCache = null; // { username, salt, hash, pv }

function hashPw(pw, salt) { return crypto.scryptSync(String(pw), salt, 64).toString('hex'); }
function checkPw(pw, a) {
  const h = Buffer.from(hashPw(pw, a.salt), 'hex'), x = Buffer.from(a.hash, 'hex');
  return h.length === x.length && crypto.timingSafeEqual(h, x);
}
function hmac(p) { return crypto.createHmac('sha256', SESSION_SECRET).update(p).digest('base64url'); }
function signSession(a) {
  const p = Buffer.from(JSON.stringify({ u: a.username, pv: a.pv, exp: Date.now() + 90 * 864e5 })).toString('base64url');
  return 's1.' + p + '.' + hmac(p);
}
function verifySession(t) {
  if (typeof t !== 'string' || !t.startsWith('s1.') || !authCache) return false;
  const [, p, s] = t.split('.');
  if (!p || !s) return false;
  const a = Buffer.from(s), b = Buffer.from(hmac(p));
  if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) return false;
  try {
    const d = JSON.parse(Buffer.from(p, 'base64url').toString());
    return d.pv === authCache.pv && d.u === authCache.username && d.exp > Date.now();
  } catch (_) { return false; }
}
function isAuthorized(token) { return token === SECRET_TOKEN || verifySession(token); }

// Freno a intentos fallidos (5 por IP cada 15 min)
const attempts = new Map();
function blocked(req) { const a = attempts.get(req.ip); return a && a.count >= 5 && a.until > Date.now(); }
function fail(req) {
  const a = attempts.get(req.ip) || { count: 0, until: 0 };
  a.count = (a.until && a.until < Date.now()) ? 1 : a.count + 1;
  a.until = Date.now() + 15 * 60000;
  attempts.set(req.ip, a);
}
function okAttempt(req) { attempts.delete(req.ip); }

async function saveAuth(username, password) {
  const salt = crypto.randomBytes(16).toString('hex');
  authCache = { username, salt, hash: hashPw(password, salt), pv: ((authCache && authCache.pv) || 0) + 1 };
  await journalStore.kvSet('auth', authCache);
  return signSession(authCache);
}
function validCreds(username, password) {
  if (!/^[a-z0-9._@-]{3,40}$/.test(username)) return 'Usuario: 3 a 40 caracteres (letras, números, . _ - @).';
  if (typeof password !== 'string' || password.length < 8 || password.length > 100) return 'La contraseña debe tener al menos 8 caracteres.';
  return null;
}

app.get('/auth/status', (req, res) => res.json({ configured: !!authCache }));

// Crear o restablecer usuario (requiere la llave maestra SECRET_TOKEN)
app.post('/auth/setup', async (req, res) => {
  if (blocked(req)) return res.status(429).json({ error: 'Demasiados intentos. Espera 15 minutos.' });
  const { master, password } = req.body || {};
  const username = String(req.body?.username || '').trim().toLowerCase();
  if (master !== SECRET_TOKEN) { fail(req); return res.status(401).json({ error: 'Llave maestra incorrecta.' }); }
  const bad = validCreds(username, password);
  if (bad) return res.status(400).json({ error: bad });
  try { const token = await saveAuth(username, password); okAttempt(req); res.json({ ok: true, token, username }); }
  catch (e) { console.error(e); res.status(500).json({ error: 'No se pudo guardar el usuario.' }); }
});

app.post('/auth/login', (req, res) => {
  if (blocked(req)) return res.status(429).json({ error: 'Demasiados intentos. Espera 15 minutos.' });
  const username = String(req.body?.username || '').trim().toLowerCase();
  const password = req.body?.password;
  if (!authCache) return res.status(409).json({ error: 'Aún no hay usuario creado. Créalo en "Primera vez / recuperar acceso".' });
  if (username !== authCache.username || typeof password !== 'string' || !checkPw(password, authCache)) {
    fail(req); return res.status(401).json({ error: 'Usuario o contraseña incorrectos.' });
  }
  okAttempt(req);
  res.json({ ok: true, token: signSession(authCache), username: authCache.username });
});

// Cambiar usuario y/o contraseña (pide la contraseña actual). Cierra las sesiones viejas.
app.post('/auth/change', async (req, res) => {
  const token = req.query.token || req.headers['x-token'];
  if (!isAuthorized(token)) return res.status(401).json({ error: 'Sesión inválida.' });
  if (!authCache) return res.status(409).json({ error: 'No hay usuario creado.' });
  if (blocked(req)) return res.status(429).json({ error: 'Demasiados intentos. Espera 15 minutos.' });
  const { currentPassword, newPassword } = req.body || {};
  if (typeof currentPassword !== 'string' || !checkPw(currentPassword, authCache)) {
    fail(req); return res.status(401).json({ error: 'La contraseña actual es incorrecta.' });
  }
  const username = String(req.body?.newUsername || authCache.username).trim().toLowerCase();
  const password = newPassword ? newPassword : currentPassword;
  const bad = validCreds(username, password);
  if (bad) return res.status(400).json({ error: bad });
  try { const t = await saveAuth(username, password); okAttempt(req); res.json({ ok: true, token: t, username }); }
  catch (e) { console.error(e); res.status(500).json({ error: 'No se pudo guardar.' }); }
});

// Cargar usuario y último respaldo de la EA antes de recibir peticiones
async function loadState() {
  try {
    authCache = await journalStore.kvGet('auth');
    const snap = await journalStore.kvGet('snapshot');
    if (snap && snap.data && !accountData) {
      accountData = snap.data; lastUpdate = snap.lastUpdate;
      lastDealCount = accountData.deals?.length || 0;
      lastBalance = accountData.account?.balance || 0;
      lastPositionCount = accountData.positions?.length || 0;
      console.log(`📦 Respaldo cargado (${lastUpdate}) — Deals: ${lastDealCount}`);
    }
    console.log(authCache ? `👤 Usuario configurado: ${authCache.username}` : '👤 Sin usuario creado todavía');
  } catch (e) { console.error('❌ Error cargando estado:', e.message); }
}

// ── STATUS ────────────────────────────────────────────────────────────────────
app.get('/status', (req, res) => {
  res.json({
    online: true,
    has_data: !!accountData,
    last_update: lastUpdate,
    deals_count: lastDealCount,
    positions_count: accountData?.positions?.length || 0,
    sse_clients: sseClients.size,
    features: { journal_images: true, images_persistent: !!journalStore.imgPersistent },
    version: '1.2.0'
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

console.log(`🖼️  Imágenes del journal: rutas activas (${journalStore.imgPersistent ? 'persistentes en Postgres' : '⚠️ disco temporal, NO persistentes'})`);
const PORT = process.env.PORT || 3000;
loadState().finally(() => app.listen(PORT, () => console.log(`🚀 TradeLens Server corriendo en puerto ${PORT}`)));
