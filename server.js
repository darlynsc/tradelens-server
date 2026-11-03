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
let lastSeq = null;      // último "seq" aceptado de la EA (orden de snapshots)

// ── EXCURSIONES DE PRECIO (MAE/MFE en puntos) ─────────────────────────────────
// La EA envía "price_exc" por posición. Aquí se valida, se conserva la unidad (puntos de SYMBOL_POINT) y la
// calidad (cov), y se FUSIONA con lo ya guardado: una entrada solo se sustituye por otra con "rev" igual o mayor
// (los reintentos no duplican; un estado atrasado no pisa uno más reciente; si la EA pierde su archivo local,
// el servidor conserva las mediciones anteriores). Un valor desconocido NUNCA se convierte en 0: va como null.
const PX_COV = new Set(['COMPLETE', 'PARTIAL', 'APPROXIMATE', 'UNAVAILABLE']);
const PX_MAX_ENTRIES = 20000;
const numOrNull = v => (typeof v === 'number' && Number.isFinite(v)) ? v : null;

function cleanPriceExc(k, e) {
  if (!/^\d{1,20}$/.test(k) || !e || typeof e !== 'object') return null;
  if (e.side !== 0 && e.side !== 1) return null;
  const p0 = numOrNull(e.p0), pt = numOrNull(e.pt), rev = numOrNull(e.rev);
  if (!(p0 > 0) || !(pt > 0) || rev === null || rev < 0) return null;
  let cov = String(e.cov);
  if (!PX_COV.has(cov)) return null;
  let mae = numOrNull(e.mae), mfe = numOrNull(e.mfe);
  if (cov === 'UNAVAILABLE') { mae = null; mfe = null; }
  else if (mae === null || mfe === null || mae < 0 || mfe < 0) { cov = 'UNAVAILABLE'; mae = null; mfe = null; } // contradictorio: no se acepta como medición
  return {
    sym: String(e.sym || '').slice(0, 40), side: e.side, p0, pt, tsz: numOrNull(e.tsz),
    pmin: numOrNull(e.pmin), pmax: numOrNull(e.pmax), mae, mfe, unit: 'points', cov,
    src: String(e.src || '').slice(0, 24), pend: e.pend === true, open: e.open === true, fin: e.fin === true,
    late: e.late === true, t0: numOrNull(e.t0), tl: numOrNull(e.tl), vol: numOrNull(e.vol), rev
  };
}
function mergePriceExc(incoming, previous) {
  const merged = Object.assign({}, previous || {});
  let ok = 0, bad = 0, older = 0;
  if (incoming && typeof incoming === 'object' && !Array.isArray(incoming)) {
    for (const [k, e] of Object.entries(incoming)) {
      const c = cleanPriceExc(k, e);
      if (!c) { bad++; continue; }
      if (merged[k] && merged[k].rev > c.rev) { older++; continue; }
      merged[k] = c; ok++;
    }
  }
  const keys = Object.keys(merged);
  if (keys.length > PX_MAX_ENTRIES) {
    keys.filter(k => merged[k].fin).sort((a, b) => (merged[a].t0 || 0) - (merged[b].t0 || 0))
      .slice(0, keys.length - PX_MAX_ENTRIES).forEach(k => delete merged[k]);
  }
  return { merged, ok, bad, older };
}

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

  // Orden: un snapshot con "seq" menor que el último aceptado llegó tarde (reintento viejo) y no pisa el estado actual.
  // Repetir el mismo seq es un reintento idempotente: el snapshot es el estado completo y simplemente lo reemplaza.
  const inSeq = req.body?.seq;
  if (typeof inSeq === 'number' && Number.isFinite(inSeq)) {
    if (lastSeq !== null && inSeq < lastSeq) {
      console.log(`⏭️  Sync descartado por antiguo (seq ${inSeq} < ${lastSeq})`);
      return res.json({ ok: true, stale: true, timestamp: lastUpdate });
    }
    lastSeq = inSeq;
  }
  // price_exc: validar y fusionar (EA antigua sin este bloque: se conserva lo que ya había)
  const hadPx = req.body && Object.prototype.hasOwnProperty.call(req.body, 'price_exc');
  const prevPx = accountData && accountData.price_exc;
  if (req.body && (hadPx || prevPx)) {
    const r = mergePriceExc(hadPx ? req.body.price_exc : null, prevPx);
    req.body.price_exc = r.merged;
    req.body.price_exc_schema = 1;
    if (r.bad || r.older) console.log(`ℹ️  price_exc: ${r.ok} aceptadas, ${r.bad} inválidas descartadas, ${r.older} antiguas ignoradas`);
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

  // Notificaciones push al teléfono (retiro, drawdown, Coach): se evalúan en el servidor, con la app cerrada
  evaluatePush(reason).catch(e => console.error('❌ Push:', e && e.message));

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
      if (typeof accountData.seq === 'number' && Number.isFinite(accountData.seq)) lastSeq = accountData.seq;
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
    features: { journal_images: true, images_persistent: !!journalStore.imgPersistent, price_excursions: true, push: !!(webpush && vapid), push_devices: pushState.subs.length },
    price_exc_count: accountData?.price_exc ? Object.keys(accountData.price_exc).length : 0,
    version: '1.3.0'
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

// ── NOTIFICACIONES PUSH (Web Push) ────────────────────────────────────────────
// El servidor decide cuándo avisar (en cada /sync de la EA) y envía el push al teléfono aunque la app esté cerrada.
// Requiere: npm install web-push. Claves VAPID: variables VAPID_PUBLIC_KEY / VAPID_PRIVATE_KEY (recomendado);
// si no existen se generan y se guardan en la base (tl_kv). Opcional: VAPID_SUBJECT (mailto:tu@correo.com).
let webpush = null;
try { webpush = require('web-push'); } catch (_) { console.warn('⚠️  Push desactivado: falta el paquete "web-push" (npm install web-push)'); }
let pushState = { subs: [], cfg: null, sent: {} };
let vapid = null;
let _pushSaved = '';

async function initPush() {
  if (!webpush) return;
  try {
    const saved = await journalStore.kvGet('push');
    if (saved && typeof saved === 'object') {
      pushState = { subs: Array.isArray(saved.subs) ? saved.subs : [], cfg: saved.cfg || null, sent: saved.sent || {} };
      _pushSaved = JSON.stringify(pushState);
    }
    let keys = (process.env.VAPID_PUBLIC_KEY && process.env.VAPID_PRIVATE_KEY)
      ? { publicKey: process.env.VAPID_PUBLIC_KEY.trim(), privateKey: process.env.VAPID_PRIVATE_KEY.trim() }
      : await journalStore.kvGet('vapid');
    if (!keys || !keys.publicKey || !keys.privateKey) {
      keys = webpush.generateVAPIDKeys();
      await journalStore.kvSet('vapid', keys);
      console.warn('🔑 Claves VAPID generadas y guardadas en la base. Si cambian, los teléfonos deben reactivar las notificaciones.');
    }
    vapid = keys;
    webpush.setVapidDetails(process.env.VAPID_SUBJECT || 'https://tradelens-server.onrender.com', keys.publicKey, keys.privateKey);
    console.log(`🔔 Push listo (${pushState.subs.length} dispositivo(s) suscrito(s))`);
  } catch (e) { console.error('❌ Error iniciando push:', e.message); vapid = null; }
}

function savePush() {
  const s = JSON.stringify(pushState);
  if (s === _pushSaved) return;
  _pushSaved = s;
  journalStore.kvSet('push', pushState).catch(e => console.error('❌ No se pudo guardar push:', e.message));
}

app.get('/push/key', (req, res) => res.json({ enabled: !!(webpush && vapid), publicKey: vapid ? vapid.publicKey : null }));

app.get('/push/status', (req, res) => {
  if (!checkToken(req, res)) return;
  res.json({ enabled: !!(webpush && vapid), devices: pushState.subs.length, hasConfig: !!pushState.cfg });
});

app.post('/push/subscribe', (req, res) => {
  if (!checkToken(req, res)) return;
  if (!webpush || !vapid) return res.status(503).json({ error: 'Push no disponible en el servidor (falta web-push).' });
  const sub = req.body && req.body.subscription;
  if (!sub || typeof sub.endpoint !== 'string' || !/^https:\/\//.test(sub.endpoint) || !sub.keys || !sub.keys.p256dh || !sub.keys.auth) {
    return res.status(400).json({ error: 'Suscripción inválida.' });
  }
  const rec = { endpoint: sub.endpoint, keys: { p256dh: String(sub.keys.p256dh), auth: String(sub.keys.auth) },
    ua: String(req.headers['user-agent'] || '').slice(0, 160), created: new Date().toISOString() };
  const prev = pushState.subs.find(x => x.endpoint === rec.endpoint);
  if (prev) rec.created = prev.created || rec.created;
  pushState.subs = pushState.subs.filter(x => x.endpoint !== rec.endpoint);
  pushState.subs.push(rec);
  if (pushState.subs.length > 10) pushState.subs = pushState.subs.slice(-10);
  savePush();
  res.json({ ok: true, devices: pushState.subs.length });
});

app.post('/push/unsubscribe', (req, res) => {
  if (!checkToken(req, res)) return;
  const ep = req.body && req.body.endpoint;
  if (typeof ep === 'string') { pushState.subs = pushState.subs.filter(x => x.endpoint !== ep); savePush(); }
  res.json({ ok: true, devices: pushState.subs.length });
});

function cleanPushCfg(c) {
  c = c && typeof c === 'object' ? c : {};
  const n = (v, lo, hi, d) => { v = +v; return Number.isFinite(v) && v >= lo && v <= hi ? v : d; };
  const w = c.wd && typeof c.wd === 'object' ? c.wd : {};
  const nt = c.notify && typeof c.notify === 'object' ? c.notify : {};
  const adj = w.adj && +w.adj.amt > 0 && Number.isFinite(+w.adj.base) ? { amt: +w.adj.amt, base: +w.adj.base } : null;
  const g = Math.round(+w.goal);
  return {
    deposit: n(c.deposit, 0, 1e9, 0), threshold: n(c.threshold, 1, 1000, 25), riskPct: n(c.riskPct, 0.01, 100, 2.5),
    ddLimitPct: n(c.ddLimitPct, 0.1, 100, 5),
    notify: { wd: nt.wd !== false, dd: nt.dd !== false, coach: nt.coach !== false },
    wd: {
      mode: w.mode === 'recuperar' ? 'recuperar' : 'conservador', pct: n(w.pct, 5, 90, 40),
      startNum: Math.round(n(w.startNum, 2, 7, 2)), goal: (g >= 2 && g <= 6) ? g : null,
      claimed: Array.isArray(w.claimed) ? w.claimed.map(Number).filter(x => x >= 2 && x <= 6) : [],
      cycleKey: String(w.cycleKey == null ? '' : w.cycleKey).slice(0, 40), adj
    }
  };
}

app.post('/push/config', (req, res) => {
  if (!checkToken(req, res)) return;
  pushState.cfg = cleanPushCfg(req.body);
  savePush();
  res.json({ ok: true });
  evaluatePush('config').catch(e => console.error('❌ Push:', e && e.message));
});

app.post('/push/test', async (req, res) => {
  if (!checkToken(req, res)) return;
  if (!webpush || !vapid) return res.status(503).json({ error: 'Push no disponible en el servidor (falta web-push).' });
  if (!pushState.subs.length) return res.status(409).json({ error: 'Este servidor no tiene ningún dispositivo suscrito.' });
  const r = await sendPush([{ title: 'TradeLens — Prueba', body: 'Si ves esto, las notificaciones push funcionan aunque la app esté cerrada.', tag: 'tl-test', url: './index.html' }]);
  res.json({ ok: r.sent > 0, ...r });
});

async function sendPush(items) {
  if (!webpush || !vapid || !pushState.subs.length) return { sent: 0, failed: 0, removed: 0 };
  let sent = 0, failed = 0; const dead = new Set();
  for (const it of items) {
    const payload = JSON.stringify(it);
    await Promise.all(pushState.subs.map(async sub => {
      try { await webpush.sendNotification(sub, payload, { TTL: 6 * 3600, urgency: 'high' }); sent++; }
      catch (e) {
        if (e && (e.statusCode === 404 || e.statusCode === 410)) dead.add(sub.endpoint);
        else { failed++; console.error('❌ Push falló:', e && (e.statusCode || e.message)); }
      }
    }));
  }
  if (dead.size) { pushState.subs = pushState.subs.filter(x => !dead.has(x.endpoint)); savePush(); }
  return { sent, failed, removed: dead.size };
}

// ── Cálculo (réplica de las funciones del dashboard) ──
const PUSH_DAY_EPS = 5; // igual que DAY_EPS del dashboard: neto entre -5 y +5 = BreakEven
const pDayClass = n => n > PUSH_DAY_EPS ? 'win' : n < -PUSH_DAY_EPS ? 'loss' : 'be';
const pFmt = n => '$' + Number(n).toLocaleString('en', { minimumFractionDigits: 2, maximumFractionDigits: 2 });

// Neto REALIZADO por día del bróker (profit+commission+swap de cada posición CERRADA, en el día de su último cierre)
function pushDays(data) {
  const deals = (data.deals || []).filter(d => d && d.entry !== 'BALANCE');
  const open = new Set((data.positions || []).flatMap(p => [String(p.ticket), ...(p.position_id != null ? [String(p.position_id)] : [])]));
  const pos = {};
  for (const d of deals) (pos[d.position_id] = pos[d.position_id] || []).push(d);
  const days = {};
  for (const [pid, ds] of Object.entries(pos)) {
    if (open.has(String(pid))) continue;
    const exits = ds.filter(d => d.entry === 'OUT' || d.entry === 'INOUT');
    if (!exits.length) continue;
    const endT = Math.max(...exits.map(d => +d.time || 0));
    const net = ds.reduce((a, d) => a + (+d.profit || 0) + (+d.commission || 0) + (+d.swap || 0), 0);
    const date = new Date(endT * 1000).toISOString().slice(0, 10);
    days[date] = +((days[date] || 0) + net).toFixed(2);
  }
  return days;
}
function pushStreak(list) { // list = [{date, net}] ordenada; devuelve la racha actual
  let run = null;
  for (const d of list) {
    const type = pDayClass(d.net);
    if (run && run.type === type && type !== 'be') { run.count++; run.net = +(run.net + d.net).toFixed(2); run.end = d.date; }
    else run = { type, count: type === 'be' ? 0 : 1, net: type === 'be' ? 0 : d.net, start: d.date, end: d.date };
  }
  return run && run.type !== 'be' ? run : { type: 'none', count: 0, net: 0, start: null, end: null };
}
function pushDayStats(nets) {
  const w = nets.filter(x => pDayClass(x) === 'win'), l = nets.filter(x => pDayClass(x) === 'loss');
  const gw = w.reduce((a, b) => a + b, 0), gl = Math.abs(l.reduce((a, b) => a + b, 0));
  return { N: nets.length, dec: w.length + l.length, exp: nets.length ? nets.reduce((a, b) => a + b, 0) / nets.length : 0, pf: gl > 0 ? gw / gl : null };
}
function wdBuildLevels(deposit, thr) { const f = 1 + thr / 100, out = []; let t = deposit; for (let i = 0; i < 6; i++) { out.push({ num: i + 1, target: +t.toFixed(2) }); t *= f; } return out; }
function wdCurrentIdx(levels, bal) { for (let i = levels.length - 1; i >= 0; i--) if (bal >= levels[i].target) return i; return 0; }
function wdCompute(levels, bal, st) {
  const f = st.pct / 100;
  const info = levels.map((l, i) => i === 0 ? null : { num: l.num, w: +((l.target - levels[i - 1].target) * f).toFixed(2), claimed: st.claimed.includes(l.num) });
  const granted = []; let sum = 0;
  if (st.mode === 'conservador') {
    for (const x of info) {
      if (!x || x.claimed || x.num < st.startNum) continue;
      if (bal - (sum + x.w) >= levels[x.num - 1].target - 1e-6) { granted.push(x); sum += x.w; } else break;
    }
  }
  return { granted, sum: +sum.toFixed(2) };
}

let _pushBusy = false;
async function evaluatePush(reason) {
  if (!webpush || !vapid || !pushState.subs.length || !accountData || _pushBusy) return;
  _pushBusy = true;
  try {
    const cfg = pushState.cfg, S = pushState.sent, acc = accountData.account || {}, balance = +acc.balance || 0;
    const nt = (cfg && cfg.notify) || { wd: true, dd: true, coach: true };
    const off = Number.isFinite(+accountData.timestamp) && lastUpdate ? Math.round((accountData.timestamp * 1000 - Date.parse(lastUpdate)) / 900000) * 900000 : 0;
    const today = new Date(Date.now() + off).toISOString().slice(0, 10); // día del servidor del bróker
    const days = pushDays(accountData);
    const out = [];

    // 1) Retiro disponible / objetivo alcanzado (misma lógica que el Plan de Retiro)
    if (cfg && nt.wd && cfg.deposit > 0 && balance > 0) {
      const st = cfg.wd, levels = wdBuildLevels(cfg.deposit, cfg.threshold);
      let eff = balance;
      if (st.adj && Math.abs(balance - st.adj.base) < 0.005) eff = balance - st.adj.amt;
      if (st.mode === 'conservador') {
        const { granted, sum } = wdCompute(levels, eff, st);
        if (granted.length) {
          const sig = st.cycleKey + '#' + granted.map(x => x.num + ':' + x.w).join('|');
          if (S.wdSig !== sig) { S.wdSig = sig; out.push({ title: 'TradeLens — Retiro disponible 💸', body: `¡Lograste el umbral de retiro! Ya puedes retirar ${pFmt(sum)} de tu cuenta.`, tag: 'tl-wd', url: './index.html#wdSection' }); }
        }
      } else {
        const goal = st.goal || Math.min(6, Math.max(2, wdCurrentIdx(levels, balance) + 2));
        const sig = st.cycleKey + '#' + goal;
        if (balance >= levels[goal - 1].target && S.goalSig !== sig) { S.goalSig = sig; out.push({ title: 'TradeLens — Objetivo alcanzado 🎯', body: `Tu cuenta llegó al Nivel ${goal} (${pFmt(levels[goal - 1].target)}).`, tag: 'tl-wd', url: './index.html#wdSection' }); }
      }
    }

    // 2) Drawdown diario superado (equity vs balance de inicio de día; mismo cálculo que el banner)
    if (cfg && nt.dd && cfg.ddLimitPct > 0 && balance > 0) {
      const realized = days[today] || 0, base = balance - realized, limit = base * cfg.ddLimitPct / 100;
      const eq = acc.equity != null && Number.isFinite(+acc.equity) ? +acc.equity : null;
      const used = eq != null ? Math.max(0, base - eq) : (realized < 0 ? -realized : 0);
      if (base > 0 && used >= limit && S.ddDay !== today) {
        S.ddDay = today;
        out.push({ title: 'TradeLens — Drawdown diario superado ⚠️', body: `Pérdida de hoy: -${pFmt(used)} (${(used / base * 100).toFixed(2)}%). Tu límite es ${cfg.ddLimitPct}% = ${pFmt(limit)}.`, tag: 'tl-dd', url: './index.html' });
      }
    }

    // 3) Coach: avisos de cambios importantes (subconjunto calculable en el servidor; solo con días ya cerrados)
    if (nt.coach) {
      const closed = Object.keys(days).filter(d => d < today).sort().map(d => ({ date: d, net: days[d] }));
      const st = pushDayStats(closed.map(d => d.net));
      const cur = pushStreak(closed);
      const streakSig = (cur.type !== 'none' && cur.count >= 3) ? `${cur.type}${cur.count}@${cur.end}` : '';
      const perf = (st.dec >= 5 ? ((st.pf != null && st.pf < 1 ? 1 : 0) | (st.exp < 0 ? 2 : 0)) : 0);
      let lvl = null, lvlTarget = 0;
      if (cfg && cfg.deposit > 0 && balance > 0) { lvl = 0; lvlTarget = cfg.deposit; const f = 1 + cfg.threshold / 100; while (balance >= lvlTarget * f && lvl < 50) { lvl++; lvlTarget *= f; } }
      const cyc = cfg ? cfg.wd.cycleKey : '';
      if (!S.coachInit) { // primera evaluación: se toma la foto actual sin avisar
        S.coachInit = true; S.streakSig = streakSig; S.perf = perf; if (lvl != null) { S.lvlMax = lvl; S.lvlCycle = cyc; }
      } else {
        if (streakSig !== S.streakSig) {
          if (streakSig) {
            out.push(cur.type === 'loss'
              ? { title: `Coach — Racha de ${cur.count} días perdedores ❄️`, body: `${cur.count} días operativos seguidos con neto negativo (${pFmt(cur.net)}). Revisa esos días en el journal antes de cambiar nada.`, tag: 'tl-coach-streak', url: './index.html#coachGrid' }
              : { title: `Coach — Racha de ${cur.count} días ganadores 🔥`, body: `${cur.count} días operativos seguidos con neto positivo (${pFmt(cur.net)}). Mantén el plan y el mismo criterio de riesgo.`, tag: 'tl-coach-streak', url: './index.html#coachGrid' });
          }
          S.streakSig = streakSig;
        }
        if (perf !== (S.perf || 0)) {
          const was = S.perf || 0;
          if ((perf & 1) && !(was & 1)) out.push({ title: 'Coach — Profit Factor Diario < 1 ⚠️', body: `PF diario ${st.pf.toFixed(2)}: la suma de los días perdedores supera a la de los ganadores (${st.N} días).`, tag: 'tl-coach-pf', url: './index.html#coachGrid' });
          if ((perf & 2) && !(was & 2)) out.push({ title: 'Coach — Expectancy Diaria Negativa ⚠️', body: `${pFmt(st.exp)} por día operado. Revisa si viene de pocos días de pérdida grande o de pérdidas pequeñas frecuentes.`, tag: 'tl-coach-exp', url: './index.html#coachGrid' });
          if (was && !perf) out.push({ title: 'Coach — Métricas diarias recuperadas ✅', body: `PF diario${st.pf != null ? ' ' + st.pf.toFixed(2) : ''} y expectancy ${pFmt(st.exp)} por día operado.`, tag: 'tl-coach-pf', url: './index.html#coachGrid' });
          S.perf = perf;
        }
        if (lvl != null) {
          if (S.lvlCycle !== cyc) { S.lvlCycle = cyc; S.lvlMax = lvl; }
          else if (S.lvlMax == null) S.lvlMax = lvl;
          else if (lvl > S.lvlMax) {
            S.lvlMax = lvl;
            out.push({ title: `Coach — Nivel de Riesgo ${lvl + 1} 📊`, body: `Riesgo actual: ${pFmt(lvlTarget * cfg.riskPct / 100)}/trade (${cfg.riskPct}% de ${pFmt(lvlTarget)}).`, tag: 'tl-coach-lvl', url: './index.html#coachGrid' });
          }
        }
      }
    }

    savePush();               // guarda lo "ya avisado" aunque no haya nada que enviar
    if (out.length) { const r = await sendPush(out); console.log(`🔔 Push (${reason}): ${out.map(o => o.tag).join(', ')} → ${r.sent} enviado(s), ${r.failed} fallido(s)`); }
  } finally { _pushBusy = false; }
}

console.log(`🖼️  Imágenes del journal: rutas activas (${journalStore.imgPersistent ? 'persistentes en Postgres' : '⚠️ disco temporal, NO persistentes'})`);
const PORT = process.env.PORT || 3000;
loadState().then(initPush).catch(() => {}).finally(() => app.listen(PORT, () => console.log(`🚀 TradeLens Server corriendo en puerto ${PORT}`)));
