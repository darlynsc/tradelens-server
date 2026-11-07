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
const zlib = require('zlib');
const path = require('path');
const swingEngine = require('./swing-engine');   // ciclos SWING (módulo puro; subirlo junto a server.js)
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

// ── CICLOS SWING (estructura DERIVADA; los datos originales de la EA no se tocan) ────────────────────────
// El estado (registros por posición, ids de ciclo, contadores, snapshots por sesión, ambigüedades) se guarda aparte
// en la tabla kv ('swing_state'). La vista (ciclos, sesiones, resultados) se recalcula en cada /sync de forma
// determinista e idempotente: repetir un sync o reiniciar el servidor no crea ciclos ni duplica importes.
let swingState = null;
let swingView = null;
let swingDirtyAt = 0, swingSaveTimer = null;
function swingUpdate(snapshot) {
  try {
    const r = swingEngine.update(swingState, snapshot, { serverNow: Date.now() / 1000 });
    swingState = r.state; swingView = r.view;
    if (r.changed) scheduleSwingSave();
  } catch (e) { console.error('❌ Motor SWING:', e.stack || e.message); }   // un fallo aquí NUNCA debe bloquear /sync
}
function scheduleSwingSave() {
  if (swingSaveTimer) return;
  swingSaveTimer = setTimeout(() => {
    swingSaveTimer = null;
    journalStore.kvSet('swing_state', swingState).catch(e => console.error('❌ No se pudo guardar el estado SWING:', e.message));
  }, 3000);
}

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
  swingUpdate(accountData);   // deriva ciclos SWING del snapshot (no modifica accountData)
  archiveDeals(accountData.deals);   // acumula deals nuevos en el archivo

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
  pushEvaluate().catch(e => console.error('❌ Push:', e.message));   // avisos al teléfono (nunca bloquea /sync)

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
  const payload = { ...accountData, deals: mergedDeals(), swing: swingView, server_time: lastUpdate };
  if (!/\bgzip\b/.test(String(req.headers['accept-encoding'] || ''))) return res.json(payload);
  const key = lastUpdate + '|' + dealArchive.size + '|' + (swingView ? 1 : 0);
  const send = buf => { res.setHeader('Content-Type', 'application/json; charset=utf-8'); res.setHeader('Content-Encoding', 'gzip'); res.setHeader('Vary', 'Accept-Encoding'); res.end(buf); };
  if (dataGzCache.key === key && dataGzCache.buf) return send(dataGzCache.buf);
  zlib.gzip(JSON.stringify(payload), (err, buf) => {
    if (err) return res.json(payload);
    dataGzCache = { key, buf };
    send(buf);
  });
});

// ── CICLOS SWING: consulta directa (ciclos, sesiones, ambigüedades) ────────────────────────────────────────
app.get('/swing', (req, res) => {
  const token = req.query.token || req.headers['x-token'];
  if (!isAuthorized(token)) return res.status(401).json({ error: 'Token inválido' });
  if (!swingView) return res.status(404).json({ error: 'Aún no hay ciclos calculados.' });
  res.json(swingView);
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
    .then(() => pool.query(`CREATE TABLE IF NOT EXISTS tl_deals (
      ticket TEXT PRIMARY KEY, t BIGINT NOT NULL DEFAULT 0, d TEXT NOT NULL)`))
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
    // ── Archivo de deals (historial acumulado; la EA solo envía los más recientes) ──
    async dealsAll() {
      await ready;
      const { rows } = await pool.query('SELECT ticket, d FROM tl_deals');
      return rows.map(r => [r.ticket, JSON.parse(r.d)]);
    },
    async dealsAdd(items) { // items: [[ticket, deal], ...] — solo inserta los que no existen
      await ready;
      for (let i = 0; i < items.length; i += 1000) {
        const c = items.slice(i, i + 1000);
        await pool.query(
          `INSERT INTO tl_deals (ticket, t, d) SELECT * FROM unnest($1::text[], $2::bigint[], $3::text[]) ON CONFLICT (ticket) DO NOTHING`,
          [c.map(x => x[0]), c.map(x => Math.round(+x[1].time) || 0), c.map(x => JSON.stringify(x[1]))]);
      }
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
    async dealsAll() { try { return JSON.parse(fs.readFileSync(path.join(dir, 'deals.json'), 'utf8')); } catch (_) { return []; } },
    async dealsAdd(items) {
      const cur = new Map(await this.dealsAll());
      items.forEach(x => { if (!cur.has(x[0])) cur.set(x[0], x[1]); });
      try { fs.mkdirSync(dir, { recursive: true }); fs.writeFileSync(path.join(dir, 'deals.json'), JSON.stringify([...cur])); } catch (_) {}
    },
    imgPersistent: false
  };
}
function parseImgs(s) {
  try { const a = typeof s === 'string' ? JSON.parse(s) : s; return Array.isArray(a) ? a.filter(x => /^[a-f0-9]{32}$/.test(x)).slice(0, 8) : []; }
  catch (_) { return []; }
}

// ── ARCHIVO DE DEALS ──────────────────────────────────────────────────────────
// La EA envía solo los deals más recientes. Aquí se acumulan (clave = ticket, inmutables) para no perder historial.
// No modifica accountData ni el snapshot: solo se usa para completar la lista de deals que se sirve en /data.
const dealArchive = new Map();
let dealWriteChain = Promise.resolve();
function archiveDeals(deals) {
  try {
    if (!Array.isArray(deals)) return;
    const fresh = [];
    for (const d of deals) {
      if (!d || d.ticket == null) continue;
      const k = String(d.ticket);
      if (dealArchive.has(k)) continue;
      dealArchive.set(k, d); fresh.push([k, d]);
    }
    if (!fresh.length) return;
    dealWriteChain = dealWriteChain
      .then(() => journalStore.dealsAdd(fresh))
      .catch(e => { fresh.forEach(x => dealArchive.delete(x[0])); console.error('❌ No se pudo archivar deals (se reintentará):', e.message); });
  } catch (e) { console.error('❌ Archivo de deals:', e.message); }
}
function mergedDeals() {
  const snap = (accountData && accountData.deals) || [];
  if (!dealArchive.size) return snap;
  const m = new Map(dealArchive), extra = [];
  for (const d of snap) { if (d && d.ticket != null) m.set(String(d.ticket), d); else extra.push(d); }
  const n = v => { const x = +v; return Number.isFinite(x) ? x : 0; };
  return [...m.values(), ...extra].sort((a, b) => (n(a.time) - n(b.time)) || (n(a.time_msc) - n(b.time_msc)) || (n(a.ticket) - n(b.ticket)));
}
let dataGzCache = { key: '', buf: null };

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

// Clave de journal: fecha (día intradía) o id de ciclo SWING (SWING-AAAA-NNNNNN)
const JKEY_RE = /^(\d{4}-\d{2}-\d{2}|SWING-\d{4}-\d{6})$/;
app.put('/journal/:day', async (req, res) => {
  if (!checkToken(req, res)) return;
  const day = req.params.day;
  if (!JKEY_RE.test(day)) return res.status(400).json({ error: 'Fecha inválida' });
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
  if (!JKEY_RE.test(day)) return res.status(400).json({ error: 'Fecha inválida' });
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
  if (!JKEY_RE.test(day) || !/^[a-f0-9]{32}$/.test(id)) return res.status(400).json({ error: 'Parámetros inválidos' });
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
    try { (await journalStore.dealsAll()).forEach(x => dealArchive.set(x[0], x[1])); console.log(`🗃️  Archivo de deals: ${dealArchive.size}`); }
    catch (e) { console.error('❌ Error cargando archivo de deals:', e.message); }
    if (accountData) archiveDeals(accountData.deals);   // siembra el archivo con el último snapshot guardado
    // Estado SWING persistido: se recupera y se recalcula la vista con el último snapshot (mismos ids, mismas relaciones)
    swingState = await journalStore.kvGet('swing_state');
    if (accountData) { swingUpdate(accountData); }
    console.log(swingView ? `🔗 Ciclos SWING: ${swingView.stats.cycles} (${swingView.stats.active} activos)` : '🔗 Ciclos SWING: sin datos todavía');
    console.log(authCache ? `👤 Usuario configurado: ${authCache.username}` : '👤 Sin usuario creado todavía');
  } catch (e) { console.error('❌ Error cargando estado:', e.message); }
}

// ── NOTIFICACIONES PUSH (Web Push: VAPID + cifrado aes128gcm; usa solo 'crypto' y 'fetch' de Node, sin paquetes nuevos) ──
// El teléfono/PC se suscribe con /push/subscribe; el servidor evalúa cada /sync de la EA y envía el aviso aunque la app esté cerrada.
// Las claves VAPID se generan una vez y se guardan junto con las suscripciones en la tabla kv ('push_state').
const PUSH_SUBJECT = process.env.VAPID_SUBJECT || 'mailto:tradelens@example.com';
let pushState = null, pushLoading = null, pushSaveTimer = null;
const pushNowUtc = () => Math.floor(Date.now() / 1000);

async function pushEnsure() {
  if (pushState) return pushState;
  if (!pushLoading) pushLoading = (async () => {
    let st = null;
    try { st = await journalStore.kvGet('push_state'); } catch (_) {}
    if (!st || !st.vapid || !st.vapid.publicKey || !st.vapid.privateJwk) {
      const kp = crypto.generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
      const jwk = kp.privateKey.export({ format: 'jwk' });
      const pub = Buffer.concat([Buffer.from([4]), Buffer.from(jwk.x, 'base64url'), Buffer.from(jwk.y, 'base64url')]);
      st = { vapid: { publicKey: pub.toString('base64url'), privateJwk: jwk }, subs: {}, cfg: {}, sent: {} };
      try { await journalStore.kvSet('push_state', st); } catch (e) { console.error('❌ No se pudo guardar push_state:', e.message); }
      console.log('🔔 Push: claves VAPID generadas');
    }
    st.subs = st.subs || {}; st.cfg = st.cfg || {}; st.sent = st.sent || {};
    pushState = st;
    return st;
  })();
  return pushLoading;
}
function pushSave() {
  if (pushSaveTimer) return;
  pushSaveTimer = setTimeout(() => {
    pushSaveTimer = null;
    journalStore.kvSet('push_state', pushState).catch(e => console.error('❌ No se pudo guardar push_state:', e.message));
  }, 500);
}
const pushHmac = (k, d) => crypto.createHmac('sha256', k).update(d).digest();
// RFC 8291 (aes128gcm). opts.asPrivate / opts.salt solo para pruebas con el vector oficial.
function pushEncrypt(sub, payload, opts) {
  const ua = Buffer.from(sub.keys.p256dh, 'base64url'), auth = Buffer.from(sub.keys.auth, 'base64url');
  const ecdh = crypto.createECDH('prime256v1');
  if (opts && opts.asPrivate) ecdh.setPrivateKey(opts.asPrivate); else ecdh.generateKeys();
  const asPub = ecdh.getPublicKey();
  const secret = ecdh.computeSecret(ua);
  const prkKey = pushHmac(auth, secret);
  const ikm = pushHmac(prkKey, Buffer.concat([Buffer.from('WebPush: info\0'), ua, asPub, Buffer.from([1])]));
  const salt = (opts && opts.salt) || crypto.randomBytes(16);
  const prk = pushHmac(salt, ikm);
  const cek = pushHmac(prk, Buffer.concat([Buffer.from('Content-Encoding: aes128gcm\0'), Buffer.from([1])])).subarray(0, 16);
  const nonce = pushHmac(prk, Buffer.concat([Buffer.from('Content-Encoding: nonce\0'), Buffer.from([1])])).subarray(0, 12);
  const cipher = crypto.createCipheriv('aes-128-gcm', cek, nonce);
  const enc = Buffer.concat([cipher.update(Buffer.concat([payload, Buffer.from([2])])), cipher.final(), cipher.getAuthTag()]);
  const rs = Buffer.alloc(4); rs.writeUInt32BE(4096);
  return Buffer.concat([salt, rs, Buffer.from([asPub.length]), asPub, enc]);
}
function pushVapidJwt(audience) {
  const head = Buffer.from(JSON.stringify({ typ: 'JWT', alg: 'ES256' })).toString('base64url');
  const claims = Buffer.from(JSON.stringify({ aud: audience, exp: pushNowUtc() + 12 * 3600, sub: PUSH_SUBJECT })).toString('base64url');
  const key = crypto.createPrivateKey({ key: pushState.vapid.privateJwk, format: 'jwk' });
  const sig = crypto.sign('sha256', Buffer.from(head + '.' + claims), { key, dsaEncoding: 'ieee-p1363' }).toString('base64url');
  return head + '.' + claims + '.' + sig;
}
// Devuelve { status, text }. 404/410 = suscripción muerta.
async function pushSendOne(sub, payloadObj) {
  const body = pushEncrypt(sub, Buffer.from(JSON.stringify(payloadObj)));
  const jwt = pushVapidJwt(new URL(sub.endpoint).origin);
  const r = await fetch(sub.endpoint, {
    method: 'POST',
    headers: { Authorization: `vapid t=${jwt}, k=${pushState.vapid.publicKey}`, 'Content-Encoding': 'aes128gcm',
      'Content-Type': 'application/octet-stream', TTL: '86400', Urgency: 'high' },
    body, signal: AbortSignal.timeout(10000)
  });
  let text = ''; if (r.status >= 300) { try { text = (await r.text()).slice(0, 200); } catch (_) {} }
  return { status: r.status, text };
}
// Envía a todos los dispositivos suscritos. Elimina los que ya no existen.
async function pushBroadcast(payload) {
  const st = await pushEnsure();
  const res = { sent: 0, failed: 0, errors: [] };
  for (const [ep, rec] of Object.entries(st.subs)) {
    try {
      const r = await pushSendOne(rec.sub, payload);
      if (r.status >= 200 && r.status < 300) res.sent++;
      else {
        res.failed++; res.errors.push(`${r.status} ${r.text}`.trim());
        if (r.status === 404 || r.status === 410) { delete st.subs[ep]; pushSave(); }
      }
    } catch (e) { res.failed++; res.errors.push(e.message); }
  }
  return res;
}
function pushAuth(req, res) {
  const t = req.headers['x-token'] || req.query.token;
  if (!isAuthorized(t)) { res.status(401).json({ error: 'Token inválido' }); return false; }
  return true;
}
app.get('/push/key', async (req, res) => {
  try { const st = await pushEnsure(); res.json({ enabled: true, publicKey: st.vapid.publicKey }); }
  catch (e) { res.json({ enabled: false, error: e.message }); }
});
app.post('/push/subscribe', async (req, res) => {
  if (!pushAuth(req, res)) return;
  try {
    const sub = req.body && req.body.subscription;
    if (!sub || typeof sub.endpoint !== 'string' || !/^https:\/\//.test(sub.endpoint) || !sub.keys || !sub.keys.p256dh || !sub.keys.auth)
      return res.status(400).json({ error: 'Suscripción inválida.' });
    const st = await pushEnsure();
    st.subs[sub.endpoint] = { sub: { endpoint: sub.endpoint, keys: { p256dh: String(sub.keys.p256dh), auth: String(sub.keys.auth) } }, added: new Date().toISOString() };
    pushSave();
    console.log(`🔔 Push: dispositivo suscrito (${Object.keys(st.subs).length} en total)`);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.post('/push/unsubscribe', async (req, res) => {
  if (!pushAuth(req, res)) return;
  try { const st = await pushEnsure(); if (req.body && req.body.endpoint) delete st.subs[req.body.endpoint]; pushSave(); res.json({ ok: true }); }
  catch (e) { res.status(500).json({ error: e.message }); }
});
app.post('/push/config', async (req, res) => {
  if (!pushAuth(req, res)) return;
  try {
    const st = await pushEnsure(), b = req.body || {}, num = (v, d) => (Number.isFinite(+v) ? +v : d);
    st.cfg = { deposit: num(b.deposit, 0), threshold: num(b.threshold, 25), riskPct: num(b.riskPct, 2.5), ddLimitPct: num(b.ddLimitPct, 5),
      notify: Object.assign({ wd: true, dd: true, coach: true }, b.notify || {}), wd: b.wd || null, updated: new Date().toISOString() };
    pushSave(); res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.post('/push/test', async (req, res) => {
  if (!pushAuth(req, res)) return;
  try {
    const st = await pushEnsure();
    if (!Object.keys(st.subs).length) return res.json({ ok: false, error: 'Ningún dispositivo registrado. Desactiva y vuelve a activar las notificaciones.' });
    const r = await pushBroadcast({ title: 'TradeLens', body: 'Notificación de prueba ✅ Todo funciona.', tag: 'tl-test' });
    if (r.sent > 0) return res.json({ ok: true, sent: r.sent, failed: r.failed });
    res.json({ ok: false, error: 'El servicio de notificaciones rechazó el envío: ' + (r.errors[0] || 'sin detalle') });
  } catch (e) { res.json({ ok: false, error: e.message }); }
});

// Pérdida del día vs límite de drawdown (misma fórmula que el dashboard). Un aviso por sesión NY.
function pushTodayRealized(data) {
  const off = (data.session && typeof data.session.broker_offset === 'number') ? data.session.broker_offset
    : (swingView && typeof swingView.brokerOffset === 'number') ? swingView.brokerOffset : 0;
  const open = new Set((data.positions || []).flatMap(p => [String(p.ticket), ...(p.position_id != null ? [String(p.position_id)] : [])]));
  const by = {};
  for (const d of (data.deals || [])) { if (!d || d.entry === 'BALANCE') continue; (by[d.position_id] = by[d.position_id] || []).push(d); }
  const u = pushNowUtc(), today = new Date((u + swingEngine.nyOffsetSec(u) + 25200) * 1000).toISOString().slice(0, 10);
  let realized = 0;
  for (const [pid, ds] of Object.entries(by)) {
    ds.sort((a, b) => a.time - b.time);
    const exits = ds.filter(d => d.entry === 'OUT' || d.entry === 'INOUT');
    if (!exits.length || open.has(String(pid))) continue;
    if (swingEngine.sessionKey(exits[exits.length - 1].time, off) !== today) continue;
    realized += ds.reduce((a, d) => a + (+d.profit || 0) + (+d.commission || 0) + (+d.swap || 0), 0);
  }
  return { today, realized };
}
async function pushEvaluate() {
  const st = await pushEnsure();
  if (!accountData || !Object.keys(st.subs).length) return;
  const cfg = st.cfg || {}, notify = cfg.notify || { dd: true };
  const acc = accountData.account || {};
  if (notify.dd !== false && acc.balance) {
    const { today, realized } = pushTodayRealized(accountData);
    const limitPct = cfg.ddLimitPct > 0 ? cfg.ddLimitPct : 5;
    const base = acc.balance - realized, limit = base * limitPct / 100;
    const realLoss = realized < 0 ? -realized : 0;
    const used = acc.equity != null ? Math.max(0, base - acc.equity) : realLoss;
    const key = 'dd:' + today;
    if (base > 0 && used >= limit && !st.sent[key]) {
      st.sent[key] = Date.now();
      for (const k of Object.keys(st.sent)) if (k.startsWith('dd:') && k !== key) delete st.sent[k];
      pushSave();
      const r = await pushBroadcast({ title: 'TradeLens — Drawdown diario superado ⚠️',
        body: `Pérdida del día: -$${used.toFixed(2)} (${(used / base * 100).toFixed(2)}%) · Límite ${limitPct}% = $${limit.toFixed(2)}`, tag: 'tl-dd' });
      console.log(`🔔 Push drawdown: enviados ${r.sent}, fallidos ${r.failed}`);
    }
  }
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
    features: { journal_images: true, images_persistent: !!journalStore.imgPersistent, price_excursions: true, swing_cycles: true, push: true },
    swing_cycles: swingView ? swingView.stats : null,
    price_exc_count: accountData?.price_exc ? Object.keys(accountData.price_exc).length : 0,
    version: '1.4.0'
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
