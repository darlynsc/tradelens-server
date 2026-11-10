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
const syncCore = require('./sync-core');         // seq, deals por cuenta+ticket, ventanas de recuperación, cambio de cuenta (subirlo junto a server.js)
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
// Un seq más de SEQ_FUTURE_MAX_MS adelantado respecto al reloj del servidor es incoherente (reloj del PC roto) y se rechaza
// para no envenenar lastSeq. Pequeñas diferencias se toleran.
const SEQ_FUTURE_MAX_MS = (+process.env.SEQ_FUTURE_MAX_MS) || 6 * 3600 * 1000;
const curAcctKey = () => (accountData && accountData.account && accountData.account.number != null) ? String(accountData.account.number) : '0';

// ── CICLOS SWING (estructura DERIVADA; los datos originales de la EA no se tocan) ────────────────────────
// El estado (registros por posición, ids de ciclo, contadores, snapshots por sesión, ambigüedades) se guarda aparte
// en la tabla kv ('swing_state'). La vista (ciclos, sesiones, resultados) se recalcula en cada /sync de forma
// determinista e idempotente: repetir un sync o reiniciar el servidor no crea ciclos ni duplica importes.
let swingState = null;
let swingView = null;
let swingDirtyAt = 0, swingSaveTimer = null, swingVersion = 0;
// Referencia del último snapshot recibido (reloj del servidor al recibirlo, UTC observado por la EA) para 'replay' y
// 'server_reevaluation' cuando el estado persistido aún no tiene state.tm.lastSnap (primer arranque tras la actualización).
function lastSnapHint() {
  if (!accountData || !lastUpdate) return null;
  const srvUtc = Date.parse(lastUpdate) / 1000;
  if (!Number.isFinite(srvUtc)) return null;
  const s = accountData.session || {};
  return { srvUtc, eaUtc: typeof s.utc_obs === 'number' ? s.utc_obs : null, sid: s.id ? String(s.id).replace(/\./g, '-') : null, brokerTs: accountData.timestamp };
}
// mode: 'fresh' (snapshot recién recibido de MT5) | 'replay' (snapshot recuperado del almacenamiento al arrancar)
//       | 'server_reevaluation' (el servidor avanza el reloj sin snapshot nuevo: no crea posiciones, cierres ni deals)
function swingUpdate(snapshot, mode) {
  try {
    const r = swingEngine.update(swingState, snapshot, { serverNow: Date.now() / 1000, mode: mode || 'fresh', lastSnapHint: lastSnapHint() });
    swingState = r.state; swingView = r.view; swingVersion++;
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
// Reevaluación sin snapshot: si el reloj del servidor ya está en una sesión NY posterior a la última resuelta, recalcula
// con el último estado conocido. Solo avanza la referencia temporal; los datos de cuenta/posiciones/deals son los del último snapshot.
function maybeReevaluate() {
  try {
    if (!swingState || !swingState.tm || !accountData) return false;
    const key = swingEngine.nySessionKeyUtc(Math.floor(Date.now() / 1000));
    if (swingState.tm.maxDay && key <= swingState.tm.maxDay) return false;
    swingUpdate(accountData, 'server_reevaluation');
    console.log(`🕔 Reevaluación sin snapshot: sesión ${swingView && swingView.nowDay} (estado marcado como no confirmado por MT5)`);
    return true;
  } catch (e) { console.error('❌ Reevaluación:', e.message); return false; }
}
// Información temporal para el dashboard (se calcula en cada respuesta: la antigüedad cambia con el tiempo)
function timeInfo() {
  const now = Date.now() / 1000;
  const ls = (swingState && swingState.tm && swingState.tm.lastSnap) || lastSnapHint();
  const age = ls && ls.srvUtc ? Math.max(0, Math.round(now - ls.srvUtc)) : null;
  const base = (swingView && swingView.time) || {};
  const stale = age == null || age > swingEngine.CFG.STALE_SECS;
  return { ...base, serverUtc: Math.round(now), snapshotAge: age, stale, state: stale ? (base.mode === 'fresh' || !base.mode ? 'potentially_stale' : 'last_known') : 'confirmed' };
}
const swingKv = { get: k => journalStore.kvGet(k), set: (k, v) => journalStore.kvSet(k, v) };
// Cambio de cuenta (o primer estado de una cuenta): archiva el estado anterior (verificado por lectura) y restaura el de la cuenta
// nueva si ya existía; solo si no existe crea uno vacío (con contadores elevados para que los ids no se repitan entre cuentas).
async function ensureSwingForAccount(acct) {
  if (!acct || (swingState && (!swingState.account || swingState.account === acct))) return;
  if (swingSaveTimer) { clearTimeout(swingSaveTimer); swingSaveTimer = null; }
  const oldAcct = swingState && swingState.account;
  const r = await syncCore.switchAccountState(swingKv, { state: swingState, snapshotBlob: accountData ? { data: accountData, lastUpdate } : null, oldAcct, newAcct: acct, engine: swingEngine });
  await journalStore.kvSet('swing_state', r.state);     // el activo se escribe DESPUÉS de verificar el archivo de la cuenta anterior
  swingState = r.state; swingView = null; lastSeq = null;
  console.log(`🔀 Cuenta ${oldAcct || '—'} → ${acct}: estado anterior archivado (swing_state:${oldAcct}); ${r.restored ? 'estado restaurado' : 'estado nuevo'}`);
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
// Respuesta (siempre JSON): ok, status ('accepted'|'duplicate'|'stale'|'future'), last_seq, deals{accepted,duplicate,rejected,persisted}
// y dealsync{status,window_ok,frontier,rescan}. Un seq obsoleto/futuro ya NO responde ok:true: la EA no lo toma como sincronizado.
app.post('/sync', async (req, res) => {
  const token = req.headers['x-token'] || req.body?.token;
  if (token !== SECRET_TOKEN) { // /sync solo lo acepta con el token maestro (la EA)
    return res.status(401).json({ error: 'Token inválido' });
  }
  const body = req.body || {};
  const acct = body.account && body.account.number != null ? String(body.account.number) : null;
  const prevSeq = lastSeq;
  try {
    // Cambio de cuenta: el seq de otra cuenta no es comparable
    if (acct && accountData && curAcctKey() !== acct) lastSeq = null;
    // Orden de snapshots
    const sq = syncCore.classifySeq(body.seq, lastSeq, Date.now(), SEQ_FUTURE_MAX_MS);
    if (sq.status === 'stale' || sq.status === 'future') {
      console.log(`⏭️  Sync rechazado (${sq.status}): seq ${body.seq} · último ${lastSeq}`);
      lastSeq = prevSeq;
      // EA antigua (sin ack_v) con seq obsoleto: se conserva la respuesta histórica; las EA nuevas reciben un error explícito.
      if (sq.status === 'stale' && body.ack_v !== 1) return res.json({ ok: true, stale: true, timestamp: lastUpdate, last_seq: lastSeq });
      return res.status(409).json({ ok: false, status: sq.status, accepted: false, last_seq: lastSeq, timestamp: lastUpdate });
    }
    if (sq.status === 'duplicate') {
      return res.json({ ok: true, status: 'duplicate', accepted: false, last_seq: lastSeq, timestamp: lastUpdate });
    }
    // Estado Swing de la cuenta correcta ANTES de aplicar nada (si el archivo falla, no se confirma el sync: la EA reintenta)
    try { await ensureSwingForAccount(acct); }
    catch (e) { lastSeq = prevSeq; console.error('❌ Cambio de cuenta no completado (nada se sobrescribe):', e.message); return res.status(503).json({ ok: false, status: 'account_switch_failed', accepted: false, last_seq: lastSeq }); }
    if (sq.status === 'accepted') lastSeq = body.seq;

    // price_exc: validar y fusionar (EA antigua sin este bloque: se conserva lo que ya había)
    const hadPx = Object.prototype.hasOwnProperty.call(body, 'price_exc');
    const prevPx = accountData && accountData.price_exc;
    if (hadPx || prevPx) {
      const r = mergePriceExc(hadPx ? body.price_exc : null, prevPx);
      body.price_exc = r.merged;
      body.price_exc_schema = 1;
      if (r.bad || r.older) console.log(`ℹ️  price_exc: ${r.ok} aceptadas, ${r.bad} inválidas descartadas, ${r.older} antiguas ignoradas`);
    }

    // Deals: lote de siempre + lote de recuperación. Se archivan por (cuenta, ticket) y se CONFIRMA lo guardado (acuse).
    const recovery = Array.isArray(body.recovery_deals) ? body.recovery_deals : [];
    delete body.recovery_deals;                         // no se persiste en el respaldo del snapshot
    const ak = acct || curAcctKey();
    const ing = await ingestDeals(ak, (Array.isArray(body.deals) ? body.deals : []).concat(recovery));
    let ds = null;
    if (body.dealsync && typeof body.dealsync === 'object') {
      // Auditoría DEAL_ENTRY_OUT_BY: la EA informa cuántos hay en el historial (hoy no se envían ni se contabilizan)
      if (+body.dealsync.outby_n > 0 && outByLogged !== body.dealsync.outby_n) { outByLogged = body.dealsync.outby_n; console.log(`🔎 Auditoría: ${outByLogged} deal(s) DEAL_ENTRY_OUT_BY en el historial de MT5 (no se envían; ver MIGRATION.md §OUT_BY)`); }
      const st = await getDealSync(ak);
      if (!ing.dbOk) ds = { status: 'db_pending', window_ok: false, frontier: st.frontier, rescan: false };
      else {
        const v = syncCore.verifyWindow(archMap(ak), st.frontier, body.dealsync);
        ds = { status: v.status, window_ok: v.window_ok, frontier: v.frontier, rescan: !!v.rescan };
        if (v.status === 'mismatch') { ds.srv_n = v.srv_n; ds.ea_n = v.ea_n; console.log(`⚠️  dealsync: ventana (${body.dealsync.from},${body.dealsync.to}] no coincide — servidor ${v.srv_n} vs EA ${v.ea_n}; la marca NO avanza`); }
        if (v.rescan) console.log('⚠️  dealsync: el prefijo completo no coincide; se pide re-escaneo desde 0 (idempotente)');
        if (v.window_ok && v.frontier > st.frontier) { st.frontier = v.frontier; st.updated = new Date().toISOString(); journalStore.kvSet('deal_sync:' + ak, st).catch(e => console.error('❌ deal_sync:', e.message)); }
      }
    }

    const newDealCount     = body.deals?.length || 0;
    const newBalance       = body.account?.balance || 0;
    const newPositionCount = body.positions?.length || 0;

    accountData = body;
    lastUpdate  = new Date().toISOString();
    // El motor recibe los deals del lote + recuperación (accountData no se modifica)
    swingUpdate(recovery.length ? { ...accountData, deals: (accountData.deals || []).concat(recovery) } : accountData, 'fresh');

    const dealChanged     = newDealCount     !== lastDealCount;
    const balanceChanged  = Math.abs(newBalance - lastBalance) > 0.01;
    const positionChanged = newPositionCount !== lastPositionCount;

    let reason = 'sync';
    if (balanceChanged)  reason = 'balance';
    if (dealChanged)     reason = 'deal';
    if (positionChanged) reason = newPositionCount < lastPositionCount ? 'cierre' : 'apertura';

    console.log(`✅ Sync — ${new Date().toLocaleTimeString()} — Deals: ${newDealCount} (+${ing.fresh} nuevos, ${ing.duplicate} ya existían) | Balance: ${newBalance} | Pos: ${newPositionCount} | Motivo: ${reason}`);

    lastDealCount     = newDealCount;
    lastBalance       = newBalance;
    lastPositionCount = newPositionCount;

    // Guardar respaldo en la nube (cambios al instante; si no hay cambios, máximo cada 60 s)
    persistSnapshot(reason);

    // Notificar al dashboard SIEMPRE que el EA envíe datos
    notifyClients(reason);
    pushEvaluate().catch(e => console.error('❌ Push:', e.message));   // avisos al teléfono (nunca bloquea /sync)

    const ack = { ok: true, status: 'accepted', accepted: true, timestamp: lastUpdate, last_seq: lastSeq,
      deals: { accepted: ing.fresh, duplicate: ing.duplicate, rejected: ing.rejected.length, rejected_tickets: ing.rejected.slice(0, 20), persisted: ing.dbOk } };
    if (ds) ack.dealsync = ds;
    res.json(ack);
  } catch (e) {
    lastSeq = prevSeq;
    console.error('❌ /sync:', e.stack || e.message);
    res.status(500).json({ ok: false, status: 'error', accepted: false, last_seq: lastSeq });
  }
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
  maybeReevaluate();
  const payload = { ...accountData, deals: mergedDeals(), swing: swingView ? { ...swingView, time: timeInfo() } : null, server_time: lastUpdate };
  if (!/\bgzip\b/.test(String(req.headers['accept-encoding'] || ''))) return res.json(payload);
  // La clave incluye la versión del estado Swing y una ventana de 15 s (la antigüedad del snapshot cambia con el tiempo)
  const key = lastUpdate + '|' + archMap(curAcctKey()).size + '|' + swingVersion + '|' + Math.floor(Date.now() / 15000);
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
  maybeReevaluate();
  if (!swingView) return res.status(404).json({ error: 'Aún no hay ciclos calculados.' });
  res.json({ ...swingView, time: timeInfo() });
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
    .then(() => pool.query(`CREATE TABLE IF NOT EXISTS tl_deals_v2 (
      acct TEXT NOT NULL, ticket TEXT NOT NULL, t BIGINT NOT NULL DEFAULT 0, d TEXT NOT NULL, src TEXT NOT NULL DEFAULT 'ea',
      PRIMARY KEY (acct, ticket))`))
    .then(() => pool.query(`CREATE TABLE IF NOT EXISTS tl_annotations (
      id TEXT PRIMARY KEY, items TEXT NOT NULL DEFAULT '[]', n INTEGER NOT NULL DEFAULT 0,
      saved TEXT NOT NULL DEFAULT '', updated TEXT NOT NULL DEFAULT '')`))
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
      if (r.rowCount > 0) { try { await pool.query('DELETE FROM tl_annotations WHERE id=$1', [id]); } catch (_) {} }
      return r.rowCount > 0;
    },
    async imgCountDay(day) {
      await ready;
      const { rows } = await pool.query('SELECT COUNT(*)::int AS n FROM tl_images WHERE day=$1', [day]);
      return rows[0].n;
    },
    // ── Archivo de deals (historial acumulado; la EA solo envía los más recientes) ──
    // dealsV2 === false (por defecto, hasta que exista kv 'deals_migration'.ok): tabla heredada tl_deals (clave = ticket, sin cuenta).
    // dealsV2 === true: tabla tl_deals_v2 con clave compuesta (acct, ticket). La tabla heredada NUNCA se modifica ni se borra.
    dealsV2: false,
    // Devuelve [[acct, ticket, deal], ...]; acct '' = fila heredada sin cuenta (el servidor la asigna a la cuenta activa en memoria)
    async dealsAll() {
      await ready;
      if (this.dealsV2) {
        const { rows } = await pool.query('SELECT acct, ticket, d FROM tl_deals_v2');
        return rows.map(r => [r.acct, r.ticket, JSON.parse(r.d)]);
      }
      const { rows } = await pool.query('SELECT ticket, d FROM tl_deals');
      return rows.map(r => ['', r.ticket, JSON.parse(r.d)]);
    },
    async dealsAdd(acct, items) { // items: [[ticket, deal], ...] — solo inserta los que no existen
      await ready;
      for (let i = 0; i < items.length; i += 1000) {
        const c = items.slice(i, i + 1000);
        if (this.dealsV2)
          await pool.query(
            `INSERT INTO tl_deals_v2 (acct, ticket, t, d) SELECT $1, * FROM unnest($2::text[], $3::bigint[], $4::text[]) ON CONFLICT (acct, ticket) DO NOTHING`,
            [acct, c.map(x => x[0]), c.map(x => Math.round(+x[1].time) || 0), c.map(x => JSON.stringify(x[1]))]);
        else
          await pool.query(
            `INSERT INTO tl_deals (ticket, t, d) SELECT * FROM unnest($1::text[], $2::bigint[], $3::text[]) ON CONFLICT (ticket) DO NOTHING`,
            [c.map(x => x[0]), c.map(x => Math.round(+x[1].time) || 0), c.map(x => JSON.stringify(x[1]))]);
      }
    },
    // Migración aditiva e idempotente tl_deals → tl_deals_v2. Solo copia (INSERT … SELECT … ON CONFLICT DO NOTHING) y verifica conteos.
    // Las filas heredadas no llevan cuenta: se asignan a `acct` (la cuenta del último snapshot), marcadas src='legacy_assumed'.
    async migrateDealsV2(acct) {
      await ready;
      const n0 = +(await pool.query('SELECT COUNT(*) AS n FROM tl_deals')).rows[0].n;
      await pool.query(`INSERT INTO tl_deals_v2 (acct, ticket, t, d, src) SELECT $1, ticket, t, d, 'legacy_assumed' FROM tl_deals ON CONFLICT (acct, ticket) DO NOTHING`, [acct]);
      const n1 = +(await pool.query('SELECT COUNT(*) AS n FROM tl_deals_v2 WHERE acct=$1', [acct])).rows[0].n;
      const miss = +(await pool.query('SELECT COUNT(*) AS n FROM tl_deals l WHERE NOT EXISTS (SELECT 1 FROM tl_deals_v2 v WHERE v.acct=$1 AND v.ticket=l.ticket)', [acct])).rows[0].n;
      return { legacy: n0, v2: n1, missing: miss, ok: miss === 0 && n1 >= n0 };
    },
    // ── Anotaciones (dibujos sobre las imágenes): gana la versión con `saved` más reciente ──
    async annGet(id) {
      await ready;
      const { rows } = await pool.query('SELECT items, saved FROM tl_annotations WHERE id=$1', [id]);
      if (!rows[0]) return null;
      let items = []; try { const a = JSON.parse(rows[0].items); if (Array.isArray(a)) items = a; } catch (_) {}
      return { items, saved: rows[0].saved };
    },
    async annPut(id, items, saved) {
      await ready;
      await pool.query(
        `INSERT INTO tl_annotations (id, items, n, saved, updated) VALUES ($1,$2,$3,$4,$5)
         ON CONFLICT (id) DO UPDATE SET items=EXCLUDED.items, n=EXCLUDED.n, saved=EXCLUDED.saved, updated=EXCLUDED.updated
         WHERE tl_annotations.saved <= EXCLUDED.saved`,
        [id, JSON.stringify(items), items.length, saved, new Date().toISOString()]);
      return this.annGet(id);
    },
    async annCounts(ids) {
      await ready;
      const { rows } = await pool.query('SELECT id, n FROM tl_annotations WHERE id = ANY($1::text[]) AND n > 0', [ids]);
      const out = {}; rows.forEach(r => { out[r.id] = { n: r.n }; });
      return out;
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
        try { const m = JSON.parse(fs.readFileSync(path.join(dir, 'annotations.json'), 'utf8')); if (m[id]) { delete m[id]; fs.writeFileSync(path.join(dir, 'annotations.json'), JSON.stringify(m)); } } catch (_) {}
        return true;
      } catch (_) { return false; }
    },
    async imgCountDay(day) {
      try {
        const d = path.join(dir, 'images');
        return fs.readdirSync(d).filter(f => f.endsWith('.json') && JSON.parse(fs.readFileSync(path.join(d, f), 'utf8')).day === day).length;
      } catch (_) { return 0; }
    },
    // Archivo local: deals_v2.json = [[acct, ticket, deal]]. El deals.json heredado se lee (acct '') pero NUNCA se modifica.
    dealsV2: true,
    async dealsAll() {
      let v2 = []; try { v2 = JSON.parse(fs.readFileSync(path.join(dir, 'deals_v2.json'), 'utf8')); } catch (_) {}
      let legacy = []; try { legacy = JSON.parse(fs.readFileSync(path.join(dir, 'deals.json'), 'utf8')).map(x => ['', x[0], x[1]]); } catch (_) {}
      return legacy.concat(v2);
    },
    async dealsAdd(acct, items) {
      let cur = []; try { cur = JSON.parse(fs.readFileSync(path.join(dir, 'deals_v2.json'), 'utf8')); } catch (_) {}
      const have = new Set(cur.map(x => x[0] + '|' + x[1]));
      items.forEach(x => { if (!have.has(acct + '|' + x[0])) cur.push([acct, x[0], x[1]]); });
      try { fs.mkdirSync(dir, { recursive: true }); fs.writeFileSync(path.join(dir, 'deals_v2.json'), JSON.stringify(cur)); } catch (_) {}
    },
    // Anotaciones en disco local: NO persistentes en Render sin disco. Solo para pruebas locales.
    async annGet(id) {
      try { const m = JSON.parse(fs.readFileSync(path.join(dir, 'annotations.json'), 'utf8')); return m[id] || null; } catch (_) { return null; }
    },
    async annPut(id, items, saved) {
      let m = {}; try { m = JSON.parse(fs.readFileSync(path.join(dir, 'annotations.json'), 'utf8')); } catch (_) {}
      if (!m[id] || (m[id].saved || '') <= saved) {
        m[id] = { items, saved };
        try { fs.mkdirSync(dir, { recursive: true }); fs.writeFileSync(path.join(dir, 'annotations.json'), JSON.stringify(m)); } catch (_) {}
      }
      return m[id];
    },
    async annCounts(ids) {
      let m = {}; try { m = JSON.parse(fs.readFileSync(path.join(dir, 'annotations.json'), 'utf8')); } catch (_) {}
      const out = {}; ids.forEach(i => { if (m[i] && m[i].items && m[i].items.length) out[i] = { n: m[i].items.length }; });
      return out;
    },
    imgPersistent: false
  };
}
function parseImgs(s) {
  try { const a = typeof s === 'string' ? JSON.parse(s) : s; return Array.isArray(a) ? a.filter(x => /^[a-f0-9]{32}$/.test(x)).slice(0, 8) : []; }
  catch (_) { return []; }
}

// ── ARCHIVO DE DEALS ──────────────────────────────────────────────────────────
// Se acumulan por (cuenta, ticket) (inmutables) para no perder historial. No modifica accountData ni el snapshot: solo completa
// la lista de deals que se sirve en /data. Un deal solo entra al Map DESPUÉS de guardarse en la base (el acuse refleja lo guardado).
const dealArchive = new Map();    // acct → Map(ticket → deal)
let outByLogged = 0;
const dealSyncCache = new Map();  // acct → { frontier, updated } (marca de recuperación verificada por el servidor)
let ingestChain = Promise.resolve();
function archMap(acct) { let m = dealArchive.get(acct); if (!m) { m = new Map(); dealArchive.set(acct, m); } return m; }
async function getDealSync(acct) {
  let st = dealSyncCache.get(acct);
  if (!st) {
    let v = null; try { v = await journalStore.kvGet('deal_sync:' + acct); } catch (_) {}
    st = { frontier: (v && Number.isFinite(+v.frontier)) ? +v.frontier : 0, updated: (v && v.updated) || '' };
    dealSyncCache.set(acct, st);
  }
  return st;
}
// Devuelve { fresh, duplicate, rejected[], dbOk }. Los duplicados no vuelven a escribirse ni a contabilizarse.
function ingestDeals(acct, deals) {
  const job = ingestChain.then(async () => {
    const m = archMap(acct);
    const c = syncCore.classifyDeals(m, deals);
    let dbOk = true;
    if (c.fresh.length) {
      try { await journalStore.dealsAdd(acct, c.fresh); c.fresh.forEach(x => m.set(x[0], x[1])); }
      catch (e) { dbOk = false; console.error('❌ No se pudo archivar deals (la EA no avanzará su marca; se reenviarán):', e.message); }
    }
    return { fresh: dbOk ? c.fresh.length : 0, duplicate: c.duplicate, rejected: c.rejected, dbOk, pending: dbOk ? 0 : c.fresh.length };
  });
  ingestChain = job.catch(() => {});
  return job;
}
function mergedDeals() {
  const snap = (accountData && accountData.deals) || [];
  const arch = archMap(curAcctKey());
  if (!arch.size) return snap;
  const m = new Map(arch), extra = [];
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

// ── ANOTACIONES (dibujos sobre las imágenes del journal) ──────────────────────
// Un registro por imagen (id de 32 hex): { items, saved }. Los dibujos se guardan en la nube y los lee cualquier aparato.
// Regla: queda la versión con `saved` más reciente (la última modificación hecha desde cualquier aparato, incluido "borrar todo").
// Un envío más antiguo que lo guardado no pisa nada (se responde ok con lo vigente); repetir el mismo `saved` es un reintento idempotente.
const ANN_MAX_ITEMS = 2000;
const ANN_MAX_BYTES = 2 * 1024 * 1024;
app.put('/annotations/:id', async (req, res) => {
  if (!checkToken(req, res)) return;
  const id = req.params.id;
  if (!/^[a-f0-9]{32}$/.test(id)) return res.status(400).json({ error: 'Id inválido' });
  const b = req.body || {};
  if (!Array.isArray(b.items) || b.items.length > ANN_MAX_ITEMS || !b.items.every(x => x && typeof x === 'object' && !Array.isArray(x)))
    return res.status(400).json({ error: 'Anotaciones inválidas' });
  if (JSON.stringify(b.items).length > ANN_MAX_BYTES) return res.status(413).json({ error: 'Las anotaciones son demasiado grandes.' });
  let saved = String(b.saved || '').slice(0, 40);
  if (!Number.isFinite(Date.parse(saved))) saved = new Date().toISOString();
  try {
    const current = await journalStore.annPut(id, b.items, saved);
    res.setHeader('Cache-Control', 'no-store');
    res.json({ ok: true, annotation: current || null });
  } catch (e) { console.error(e); res.status(500).json({ error: 'Error guardando anotaciones' }); }
});

app.get('/annotations/:id', async (req, res) => {
  if (!checkToken(req, res)) return;
  const id = req.params.id;
  if (!/^[a-f0-9]{32}$/.test(id)) return res.status(400).json({ error: 'Id inválido' });
  try {
    res.setHeader('Cache-Control', 'no-store');
    res.json({ annotation: (await journalStore.annGet(id)) || null });
  } catch (e) { console.error(e); res.status(500).json({ error: 'Error leyendo anotaciones' }); }
});

// Cantidad de dibujos por imagen (distintivo ✏️ en las miniaturas): /annotations?ids=id1,id2,...
app.get('/annotations', async (req, res) => {
  if (!checkToken(req, res)) return;
  const ids = String(req.query.ids || '').split(',').filter(x => /^[a-f0-9]{32}$/.test(x)).slice(0, 200);
  try {
    res.setHeader('Cache-Control', 'no-store');
    res.json({ annotations: ids.length ? await journalStore.annCounts(ids) : {} });
  } catch (e) { console.error(e); res.status(500).json({ error: 'Error leyendo anotaciones' }); }
});

app.get('/journal-config', (req, res) => {
  if (!checkToken(req, res)) return;
  res.json({ images: true, persistent: !!journalStore.imgPersistent, maxBytes: IMG_MAX_BYTES, maxPerDay: IMG_MAX_PER_DAY });
});


// ── CAPTURA DE GRÁFICOS DE MT5 (a pedido desde el Journal) ───────────────────
// Flujo: el Journal pide una captura (/capture/request) → la EA la recoge consultando /capture/poll → hace ChartScreenShot
// (calidad nativa de MT5) → la sube a /capture/upload, que la guarda y la agrega al journal del día → el Journal consulta /capture/status.
// Para no mantener el servidor despierto sin motivo, la EA consulta cada 20 s y cada 1 s mientras el Journal esté "armado" (abierto o con una solicitud reciente).
const capJobs = new Map();
let capArmedUntil = 0, capLastPoll = 0;
const CAP_TTL_MS = 60000;

async function attachImageToEntry(day, imgId) {
  const all = await journalStore.all();
  const cur = all[day] || { tag: '', note: '', img: '', imgs: [] };
  let saved = new Date().toISOString();
  const prev = Date.parse(cur.saved || '');
  if (Number.isFinite(prev) && prev >= Date.parse(saved)) saved = new Date(prev + 1).toISOString();
  await journalStore.put(day, { tag: cur.tag || '', note: cur.note || '', img: cur.img || '', imgs: parseImgs([...(cur.imgs || []), imgId]), saved });
}

app.post('/capture/arm', (req, res) => {
  if (!checkToken(req, res)) return;
  capArmedUntil = Date.now() + 5 * 60000;
  res.json({ ok: true });
});

app.post('/capture/request', (req, res) => {
  if (!checkToken(req, res)) return;
  const b = req.body || {};
  const day = String(b.day || '');
  if (!JKEY_RE.test(day)) return res.status(400).json({ error: 'Fecha inválida' });
  const ref = String(b.ref || '').replace(/[^0-9A-Za-z_-]/g, '').slice(0, 30);
  const now = Date.now();
  for (const [k, j] of capJobs) if (now - j.t > 10 * 60000) capJobs.delete(k);
  const id = crypto.randomBytes(6).toString('hex');
  capJobs.set(id, { id, day, ref, t: now, state: 'waiting', imgs: [], error: '' });
  capArmedUntil = now + 5 * 60000;
  res.json({ ok: true, id, eaSeen: now - capLastPoll < 60000 });
});

// La EA pregunta si hay una solicitud pendiente (entrega cada solicitud una sola vez)
app.get('/capture/poll', (req, res) => {
  if (!checkToken(req, res)) return;
  const now = Date.now();
  capLastPoll = now;
  let job = null;
  for (const j of capJobs.values()) {
    if (j.state !== 'waiting') continue;
    if (now - j.t > CAP_TTL_MS) { j.state = 'failed'; j.error = 'Caducó: la EA no la recogió a tiempo.'; continue; }
    job = j; break;
  }
  if (job) job.state = 'taken';
  res.json({ job: job ? { id: job.id, day: job.day, ref: job.ref } : null, fast: !!job || now < capArmedUntil });
});

app.post('/capture/upload', (req, res, next) => {
  if (!checkToken(req, res)) return;
  next();
}, rawImage, async (req, res) => {
  const job = capJobs.get(String(req.query.id || ''));
  if (!job || job.state !== 'taken') return res.status(404).json({ error: 'Solicitud de captura no encontrada o caducada.' });
  const buf = req.body;
  if (!Buffer.isBuffer(buf) || !buf.length) return res.status(400).json({ error: 'No se recibió ninguna imagen.' });
  if (buf.length > IMG_MAX_BYTES) return res.status(413).json({ error: 'La imagen supera 5 MB.' });
  const mime = sniffImage(buf);
  if (!mime) return res.status(415).json({ error: 'Formato no permitido.' });
  try {
    if ((await journalStore.imgCountDay(job.day)) >= IMG_MAX_PER_DAY) return res.status(409).json({ error: `Máximo ${IMG_MAX_PER_DAY} imágenes por día.` });
    const id = crypto.randomBytes(16).toString('hex');
    await journalStore.imgPut(id, job.day, job.ref, mime, buf);
    await attachImageToEntry(job.day, id);
    job.imgs.push({ id, ref: job.ref });
    notifyJournal(job.day);
    res.json({ ok: true, id });
  } catch (e) { console.error(e); res.status(500).json({ error: 'No se pudo guardar la imagen.' }); }
});

app.post('/capture/done', (req, res) => {
  if (!checkToken(req, res)) return;
  const j = capJobs.get(String((req.body || {}).id || ''));
  if (j) { j.state = j.imgs.length ? 'done' : 'failed'; if (!j.imgs.length && !j.error) j.error = 'La EA no subió ninguna imagen.'; }
  res.json({ ok: true });
});

app.post('/capture/fail', (req, res) => {
  if (!checkToken(req, res)) return;
  const b = req.body || {};
  const j = capJobs.get(String(b.id || ''));
  if (j) { j.state = 'failed'; j.error = String(b.error || 'La EA no pudo capturar el gráfico.').slice(0, 300); }
  res.json({ ok: true });
});

app.get('/capture/status', (req, res) => {
  if (!checkToken(req, res)) return;
  const j = capJobs.get(String(req.query.id || ''));
  if (!j) return res.status(404).json({ error: 'Solicitud no encontrada.' });
  if (j.state === 'waiting' && Date.now() - j.t > CAP_TTL_MS) { j.state = 'failed'; j.error = 'La EA no respondió. Comprueba que MT5 esté abierto y la EA actualizada.'; }
  res.json({ state: j.state, imgs: j.imgs, error: j.error });
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
    // Archivo de deals por cuenta. Modo v2 (clave compuesta) solo si ya se migró (kv 'deals_migration'.ok); la migración
    // solo se ejecuta con TL_MIGRATE=1 y NUNCA borra la tabla heredada.
    const acct0 = accountData && accountData.account && accountData.account.number != null ? String(accountData.account.number) : null;
    try {
      let mig = await journalStore.kvGet('deals_migration');
      if (DATABASE_URL && journalStore.migrateDealsV2 && process.env.TL_MIGRATE === '1' && !(mig && mig.ok)) {
        if (!acct0) console.error('⛔ TL_MIGRATE=1 pero no hay snapshot con cuenta: migración de tl_deals NO ejecutada.');
        else {
          const rep = await journalStore.migrateDealsV2(acct0);
          console.log('🧭 Migración tl_deals → tl_deals_v2:', JSON.stringify(rep));
          if (rep.ok) { mig = { ok: true, at: new Date().toISOString(), acct: acct0, ...rep }; await journalStore.kvSet('deals_migration', mig); }
          else console.error('⛔ Migración incompleta: se sigue usando la tabla heredada tl_deals.');
        }
      }
      // Siempre v2 (cuenta+ticket): la tabla heredada tl_deals (sin cuenta, posible mezcla real/demo) NO se lee ni se modifica; el archivo por cuenta empieza limpio y se llena con los deals que envía la EA.
      if (DATABASE_URL) journalStore.dealsV2 = true;
      console.log(`🗃️  Archivo de deals: modo ${journalStore.dealsV2 ? 'v2 (cuenta+ticket)' : 'heredado (solo ticket — sin aislamiento entre cuentas; ver MIGRATION.md)'}`);
      const rows = await journalStore.dealsAll(), legacyItems = [];
      for (const [a, k, d] of rows) {
        const key = a === '' ? (acct0 || '0') : a;
        archMap(key).set(k, d);
        if (a === '' && journalStore.dealsV2 && !DATABASE_URL) legacyItems.push([k, d]);   // modo archivo: copia el heredado a deals_v2.json (sin tocar deals.json)
      }
      if (legacyItems.length) await journalStore.dealsAdd(acct0 || '0', legacyItems);
      let tot = 0; for (const m of dealArchive.values()) tot += m.size;
      console.log(`🗃️  Archivo de deals: ${tot} (${dealArchive.size} cuenta/s)`);
    } catch (e) { console.error('❌ Error cargando archivo de deals:', e.message); }
    if (accountData) await ingestDeals(acct0 || '0', accountData.deals).catch(() => {});   // siembra el archivo con el último snapshot guardado
    // Estado SWING persistido: se recupera tal cual (mismos ids, notas, contadores). Se espeja bajo swing_state:<cuenta> (idempotente)
    // para poder volver a esta cuenta tras un cambio. Después: REPLAY del último snapshot (con su UTC original, sin actividad nueva)
    // y REEVALUACIÓN con el reloj del servidor (la sesión puede haber cambiado mientras el servidor estaba apagado).
    swingState = await journalStore.kvGet('swing_state');
    if (swingState && swingState.account) {
      try { if (!(await journalStore.kvGet('swing_state:' + swingState.account))) await syncCore.archiveAccount(swingKv, swingState.account, swingState, accountData ? { data: accountData, lastUpdate } : null); }
      catch (e) { console.error('⚠️  No se pudo espejar el estado por cuenta:', e.message); }
    }
    if (accountData) { swingUpdate(accountData, 'replay'); maybeReevaluate(); }
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
    const tx = exits[exits.length - 1].time;
    if ((swingState && swingState.tm ? swingEngine.sessionKeyAt(swingState, tx, off) : swingEngine.sessionKey(tx, off)) !== today) continue;
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
    features: { mt5_capture: true, journal_images: true, annotations: true, images_persistent: !!journalStore.imgPersistent, price_excursions: true, swing_cycles: true, push: true },
    swing_cycles: swingView ? swingView.stats : null,
    swing_time: swingView ? timeInfo() : null,
    price_exc_count: accountData?.price_exc ? Object.keys(accountData.price_exc).length : 0,
    version: '1.5.0'
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
// Reevaluación periódica (sin depender de que MT5 envíe nada): comprueba el cruce de sesión NY con el reloj del servidor
setInterval(() => { const v = swingVersion; if (maybeReevaluate() && swingVersion !== v) notifyClients('reevaluacion'); }, 15000).unref();
loadState().finally(() => app.listen(PORT, () => console.log(`🚀 TradeLens Server corriendo en puerto ${PORT}`)));
