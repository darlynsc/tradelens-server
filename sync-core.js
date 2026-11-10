// TradeLens — núcleo puro de sincronización (sin I/O de red ni de base de datos; el almacenamiento se inyecta).
// Contiene: clasificación de "seq", validación/ingesta de deals por cuenta+ticket, verificación de ventanas de
// recuperación (conteo + suma de tickets) y archivo/restauración del estado Swing al cambiar de cuenta.
'use strict';

const CFG = { SEQ_FUTURE_MAX_MS: 6 * 3600 * 1000 };
const MOD = 2147483647;

// Contribución de un ticket a la suma de verificación (idéntica a la de la EA: ticket % 2147483647).
function ticketMod(t) {
  const s = String(t);
  if (!/^\d{1,20}$/.test(s)) return null;
  return s.length <= 15 ? Number(s) % MOD : Number(BigInt(s) % BigInt(MOD));
}

// ── seq ────────────────────────────────────────────────────────────────────────────────────────────────
// none = EA antigua sin seq (se procesa); accepted | duplicate | stale | future
function classifySeq(inSeq, lastSeq, nowMs, futureMaxMs) {
  if (typeof inSeq !== 'number' || !Number.isFinite(inSeq)) return { status: 'none' };
  if (inSeq > nowMs + (futureMaxMs == null ? CFG.SEQ_FUTURE_MAX_MS : futureMaxMs)) return { status: 'future' };
  if (lastSeq == null) return { status: 'accepted' };
  if (inSeq < lastSeq) return { status: 'stale' };
  if (inSeq === lastSeq) return { status: 'duplicate' };
  return { status: 'accepted' };
}

// ── deals ──────────────────────────────────────────────────────────────────────────────────────────────
function validDeal(d) {
  return !!d && typeof d === 'object' && ticketMod(d.ticket) !== null && typeof d.time === 'number' && Number.isFinite(d.time) && d.time >= 0;
}
// Clasifica un lote contra el archivo en memoria de UNA cuenta (Map ticket→deal). No modifica el Map.
// Devuelve { fresh:[[ticket,deal]], duplicate:n, rejected:[ticket...] }. Un ticket repetido dentro del lote cuenta como duplicado.
function classifyDeals(map, deals) {
  const fresh = [], rejected = [], seen = new Set();
  let duplicate = 0;
  for (const d of (deals || [])) {
    if (!validDeal(d)) { rejected.push(String(d && d.ticket)); continue; }
    const k = String(d.ticket);
    if (map.has(k) || seen.has(k)) { duplicate++; continue; }
    seen.add(k); fresh.push([k, d]);
  }
  return { fresh, duplicate, rejected };
}
// Conteo y suma de tickets de los deals con tiempo en (from, to]
function aggregates(map, from, to) {
  let n = 0, tsum = 0;
  for (const [k, d] of map) {
    const t = +d.time;
    if (t > from && t <= to) { n++; tsum += ticketMod(k) || 0; }
  }
  return { n, tsum };
}
// Verifica una ventana (from, to] enviada por la EA contra el archivo del servidor.
// La marca (frontier) solo avanza si: la ventana es contigua (from ≤ frontier) y el servidor posee EXACTAMENTE
// los mismos deals que la EA (mismo conteo y misma suma de tickets). Un hueco, un ticket ausente o un lote incompleto
// producen 'mismatch' y la marca NO avanza. 'gap' devuelve la marca del servidor para que la EA retroceda.
function verifyWindow(map, frontier, ds) {
  const num = v => typeof v === 'number' && Number.isFinite(v);
  if (!ds || !num(ds.from) || !num(ds.to) || !num(ds.n) || !num(ds.tsum) || ds.to < ds.from)
    return { status: 'invalid', window_ok: false, frontier };
  if (ds.from > frontier) return { status: 'gap', window_ok: false, frontier };
  const w = aggregates(map, ds.from, ds.to);
  if (w.n !== ds.n || w.tsum !== ds.tsum)
    return { status: 'mismatch', window_ok: false, frontier, srv_n: w.n, ea_n: ds.n };
  let rescan = false;
  if (num(ds.all_n) && num(ds.all_tsum)) {
    const p = aggregates(map, 0, ds.to);
    if (p.n !== ds.all_n || p.tsum !== ds.all_tsum) rescan = true;   // algo anterior a la ventana no coincide: re-escaneo desde 0
  }
  return { status: 'ok', window_ok: true, frontier: Math.max(frontier, ds.to), rescan, srv_n: w.n, ea_n: ds.n };
}

// ── cambio de cuenta: archivar → verificar → restaurar o crear ─────────────────────────────────────────────
// kv = { get(k), set(k, v) } (async). Nunca escribe un estado vacío sobre uno válido: el estado anterior se archiva y se
// verifica por lectura antes de devolver el nuevo. Los contadores SWING del estado devuelto se elevan (nunca bajan) hasta el
// máximo de las demás cuentas para que ningún id SWING-AAAA-NNNNNN se repita entre cuentas (las notas del Journal usan el id).
async function counterFloor(kv, excludeAcct, extraStates) {
  const idx = (await kv.get('swing_accounts')) || [];
  const floor = {};
  const take = st => { if (st && st.counters) for (const [y, n] of Object.entries(st.counters)) floor[y] = Math.max(floor[y] || 0, +n || 0); };
  for (const a of idx) if (a !== excludeAcct) take(await kv.get('swing_state:' + a));
  for (const st of (extraStates || [])) take(st);
  return floor;
}
function raiseCounters(state, floor) {
  state.counters = state.counters || {};
  for (const [y, n] of Object.entries(floor)) if ((state.counters[y] || 0) < n) state.counters[y] = n;
}
async function archiveAccount(kv, acct, state, snapshotBlob) {
  if (!acct || !state) return false;
  await kv.set('swing_state:' + acct, state);
  const back = await kv.get('swing_state:' + acct);
  if (!back || JSON.stringify(back) !== JSON.stringify(state)) throw new Error('archivo_no_verificado:' + acct);
  if (snapshotBlob) await kv.set('snapshot:' + acct, snapshotBlob);
  const idx = (await kv.get('swing_accounts')) || [];
  if (!idx.includes(acct)) { idx.push(acct); await kv.set('swing_accounts', idx); }
  return true;
}
// o = { state, snapshotBlob, oldAcct, newAcct, engine }
async function switchAccountState(kv, o) {
  if (o.state && o.oldAcct) await archiveAccount(kv, o.oldAcct, o.state, o.snapshotBlob);
  let next = await kv.get('swing_state:' + o.newAcct), restored = !!next;
  const floor = await counterFloor(kv, o.newAcct, o.state && o.oldAcct !== o.newAcct ? [o.state] : []);
  if (!next) next = o.engine.emptyState(o.newAcct, floor);
  else raiseCounters(next, floor);
  return { state: next, restored, floor };
}

module.exports = { CFG, MOD, ticketMod, classifySeq, validDeal, classifyDeals, aggregates, verifyWindow,
  counterFloor, raiseCounters, archiveAccount, switchAccountState };
