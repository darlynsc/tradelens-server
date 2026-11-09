// TradeLens — Motor de ciclos SWING (módulo puro, sin I/O)
//
// Entrada : snapshot completo de la EA (deals, positions, timestamp, account) + estado persistido.
// Salida  : estado persistido actualizado + vista derivada (ciclos, sesiones, resultados).
//
// PRINCIPIOS
//  · Los datos originales de MT5 (accountData) NUNCA se modifican: aquí solo se DERIVA una estructura aparte.
//  · Determinista e idempotente: repetir el mismo snapshot produce exactamente el mismo resultado.
//  · Continuidad de un ciclo: mientras exista ≥1 posición del ciclo abierta, toda posición nueva del mismo
//    símbolo y dirección se incorpora. Cuando todas quedan cerradas, el ciclo termina y no admite más.
//  · No se inventan relaciones: lo que no se puede verificar se marca (flags / ambigüedades), no se une.
//  · Fechas = SESIÓN OPERATIVA DE NUEVA YORK: la sesión cambia a las 17:00 hora de Nueva York (con horario de verano
//    de EE. UU.). Los deals llevan la hora del servidor del bróker codificada como epoch; se pasa a UTC restando el
//    offset del bróker (TimeCurrent − TimeGMT, que envía la EA) y de UTC a Nueva York. Clave de sesión = fecha NY de
//    (hora NY + 7 h). Una posición es SWING si abarca más de una sesión NY.
//
// REFERENCIA TEMPORAL (esta versión; ver TIME_NOTES al final):
//  · nowDay ya NO sale de snapshot.timestamp (hora del bróker, congelada sin cotizaciones) sino de resolveNow(): UTC del
//    servidor (+ UTC observado por la EA como evidencia) → sesión NY. Tres modos: 'fresh' (snapshot nuevo), 'replay'
//    (snapshot recuperado del almacenamiento al arrancar) y 'server_reevaluation' (el servidor avanza el reloj sin snapshot).
//  · La conversión bróker→UTC usa un historial de regímenes de offset (offsetAt). El offset heredado se captura UNA vez en la
//    migración y queda congelado para todo lo anterior al primer régimen validado. NO hay reconstrucción retroactiva.
//  · Un ciclo con id SWING nunca se degrada. Ids nuevos solo con confianza HIGH/MEDIUM.
'use strict';

const EPS = 1e-7;
const VERSION = 1;          // NO incrementar: un cambio de VERSION reinicializa el estado (ver update()). La ampliación es aditiva (state.tm).
const TM_SCHEMA = 1;        // versión del bloque temporal aditivo state.tm
const MAX_SESSION_LOG = 90;
// Parámetros temporales centralizados (configurables con setConfig; el servidor puede sobrescribirlos).
const CFG = {
  QUOTE_MAX_AGE: 120,        // s: antigüedad máxima de la última cotización para aceptar una observación de offset
  OFFSET_STEP: 900,          // el offset debe ser múltiplo de 15 min
  OFFSET_MAX: 14 * 3600,     // |offset| máximo
  OFFSET_XCHECK_TOL: 600,    // s: coherencia offset ↔ reloj del servidor (450 de redondeo a 900 + latencia/deriva)
  CLOCK_TOL: 120,            // s: tolerancia entre el UTC de la EA y el reloj del servidor
  CONFIRM_N: 2,              // observaciones coherentes y distintas para confirmar un cambio de offset
  STALE_SECS: 180,           // s: pasado este tiempo sin snapshot el estado se considera potencialmente desactualizado
  MAX_REEVAL_AGE: 48 * 3600, // s: con un snapshot más antiguo que esto la reevaluación pasa a confianza LOW
  PERSIST_EVERY: 60          // s: frecuencia máxima con que los campos temporales rutinarios marcan el estado como "changed"
};
function setConfig(o) { Object.assign(CFG, o || {}); }

const dayKey = t => new Date(t * 1000).toISOString().slice(0, 10);   // fecha de un epoch ya convertido
// ── Sesión de Nueva York ──────────────────────────────────────────────────────────────────────────────────
function nthSunday(y, m, n) { const first = 1 + ((7 - new Date(Date.UTC(y, m, 1)).getUTCDay()) % 7); return first + 7 * (n - 1); }
// Offset NY respecto a UTC: DST de EE. UU. = 2.º domingo de marzo 07:00 UTC → 1.er domingo de noviembre 06:00 UTC
function nyOffsetSec(utc) {
  const y = new Date(utc * 1000).getUTCFullYear();
  const s = Date.UTC(y, 2, nthSunday(y, 2, 2), 7) / 1000, e = Date.UTC(y, 10, nthSunday(y, 10, 1), 6) / 1000;
  return (utc >= s && utc < e) ? -4 * 3600 : -5 * 3600;
}
// t = epoch con hora del servidor del bróker; off = offset del bróker en segundos (hora servidor − UTC)
// Clave de sesión de un instante UTC (única implementación; el dashboard usa la misma fórmula)
function nySessionKeyUtc(u) { return new Date((u + nyOffsetSec(u) + 7 * 3600) * 1000).toISOString().slice(0, 10); }
function sessionKey(t, off) { return nySessionKeyUtc(t - (off || 0)); }
const n0 = v => (typeof v === 'number' && Number.isFinite(v)) ? v : 0;
const r2 = v => { const x = Math.round((v + Number.EPSILON) * 100) / 100; return x === 0 ? 0 : x; };
const tk = v => { const x = Number(v); return Number.isFinite(x) ? x : 0; };

// Orden real de ejecución: tiempo → milisegundos (si existen) → ticket (crece con la ejecución en MT5).
function cmpOrd(a, b) {
  if (a.t !== b.t) return a.t - b.t;
  const am = a.msc == null ? null : a.msc, bm = b.msc == null ? null : b.msc;
  if (am != null && bm != null && am !== bm) return am - bm;
  if (a.ticket !== b.ticket) return a.ticket - b.ticket;
  return 0;
}
// ¿El orden entre dos eventos solo se pudo decidir por ticket dentro del mismo segundo sin ms?
function weakOrder(a, b) {
  return a.t === b.t && !(a.msc != null && b.msc != null && a.msc !== b.msc);
}

function emptyState(account, counterFloor) {
  const s = { v: VERSION, account: account || null, records: {}, ids: {}, counters: {}, sessionLog: {}, ambiguities: {} };
  if (counterFloor) for (const [y, n] of Object.entries(counterFloor)) s.counters[y] = n;   // ids únicos entre cuentas
  migrate(s);
  return s;
}

// ── MIGRACIÓN ADITIVA E IDEMPOTENTE ───────────────────────────────────────────────────────────────────────
// Solo AÑADE campos ausentes (state.tm, state.dealMeta, state.swingMeta). No toca records, ids, counters, sessionLog,
// ambiguities ni campos desconocidos. El offset heredado se captura UNA sola vez (la primera migración) y queda congelado.
function migrate(state, opts) {
  let ch = false;
  if (!state.tm || typeof state.tm !== 'object') { state.tm = {}; ch = true; }
  const tm = state.tm;
  if (tm.schema !== TM_SCHEMA) { tm.schema = TM_SCHEMA; ch = true; }
  if (!tm.offsets || typeof tm.offsets !== 'object') {
    const bo = (typeof state.brokerOffset === 'number' && Number.isFinite(state.brokerOffset)) ? state.brokerOffset : null;
    tm.offsets = { legacy: { off: bo, frozen: true, uncertain: true, verifiedUtc: null,
                             reason: bo == null ? 'sin_offset_persistido' : 'no_verificado', capturedUtc: (opts && opts.nowUtc) || null },
                   regimes: [], pending: null, lastValid: null };
    ch = true;
  }
  if (!Array.isArray(tm.offsets.regimes)) { tm.offsets.regimes = []; ch = true; }
  if (typeof tm.maxUtc !== 'number') { tm.maxUtc = 0; ch = true; }
  if (typeof tm.maxDay !== 'string') { tm.maxDay = ''; ch = true; }
  if (typeof tm.persistedAt !== 'number') { tm.persistedAt = 0; ch = true; }
  if (tm.lastSnap === undefined) { tm.lastSnap = null; ch = true; }
  if (!state.dealMeta || typeof state.dealMeta !== 'object') { state.dealMeta = {}; ch = true; }
  if (!state.swingMeta || typeof state.swingMeta !== 'object') { state.swingMeta = {}; ch = true; }
  return ch;
}

// ── REGÍMENES DE OFFSET ───────────────────────────────────────────────────────────────────────────────────
function curOffset(state) {
  const o = state.tm.offsets;
  return o.regimes.length ? o.regimes[o.regimes.length - 1].off : o.legacy.off;
}
// Offset (s) aplicable a un instante de hora de bróker. Antes del primer régimen validado → offset heredado congelado.
// Sin heredado ni regímenes → estimación `est` (comportamiento anterior) o 0.
function offsetAt(state, tBroker, est) {
  const o = state.tm.offsets;
  for (let i = o.regimes.length - 1; i >= 0; i--) if (o.regimes[i].fromBroker <= tBroker) return o.regimes[i].off;
  if (o.legacy.off != null) return o.legacy.off;
  return est != null ? est : 0;
}
function sessionKeyAt(state, tBroker, est) { return sessionKey(tBroker, offsetAt(state, tBroker, est)); }
// Tabla compacta para el dashboard (misma regla que offsetAt)
function offsetTable(state) {
  const o = state.tm.offsets;
  return { legacy: { off: o.legacy.off, uncertain: !!o.legacy.uncertain, reason: o.legacy.reason || null },
           regimes: o.regimes.map(r => ({ from: r.fromBroker, off: r.off, conf: r.conf, validated: !!r.validated })) };
}

// Observación de offset: SOLO es fiable con los campos nuevos de la EA (quote_age, utc_obs, connected) y cotización reciente.
function offsetObservation(snap, serverUtc) {
  const s = snap && snap.session;
  if (!s) return { valid: false, reason: 'sin_session' };
  const off = s.broker_offset;
  if (typeof off !== 'number' || !Number.isFinite(off)) return { valid: false, reason: 'sin_offset' };
  if (typeof s.utc_obs !== 'number' || typeof s.quote_age !== 'number') return { valid: false, reason: 'ea_sin_campos_nuevos' };
  if (s.connected === false) return { valid: false, reason: 'terminal_desconectado' };
  if (s.quote_age < 0 || s.quote_age > CFG.QUOTE_MAX_AGE) return { valid: false, reason: 'cotizacion_no_fresca' };
  if (off % CFG.OFFSET_STEP !== 0) return { valid: false, reason: 'no_multiplo_900' };
  if (Math.abs(off) > CFG.OFFSET_MAX) return { valid: false, reason: 'fuera_de_rango' };
  const bt = n0(snap.timestamp);
  // La hora real del bróker ≈ TimeCurrent + antigüedad de la cotización; contra el reloj INDEPENDIENTE del servidor
  // debe dar el offset (descarta reloj del PC desfasado u hora congelada).
  const diff = (bt + s.quote_age) - serverUtc - off;
  if (Math.abs(diff) > CFG.OFFSET_XCHECK_TOL) return { valid: false, reason: 'incoherente_con_reloj_servidor' };
  return { valid: true, off, obsBroker: bt, obsUtc: serverUtc, age: s.quote_age };
}
// Aplica una observación válida. Devuelve { changed, event }. Un cambio requiere CONFIRM_N observaciones coherentes y
// DISTINTAS (por seq); el nuevo régimen rige desde la hora de bróker de la observación que lo confirma (nunca hacia atrás).
function applyOffsetObservation(state, obs, seq, amb) {
  const o = state.tm.offsets, cur = curOffset(state);
  let changed = false, event = null;
  if (cur == null) {                       // nunca hubo offset: primer régimen directo
    // No hay offset heredado que preservar (estado nuevo / cuenta nueva): el primer offset validado rige desde el epoch (0; JSON no admite -Infinity).
    o.regimes.push({ off: obs.off, fromBroker: 0, obsUtc: obs.obsUtc, obsBroker: obs.obsBroker, src: 'observed', conf: 'HIGH', validated: true, n: 1, firstEver: true });
    o.pending = null; return { changed: true, event: 'first_regime' };
  }
  if (obs.off === cur) {
    if (o.pending) { o.pending = null; changed = true; }
    if (!o.regimes.length && o.legacy.uncertain) {     // el heredado coincide con una observación válida: deja de ser incierto
      o.legacy.uncertain = false; o.legacy.verifiedUtc = obs.obsUtc; o.legacy.reason = 'verificado'; changed = true; event = 'legacy_verified';
    }
    if (!o.lastValid || obs.obsUtc - o.lastValid.utc >= CFG.PERSIST_EVERY || o.lastValid.off !== obs.off) {
      o.lastValid = { off: obs.off, utc: obs.obsUtc, broker: obs.obsBroker }; changed = true;
    }
    return { changed, event };
  }
  // difiere del vigente
  if (!o.regimes.length && o.legacy.off != null && o.legacy.uncertain && amb)
    amb.push({ code: 'offset_heredado_discrepante', pids: [], msg: 'Una observación válida difiere del offset heredado (' + o.legacy.off + ' s vs ' + obs.off + ' s). Se conserva el heredado para lo anterior; no se reconstruye nada.' });
  if (o.pending && o.pending.off === obs.off) { if (o.pending.lastSeq !== seq || seq == null) { o.pending.n++; o.pending.lastSeq = seq; } }
  else o.pending = { off: obs.off, n: 1, firstBroker: obs.obsBroker, firstUtc: obs.obsUtc, lastSeq: seq };
  changed = true;
  if (o.pending.n >= CFG.CONFIRM_N) {
    const last = o.regimes.length ? o.regimes[o.regimes.length - 1].fromBroker : -Infinity;
    if (obs.obsBroker > last) {
      o.regimes.push({ off: obs.off, fromBroker: obs.obsBroker, obsUtc: obs.obsUtc, obsBroker: obs.obsBroker, src: 'observed', conf: 'HIGH', validated: true, n: o.pending.n, firstUtc: o.pending.firstUtc });
      event = 'new_regime';
    }
    o.pending = null;
  }
  return { changed, event };
}

// ── RESOLUCIÓN DE LA SESIÓN ACTUAL (única función central) ────────────────────────────────────────────────
// Confianza de una referencia de snapshot { srvUtc, eaUtc, sid }
function snapConf(ref) {
  if (!ref) return 'LOW';
  if (ref.conf) return ref.conf;
  if (typeof ref.eaUtc === 'number' && ref.eaUtc > 0) {
    if (Math.abs(ref.eaUtc - ref.srvUtc) > CFG.CLOCK_TOL) return 'MEDIUM';
    return (!ref.sid || ref.sid === nySessionKeyUtc(ref.srvUtc) || ref.sid === nySessionKeyUtc(ref.eaUtc)) ? 'HIGH' : 'MEDIUM';
  }
  // EA antigua (sin UTC propio): el session.id de la EA coincidiendo con el reloj del servidor es evidencia suficiente → MEDIUM
  return (ref.sid && ref.sid === nySessionKeyUtc(ref.srvUtc)) ? 'MEDIUM' : 'LOW';
}
// ctx = { mode, serverUtc, snapshot, lastSnapHint, nowT, legacyOff }
function resolveNow(state, ctx) {
  const tm = state.tm, mode = ctx.mode, srv = ctx.serverUtc, flags = [];
  let utc, conf, source, eaUtc = null, changed = false;
  const ls = tm.lastSnap || ctx.lastSnapHint || null;
  if (mode === 'fresh') {
    const s = ctx.snapshot && ctx.snapshot.session;
    eaUtc = (s && typeof s.utc_obs === 'number' && s.utc_obs > 0) ? s.utc_obs : null;
    const sid = s && s.id ? String(s.id).replace(/\./g, '-') : null;
    utc = srv;                                                    // el servidor es la autoridad
    if (eaUtc != null) {
      if (Math.abs(eaUtc - srv) <= CFG.CLOCK_TOL) { source = 'server+ea'; conf = snapConf({ srvUtc: srv, eaUtc, sid }); if (conf === 'MEDIUM') flags.push('session_id_discrepante'); }
      else { source = 'server_clock'; conf = 'MEDIUM'; flags.push('reloj_ea_sospechoso'); }
    } else { source = 'server_fallback'; flags.push('ea_sin_utc'); conf = snapConf({ srvUtc: srv, eaUtc: null, sid }); }
    tm.lastSnap = { srvUtc: srv, eaUtc, sid, conf, seq: (ctx.snapshot && typeof ctx.snapshot.seq === 'number') ? ctx.snapshot.seq : null, brokerTs: ctx.nowT };
    changed = true;
  } else if (mode === 'replay') {
    if (ls) { utc = ls.srvUtc; conf = snapConf(ls); source = 'replay'; if (!ls.srvUtc) { utc = tm.maxUtc; conf = 'LOW'; } }
    else { utc = ctx.nowT - (ctx.legacyOff || 0); conf = 'LOW'; source = 'legacy_broker_time'; flags.push('sin_referencia_previa'); }
  } else {                                                        // server_reevaluation
    utc = srv; source = 'server_clock';
    if (!ls) { conf = 'LOW'; flags.push('sin_snapshot_previo'); }
    else if (srv - ls.srvUtc > CFG.MAX_REEVAL_AGE) { conf = 'LOW'; flags.push('snapshot_muy_antiguo'); }
    else conf = snapConf(ls) === 'LOW' ? 'LOW' : 'MEDIUM';        // sin snapshot nuevo nunca es HIGH
  }
  // Monotonicidad: una observación vieja no hace retroceder el reloj ni la sesión
  if (utc < tm.maxUtc) { utc = tm.maxUtc; flags.push('utc_retroceso_ignorado'); }
  let day = nySessionKeyUtc(utc);
  if (tm.maxDay && day < tm.maxDay) { day = tm.maxDay; flags.push('sesion_retroceso_ignorada'); }
  if (utc > tm.maxUtc) { tm.maxUtc = utc; if (utc - tm.persistedAt >= CFG.PERSIST_EVERY) { tm.persistedAt = utc; changed = true; } }
  if (day > tm.maxDay) { tm.maxDay = day; changed = true; }
  const snapAge = ls && ls.srvUtc ? Math.max(0, Math.round(srv - ls.srvUtc)) : null;
  return { utc, day, conf, source, flags, mode, eaUtc, snapAge, changed, lastSnapUtc: ls ? ls.srvUtc : null };
}

function normDeal(d) {
  if (!d || d.ticket == null || d.position_id == null) return null;
  if (d.entry !== 'IN' && d.entry !== 'OUT' && d.entry !== 'INOUT') return null;
  if (d.type !== 'BUY' && d.type !== 'SELL') return null;
  if (!d.symbol) return null;
  return {
    ticket: String(d.ticket), pid: String(d.position_id), sym: String(d.symbol), type: d.type, entry: d.entry,
    vol: n0(d.volume), price: n0(d.price), profit: n0(d.profit), comm: n0(d.commission), swap: n0(d.swap),
    t: n0(d.time), msc: (typeof d.time_msc === 'number' && Number.isFinite(d.time_msc)) ? d.time_msc : null
  };
}
const ordOfDeal = d => ({ t: d.t, msc: d.msc, ticket: tk(d.ticket) });

// ── 1) Incorporar deals al almacén de registros (acumula; mismo ticket = mismo deal, no se duplica) ──────────
function mergeDeals(state, deals, newDeals) {
  let changed = false;
  for (const raw of (deals || [])) {
    const d = normDeal(raw); if (!d) continue;
    const rec = state.records[d.pid] || (state.records[d.pid] = { pid: d.pid, deals: {} });
    const prev = rec.deals[d.ticket];
    if (!prev || JSON.stringify(prev) !== JSON.stringify(d)) { rec.deals[d.ticket] = d; changed = true; if (!prev && newDeals) newDeals.push(d); }
  }
  return changed;
}

// ── 2) Derivar el estado de cada posición a partir de sus deals + la lista de posiciones abiertas ─────────────
function deriveRecord(rec, op) {
  const ds = Object.values(rec.deals).sort((a, b) => cmpOrd(ordOfDeal(a), ordOfDeal(b)));
  const ins = ds.filter(d => d.entry === 'IN');
  const outs = ds.filter(d => d.entry === 'OUT' || d.entry === 'INOUT');
  const inVol = ins.reduce((a, d) => a + d.vol, 0);
  const outVol = outs.reduce((a, d) => a + d.vol, 0);
  const flags = [];
  if (ds.some(d => d.entry === 'INOUT')) flags.push('INOUT_no_verificable');
  const fromPositionOnly = ds.length === 0 && !!op;          // abierta, sin deals en el historial recibido
  const noOpenDeal = ins.length === 0;                        // sin deal de apertura (historial parcial)
  let sym = ds.length ? ds[0].sym : (op ? op.symbol : '');
  let side = ins.length ? ins[0].type : (outs.length ? (outs[0].type === 'SELL' ? 'BUY' : 'SELL') : (op ? op.side : ''));
  if (op && op.side) side = op.side;
  if (op && op.symbol) sym = op.symbol;
  const isOpen = !!op;
  let status;
  if (isOpen) status = outVol > EPS ? 'partial' : 'open';
  else if (outs.length && (noOpenDeal || outVol >= inVol - EPS)) status = 'closed';
  else status = 'indeterminate'; // ni listada como abierta ni cerrada por completo: dato inconsistente (retraso de sync)
  if (noOpenDeal) flags.push('sin_deal_apertura');
  let openOrd = null;
  if (ins.length) openOrd = ordOfDeal(ins[0]);
  else if (fromPositionOnly) openOrd = { t: n0(op.open_time), msc: null, ticket: tk(op.position_id != null ? op.position_id : op.ticket) };
  let closeOrd = null;
  if (status === 'closed') closeOrd = ordOfDeal(outs[outs.length - 1]);
  return { pid: rec.pid, sym, side, ds, ins, outs, inVol, outVol, status, flags, openOrd, closeOrd,
    noOpenDeal, fromPositionOnly, op: op || null };
}

// ── 3) Barrido de eventos: aquí vive la regla de continuidad ──────────────────────────────────────────────
function sweep(derived, state) {
  const amb = [];
  const chainable = derived.filter(r => r.sym && r.side && !r.noOpenDeal && r.openOrd);
  const loose = derived.filter(r => !chainable.includes(r));
  const events = [];
  for (const r of chainable) {
    events.push({ kind: 'OPEN', ord: r.openOrd, r });
    if (r.status === 'closed') events.push({ kind: 'CLOSE', ord: r.closeOrd, r });
  }
  events.sort((a, b) => {
    const c = cmpOrd(a.ord, b.ord);
    if (c !== 0) return c;
    if (a.kind !== b.kind) return a.kind === 'CLOSE' ? -1 : 1; // empate total: el cierre primero (no se mantiene un ciclo artificialmente)
    return 0;
  });
  // Empates solo resolubles por ticket dentro del mismo segundo: se resuelven (el ticket es el orden real) pero se registran.
  for (let i = 1; i < events.length; i++) {
    const a = events[i - 1], b = events[i];
    if (a.kind !== b.kind && a.r.sym === b.r.sym && a.r.side === b.r.side && weakOrder(a.ord, b.ord)) {
      const closeEv = a.kind === 'CLOSE' ? a : b, openEv = a.kind === 'OPEN' ? a : b;
      amb.push({ code: 'orden_mismo_segundo', pids: [closeEv.r.pid, openEv.r.pid],
        msg: 'Cierre y apertura en el mismo segundo sin milisegundos: orden decidido por ticket de ejecución.' });
    }
  }
  const active = new Map();   // sym|side → ciclo activo
  const cycles = [];
  const cycleOf = new Map();  // pid → ciclo
  for (const ev of events) {
    const r = ev.r, key = r.sym + '|' + r.side;
    if (ev.kind === 'OPEN') {
      let c = active.get(key);
      if (!c || c.openSet.size === 0) {
        c = { key, sym: r.sym, side: r.side, recs: [], openSet: new Set(), startOrd: ev.ord, endOrd: null, verifiable: true, flags: [] };
        active.set(key, c); cycles.push(c);
      }
      c.recs.push(r); c.openSet.add(r.pid); cycleOf.set(r.pid, c);
    } else {
      const c = cycleOf.get(r.pid);
      c.openSet.delete(r.pid);
      if (c.openSet.size === 0) { c.endOrd = ev.ord; active.delete(key); }   // fin inmediato: ya no admite incorporaciones
    }
  }
  // Posiciones sin deal de apertura (historial parcial): ciclo propio NO verificable; nunca se unen a una cadena.
  for (const r of loose) {
    if (!r.sym || !r.side) continue;
    const first = r.ds[0];
    const c = { key: r.sym + '|' + r.side, sym: r.sym, side: r.side, recs: [r], openSet: new Set(r.status === 'closed' ? [] : [r.pid]),
      startOrd: first ? ordOfDeal(first) : (r.openOrd || { t: 0, msc: null, ticket: 0 }), endOrd: r.status === 'closed' ? r.closeOrd : null,
      verifiable: false, flags: ['sin_deal_apertura'] };
    cycles.push(c); cycleOf.set(r.pid, c);
    amb.push({ code: 'sin_apertura', pids: [r.pid], msg: 'Posición sin deal de apertura en el historial recibido: se conserva sola, sin unirla a ningún ciclo.' });
  }
  for (const r of derived) if (r.status === 'indeterminate')
    amb.push({ code: 'estado_indeterminado', pids: [r.pid], msg: 'La posición no está abierta ni cerrada por completo en el snapshot (posible retraso de sincronización). Se trata como abierta hasta confirmar.' });
  cycles.sort((a, b) => cmpOrd(a.startOrd, b.startOrd));
  return { cycles, amb };
}

// ── 4) Resumen de un ciclo: posiciones, sesiones, resultados ──────────────────────────────────────────────
function summarize(c, nowT, nowDay, positionsByPid, sk, ctx) {
  const recs = c.recs.slice().sort((a, b) => cmpOrd(a.ds.length ? ordOfDeal(a.ds[0]) : a.openOrd, b.ds.length ? ordOfDeal(b.ds[0]) : b.openOrd));
  const finished = c.endOrd != null;
  const startT = c.startOrd.t, endT = finished ? c.endOrd.t : null;
  const startDay = sk(startT), endDay = finished ? sk(endT) : null;
  // Regla vigente (sin duración mínima): ciclo cerrado → sesión de apertura ≠ de cierre; ciclo abierto → apertura ≠ sesión actual.
  const natural = c.verifiable && startDay !== (finished ? endDay : nowDay);
  const hasId = !!(ctx && ctx.hasId), canAssign = ctx ? ctx.canAssign !== false : true;
  // Un ciclo con id no se degrada; uno nuevo solo se promueve con confianza suficiente (si no, queda provisional).
  const isSwing = hasId || (natural && canAssign);
  const provisional = natural && !isSwing;
  const noDegrade = hasId && !natural;

  // Eventos de deals con estado acumulado por posición
  const all = [];
  for (const r of recs) for (const d of r.ds) all.push({ d, r });
  all.sort((a, b) => cmpOrd(ordOfDeal(a.d), ordOfDeal(b.d)));
  const inSoFar = {}, outSoFar = {}, partials = {};
  let cycVol = 0;
  const events = [];
  for (const { d, r } of all) {
    const pid = r.pid;
    inSoFar[pid] = inSoFar[pid] || 0; outSoFar[pid] = outSoFar[pid] || 0;
    let kind;
    if (d.entry === 'IN') { kind = inSoFar[pid] <= EPS ? 'OPEN' : 'ADD'; inSoFar[pid] += d.vol; cycVol += d.vol; }
    else {
      outSoFar[pid] += d.vol; cycVol -= d.vol;
      const isFinal = r.status === 'closed' && d.ticket === r.outs[r.outs.length - 1].ticket;
      kind = isFinal ? 'CLOSE' : 'PARTIAL';
      if (kind === 'PARTIAL') partials[pid] = (partials[pid] || 0) + 1;
    }
    events.push({ t: d.t, day: sk(d.t), kind, pid, ticket: d.ticket, entry: d.entry, vol: r2(d.vol), price: d.price,
      profit: r2(d.profit), comm: r2(d.comm), swap: r2(d.swap), net: r2(d.profit + d.comm + d.swap),
      posRemaining: r2(Math.max(0, inSoFar[pid] - outSoFar[pid])), cycRemaining: r2(Math.max(0, cycVol)) });
  }

  // Posiciones
  const positions = recs.map(r => {
    const gross = r.ds.reduce((a, d) => a + d.profit, 0), comm = r.ds.reduce((a, d) => a + d.comm, 0), swap = r.ds.reduce((a, d) => a + d.swap, 0);
    const op = r.op;
    const volRemaining = r.status === 'closed' ? 0 : (op ? n0(op.volume) : Math.max(0, r.inVol - r.outVol));
    return { pid: r.pid, status: r.status, openT: r.openOrd ? r.openOrd.t : null, closeT: r.closeOrd ? r.closeOrd.t : null,
      volIn: r2(r.inVol), volOut: r2(r.outVol), volRemaining: r2(volRemaining), partials: partials[r.pid] || 0, deals: r.ds.length,
      gross: r2(gross), comm: r2(comm), swap: r2(swap), net: r2(gross + comm + swap), flags: r.flags };
  });
  const closedN = positions.filter(p => p.status === 'closed').length;
  const openN = positions.length - closedN;
  const partialOpenN = positions.filter(p => p.status === 'partial').length;

  // Resultado realizado (todos los deals del ciclo, una sola vez): DEAL_PROFIT es bruto; comisión y swap vienen aparte.
  const gross = all.reduce((a, x) => a + x.d.profit, 0), comm = all.reduce((a, x) => a + x.d.comm, 0), swap = all.reduce((a, x) => a + x.d.swap, 0);
  const realized = { gross: r2(gross), comm: r2(comm), swap: r2(swap), net: r2(gross + comm + swap) };

  // Flotante (no realizado): solo posiciones abiertas del ciclo, desde la lista de posiciones del snapshot
  let fProfit = 0, fSwap = 0, fKnown = true;
  for (const r of recs) if (r.status !== 'closed') { if (r.op) { fProfit += n0(r.op.profit); fSwap += n0(r.op.swap); } else fKnown = false; }
  const floating = finished ? { profit: 0, swap: 0, total: 0, known: true }
    : { profit: r2(fProfit), swap: r2(fSwap), total: r2(fProfit + fSwap), known: fKnown };

  // Actividad por sesión (solo días con actividad real). Cada deal pertenece al día en que ocurrió.
  const byDay = {};
  for (const e of events) (byDay[e.day] = byDay[e.day] || []).push(e);
  const sessions = Object.keys(byDay).sort().map(day => {
    const ev = byDay[day];
    const outs = ev.filter(e => e.entry !== 'IN');
    const gains = outs.reduce((a, e) => a + (e.profit + e.swap > 0 ? e.profit + e.swap : 0), 0);
    const losses = outs.reduce((a, e) => a + (e.profit + e.swap < 0 ? e.profit + e.swap : 0), 0);
    const sComm = ev.reduce((a, e) => a + e.comm, 0);
    const sSwap = ev.reduce((a, e) => a + e.swap, 0);
    const net = ev.reduce((a, e) => a + e.net, 0);
    const openedPids = [...new Set(ev.filter(e => e.kind === 'OPEN').map(e => e.pid))];
    const closedPids = [...new Set(ev.filter(e => e.kind === 'CLOSE').map(e => e.pid))];
    const partialPids = [...new Set(ev.filter(e => e.kind === 'PARTIAL').map(e => e.pid))];
    const last = ev[ev.length - 1];
    // posiciones que permanecen abiertas al terminar ese día
    const openAtEnd = recs.filter(r => {
      const o = r.openOrd ? sk(r.openOrd.t) : null;
      if (!o || o > day) return false;
      if (r.status === 'closed' && sk(r.closeOrd.t) <= day) return false;
      return true;
    }).map(r => r.pid);
    return { day, events: ev, opened: openedPids.length, closed: closedPids.length, partialPids,
      openedPids, closedPids, openAtEnd,
      volOpened: r2(ev.filter(e => e.entry === 'IN').reduce((a, e) => a + e.vol, 0)),
      volClosed: r2(outs.reduce((a, e) => a + e.vol, 0)),
      volRemaining: last.cycRemaining,
      gains: r2(gains), losses: r2(losses), comm: r2(sComm), swap: r2(sSwap), net: r2(net) };
  });

  const lastDay = finished ? endDay : nowDay;
  const spanDays = Math.round((Date.parse(lastDay) - Date.parse(startDay)) / 864e5) + 1;
  const flags = c.flags.slice();
  recs.forEach(r => r.flags.forEach(f => { if (!flags.includes(f)) flags.push(f); }));
  if (recs.some(r => r.status === 'indeterminate')) flags.push('estado_indeterminado');
  return {
    id: null, sym: c.sym, side: c.side, state: finished ? 'finished' : 'active', isSwing, verifiable: c.verifiable,
    provisional, noDegrade, lastKnownOpen: !finished && !!(ctx && ctx.mode && ctx.mode !== 'fresh'),
    startT, endT, startDay, endDay, lastDay, spanDays,
    pids: positions.map(p => p.pid), positions,
    counts: { total: positions.length, closed: closedN, open: openN, partialOpen: partialOpenN },
    realized, floating, sessions, flags: [...new Set(flags)],
    nowDay
  };
}

// ── 5) Actualización completa: snapshot → estado + vista ──────────────────────────────────────────────────
// opts.snapshot = cuerpo de /sync (ya validado por el servidor). Devuelve { state, view, changed }.
function update(prevState, snapshot, opts) {
  opts = opts || {};
  const mode = opts.mode || 'fresh';                         // 'fresh' | 'replay' | 'server_reevaluation'
  const fresh = mode === 'fresh';
  const serverUtc = typeof opts.serverNow === 'number' ? opts.serverNow : Date.now() / 1000;
  const acct = snapshot && snapshot.account ? String(snapshot.account.number) : null;
  // El servidor archiva el estado de la cuenta anterior ANTES de llamar aquí (sync-core.switchAccountState); esta rama es solo red de seguridad.
  let state = prevState && prevState.v === VERSION && (!acct || !prevState.account || prevState.account === acct) ? prevState : emptyState(acct, opts.counterFloor);
  let changed = state !== prevState;
  if (acct && state.account !== acct) { state.account = acct; changed = true; }
  if (migrate(state, { nowUtc: Math.floor(serverUtc) })) changed = true;

  // Replay y reevaluación NO incorporan datos: no hay deals nuevos ni registros nuevos (el servidor no inventa actividad).
  const newDeals = [];
  if (fresh) {
    if (mergeDeals(state, snapshot.deals, newDeals)) changed = true;
    if (mergeDeals(state, snapshot.recovery_deals, newDeals)) changed = true;
  }

  const openMap = {};
  for (const p of (snapshot.positions || [])) {
    const id = String(p.position_id != null ? p.position_id : p.ticket);
    openMap[id] = p;
    if (fresh && !state.records[id]) { state.records[id] = { pid: id, deals: {} }; changed = true; }
  }
  const nowT = n0(snapshot.timestamp) || Math.max(0, ...Object.values(state.records).flatMap(r => Object.values(r.deals).map(d => d.t)));
  const amb = [];

  // Offset: observación validada (solo snapshot nuevo). Sin observación fiable se conserva el último régimen validado/heredado.
  if (fresh) {
    const obs = offsetObservation(snapshot, serverUtc);
    if (obs.valid) {
      const r = applyOffsetObservation(state, obs, typeof snapshot.seq === 'number' ? snapshot.seq : null, amb);
      if (r.changed) changed = true;
    } else if (snapshot.session && typeof snapshot.session.broker_offset === 'number' && obs.reason !== 'ea_sin_campos_nuevos') {
      // observación descartada (cotización vieja, desconectado, incoherente…): se conserva el offset vigente; no es un cambio real
    }
  }
  const o0 = state.tm.offsets;
  const estOff = (o0.legacy.off == null && !o0.regimes.length) ? Math.round((nowT - serverUtc) / 900) * 900 : null;   // comportamiento anterior, solo sin offset alguno
  const off = (curOffset(state) != null) ? curOffset(state) : (estOff != null ? estOff : 0);
  const sk = t => sessionKeyAt(state, t, estOff);

  // Sesión actual: función central. nowDay deja de depender de snapshot.timestamp.
  const rn = resolveNow(state, { mode, serverUtc, snapshot, lastSnapHint: opts.lastSnapHint, nowT, legacyOff: off });
  if (rn.changed) changed = true;
  const nowDay = rn.day;
  const canAssign = mode !== 'replay' && rn.conf !== 'LOW';

  // Metadatos independientes por deal NUEVO (state.dealMeta; nunca dentro del deal, para que mergeDeals no los pise).
  // Estimación UTC solo si la ejecución está próxima a la recepción; los deals ya existentes no tienen meta (= heredados).
  if (fresh) for (const d of newDeals) {
    if (state.dealMeta[d.ticket]) continue;
    const near = rn.conf !== 'LOW' && Math.abs(d.t - nowT) <= 120;
    state.dealMeta[d.ticket] = { seenUtc: Math.floor(serverUtc), off: offsetAt(state, d.t, estOff), est: near ? Math.round(serverUtc - (nowT - d.t)) : null,
      conf: rn.conf, src: near ? 'recv' : 'late' };
    changed = true;
  }

  const derived = Object.values(state.records).map(rec => deriveRecord(rec, openMap[rec.pid]));
  const sw = sweep(derived, state);
  const cycles = sw.cycles; sw.amb.forEach(a => amb.push(a));
  if (snapshot.deals_truncated === true)
    amb.push({ code: 'historial_truncado', pids: [], msg: 'La EA envía solo los deals más recientes: ciclos anteriores a esa ventana pueden estar incompletos. Los ya conocidos se conservan.' });

  // Resumen + identificadores persistentes (solo los ciclos SWING reciben id; el id se ancla a sus posiciones)
  const out = [], pending = [];
  for (const c of cycles) {
    const hasId = c.recs.some(r => state.ids[r.pid]);
    const s = summarize(c, nowT, nowDay, openMap, sk, { hasId, canAssign, mode });
    if (s.noDegrade) amb.push({ code: 'swing_no_reconfirmable', pids: s.pids, msg: 'El ciclo ya tiene id SWING y la referencia temporal actual no lo reproduce; se conserva como SWING (no se degrada).' });
    if (s.isSwing) {
      const known = [...new Set(s.pids.map(p => state.ids[p]).filter(Boolean))];
      let id = known.length ? known.sort()[0] : null;
      if (known.length > 1) amb.push({ code: 'ids_previos_distintos', pids: s.pids, msg: 'Posiciones del mismo ciclo tenían ids distintos: se conserva el más antiguo (' + id + ').' });
      if (!id) {
        const y = s.startDay.slice(0, 4);
        state.counters[y] = (state.counters[y] || 0) + 1;
        id = 'SWING-' + y + '-' + String(state.counters[y]).padStart(6, '0');
        state.swingMeta[id] = { firstUtc: Math.floor(serverUtc), mode, conf: rn.conf, source: rn.source };
        changed = true;
      }
      for (const p of s.pids) if (state.ids[p] !== id) { state.ids[p] = id; changed = true; }
      s.id = id;
    } else if (s.provisional) pending.push(s);
    out.push(s);
  }

  // Snapshots por sesión (evidencia de continuidad): una entrada por sesión operativa, sin crear deals ni aperturas.
  const sess = fresh ? snapshot.session : null;
  if (sess && sess.id) {
    const pids = Object.keys(openMap).sort();
    const prev = state.sessionLog[sess.id];
    const same = prev && JSON.stringify(prev.pids) === JSON.stringify(pids);
    if (!same) {
      state.sessionLog[sess.id] = { id: sess.id, kind: (prev && prev.kind === 'session_start') ? 'session_start' : String(sess.kind || 'sync').slice(0, 24),
        firstTs: prev ? prev.firstTs : nowT, ts: nowT, pids };
      changed = true;
    }
    const ks = Object.keys(state.sessionLog).sort();
    while (ks.length > MAX_SESSION_LOG) { delete state.sessionLog[ks.shift()]; changed = true; }
  }
  // Confirmaciones por ciclo: sesiones cuyo snapshot lo vio abierto
  for (const s of out) {
    s.confirmedSessions = Object.values(state.sessionLog).filter(l => l.pids.some(p => s.pids.includes(p))).map(l => ({ id: l.id, kind: l.kind, ts: l.ts })).sort((a, b) => a.id < b.id ? -1 : 1);
    // ¿El cruce lo confirmó MT5 o solo lo detectó el reloj del servidor?
    if (s.isSwing) s.crossing = { sOpen: s.startDay, sEnd: s.state === 'finished' ? s.endDay : nowDay,
      basis: s.state === 'finished' ? 'mt5_deals' : (s.confirmedSessions.some(l => { const d = String(l.id).replace(/\./g, '-'); return d > s.startDay; }) ? 'mt5_snapshot' : 'server_inferred') };
  }

  // Ambigüedades (deduplicadas, persistentes)
  for (const a of amb) {
    const key = a.code + ':' + a.pids.join(',');
    if (!state.ambiguities[key]) { state.ambiguities[key] = { ...a, key, firstSeen: nowT }; changed = true; }
    state.ambiguities[key].active = true;
  }
  for (const k of Object.keys(state.ambiguities)) {
    const stillThere = amb.some(a => a.code + ':' + a.pids.join(',') === k);
    if (!stillThere && state.ambiguities[k].active) { state.ambiguities[k].active = false; changed = true; }
  }

  const swing = out.filter(s => s.isSwing);
  const lsRef = state.tm.lastSnap || opts.lastSnapHint || null;
  const view = {
    version: VERSION, account: state.account, nowDay, brokerOffset: off, sessionRule: 'NY_1700',
    time: { nowDay, utc: Math.floor(rn.utc), source: rn.source, confidence: rn.conf, mode, flags: rn.flags, canAssign,
            eaUtc: rn.eaUtc, clockSuspect: rn.flags.includes('reloj_ea_sospechoso'), lastSnapshotUtc: lsRef ? lsRef.srvUtc : null,
            confirmedBySnapshot: fresh, serverReevaluated: mode === 'server_reevaluation', maxDay: state.tm.maxDay },
    offsets: offsetTable(state),
    pending,
    cycles: swing,
    pidCycle: Object.fromEntries(swing.flatMap(s => s.pids.map(p => [p, s.id]))),
    stats: {
      cycles: swing.length,
      finished: swing.filter(s => s.state === 'finished').length,
      active: swing.filter(s => s.state === 'active').length,
      wins: swing.filter(s => s.state === 'finished' && s.realized.net > 0).length,
      losses: swing.filter(s => s.state === 'finished' && s.realized.net < 0).length,
      positions: swing.reduce((a, s) => a + s.counts.total, 0),
      pending: pending.length
    },
    ambiguities: Object.values(state.ambiguities).filter(a => a.active)
  };
  return { state, view, changed, resolve: rn };
}

module.exports = { update, emptyState, migrate, dayKey, sessionKey, sessionKeyAt, nySessionKeyUtc, nyOffsetSec, offsetAt, offsetTable, curOffset,
  offsetObservation, applyOffsetObservation, resolveNow, snapConf, setConfig, CFG, VERSION, TM_SCHEMA,
  _internals: { mergeDeals, deriveRecord, sweep, summarize } };
