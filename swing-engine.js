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
'use strict';

const EPS = 1e-7;
const VERSION = 1;
const MAX_SESSION_LOG = 90;

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
function sessionKey(t, off) { const u = t - (off || 0); return new Date((u + nyOffsetSec(u) + 7 * 3600) * 1000).toISOString().slice(0, 10); }
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

function emptyState(account) {
  return { v: VERSION, account: account || null, records: {}, ids: {}, counters: {}, sessionLog: {}, ambiguities: {} };
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
function mergeDeals(state, deals) {
  let changed = false;
  for (const raw of (deals || [])) {
    const d = normDeal(raw); if (!d) continue;
    const rec = state.records[d.pid] || (state.records[d.pid] = { pid: d.pid, deals: {} });
    const prev = rec.deals[d.ticket];
    if (!prev || JSON.stringify(prev) !== JSON.stringify(d)) { rec.deals[d.ticket] = d; changed = true; }
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
function summarize(c, nowT, nowDay, positionsByPid, sk) {
  const recs = c.recs.slice().sort((a, b) => cmpOrd(a.ds.length ? ordOfDeal(a.ds[0]) : a.openOrd, b.ds.length ? ordOfDeal(b.ds[0]) : b.openOrd));
  const finished = c.endOrd != null;
  const startT = c.startOrd.t, endT = finished ? c.endOrd.t : null;
  const startDay = sk(startT), endDay = finished ? sk(endT) : null;
  const isSwing = c.verifiable && startDay !== (finished ? endDay : nowDay);

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
  const acct = snapshot && snapshot.account ? String(snapshot.account.number) : null;
  let state = prevState && prevState.v === VERSION && (!acct || !prevState.account || prevState.account === acct) ? prevState : emptyState(acct);
  let changed = state !== prevState;
  if (acct && state.account !== acct) { state.account = acct; changed = true; }

  if (mergeDeals(state, snapshot.deals)) changed = true;

  const openMap = {};
  for (const p of (snapshot.positions || [])) {
    const id = String(p.position_id != null ? p.position_id : p.ticket);
    openMap[id] = p;
    if (!state.records[id]) { state.records[id] = { pid: id, deals: {} }; changed = true; }
  }
  const nowT = n0(snapshot.timestamp) || Math.max(0, ...Object.values(state.records).flatMap(r => Object.values(r.deals).map(d => d.t)));
  // Offset del bróker: lo que envía la EA (exacto) → el último conocido → estimación por reloj del servidor → 0
  let off = null;
  const eaOff = snapshot.session && typeof snapshot.session.broker_offset === 'number' ? snapshot.session.broker_offset : null;
  if (eaOff != null && Number.isFinite(eaOff)) { off = eaOff; if (state.brokerOffset !== off) { state.brokerOffset = off; changed = true; } }
  else if (typeof state.brokerOffset === 'number') off = state.brokerOffset;
  else if (opts && typeof opts.serverNow === 'number') off = Math.round((nowT - opts.serverNow) / 900) * 900;
  else off = 0;
  const sk = t => sessionKey(t, off);
  const nowDay = sk(nowT);

  const derived = Object.values(state.records).map(rec => deriveRecord(rec, openMap[rec.pid]));
  const { cycles, amb } = sweep(derived, state);
  if (snapshot.deals_truncated === true)
    amb.push({ code: 'historial_truncado', pids: [], msg: 'La EA envía solo los deals más recientes: ciclos anteriores a esa ventana pueden estar incompletos. Los ya conocidos se conservan.' });

  // Resumen + identificadores persistentes (solo los ciclos SWING reciben id; el id se ancla a sus posiciones)
  const out = [];
  for (const c of cycles) {
    const s = summarize(c, nowT, nowDay, openMap, sk);
    if (s.isSwing) {
      const known = [...new Set(s.pids.map(p => state.ids[p]).filter(Boolean))];
      let id = known.length ? known.sort()[0] : null;
      if (known.length > 1) amb.push({ code: 'ids_previos_distintos', pids: s.pids, msg: 'Posiciones del mismo ciclo tenían ids distintos: se conserva el más antiguo (' + id + ').' });
      if (!id) {
        const y = s.startDay.slice(0, 4);
        state.counters[y] = (state.counters[y] || 0) + 1;
        id = 'SWING-' + y + '-' + String(state.counters[y]).padStart(6, '0');
        changed = true;
      }
      for (const p of s.pids) if (state.ids[p] !== id) { state.ids[p] = id; changed = true; }
      s.id = id;
    }
    out.push(s);
  }

  // Snapshots por sesión (evidencia de continuidad): una entrada por sesión operativa, sin crear deals ni aperturas.
  const sess = snapshot.session;
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
  const view = {
    version: VERSION, account: state.account, nowDay, brokerOffset: off, sessionRule: 'NY_1700',
    cycles: swing,
    pidCycle: Object.fromEntries(swing.flatMap(s => s.pids.map(p => [p, s.id]))),
    stats: {
      cycles: swing.length,
      finished: swing.filter(s => s.state === 'finished').length,
      active: swing.filter(s => s.state === 'active').length,
      wins: swing.filter(s => s.state === 'finished' && s.realized.net > 0).length,
      losses: swing.filter(s => s.state === 'finished' && s.realized.net < 0).length,
      positions: swing.reduce((a, s) => a + s.counts.total, 0)
    },
    ambiguities: Object.values(state.ambiguities).filter(a => a.active)
  };
  return { state, view, changed };
}

module.exports = { update, emptyState, dayKey, sessionKey, nyOffsetSec, VERSION, _internals: { mergeDeals, deriveRecord, sweep, summarize } };
