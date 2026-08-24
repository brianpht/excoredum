// Account & portfolio (Screen B): equity chips, per-currency balances
// (available / reserved / total), the direct-exchange reservation breakdown, the
// order lifecycle history, and the user's fills with fees.

import { el, clear } from '../dom.js';
import { store, currency, selectedSymbol } from '../store.js';
import * as api from '../api.js';
import { fmt, price, size, shortTime, esc, sideClass } from '../fmt.js';

export function render(root, deps) {
  clear(root);
  const page = el('section', { class: 'sec' }, [
    el('h1', { class: 'page-title' }, `Account & Portfolio · User ${deps.uid()}`),
    el('p', { class: 'sub' }, 'readClient.singleUserReport + orderHistory + userTrades'),
  ]);
  root.append(page);
  const chipsRow = el('div', { class: 'chips' });
  page.append(chipsRow);
  const grid = el('div', { class: 'grid grid-2' });
  grid.append(
    el('div', {}, [el('div', { id: 'balance-panel' }), el('div', { id: 'reserve-panel' })]),
    el('div', {}, [el('div', { id: 'hist-panel' }), el('div', { id: 'trades-panel' })]),
  );
  page.append(grid);

  async function refresh() {
    const uid = deps.uid();
    const [report, cons, hist, trades] = await Promise.all([
      api.getUserReport(uid).catch(() => null),
      api.getConservation().catch(() => null),
      api.getOrderHistory(uid).catch(() => []),
      api.getUserTrades(uid, 100).catch(() => []),
    ]);
    renderChips(report, cons);
    renderBalances(report);
    renderReservations(report);
    renderHistory(hist);
    renderTrades(trades);
  }

  // ---- chips ----
  function renderChips(report, cons) {
    clear(chipsRow);
    const s = selectedSymbol();
    const qcur = s ? currency(s.quoteCurrency) : null;
    if (!qcur) {
      chipsRow.append(el('div', { class: 'empty' }, 'Select a symbol'));
      return;
    }
    const qt = cons && cons.totals ? cons.totals.find((t) => Number(t.currency) === Number(s.quoteCurrency)) : null;
    chipsRow.append(
      chip(`Total Equity (${qcur.code})`, qt ? fmt(qt.total, qcur.scaleK) : '—', ''),
      chip('In Orders (reserved)', qt ? fmt(qt.reserved, qcur.scaleK) : '—', 'amber'),
      chip('Realized P&L', '—', ''),
      chip(`Collected Fees (uid 0)`, qt ? fmt(qt.fees, qcur.scaleK) : '—', 'purple'),
    );
  }

  // ---- balances ----
  function renderBalances(report) {
    const panel = root.querySelector('#balance-panel');
    clear(panel);
    if (!report || report.exists === false) {
      panel.append(el('div', { class: 'panel' }, [el('span', { class: 'bar green' }), el('h2', {}, 'Balances'), el('p', { class: 'sub' }, 'singleUserReport'), el('div', { class: 'empty' }, 'User not found')]));
      return;
    }
    const rows = computeBalanceRows(report);
    panel.append(el('div', { class: 'panel' }, [
      el('span', { class: 'bar green' }),
      el('h2', {}, 'Balances'),
      el('p', { class: 'sub' }, 'available / reserved / total · singleUserReport'),
      el('table', {}, el('thead', {}, el('tr', {}, [
        el('th', {}, 'CUR'), el('th', { class: 'num' }, 'AVAILABLE'), el('th', { class: 'num' }, 'RESERVED'), el('th', { class: 'num' }, 'TOTAL'),
      ])), el('tbody', {}, rows.map((r) => el('tr', {}, [
        el('td', {}, esc(r.code)),
        el('td', { class: 'num' }, fmt(r.available, r.scaleK)),
        el('td', { class: 'num amber' }, fmt(r.reserved, r.scaleK)),
        el('td', { class: 'num' }, fmt(r.available + r.reserved, r.scaleK)),
      ]))))),
    ]));
  }

  function computeBalanceRows(report) {
    const reservedByCur = computeReservedByCurrency(report);
    const set = new Set();
    const rows = [];
    for (const b of report.balances || []) {
      const id = Number(b.currency);
      const cur = currency(id) || { code: String(id), scaleK: 1 };
      rows.push({ id, code: cur.code, scaleK: cur.scaleK, available: Number(b.balance), reserved: reservedByCur.get(id) || 0 });
      set.add(id);
    }
    for (const [id, reserved] of reservedByCur) {
      if (set.has(id)) continue;
      const cur = currency(id) || { code: String(id), scaleK: 1 };
      rows.push({ id, code: cur.code, scaleK: cur.scaleK, available: 0, reserved });
    }
    return rows;
  }

  function computeReservedByCurrency(report) {
    const map = new Map();
    const add = (id, v) => map.set(id, (map.get(id) || 0) + v);
    for (const o of report.orders || []) {
      const sym = symFor(o.symbolId);
      if (!sym) continue;
      if (o.side === 'ASK') {
        add(Number(sym.baseCurrency), o.remaining * (sym.baseScaleK || 1));
      } else {
        add(Number(sym.quoteCurrency), o.remaining * (o.reserveBidPrice * (sym.quoteScaleK || 1) + (sym.takerFee || 0)));
      }
    }
    return map;
  }

  // ---- reservations ----
  function renderReservations(report) {
    const panel = root.querySelector('#reserve-panel');
    clear(panel);
    const s = selectedSymbol();
    const qcur = currency(s ? s.quoteCurrency : 0) || { code: 'QUOTE', scaleK: 1 };
    const bcur = currency(s ? s.baseCurrency : 0) || { code: 'BASE', scaleK: 1 };
    let bidHold = 0;
    let askHold = 0;
    let feeHold = 0;
    for (const o of report ? report.orders || [] : []) {
      const sym = symFor(o.symbolId);
      if (!sym) continue;
      if (o.side === 'ASK') askHold += o.remaining * (sym.baseScaleK || 1);
      else {
        const notional = o.remaining * o.reserveBidPrice * (sym.quoteScaleK || 1);
        const fee = o.remaining * (sym.takerFee || 0);
        bidHold += notional;
        feeHold += fee;
      }
    }
    panel.append(el('div', { class: 'panel' }, [
      el('span', { class: 'bar amber' }),
      el('h2', {}, 'Funds Reserved'),
      el('p', { class: 'sub' }, 'direct-exchange risk holds'),
      el('div', { class: 'counter-list' }, [
        ctr('Bid holds (size x reserve x quoteScale)', `${fmt(bidHold, qcur.scaleK)} ${qcur.code}`),
        ctr('Ask holds (size x baseScale)', `${fmt(askHold, bcur.scaleK)} ${bcur.code}`),
        ctr('Reserved taker fee', `${fmt(feeHold, qcur.scaleK)} ${qcur.code}`),
      ]),
    ]));
  }

  // ---- history ----
  function renderHistory(hist) {
    const panel = root.querySelector('#hist-panel');
    clear(panel);
    panel.append(el('div', { class: 'panel' }, [
      el('span', { class: 'bar cyan' }),
      el('h2', {}, 'Order History'),
      el('p', { class: 'sub' }, 'readClient.orderHistory · lifecycle + per-fill'),
      el('div', { style: 'max-height:300px;overflow:auto' }, hist.length ? el('table', {}, [
        el('thead', {}, el('tr', {}, [el('th', {}, 'ID'), el('th', {}, 'PAIR'), el('th', {}, 'SIDE'), el('th', {}, 'TYPE'), el('th', { class: 'num' }, 'PRICE'), el('th', { class: 'num' }, 'SIZE'), el('th', {}, 'STATUS')])),
        el('tbody', {}, hist.map((o) => el('tr', {}, [
          el('td', {}, String(o.orderId)),
          el('td', {}, esc(pairName(o.symbolId))),
          el('td', { class: sideClass(o.side) }, o.side),
          el('td', {}, o.orderType),
          el('td', { class: 'num' }, price(o.price, symFor(o.symbolId))),
          el('td', { class: 'num' }, size(o.size, symFor(o.symbolId))),
          el('td', {}, el('span', { class: 'tag ' + (o.state || '') }, o.state || '—')),
        ]))),
      ]) : el('div', { class: 'empty' }, 'No orders')),
    ]));
  }

  // ---- trades ----
  function renderTrades(trades) {
    const panel = root.querySelector('#trades-panel');
    clear(panel);
    const qcur = currency(selectedSymbol() ? selectedSymbol().quoteCurrency : 0) || { code: 'QUOTE', scaleK: 1 };
    panel.append(el('div', { class: 'panel' }, [
      el('span', { class: 'bar purple' }),
      el('h2', {}, 'My Trades'),
      el('p', { class: 'sub' }, 'readClient.userTrades · fills with counterparty'),
      el('div', { style: 'max-height:300px;overflow:auto' }, trades.length ? el('table', {}, [
        el('thead', {}, el('tr', {}, [el('th', {}, 'TIME'), el('th', {}, 'PAIR'), el('th', {}, 'SIDE'), el('th', { class: 'num' }, 'PRICE'), el('th', { class: 'num' }, 'SIZE'), el('th', { class: 'num' }, 'FEE')])),
        el('tbody', {}, trades.map((t) => el('tr', {}, [
          el('td', {}, shortTime(t.timestamp)),
          el('td', {}, esc(pairName(t.symbolId))),
          el('td', { class: 'green' }, 'TRADE'),
          el('td', { class: 'num' }, price(t.price, symFor(t.symbolId))),
          el('td', { class: 'num' }, size(t.size, symFor(t.symbolId))),
          el('td', { class: 'num' }, '—'),
        ]))),
      ]) : el('div', { class: 'empty' }, 'No trades')),
    ]));
  }

  function pairName(symbolId) {
    const s = symFor(symbolId);
    return s ? s.name : String(symbolId);
  }

  function symFor(id) {
    return store.symbols.find((s) => s.symbolId === Number(id)) || null;
  }

  function chip(k, v, tone) {
    return el('div', { class: 'chip' }, [el('div', { class: 'k' }, k), el('div', { class: 'v ' + tone }, v)]);
  }
  function ctr(k, v) {
    return el('div', { class: 'ctr' }, [el('span', {}, k), el('span', {}, v)]);
  }

  refresh();
  const iv = setInterval(refresh, 8000);
  return {
    cleanup() {
      clearInterval(iv);
    },
    refresh,
  };
}

export const name = 'Portfolio';
