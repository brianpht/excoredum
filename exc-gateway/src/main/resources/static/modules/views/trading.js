// Spot trading terminal (Screen A): L2 order book with depth, a canvas candle
// chart over the trade tape, market trade tape, a place-order ticket (GTC / IOC /
// FOK-BUDGET), and the user's open orders with cancel / move / reduce actions.

import { el, clear } from '../dom.js';
import { store, selectedSymbol, on, l2For, tapeFor } from '../store.js';
import * as api from '../api.js';
import { price, size, fmt, time, esc, sideClass } from '../fmt.js';

export function render(root, deps) {
  clear(root);
  const sym = selectedSymbol();
  if (!sym) {
    root.append(el('div', { class: 'empty' }, 'No symbols configured'));
    return { cleanup() {}, refresh() {} };
  }

  // ---- form state ----
  let orderType = 'GTC';
  let side = 'BUY';

  const page = el('section', { class: 'sec' }, [el('div', { class: 'grid grid-market' })]);

  // ---- order book panel ----
  const bookPanel = el('div', { class: 'panel' }, [
    el('span', { class: 'bar cyan' }),
    el('h2', {}, esc(sym.name)),
    el('p', { class: 'sub' }, 'L2 order book · readClient.orderBook'),
    el('div', { class: 'book', id: 'book-body' }),
  ]);

  // ---- chart + tape ----
  const chartPanel = el('div', { class: 'panel' }, [
    el('span', { class: 'bar cyan' }),
    el('h2', {}, 'Price Chart'),
    el('p', { class: 'sub' }, 'aggregated from trades + L2'),
    el('div', { class: 'chart-box' }, el('canvas', { id: 'chart' })),
  ]);
  const tapePanel = el('div', { class: 'panel' }, [
    el('span', { class: 'bar purple' }),
    el('h2', {}, 'Market Trade Tape'),
    el('p', { class: 'sub' }, 'readClient.marketTrades · realtime'),
    el('div', { id: 'tape-body', style: 'max-height:200px;overflow:auto' }),
  ]);

  // ---- ticket + open orders ----
  const ticket = el('div', { class: 'panel' }, [
    el('span', { class: 'bar green' }),
    el('h2', {}, 'Place Order'),
    el('p', { class: 'sub' }, 'placeGtc / placeIoc / placeFokBudget'),
    el('div', { class: 'row', style: 'gap:12px;margin-bottom:12px' }, [
      el('button', { class: 'btn green', id: 'side-buy', style: 'flex:1' }, 'BUY / BID'),
      el('button', { class: 'btn red', id: 'side-sell', style: 'flex:1' }, 'SELL / ASK'),
    ]),
    el('div', { class: 'field' }, [el('label', {}, 'Order type'), orderTypeSeg()]),
    priceInput(), sizeInput(), reserveInput(), feeState(),
    el('button', { class: 'btn green', id: 'place', style: 'width:100%;margin-top:12px' }, 'PLACE ORDER'),
  ]);
  const ordersPanel = el('div', { class: 'panel' }, [
    el('span', { class: 'bar cyan' }),
    el('h2', {}, 'Open Orders'),
    el('p', { class: 'sub' }, 'readClient.activeOrders · cancel / move / reduce'),
    el('div', { id: 'orders-body' }),
  ]);

  const grid = page.querySelector('.grid');
  grid.append(bookPanel, el('div', {}, [chartPanel, tapePanel]), el('div', {}, [ticket, ordersPanel]));
  root.append(page);

  // dynamic elements
  const priceField = () => root.querySelector('#price-field input');
  const sizeField = () => root.querySelector('#size-field input');
  const reserveField = () => root.querySelector('#reserve-field input');
  const feeEl = () => root.querySelector('#fee-state');
  const bookBody = () => root.querySelector('#book-body');
  const tapeBody = () => root.querySelector('#tape-body');
  const ordersBody = () => root.querySelector('#orders-body');
  const canvas = () => root.querySelector('#chart');

  function orderTypeSeg() {
    const wrap = el('div', { class: 'seg' });
    for (const t of ['GTC', 'IOC', 'FOK_BUDGET']) {
      wrap.append(el('button', { class: 'opt' + (t === orderType ? ' on' : ''), 'data-type': t }, t === 'FOK_BUDGET' ? 'FOK-BUDGET' : t));
    }
    return wrap;
  }

  function priceInput() {
    return el('div', { id: 'price-field', class: 'field' }, [el('label', {}, 'Price'), el('input', { value: '' })]);
  }
  function sizeInput() {
    return el('div', { id: 'size-field', class: 'field' }, [el('label', {}, 'Size'), el('input', { value: '' })]);
  }
  function reserveInput() {
    return el('div', { id: 'reserve-field', class: 'field' }, [el('label', {}, 'Reserve limit (bid)'), el('input', { value: '' })]);
  }
  function feeState() {
    return el('div', { id: 'fee-state', class: 'field' }, [el('label', {}, 'Est. taker fee'), el('div', { style: 'font-family:var(--mono)' }, '—')]);
  }

  // ---- book rendering ----
  function renderBook(snapshot) {
    if (!snapshot) return;
    const asks = (snapshot.asks || []).slice().sort((a, b) => a.price - b.price).reverse(); // best (lowest) at bottom
    const bids = (snapshot.bids || []).slice().sort((a, b) => b.price - a.price); // best (highest) at top
    const maxAsk = cumMax(asks);
    const maxBid = cumMax(bids);
    const bestAsk = asks[0];
    const bestBid = bids[0];
    const spread = bestAsk && bestBid ? bestAsk.price - bestBid.price : null;
    const body = bookBody();
    clear(body);
    body.append(el('div', { class: 'head' }, [el('span', {}, 'PRICE'), el('span', {}, 'SIZE'), el('span', {}, 'TOTAL')]));
    for (const row of levelRows(asks, 'ask', maxAsk)) body.append(row);
    const mid = el('div', { class: 'header' });
    mid.textContent = bestBid && bestAsk
      ? `BID ${price(bestBid.price, selectedSymbol())}  ·  SPREAD ${formatSpread(spread, selectedSymbol())}`
      : (bestBid ? `BID ${price(bestBid.price, selectedSymbol())}` : 'NO BOOK');
    body.append(mid);
    for (const row of levelRows(bids, 'bid', maxBid)) body.append(row);
  }

  function cumMax(levels) {
    let cum = 0;
    let max = 0;
    for (const l of levels) {
      cum += l.size;
      if (cum > max) max = cum;
    }
    return max;
  }

  function levelRows(levels, cls, maxTotal) {
    const rows = [];
    let cum = 0;
    for (const l of levels) {
      cum += l.size;
      const w = maxTotal ? Math.round((cum / maxTotal) * 100) : 0;
      const row = el('div', { class: 'row ' + cls }, [
        el('span', { class: 'depth', style: `width:${w}%` }),
        el('span', { class: 'c' }, price(l.price, selectedSymbol())),
        el('span', { class: 'c' }, size(l.size, selectedSymbol())),
        el('span', { class: 'c' }, size(cum, selectedSymbol())),
      ]);
      rows.push(row);
    }
    return rows;
  }

  function formatSpread(v, sym) {
    if (v == null) return '—';
    return fmt(v, sym.quoteScaleK);
  }

  // ---- tape rendering ----
  function renderTape(list) {
    const body = tapeBody();
    clear(body);
    if (!list.length) {
      body.append(el('div', { class: 'empty' }, 'No trades yet'));
      return;
    }
    body.append(el('table', {}, el('thead', {}, el('tr', {}, [
      el('th', {}, 'Time'), el('th', { class: 'num' }, 'Price'), el('th', { class: 'num' }, 'Size'), el('th', {}, 'Side'),
    ])), el('tbody', {}, list.slice(0, 80).map((t) => el('tr', {}, [
      el('td', {}, time(t.timestamp)),
      el('td', { class: 'num' }, price(t.price, selectedSymbol())),
      el('td', { class: 'num' }, size(t.size, selectedSymbol())),
      el('td', { class: t.side === 'SELL' ? 'red' : 'green' }, t.side || (t.makerUid && t.takerUid ? 'TAKE' : '—')),
    ])))));
  }

  // ---- chart rendering ----
  function renderChart(list) {
    const cv = canvas();
    if (!cv) return;
    const dpr = window.devicePixelRatio || 1;
    const W = (cv.clientWidth || 600) * dpr;
    const H = 250 * dpr;
    cv.width = W;
    cv.height = H;
    const ctx = cv.getContext('2d');
    ctx.clearRect(0, 0, W, H);
    if (!list.length) {
      ctx.fillStyle = '#5c6a77';
      ctx.fillText('No trades', 8 * dpr, 16 * dpr);
      return;
    }
    const buckets = new Map();
    for (const t of list.slice().reverse()) {
      const b = Math.floor(t.timestamp / 1000);
      const cur = buckets.get(b) || { o: t.price, h: t.price, l: t.price, c: t.price };
      cur.h = Math.max(cur.h, t.price);
      cur.l = Math.min(cur.l, t.price);
      cur.c = t.price;
      buckets.set(b, cur);
    }
    const arr = [...buckets.values()].slice(-40);
    const prices = arr.flatMap((b) => [b.h, b.l]);
    const min = Math.min(...prices);
    const max = Math.max(...prices);
    const pad = (max - min) * 0.1 || 1;
    const y = (p) => H - ((p - min + pad) / (max - min + 2 * pad)) * H;
    const x = (i) => ((i + 0.5) / arr.length) * W;
    const bw = Math.max(1 * dpr, (W / arr.length) * 0.6);
    arr.forEach((b, i) => {
      const cx = x(i);
      const up = b.c >= b.o;
      const col = up ? '#22c55e' : '#ef4444';
      ctx.strokeStyle = col;
      ctx.fillStyle = col;
      ctx.beginPath();
      ctx.moveTo(cx, y(b.h));
      ctx.lineTo(cx, y(b.l));
      ctx.stroke();
      const top = y(Math.max(b.o, b.c));
      const bh = Math.max(1, Math.abs(y(b.o) - y(b.c)));
      ctx.fillRect(cx - bw / 2, top, bw, bh);
    });
    // last price marker
    const last = arr[arr.length - 1].c;
    ctx.strokeStyle = '#38bdf8';
    ctx.setLineDash([4, 4]);
    ctx.beginPath();
    ctx.moveTo(0, y(last));
    ctx.lineTo(W, y(last));
    ctx.stroke();
    ctx.setLineDash([]);
    ctx.fillStyle = '#38bdf8';
    ctx.font = `${10 * dpr}px ${getComputedStyle(document.body).getPropertyValue('--mono')}`;
    ctx.fillText(price(last, selectedSymbol()), 6 * dpr, Math.max(10 * dpr, y(last) - 4 * dpr));
  }

  // ---- open orders ----
  async function refreshOpenOrders() {
    const uid = deps.uid();
    const list = await api.getActiveOrders(uid).catch(() => []);
    const body = ordersBody();
    clear(body);
    if (!list.length) {
      body.append(el('div', { class: 'empty' }, 'No open orders'));
      return;
    }
    body.append(el('table', {}, el('thead', {}, el('tr', {}, [
      el('th', {}, 'ID'), el('th', {}, 'Side'), el('th', { class: 'num' }, 'Price'),
      el('th', { class: 'num' }, 'Size'), el('th', { class: 'num' }, 'Filled'), el('th', {}, ''),
    ])), el('tbody', {}, list.map((o) => el('tr', {}, [
      el('td', {}, String(o.orderId)),
      el('td', { class: sideClass(o.side) }, o.side),
      el('td', { class: 'num' }, price(o.price, symFor(o.symbolId))),
      el('td', { class: 'num' }, size(o.size, symFor(o.symbolId))),
      el('td', { class: 'num' }, size(o.filled, symFor(o.symbolId))),
      el('td', {},
        el('button', { class: 'btn ghost', 'data-cancel': o.orderId }, 'X'),
        el('button', { class: 'btn ghost', 'data-move': o.orderId }, 'Move'),
        el('button', { class: 'btn ghost', 'data-reduce': o.orderId }, 'Reduce')),
    ])))));
    body.onclick = (e) => {
      const c = e.target.closest('[data-cancel]');
      if (c) return doCancel(Number(c.dataset.cancel));
      const m = e.target.closest('[data-move]');
      if (m) return doMove(Number(m.dataset.move));
      const r = e.target.closest('[data-reduce]');
      if (r) return doReduce(Number(r.dataset.reduce));
    };
  }

  function symFor(id) {
    return store.symbols.find((s) => s.symbolId === Number(id)) || selectedSymbol();
  }

  async function doCancel(orderId) {
    const s = selectedSymbol();
    try {
      await api.cancelOrder(orderId, s.symbolId, deps.uid());
      deps.notify(`Cancel ${orderId} submitted`, 'ok');
      refreshOpenOrders();
    } catch (e) {
      deps.notify(e.message, 'err');
    }
  }
  async function doMove(orderId) {
    const v = window.prompt('New price:');
    if (v == null) return;
    const s = selectedSymbol();
    try {
      await api.modifyOrder(orderId, { symbolId: s.symbolId, uid: deps.uid(), price: Number(v) });
      deps.notify(`Move ${orderId} submitted`, 'ok');
      refreshOpenOrders();
    } catch (e) {
      deps.notify(e.message, 'err');
    }
  }
  async function doReduce(orderId) {
    const v = window.prompt('New size:');
    if (v == null) return;
    const s = selectedSymbol();
    try {
      await api.modifyOrder(orderId, { symbolId: s.symbolId, uid: deps.uid(), size: Number(v) });
      deps.notify(`Reduce ${orderId} submitted`, 'ok');
      refreshOpenOrders();
    } catch (e) {
      deps.notify(e.message, 'err');
    }
  }

  // ---- ticket wiring ----
  page.addEventListener('click', (e) => {
    const type = e.target.closest('[data-type]');
    if (type) {
      orderType = type.dataset.type;
      root.querySelectorAll('[data-type]').forEach((b) => b.classList.toggle('on', b === type));
      return;
    }
    if (e.target.closest('#side-buy')) {
      side = 'BUY';
      setTicketTone();
      return;
    }
    if (e.target.closest('#side-sell')) {
      side = 'SELL';
      setTicketTone();
      return;
    }
    if (e.target.closest('#place')) {
      place();
    }
  });

  function setTicketTone() {
    const buy = root.querySelector('#side-buy');
    const sell = root.querySelector('#side-sell');
    buy.className = 'btn ' + (side === 'BUY' ? 'green' : 'ghost');
    sell.className = 'btn ' + (side === 'SELL' ? 'red' : 'ghost');
  }

  function updateFee() {
    const s = selectedSymbol();
    if (!s) return;
    const raw = Number(s.takerFee || 0) * Number(sizeField().value || 0);
    const cur = store.currencyMap.get(Number(s.quoteCurrency));
    feeEl().lastChild.textContent = cur ? `${fmt(raw, cur.scaleK)} ${cur.code}` : fmt(raw, 1);
  }

  function place() {
    const s = selectedSymbol();
    const priceV = Number(priceField().value);
    const sizeV = Number(sizeField().value);
    if (!priceV || !sizeV) {
      deps.notify('Price and size are required', 'err');
      return;
    }
    const uid = deps.uid();
    const body = {
      symbolId: s.symbolId,
      orderId: Date.now(),
      ask: side === 'SELL',
      type: orderType,
      price: priceV,
      size: sizeV,
      reserveBidPrice: Number(reserveField().value || priceV),
      uid,
      userCookie: 0,
    };
    api.placeOrder(body)
      .then((r) => {
        const code = r && r.resultCode;
        deps.notify(code === 'SUCCESS' ? `Order placed (${code})` : `Order: ${code}`, code === 'SUCCESS' ? 'ok' : 'err');
        refreshOpenOrders();
      })
      .catch((e) => deps.notify(e.message, 'err'));
  }

  // ---- mount ----
  function refresh() {
    const s = selectedSymbol();
    if (!s) return;
    api.getOrderBook(s.symbolId, 32)
      .then((book) => {
        if (book) store.setL2(s.symbolId, book);
      })
      .catch(() => {});
    api.getMarketTrades(s.symbolId, 100)
      .then((tape) => {
        if (tape && tape.length) store.appendTape(s.symbolId, tape);
      })
      .catch(() => {});
    refreshOpenOrders();
  }

  const unsubs = [
    on('l2', (snap) => renderBook(snap)),
    on('tape', (list) => {
      renderTape(list);
      renderChart(list);
    }),
    on('symbol', () => {
      const s = selectedSymbol();
      bookPanel.querySelector('h2').textContent = s ? s.name : '—';
      renderBook(l2For(s && s.symbolId));
      renderTape(tapeFor(s && s.symbolId));
      renderChart(tapeFor(s && s.symbolId));
      refresh();
    }),
  ];

  root.querySelectorAll('.field input').forEach((i) => i.addEventListener('input', updateFee));

  refresh();
  renderBook(l2For(sym.symbolId));
  renderTape(tapeFor(sym.symbolId));
  renderChart(tapeFor(sym.symbolId));

  const iv = setInterval(refreshOpenOrders, 8000);

  return {
    cleanup() {
      clearInterval(iv);
      unsubs.forEach((un) => un());
    },
    refresh,
  };
}

export const name = 'Spot';
