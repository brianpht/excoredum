// System health & ops (Screen D): replica / client health from GET /health, the
// available counters, conservation totals, and a live feed of engine egress
// events (TRADE / REDUCE / REJECT) over the WebSocket. Raft roles and the full
// CoreMetrics counter set are not on the gateway query protocol (see GATEWAY.md).

import { el, clear } from '../dom.js';
import { store, on, currency } from '../store.js';
import * as api from '../api.js';
import { fmt, time } from '../fmt.js';

const events = [];

export function render(root, deps) {
  clear(root);
  const page = el('section', { class: 'sec' }, [
    el('h1', { class: 'page-title' }, 'System Health · Determinism & Replication'),
    el('p', { class: 'sub' }, 'readClient health + conservation · live egress events'),
  ]);
  root.append(page);
  const grid = el('div', { class: 'grid grid-2' });
  grid.append(el('div', { id: 'health-panel' }), el('div', { id: 'counters-panel' }), el('div', { id: 'cons-panel' }), el('div', { id: 'events-panel' }));
  page.append(grid);

  async function refresh() {
    const h = await api.getHealth().catch(() => null);
    renderHealth(h);
    renderCounters(h);
    renderConservation(h);
  }

  function renderHealth(h) {
    const panel = root.querySelector('#health-panel');
    clear(panel);
    panel.append(el('div', { class: 'panel' }, [
      el('span', { class: 'bar cyan' }),
      el('h2', {}, 'Replica Health'),
      el('p', { class: 'sub' }, 'readClient.health · appliedPosition + stateHash'),
      el('div', { class: 'counter-list' }, [
        ctr('applied position', h ? String(h.appliedPosition) : '—'),
        ctr('state hash', h ? `0x${hex(h.stateHash)}` : '—'),
        ctr('conservation currency count', h && h.totals ? String(h.totals.length) : '—'),
      ]),
    ]));
  }

  function renderCounters(h) {
    const panel = root.querySelector('#counters-panel');
    clear(panel);
    panel.append(el('div', { class: 'panel' }, [
      el('span', { class: 'bar amber' }),
      el('h2', {}, 'Client Counters'),
      el('p', { class: 'sub' }, 'readClient submit/poll stats · CoreMetrics not on query protocol'),
      el('div', { class: 'counter-list' }, [
        ctr('read queries submitted', h ? String(h.submitted) : '—'),
        ctr('read queries completed', h ? String(h.completed) : '—'),
        ctr('expired queries', h ? String(h.expired) : '—'),
        ctr('backpressure stalls', h ? String(h.backpressure) : '—'),
        ctr('orders in-flight (per-symbol)', '—'),
        ctr('dedup hits', '—'),
        ctr('journal recorder errors', '—'),
      ]),
    ]));
  }

  function renderConservation(h) {
    const panel = root.querySelector('#cons-panel');
    clear(panel);
    panel.append(el('div', { class: 'panel' }, [
      el('span', { class: 'bar green' }),
      el('h2', {}, 'Value Conservation'),
      el('p', { class: 'sub' }, 'client + fees + reserved = conserved total'),
      el('div', { id: 'cons-body' }),
    ]));
    const body = root.querySelector('#cons-body');
    clear(body);
    if (!h || !h.totals) {
      body.append(el('div', { class: 'empty' }, 'No totals'));
      return;
    }
    const s = store.symbols[0];
    const rows = h.totals.map((t) => {
      const cur = currency(t.currency) || { code: String(t.currency), scaleK: 1 };
      return el('div', { class: 'ctr' }, [
        el('span', {}, `${cur.code} total`),
        el('span', {}, `${fmt(t.total, cur.scaleK)} (fees ${fmt(t.fees, cur.scaleK)})`),
      ]);
    });
    body.append(...rows);
  }

  function renderEvents() {
    const panel = root.querySelector('#events-panel');
    clear(panel);
    panel.append(el('div', { class: 'panel' }, [
      el('span', { class: 'bar purple' }),
      el('h2', {}, 'Live Egress Events'),
      el('p', { class: 'sub' }, 'WebSocket broadcast · trades / reduces / rejects'),
      el('div', { style: 'max-height:280px;overflow:auto' }, events.slice(0, 60).length
        ? el('table', {}, [
            el('thead', {}, el('tr', {}, [
              el('th', {}, 'TIME'), el('th', {}, 'TYPE'), el('th', {}, 'SYM'), el('th', {}, 'ORDER'),
              el('th', { class: 'num' }, 'PRICE'), el('th', { class: 'num' }, 'SIZE'),
            ])),
            el('tbody', {}, events.slice(0, 60).map((e) => el('tr', {}, [
              el('td', {}, time(e.ts)),
              el('td', { class: e.type === 'TRADE' ? 'green' : e.type === 'REJECT' ? 'red' : 'cyan' }, e.type),
              el('td', {}, e.symbolId),
              el('td', {}, e.orderId != null ? String(e.orderId) : (e.makerOrderId != null ? String(e.makerOrderId) : '—')),
              el('td', { class: 'num' }, e.price != null ? fmt(e.price, 1) : '—'),
              el('td', { class: 'num' }, e.size != null ? fmt(e.size, 1) : (e.reducedBy != null ? fmt(e.reducedBy, 1) : '—')),
            ]))),
          ])
        : el('div', { class: 'empty' }, 'No events yet — place an order to stream egress')),
    ]));
  }

  function ctr(k, v) {
    return el('div', { class: 'ctr' }, [el('span', {}, k), el('span', {}, v)]);
  }

  const push = (e) => {
    events.unshift({ ts: Date.now(), type: e.type || '', symbolId: e.symbolId, orderId: e.orderId, makerOrderId: e.makerOrderId, price: e.price, size: e.size, reducedBy: e.reducedBy });
    if (events.length > 200) events.pop();
    renderEvents();
  };

  const unsubs = [
    on('trade', pushWith('TRADE')),
    on('reduce', pushWith('REDUCE')),
    on('reject', pushWith('REJECT')),
  ];

  function pushWith(type) {
    return (e) => push({ ...e, type });
  }

  refresh();
  renderEvents();
  const iv = setInterval(refresh, 5000);
  return {
    cleanup() {
      clearInterval(iv);
      unsubs.forEach((u) => u());
    },
    refresh,
  };
}

function hex(n) {
  return Number(n >>> 0).toString(16).padStart(8, '0');
}

export const name = 'Ops';
