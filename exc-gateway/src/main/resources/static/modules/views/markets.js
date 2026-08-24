// Markets landing: a list of configured symbols with a live last price (mid of
// the best bid/ask from the order book). "Trade" opens the selected pair in the
// Spot (trading) view.

import { el, clear } from '../dom.js';
import { store } from '../store.js';
import { getOrderBook } from '../api.js';
import { price, esc } from '../fmt.js';

export function render(root, deps) {
  clear(root);
  const page = el('section', { class: 'sec' }, [
    el('h1', { class: 'page-title' }, 'Markets'),
    el('p', { class: 'sub' }, 'Configured symbols · last price from L2 best bid/ask'),
  ]);
  const table = el('table', {}, [
    el('thead', {}, el('tr', {}, [
      el('th', {}, 'Symbol'),
      el('th', {}, 'Base'),
      el('th', {}, 'Quote'),
      el('th', { class: 'num' }, 'Last Price'),
      el('th', { class: 'num' }, 'Best Bid'),
      el('th', { class: 'num' }, 'Best Ask'),
      el('th', {}, ''),
    ])),
    el('tbody', { id: 'mk-body' }),
  ]);
  page.append(table);
  root.append(page);

  async function refresh() {
    const body = root.querySelector('#mk-body');
    clear(body);
    for (const sym of store.symbols) {
      const tr = el('tr', {}, [
        el('td', {}, esc(sym.name)),
        el('td', { class: 'muted' }, esc(String(sym.baseCurrency))),
        el('td', { class: 'muted' }, esc(String(sym.quoteCurrency))),
        el('td', { class: 'num', id: `mk-${sym.symbolId}-last` }, '—'),
        el('td', { class: 'num green', id: `mk-${sym.symbolId}-bid` }, '—'),
        el('td', { class: 'num red', id: `mk-${sym.symbolId}-ask` }, '—'),
        el('td', {}, el('button', { class: 'btn cyan', 'data-trade': sym.symbolId }, 'Trade')),
      ]);
      body.append(tr);
      getOrderBook(sym.symbolId, 1)
        .then((book) => updateRow(sym, book))
        .catch(() => updateRow(sym, null));
    }
  }

  function updateRow(sym, book) {
    const bestBid = book && book.bids && book.bids[0];
    const bestAsk = book && book.asks && book.asks[0];
    const bidPrice = bestBid ? bestBid.price : null;
    const askPrice = bestAsk ? bestAsk.price : null;
    const last = bidPrice != null && askPrice != null ? (bidPrice + askPrice) / 2 : bidPrice || askPrice;
    set(rowEl(sym.symbolId, 'last'), last != null ? price(last, sym) : '—');
    set(rowEl(sym.symbolId, 'bid'), bidPrice != null ? price(bidPrice, sym) : '—');
    set(rowEl(sym.symbolId, 'ask'), askPrice != null ? price(askPrice, sym) : '—');
  }

  function rowEl(symbolId, part) {
    return root.querySelector(`#mk-${symbolId}-${part}`);
  }

  function set(node, v) {
    if (node) node.textContent = v;
  }

  page.addEventListener('click', (e) => {
    const btn = e.target.closest('[data-trade]');
    if (!btn) return;
    deps.select(Number(btn.dataset.trade));
    deps.goto('spot');
  });

  refresh();
  const iv = setInterval(refresh, 4000);
  return {
    cleanup() {
      clearInterval(iv);
    },
    refresh,
  };
}

export const name = 'Markets';
