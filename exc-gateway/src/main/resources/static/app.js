// SPA entry: boot config, wire the top bar identity/nav, connect the WebSocket,
// and switch between the view modules. Each view returns { cleanup(), refresh() }.

import { store, on, setSymbols, setCurrencies, setSelectedSymbol } from './modules/store.js';
import { getSymbols, getCurrencies } from './modules/api.js';
import * as ws from './modules/ws.js';
import * as markets from './modules/views/markets.js';
import * as spot from './modules/views/trading.js';
import * as portfolio from './modules/views/portfolio.js';
import * as admin from './modules/views/admin.js';
import * as ops from './modules/views/ops.js';

const views = { markets, spot, portfolio, admin, ops };
let current = 'markets';
let handle = null;

const deps = {
  uid: () => store.currentUid,
  admin: () => store.adminUid,
  goto: (v) => show(v),
  select: (id) => setSelectedSymbol(id),
  notify: (msg, tone = 'err') => toast(msg, tone),
};

async function boot() {
  // identity (persisted per browser)
  const uidEl = document.getElementById('uid');
  const adminEl = document.getElementById('adminUid');
  uidEl.value = localStorage.getItem('exc.uid') || '1';
  adminEl.value = localStorage.getItem('exc.admin') || '';
  store.currentUid = Number(uidEl.value) || 1;
  store.adminUid = adminEl.value.trim();
  uidEl.addEventListener('change', () => {
    store.currentUid = Number(uidEl.value) || 1;
    localStorage.setItem('exc.uid', uidEl.value);
    if (views[current].refresh) views[current].refresh();
  });
  adminEl.addEventListener('change', () => {
    store.adminUid = adminEl.value.trim();
    localStorage.setItem('exc.admin', adminEl.value);
  });

  // config-driven registry (symbols + currencies)
  try {
    setSymbols(await getSymbols());
  } catch (e) {
    toast(`symbols: ${e.message}`);
  }
  try {
    setCurrencies(await getCurrencies());
  } catch (e) {
    toast(`currencies: ${e.message}`);
  }

  // nav
  document.getElementById('nav').addEventListener('click', (e) => {
    const btn = e.target.closest('.tab');
    if (btn) show(btn.dataset.view);
  });

  // connection indicator + auto refresh market views on reconnect
  on('conn', (up) => {
    const el = document.getElementById('conn');
    el.textContent = up ? '● live' : '○ disconnected';
    el.className = 'conn ' + (up ? 'conn-up' : 'conn-down');
    if (up && views[current].refresh) views[current].refresh();
  });

  on('symbol', () => {
    if (current === 'spot' && views.spot.refresh) views.spot.refresh();
    if (current === 'markets' && views.markets.refresh) views.markets.refresh();
  });

  ws.connect();
  show('markets');
}

function show(view) {
  if (!views[view]) return;
  if (view === current && handlesRefresh()) {
    refresh();
    return;
  }
  if (handle && handle.cleanup) handle.cleanup();
  current = view;
  document.querySelectorAll('#nav .tab').forEach((b) => b.classList.toggle('active', b.dataset.view === view));
  const root = document.getElementById('view');
  root.innerHTML = '';
  handle = views[view].render(root, deps) || null;
}

function handlesRefresh() {
  return views[current] && views[current].refresh;
}
function refresh() {
  if (views[current] && views[current].refresh) views[current].refresh();
}

let toastTimer = null;
function toast(msg, tone = 'err') {
  const el = document.getElementById('toast');
  el.textContent = msg;
  el.className = 'toast ' + tone;
  if (toastTimer) clearTimeout(toastTimer);
  toastTimer = setTimeout(() => el.classList.add('hidden'), 3200);
}

boot();
