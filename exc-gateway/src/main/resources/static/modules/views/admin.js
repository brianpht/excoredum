// Operator console (Screen C): symbols & fees (config-driven, fee per lot), the
// add-symbol command, per-user status/balances with account actions (add user,
// adjust balance, suspend, resume), and the value-conservation report.

import { el, clear } from '../dom.js';
import { store, currency } from '../store.js';
import * as api from '../api.js';
import { fmt, esc } from '../fmt.js';

export function render(root, deps) {
  clear(root);
  const page = el('section', { class: 'sec' }, [
    el('h1', { class: 'page-title' }, 'Operator Console · Exchange Administration'),
    el('p', { class: 'sub' }, 'admin routes require an X-User-Id in gateway.admin.uids'),
  ]);
  root.append(page);
  const grid = el('div', { class: 'grid grid-2' });
  grid.append(el('div', { id: 'symbols-panel' }), el('div', {}, [el('div', { id: 'users-panel' }), el('div', { id: 'cons-panel' })]));
  page.append(grid);

  let targetUid = deps.uid();

  async function refresh() {
    const syms = store.symbols;
    renderSymbols(syms);
    renderConservation();
  }

  // ---- symbols & fees ----
  function renderSymbols(syms) {
    const panel = root.querySelector('#symbols-panel');
    clear(panel);
    panel.append(el('div', { class: 'panel' }, [
      el('span', { class: 'bar purple' }),
      el('h2', {}, 'Symbols & Fees'),
      el('p', { class: 'sub' }, 'addSymbol · fees are per-lot quote units'),
      el('div', { style: 'max-height:260px;overflow:auto' }, syms.length ? el('table', {}, [
        el('thead', {}, el('tr', {}, [el('th', {}, 'SYMBOL'), el('th', {}, 'BASE'), el('th', {}, 'QUOTE'), el('th', { class: 'num' }, 'MAKER'), el('th', { class: 'num' }, 'TAKER')])),
        el('tbody', {}, syms.map((s) => el('tr', {}, [
          el('td', {}, esc(s.name)),
          el('td', { class: 'muted' }, esc(String(s.baseCurrency))),
          el('td', { class: 'muted' }, esc(String(s.quoteCurrency))),
          el('td', { class: 'num' }, s.makerFee),
          el('td', { class: 'num' }, s.takerFee),
        ]))),
      ]) : el('div', { class: 'empty' }, 'No symbols configured')),
      el('div', { class: 'row', style: 'gap:10px;margin-top:14px' }, [
        el('button', { class: 'btn purple', id: 'open-add-symbol' }, 'ADD SYMBOL'),
      ]),
      el('div', { class: 'hidden', id: 'add-symbol-form', style: 'margin-top:12px' }),
    ]));

    const form = root.querySelector('#add-symbol-form');
    const open = root.querySelector('#open-add-symbol');
    open.addEventListener('click', () => {
      form.classList.toggle('hidden');
      if (!form.classList.contains('hidden') && !form.children.length) {
        form.append(
          addField('symbolId', 'number'), addField('baseCurrency', 'number'), addField('quoteCurrency', 'number'),
          addField('baseScaleK', 'number', '100000000'), addField('quoteScaleK', 'number', '1000000'),
          addField('makerFee', 'number', '1000'), addField('takerFee', 'number', '5000'),
          el('button', { class: 'btn purple', id: 'submit-symbol' }, 'SUBMIT'),
        );
        root.querySelector('#submit-symbol').addEventListener('click', submitSymbol);
      }
    });
  }

  function addField(name, type, value = '') {
    return el('div', { class: 'field' }, [el('label', {}, esc(name)), el('input', { id: `sym-${name}`, type, value })]);
  }

  async function submitSymbol() {
    const g = (n) => Number(root.querySelector(`#sym-${n}`).value);
    const body = {
      symbolId: g('symbolId'),
      baseCurrency: g('baseCurrency'),
      quoteCurrency: g('quoteCurrency'),
      baseScaleK: g('baseScaleK'),
      quoteScaleK: g('quoteScaleK'),
      takerFee: g('takerFee'),
      makerFee: g('makerFee'),
    };
    try {
      await api.addSymbol(body);
      deps.notify('addSymbol submitted', 'ok');
    } catch (e) {
      deps.notify(e.message, 'err');
    }
  }

  // ---- users ----
  async function refreshUser() {
    const panel = root.querySelector('#users-panel');
    clear(panel);
    panel.append(el('div', { class: 'panel' }, [
      el('span', { class: 'bar cyan' }),
      el('h2', {}, 'Users & Status'),
      el('p', { class: 'sub' }, 'addUser / adjustBalance / suspend / resume'),
      el('div', { class: 'row', style: 'gap:10px;margin-bottom:12px' }, [
        el('label', {}, 'UID'), el('input', { id: 'target-uid', type: 'number', value: targetUid, style: 'width:110px' }),
        el('button', { class: 'btn cyan', id: 'load-user' }, 'LOAD'),
      ]),
      el('div', { id: 'user-result' }),
    ]));
    root.querySelector('#target-uid').addEventListener('change', (e) => {
      targetUid = Number(e.target.value);
      loadUser();
    });
    root.querySelector('#load-user').addEventListener('click', loadUser);
    await loadUser();
  }

  async function loadUser() {
    const box = root.querySelector('#user-result');
    clear(box);
    const uid = targetUid;
    const report = await api.getUserReport(uid).catch(() => null);
    if (!report) {
      box.append(el('div', { class: 'empty' }, 'No data for this uid'));
    } else {
      const status = report.suspended ? 'SUSPENDED' : Number(uid) === 0 ? 'FEE ACCT' : 'ACTIVE';
      const row = el('table', {}, el('thead', {}, el('tr', {}, [el('th', {}, 'UID'), el('th', { class: 'num' }, 'BALANCE QUOTE'), el('th', { class: 'num' }, 'BALANCE BASE'), el('th', {}, 'STATUS')])), el('tbody', {}, el('tr', {}, [
        el('td', {}, String(uid)),
        el('td', { class: 'num' }, balanceOf(report, 'quote')),
        el('td', { class: 'num' }, balanceOf(report, 'base')),
        el('td', {}, el('span', { class: 'tag ' + status }, status)),
      ])));
      box.append(row);
    }
    box.append(actionRow(uid));
  }

  function balanceOf(report, which) {
    const s = store.symbols[0];
    if (!s) return '—';
    const id = which === 'quote' ? s.quoteCurrency : s.baseCurrency;
    const cur = currency(id) || { code: String(id), scaleK: 1 };
    const b = (report.balances || []).find((x) => Number(x.currency) === Number(id));
    return b ? `${fmt(b.balance, cur.scaleK)} ${cur.code}` : `0.000000 ${cur.code}`;
  }

  function actionRow(uid) {
    const wrap = el('div', { class: 'row', style: 'gap:8px;margin-top:12px;flex-wrap:wrap' });
    wrap.append(
      el('button', { class: 'btn cyan', 'data-act': 'add' }, 'ADD USER'),
      el('button', { class: 'btn amber', 'data-act': 'bal' }, 'ADJ BAL'),
      el('button', { class: 'btn red', 'data-act': 'suspend' }, 'SUSPEND'),
      el('button', { class: 'btn green', 'data-act': 'resume' }, 'RESUME'),
    );
    wrap.addEventListener('click', async (e) => {
      const act = e.target.closest('[data-act]');
      if (!act || act.classList.contains('done')) return;
      act.classList.add('done');
      try {
        if (act.dataset.act === 'add') {
          await api.addUser(uid);
          deps.notify(`addUser ${uid} submitted`, 'ok');
        } else if (act.dataset.act === 'suspend') {
          await api.suspendUser(uid);
          deps.notify(`suspendUser ${uid} submitted`, 'ok');
        } else if (act.dataset.act === 'resume') {
          await api.resumeUser(uid);
          deps.notify(`resumeUser ${uid} submitted`, 'ok');
        } else if (act.dataset.act === 'bal') {
          const cur = Number(window.prompt('Currency id:', store.symbols[0] && store.symbols[0].quoteCurrency));
          const amount = Number(window.prompt('Amount:'));
          await api.adjustBalance(uid, { currency: cur, amount });
          deps.notify(`adjustBalance ${uid} submitted`, 'ok');
        }
        loadUser();
      } catch (err) {
        deps.notify(err.message, 'err');
      } finally {
        act.classList.remove('done');
      }
    });
    return wrap;
  }

  // ---- conservation ----
  async function renderConservation() {
    const panel = root.querySelector('#cons-panel');
    clear(panel);
    panel.append(el('div', { class: 'panel' }, [
      el('span', { class: 'bar green' }),
      el('h2', {}, 'Risk & Value Conservation'),
      el('p', { class: 'sub' }, 'report.totalCurrencyBalance'),
      el('div', { id: 'cons-body' }),
    ]));
    const cons = await api.getConservation().catch(() => null);
    const body = root.querySelector('#cons-body');
    clear(body);
    if (!cons || !cons.totals) {
      body.append(el('div', { class: 'empty' }, 'No conservation data'));
      return;
    }
    const list = el('div', { class: 'counter-list' });
    for (const t of cons.totals) {
      const cur = currency(t.currency) || { code: String(t.currency), scaleK: 1 };
      list.append(
        ctr(`${cur.code} total`, fmt(t.total, cur.scaleK)),
        ctr(`· client balances`, fmt(t.accountBalance, cur.scaleK)),
        ctr(`· collected fees`, fmt(t.fees, cur.scaleK)),
        ctr(`· reserved`, fmt(t.reserved, cur.scaleK)),
      );
    }
    body.append(list);
    const s = store.symbols[0];
    if (s) body.append(el('div', { class: 'ctr' }, [el('span', {}, 'appliedPosition'), el('span', {}, String(cons.appliedPosition))]));
  }

  function ctr(k, v) {
    return el('div', { class: 'ctr' }, [el('span', {}, k), el('span', {}, v)]);
  }

  refresh();
  refreshUser();
  const iv = setInterval(refresh, 8000);
  return {
    cleanup() {
      clearInterval(iv);
    },
    refresh,
  };
}

export const name = 'Admin';
