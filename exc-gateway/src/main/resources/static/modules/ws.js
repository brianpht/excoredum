// WebSocket client for /ws. The gateway broadcast-all; we filter client-side by
// the selected symbol. Reconnect with exponential backoff; the connection status
// is surfaced to the top bar so the user knows when live data is flowing.

import { store, emit, setL2, appendTape, selectedSymbol } from './store.js';

let ws = null;
let retry = 0;
let timer = null;

export function connect() {
  if (typeof WebSocket === 'undefined') return;
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  ws = new WebSocket(`${proto}://${location.host}/ws`);
  ws.onopen = () => {
    store.wsConnected = true;
    retry = 0;
    if (timer) {
      clearTimeout(timer);
      timer = null;
    }
    emit('conn', true);
  };
  ws.onmessage = (e) => {
    let ev;
    try {
      ev = JSON.parse(e.data);
    } catch {
      return;
    }
    handle(ev);
  };
  ws.onerror = () => {
    try {
      ws.close();
    } catch {
      /* already closed */
    }
  };
  ws.onclose = () => {
    store.wsConnected = false;
    emit('conn', false);
    scheduleReconnect();
  };
}

function scheduleReconnect() {
  const delay = Math.min(500 * 2 ** retry, 10000);
  retry += 1;
  if (timer) clearTimeout(timer);
  timer = setTimeout(connect, delay);
}

function handle(ev) {
  const sel = selectedSymbol();
  if (ev.symbolId !== undefined && (!sel || Number(ev.symbolId) !== Number(sel.symbolId))) return;
  switch (ev.type) {
    case 'L2':
      setL2(ev.symbolId, {
        asks: ev.asks || [],
        bids: ev.bids || [],
        appliedPosition: ev.appliedPosition,
        found: true,
      });
      break;
    case 'MARKET_TAPE':
      appendTape(
        ev.symbolId,
        (ev.trades || []).map((t) => ({
          timestamp: t.timestamp,
          price: t.price,
          size: t.size,
          makerUid: t.makerUid,
          takerUid: t.takerUid,
          side: null,
        })),
      );
      break;
    case 'TRADE':
      appendTape(ev.symbolId, [
        {
          timestamp: Date.now(),
          price: ev.price,
          size: ev.size,
          makerUid: ev.makerUid,
          takerUid: ev.takerUid,
          side: null,
        },
      ]);
      emit('trade', ev);
      break;
    case 'REDUCE':
      emit('reduce', ev);
      break;
    case 'REJECT':
      emit('reject', ev);
      break;
    default:
      break;
  }
}
