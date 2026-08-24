// Shared UI state plus a tiny event emitter. Symbols and currencies come from the
// config-driven gateway API; market data (L2, tape, trades) streams over the
// WebSocket and is filtered by the selected symbol here (the gateway broadcast-all).

export const store = {
  symbols: [],
  currencies: [],
  currencyMap: new Map(),
  selectedSymbolId: null,
  currentUid: 1,
  adminUid: '',
  l2: new Map(), // symbolId -> {asks[], bids[], appliedPosition}
  tape: new Map(), // symbolId -> [ {timestamp, price, size, ...} ]
  wsConnected: false,
};

const listeners = new Map();

export function on(event, fn) {
  let set = listeners.get(event);
  if (!set) {
    set = new Set();
    listeners.set(event, set);
  }
  set.add(fn);
  return () => set.delete(fn);
}

export function emit(event, payload) {
  const set = listeners.get(event);
  if (!set) return;
  for (const fn of set) fn(payload);
}

export function selectedSymbol() {
  return store.symbols.find((s) => s.symbolId === store.selectedSymbolId) || store.symbols[0] || null;
}

export function currency(id) {
  return store.currencyMap.get(Number(id)) || null;
}

export function setSymbols(symbols) {
  store.symbols = symbols || [];
  if (!store.selectedSymbolId && store.symbols.length) store.selectedSymbolId = store.symbols[0].symbolId;
  emit('symbols', store.symbols);
}

export function setCurrencies(currencies) {
  store.currencies = currencies || [];
  store.currencyMap = new Map(store.currencies.map((c) => [c.id, c]));
  emit('currencies', store.currencies);
}

export function setSelectedSymbol(id) {
  const next = Number(id);
  if (store.selectedSymbolId === next) return;
  store.selectedSymbolId = next;
  emit('symbol', next);
}

export function setL2(symbolId, snapshot) {
  const key = String(symbolId);
  store.l2.set(key, snapshot);
  if (Number(symbolId) === store.selectedSymbolId) emit('l2', snapshot);
}

export function l2For(symbolId) {
  return store.l2.get(String(symbolId)) || null;
}

export function appendTape(symbolId, items) {
  const key = String(symbolId);
  const list = store.tape.get(key) || [];
  store.tape.set(key, items.concat(list).slice(0, 200));
  if (Number(symbolId) === store.selectedSymbolId) emit('tape', store.tape.get(key));
}

export function tapeFor(symbolId) {
  return store.tape.get(String(symbolId)) || [];
}
