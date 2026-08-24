// Fixed-point to display helpers. The engine stores price/size/amounts as 64-bit
// ints scaled by quoteScaleK (price), baseScaleK (size), or the currency scaleK
// (balances). These helpers divide by the scale to render human-readable numbers.

const MAX_DIGITS = 8;

export function digitsFor(scaleK) {
  const k = Number(scaleK);
  if (!k || k <= 0) return 0;
  return Math.max(0, Math.min(Math.round(Math.log10(k)), MAX_DIGITS));
}

export function fmt(raw, scaleK, maxDigits = MAX_DIGITS) {
  const value = Number(raw) / Number(scaleK || 1);
  const digits = Math.min(maxDigits, Math.max(0, digitsFor(scaleK)));
  return value.toLocaleString('en-US', { minimumFractionDigits: 0, maximumFractionDigits: digits });
}

export function int(raw) {
  return Number(raw).toLocaleString('en-US');
}

export function price(raw, sym) {
  return sym ? fmt(raw, sym.quoteScaleK) : fmt(raw, 1);
}

export function size(raw, sym) {
  return sym ? fmt(raw, sym.baseScaleK) : fmt(raw, 1);
}

export function amt(raw, cur) {
  return cur ? fmt(raw, cur.scaleK) : fmt(raw, 1);
}

export function formatPrice(raw, sym) {
  return price(raw, sym);
}

export function time(ts) {
  const d = new Date(Number(ts));
  const p = (n, w = 2) => String(n).padStart(w, '0');
  return `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}.${p(d.getMilliseconds(), 3)}`;
}

export function shortTime(ts) {
  const d = new Date(Number(ts));
  const p = (n) => String(n).padStart(2, '0');
  return `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
}

export function sideClass(side) {
  return side === 'ASK' || side === 'SELL' ? 'red' : 'green';
}

export function sideText(side) {
  return side === 'ASK' || side === 'SELL' ? 'SELL' : 'BUY';
}

export const esc = (s) =>
  String(s == null ? '' : s).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));

export function minusSign(n) {
  return Number(n) < 0 ? '-' : '';
}
