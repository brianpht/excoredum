// Thin REST wrapper for the gateway JSON API. Reads carry an implicit identity
// from the UI's current user; admin routes attach X-User-Id from the configured
// admin allow-list.

import { store } from './store.js';

async function req(method, path, body, admin = false) {
  const headers = { Accept: 'application/json' };
  if (body !== undefined) headers['Content-Type'] = 'application/json';
  if (admin && store.adminUid) headers['X-User-Id'] = store.adminUid;
  const res = await fetch(path, {
    method,
    headers,
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await res.text();
  let data = null;
  try {
    data = text ? JSON.parse(text) : null;
  } catch {
    data = text;
  }
  if (!res.ok) {
    const message = (data && data.message) || `${res.status} ${res.statusText}`;
    const err = new Error(message);
    err.status = res.status;
    err.body = data;
    throw err;
  }
  return data;
}

export const get = (p) => req('GET', p);
export const post = (p, body, admin = false) => req('POST', p, body, admin);
export const del = (p) => req('DELETE', p);
export const patch = (p, body, admin = false) => req('PATCH', p, body, admin);

// reads
export const getSymbols = () => get('/api/v1/symbols');
export const getCurrencies = () => get('/api/v1/currencies');
export const getHealth = () => get('/api/v1/health');
export const getConservation = () => get('/api/v1/report/conservation');
export const getOrderBook = (symbolId, maxLevels = 32) =>
  get(`/api/v1/orderbook?symbolId=${symbolId}&maxLevels=${maxLevels}`);
export const getMarketTrades = (symbolId, limit = 100) =>
  get(`/api/v1/markettrades?symbolId=${symbolId}&limit=${limit}`);
export const getUserReport = (uid) => get(`/api/v1/users/${uid}/balances`);
export const getActiveOrders = (uid) => get(`/api/v1/users/${uid}/orders/active`);
export const getOrderHistory = (uid) => get(`/api/v1/users/${uid}/orders`);
export const getUserTrades = (uid, limit = 100) => get(`/api/v1/users/${uid}/trades?limit=${limit}`);
export const getOrder = (orderId) => get(`/api/v1/orders/${orderId}`);

// writes (trading)
export const placeOrder = (body) => post('/api/v1/orders', body);
export const cancelOrder = (orderId, symbolId, uid) =>
  del(`/api/v1/orders/${orderId}?symbolId=${symbolId}&uid=${uid}`);
export const modifyOrder = (orderId, body) => patch(`/api/v1/orders/${orderId}`, body);
export const requestOrderBook = (symbolId, uid) => post(`/api/v1/orderbook/${symbolId}/request`, { uid });

// writes (admin, X-User-Id)
export const addSymbol = (body) => post('/api/v1/symbols', body, true);
export const addUser = (uid) => post('/api/v1/users', { uid }, true);
export const adjustBalance = (uid, body) => post(`/api/v1/users/${uid}/balance`, body, true);
export const suspendUser = (uid) => post(`/api/v1/users/${uid}/suspend`, {}, true);
export const resumeUser = (uid) => post(`/api/v1/users/${uid}/resume`, {}, true);
