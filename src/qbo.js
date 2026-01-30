import OAuthClient from 'intuit-oauth';
import { db } from './db.js';
import { withFreshClient } from './oauth.js';

const BASE = (realmId) => `https://quickbooks.api.intuit.com/v3/company/${realmId}`;

// Generic JSON fetch using intuit-oauth makeApiCall
export async function qboFetchJson(oauthClient, { url, method='GET', body=null, headers={} }) {
  const opts = { url, method, headers: { 'Accept': 'application/json', ...headers } };
  if (body) opts.body = body;

  const resp = await oauthClient.makeApiCall(opts);

  // Different versions/envs return different shapes:
  if (resp && typeof resp.getJson === 'function') return resp.getJson();
  if (resp && resp.json) return resp.json;
  if (resp && typeof resp.body === 'string') return JSON.parse(resp.body);

  throw new Error('Unexpected response format from Intuit makeApiCall');
}


export async function qboQuery(oauthClient, realmId, query) {
  const url = `${BASE(realmId)}/query?query=${encodeURIComponent(query)}&minorversion=75`;
  return qboFetchJson(oauthClient, { url });
}

export async function qboReadInvoice(oauthClient, realmId, invoiceId) {
  const url = `${BASE(realmId)}/invoice/${invoiceId}?minorversion=75`;
  return qboFetchJson(oauthClient, { url });
}

export async function qboUpdateInvoice(oauthClient, realmId, payload) {
  const url = `${BASE(realmId)}/invoice?minorversion=75`;
  return qboFetchJson(oauthClient, { url, method: 'POST', body: JSON.stringify(payload), headers: { 'Content-Type': 'application/json' } });
}

export async function qboReadCustomer(oauthClient, realmId, customerId) {
  const url = `${BASE(realmId)}/customer/${customerId}?minorversion=75`;
  return qboFetchJson(oauthClient, { url });
}

export async function qboReadItemByName(oauthClient, realmId, itemName) {
  // Query by name
  const q = `select * from Item where Name = '${itemName.replace(/'/g, "\'")}' maxresults 1`;
  const r = await qboQuery(oauthClient, realmId, q);
  return r?.QueryResponse?.Item?.[0] || null;
}

export async function qboBatchReadItems(oauthClient, realmId, ids) {
  // Batch supports read operations; we chunk to 30 items per batch to be safe.
  const out = new Map();
  const chunkSize = 30;

  for (let i=0; i<ids.length; i+=chunkSize) {
    const chunk = ids.slice(i, i+chunkSize);
    const body = {
      BatchItemRequest: chunk.map((id, idx) => ({
        bId: `item_${i}_${idx}`,
        operation: 'read',
        Item: { Id: id }
      }))
    };

    const url = `${BASE(realmId)}/batch?minorversion=4`;
    const resp = await qboFetchJson(oauthClient, {
      url,
      method: 'POST',
      body: JSON.stringify(body),
      headers: { 'Content-Type': 'application/json' }
    });

    const arr = resp?.BatchItemResponse || [];
    for (const r of arr) {
      const item = r?.Item;
      if (item?.Id) out.set(item.Id, item);
    }
  }
  return out;
}
