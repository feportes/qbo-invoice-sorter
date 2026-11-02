import crypto from 'crypto';

const BASE = (realmId) => `https://quickbooks.api.intuit.com/v3/company/${realmId}`;

export async function qboInvoicePdf(oauthClient,realmId,invoiceId){
  if(!/^\d{1,30}$/.test(String(invoiceId)))throw Error('Invalid QuickBooks invoice identity.');
  const token=oauthClient.getToken().access_token;
  const response=await fetch(`${BASE(realmId)}/invoice/${invoiceId}/pdf`,{headers:{Authorization:'Bearer '+token,Accept:'application/pdf'},signal:AbortSignal.timeout(30000),redirect:'error'});
  if(!response.ok)throw Error('QuickBooks could not provide the invoice PDF.');
  if(Number(response.headers.get('content-length')||0)>10000000)throw Error('Invoice PDF is too large.');
  const reader=response.body.getReader(),parts=[];let size=0;
  for(;;){const {value,done}=await reader.read();if(done)break;size+=value.length;if(size>10000000){await reader.cancel();throw Error('Invoice PDF is too large.');}parts.push(Buffer.from(value));}
  const bytes=Buffer.concat(parts);if(!bytes.subarray(0,1024).includes(Buffer.from('%PDF-')))throw Error('QuickBooks did not return a PDF.');return bytes;
}

// Generic JSON fetch using intuit-oauth makeApiCall
export async function qboFetchJson(oauthClient, { url, method = 'GET', body = null, headers = {} }) {
  const opts = { url, method, headers: { Accept: 'application/json', ...headers } };
  if (body) opts.body = body;

  const resp = await oauthClient.makeApiCall(opts);

  // Different versions/envs return different shapes:
  if (resp && typeof resp.getJson === 'function') return resp.getJson();
  if (resp && resp.json) return resp.json;
  if (resp && typeof resp.body === 'string') {
    try { return JSON.parse(resp.body); } catch {}
  }

  if (resp && resp.response && typeof resp.response.body === 'string') {
    try { return JSON.parse(resp.response.body); } catch {}
  }

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

export async function qboSendInvoice(oauthClient, realmId, invoiceId) {
  // Common QBO send endpoint
  const url = `${BASE(realmId)}/invoice/${invoiceId}/send?minorversion=75`;
  // Intuit uses POST for send actions
  return qboFetchJson(oauthClient, { url, method: 'POST' });
}

/**
 * QBO sometimes fires a webhook before the Invoice is immediately readable via the Read endpoint.
 * This retries on "not found" responses with exponential-ish backoff.
 */
export async function qboReadInvoiceWithRetry(oauthClient, realmId, invoiceId, tries = 6) {
  let lastErr = null;

  for (let i = 0; i < tries; i++) {
    try {
      return await qboReadInvoice(oauthClient, realmId, invoiceId);
    } catch (e) {
      lastErr = e;
      const msg = (e?.message || String(e)).toLowerCase();

      const isNotFound =
        msg.includes('not found') ||
        msg.includes('404') ||
        msg.includes('object not found');

      if (!isNotFound) throw e;

      // 0.5s, 1s, 2s, 3s, 5s, 8s
      const delays = [500, 1000, 2000, 3000, 5000, 8000];
      const delay = delays[Math.min(i, delays.length - 1)];
      await new Promise(r => setTimeout(r, delay));
    }
  }

  throw lastErr || new Error(`Invoice not found after retry: ${invoiceId}`);
}

export async function qboUpdateInvoice(oauthClient, realmId, payload) {
  const url = `${BASE(realmId)}/invoice?minorversion=75`;
  return qboFetchJson(oauthClient, {
    url,
    method: 'POST',
    body: JSON.stringify(payload),
    headers: { 'Content-Type': 'application/json' }
  });
}

export async function qboReadCustomer(oauthClient, realmId, customerId) {
  const url = `${BASE(realmId)}/customer/${customerId}?minorversion=75`;
  return qboFetchJson(oauthClient, { url });
}

export async function qboReadItemByName(oauthClient, realmId, itemName) {
  const safe = String(itemName).replace(/'/g, "\\'");
  const q = `select * from Item where Name = '${safe}' maxresults 1`;
  const r = await qboQuery(oauthClient, realmId, q);
  return r?.QueryResponse?.Item?.[0] || null;
}

export async function qboBatchReadItems(oauthClient, realmId, ids) {
  // Batch supports read operations; chunk to 30 items per batch to be safe.
  const out = new Map();
  const chunkSize = 30;

  for (let i = 0; i < ids.length; i += chunkSize) {
    const chunk = ids.slice(i, i + chunkSize);
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

