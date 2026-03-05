import { db } from './db.js';
import { qboQuery } from './qbo.js';

export async function syncCustomers(oauthClient, realmId) {
  let start = 1;
  const pageSize = 1000;
  let total = 0;

  while (true) {
    const q = `select Id, DisplayName, PrimaryEmailAddr from Customer startposition ${start} maxresults ${pageSize}`;
    const r = await qboQuery(oauthClient, realmId, q);
    const customers = r?.QueryResponse?.Customer || [];
    for (const c of customers) {
      db.upsertCustomer({
        id: c.Id,
        display_name: c.DisplayName || c.FullyQualifiedName || c.Id,
        qbo_email: c.PrimaryEmailAddr?.Address || null
      });
    }
    total += customers.length;
    if (customers.length < pageSize) break;
    start += pageSize;
  }
  return total;
}

export async function syncCategories(oauthClient, realmId) {
  let start = 1;
  const pageSize = 1000;
  let total = 0;

  while (true) {
    const q = `select Id, Name, Type from Item where Type = 'Category' startposition ${start} maxresults ${pageSize}`;
    const r = await qboQuery(oauthClient, realmId, q);
    const cats = r?.QueryResponse?.Item || [];
    for (const c of cats) {
      db.upsertCategory({ id: c.Id, name: c.Name || `Category ${c.Id}` });
    }
    total += cats.length;
    if (cats.length < pageSize) break;
    start += pageSize;
  }
  return total;
}
