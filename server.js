import 'dotenv/config';
import express from 'express';
import morgan from 'morgan';
import path from 'path';
import { fileURLToPath } from 'url';

import { db } from './src/db.js';
import { ensureSchema, seedDefaults } from './src/schema.js';
import { getOAuthClient, authStart, authCallback, requireConnected } from './src/oauth.js';
import { qboReadItemByName } from './src/qbo.js';
import { syncCustomers, syncCategories } from './src/sync.js';
import { verifyIntuitWebhook, rawBodySaver } from './src/webhooks.js';
import { processInvoice } from './src/processor.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();

// Ensure DB schema exists + seed defaults/locations
ensureSchema();
seedDefaults();

// Views
app.set('views', path.join(__dirname, 'src', 'views'));
app.set('view engine', 'ejs');

// Logging
app.use(morgan('dev'));

// Body parsing (webhooks need raw body)
app.use(express.json({ verify: rawBodySaver, limit: '2mb' }));
app.use(express.urlencoded({ extended: true }));

// Home
app.get('/', (req, res) => {
  const conn = db.getConnection();
  res.render('index', {
    connected: !!conn,
    realmId: conn?.realm_id || null,
    companyName: conn?.company_name || null,
  });
});

// Auth
app.get('/auth/start', authStart);
app.get('/auth/callback', authCallback);

// Admin pages
app.get('/admin', requireConnected, (req, res) => res.redirect('/admin/categories'));

app.get('/admin/sync', requireConnected, async (req, res) => {
  const conn = db.getConnectionOrThrow();
  const oauthClient = getOAuthClient(conn);
  try {
    const customerCount = await syncCustomers(oauthClient, conn.realm_id);
    const categoryCount = await syncCategories(oauthClient, conn.realm_id);

    // Ensure surcharge item id cached
    const surchargeItemName = db.getSetting('surcharge_item_name');
    const surcharge = await qboReadItemByName(oauthClient, conn.realm_id, surchargeItemName);
    if (surcharge?.Id) db.setSetting('surcharge_item_id', surcharge.Id);

    res.render('sync', {
      customerCount,
      categoryCount,
      surchargeItemName,
      surchargeItemId: surcharge?.Id || null
    });
  } catch (e) {
    res.status(500).send(`Sync failed: ${e?.message || e}`);
  }
});

app.get('/admin/categories', requireConnected, (req, res) => {
  const categories = db.listCategoriesOrdered();
  res.render('categories', { categories });
});

app.post('/admin/categories/save', requireConnected, (req, res) => {
  const order = req.body.order;
  const arr = Array.isArray(order) ? order : (order ? [order] : []);
  db.saveCategoryOrder(arr);
  res.redirect('/admin/categories');
});

app.get('/admin/rules', requireConnected, (req, res) => {
  const customers = db.listCustomers();
  const rules = db.listRules();
  res.render('rules', { customers, rules });
});

app.post('/admin/rules/upsert', requireConnected, (req, res) => {
  const body = req.body;
  db.upsertRule({
    id: body.rule_id || null,
    match_type: body.match_type,
    customer_id: body.customer_id || null,
    prefix: body.prefix || null,
    rule_type: body.rule_type,
    threshold: body.threshold ? Number(body.threshold) : null,
    amount: body.amount ? Number(body.amount) : null,
    enabled: body.enabled === 'on' ? 1 : 0
  });
  res.redirect('/admin/rules');
});

app.post('/admin/rules/delete', requireConnected, (req, res) => {
  const { rule_id } = req.body;
  if (rule_id) db.deleteRule(rule_id);
  res.redirect('/admin/rules');
});

// Manual process endpoint (for testing invoice sorting/surcharge)
app.post('/admin/process-invoice', requireConnected, async (req, res) => {
  const { invoice_id } = req.body;
  const conn = db.getConnectionOrThrow();
  const oauthClient = getOAuthClient(conn);
  try {
    const result = await processInvoice({
      oauthClient,
      realmId: conn.realm_id,
      invoiceId: invoice_id,
      source: 'manual'
    });
    res.render('process_result', { result });
  } catch (e) {
    res.status(500).send(`Process failed: ${e?.message || e}`);
  }
});


// ==========================================================
// Inventory UI
// ==========================================================

// Container map
app.get('/inventory/map', requireConnected, (req, res) => {
  const containerNo = Number(req.query.c || 1);
  const containers = db.listContainers();

  const pallets = db.listPalletsInContainer(containerNo);
  const palletByLoc = new Map();
  for (const p of pallets) palletByLoc.set(p.location_code, p);

  // Build two columns (L/R) from door to deep (01..10)
  const left = [];
  const right = [];
  for (let depth = 1; depth <= 10; depth++) {
    const lCode = `C${containerNo}-L${String(depth).padStart(2, '0')}`;
    const rCode = `C${containerNo}-R${String(depth).padStart(2, '0')}`;
    left.push({ code: lCode, pallet: palletByLoc.get(lCode) || null });
    right.push({ code: rCode, pallet: palletByLoc.get(rCode) || null });
  }

  res.render('inventory_map', { containerNo, containers, left, right });
});

// Walk-in list
app.get('/inventory/walkin', requireConnected, (req, res) => {
  const rows = db.listWalkinLoose();
  res.render('inventory_walkin', { rows });
});

// Quick add pallet
app.get('/inventory/add-pallet', requireConnected, (req, res) => {
  const skus = db.listSkus();
  res.render('inventory_add_pallet', { skus, msg: null });
});

app.post('/inventory/add-pallet', requireConnected, (req, res) => {
  try {
    const { sku_id, lot_id, pallet_config_id, location_code, qty_units, notes } = req.body;

    db.createPallet({
      skuId: Number(sku_id),
      lotId: lot_id ? Number(lot_id) : null,
      palletConfigId: pallet_config_id ? Number(pallet_config_id) : null,
      locationCode: location_code,
      qtyUnits: Number(qty_units),
      notes
    });

    // redirect to the container map for that container if location is Cx-...
    const cMatch = (location_code || '').match(/^C(\d+)-/);
    const cNo = cMatch ? Number(cMatch[1]) : 1;
    res.redirect(`/inventory/map?c=${cNo}`);
  } catch (e) {
    const skus = db.listSkus();
    res.status(400).render('inventory_add_pallet', { skus, msg: e?.message || String(e) });
  }
});

// Move pallet between slots
app.post('/inventory/move', requireConnected, (req, res) => {
  const { pallet_id, to_slot, container_no } = req.body;
  try {
    const loc = db.getLocationByCode(to_slot);
    if (!loc) throw new Error(`Destination slot not found: ${to_slot}`);

    db.movePallet(Number(pallet_id), loc.id, 'user');
    res.redirect(`/inventory/map?c=${encodeURIComponent(container_no)}`);
  } catch (e) {
    res.status(500).send(`Move failed: ${e?.message || e}`);
  }
});

// Break pallet into walk-in (pull some qty for picking)
app.post('/inventory/break-to-walkin', requireConnected, (req, res) => {
  const { pallet_id, qty, container_no } = req.body;
  try {
    db.breakPalletToWalkin({
      palletId: Number(pallet_id),
      qty: Number(qty),
      userName: 'user'
    });
    res.redirect(`/inventory/map?c=${encodeURIComponent(container_no)}`);
  } catch (e) {
    res.status(500).send(`Break failed: ${e?.message || e}`);
  }
});


// Webhook endpoint
app.post('/webhooks/qbo', verifyIntuitWebhook, async (req, res) => {
  // Always respond quickly
  res.status(200).send('OK');

  try {
    const conn = db.getConnection();
    if (!conn) return;

    const oauthClient = getOAuthClient(conn);
    const payload = req.body;

    if (process.env.DEBUG_WEBHOOKS === '1') {
      db.addLog({
        invoice_id: null,
        customer_name: null,
        action: 'webhook_payload',
        detail: JSON.stringify(payload).slice(0, 5000),
        source: 'webhook'
      });
    }

    const notifications = payload?.eventNotifications || [];
    for (const n of notifications) {
      const entities = n?.dataChangeEvent?.entities || [];
      for (const ent of entities) {
        if (ent?.name === 'Invoice' && ent?.id) {
          const invoiceId = ent.id;
          processInvoice({ oauthClient, realmId: conn.realm_id, invoiceId, source: 'webhook' })
            .catch(err => {
              db.addLog({
                invoice_id: invoiceId,
                customer_name: null,
                action: 'error',
                detail: `Processing failed: ${err?.message || err}`,
                source: 'webhook'
              });
            });
        }
      }
    }
  } catch (e) {
    // response already sent
  }
});

const port = process.env.PORT || 3000;
app.listen(port, () => console.log(`App running on http://localhost:${port}`));
