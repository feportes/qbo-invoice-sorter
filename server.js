import 'dotenv/config';
import express from 'express';
import morgan from 'morgan';
import path from 'path';
import { fileURLToPath } from 'url';

import { db } from './src/db.js';
import { ensureSchema, seedDefaults } from './src/schema.js';
import { getOAuthClient, authStart, authCallback, requireConnected } from './src/oauth.js';
import { qboReadItemByName, qboQuery, qboReadInvoiceWithRetry } from './src/qbo.js';
import { syncCustomers, syncCategories } from './src/sync.js';
import { verifyIntuitWebhook, rawBodySaver } from './src/webhooks.js';
import { processInvoice } from './src/processor.js';
import { runAutoAllocateForInvoice } from './src/inventory_engine.js';

// ✅ Inventory allocation engine (you created this file)
import { buildPlanFromInvoice, applyPlan } from './src/inventory_allocate.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();

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

// ==========================================================
// Helper: retry processInvoice on "Invoice not found"
// ==========================================================
async function processInvoiceWithRetry({ oauthClient, realmId, invoiceId, source, retries = 6 }) {
  let lastErr = null;

  for (let i = 0; i < retries; i++) {
    try {
      return await processInvoice({ oauthClient, realmId, invoiceId, source });
    } catch (e) {
      lastErr = e;
      const msg = String(e?.message || e).toLowerCase();
      const isNotFound = msg.includes('invoice not found') || msg.includes('not found') || msg.includes('404');

      if (!isNotFound) throw e;

      // backoff: 1s, 2s, 3s, 5s, 8s...
      const delays = [1000, 2000, 3000, 5000, 8000, 12000];
      const delay = delays[Math.min(i, delays.length - 1)];
      await new Promise(r => setTimeout(r, delay));
    }
  }

  throw lastErr || new Error(`Invoice not found after retry: ${invoiceId}`);
}

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

// ==========================================================
// ADMIN (invoice sorter UI) — RESTORED
// ==========================================================
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

app.post('/admin/process-invoice', requireConnected, async (req, res) => {
  const { invoice_id } = req.body;
  const conn = db.getConnectionOrThrow();
  const oauthClient = getOAuthClient(conn);

  try {
    const result = await processInvoiceWithRetry({
      oauthClient,
      realmId: conn.realm_id,
      invoiceId: invoice_id,
      source: 'manual',
      retries: 6
    });

    res.render('process_result', { result });
  } catch (e) {
    res.status(500).send(`Process failed: ${e?.message || e}`);
  }
});

// Debug endpoint (optional)
app.get('/admin/qbo-items-check', requireConnected, async (req, res) => {
  const conn = db.getConnectionOrThrow();
  const oauthClient = getOAuthClient(conn);

  try {
    const q = `select Id, Name, Type, Active from Item startposition 1 maxresults 25`;
    const r = await qboQuery(oauthClient, conn.realm_id, q);

    const items = r?.QueryResponse?.Item || [];
    const meta = db.getConnection()
      ? {
          realm_id: conn.realm_id,
          company_name: conn.company_name || null,
          returned_count: items.length,
          startPosition: r?.QueryResponse?.startPosition ?? null,
          maxResults: r?.QueryResponse?.maxResults ?? null,
          totalCount: r?.QueryResponse?.totalCount ?? null
        }
      : null;

    const preview = items.slice(0, 10).map(it => ({
      Id: it.Id,
      Name: it.Name,
      Type: it.Type,
      Active: it.Active
    }));

    res.status(200).send(`<pre>${JSON.stringify({ meta, preview }, null, 2)}</pre>`);
  } catch (e) {
    res.status(500).send(`QBO items check failed: ${e?.message || e}`);
  }
});

// ==========================================================
// INVENTORY — container settings, SKU settings, yard/map
// ==========================================================

// Container Settings (mode + flip)
app.get('/inventory/settings/containers', requireConnected, (req, res) => {
  const current = {};
  for (let c = 1; c <= 7; c++) {
    const modeKey = `container_mode_C${c}`;
    const flipKey = `container_flip_C${c}`;
    const modeDefault = (c === 1) ? '8-slot' : '18-slot';
    const flipDefault = 'L_LONG';
    const mode = db.getSetting(modeKey) || modeDefault;
    const flip = db.getSetting(flipKey) || flipDefault;
    const label = db.getContainerDepths(c).label;
    current[`C${c}`] = { mode, flip, label };
  }
  res.render('inventory_container_settings', { current, msg: null });
});

app.post('/inventory/settings/containers', requireConnected, (req, res) => {
  try {
    const validC1 = new Set(['8-slot', '9-slot']);
    const valid40 = new Set(['18-slot', '20-slot']);
    const validFlip = new Set(['L_LONG', 'R_LONG']);

    const modeC1 = req.body.mode_C1;
    const flipC1 = req.body.flip_C1;
    if (!validC1.has(modeC1)) throw new Error('Invalid mode for C1');
    if (!validFlip.has(flipC1)) throw new Error('Invalid flip for C1');
    db.setSetting('container_mode_C1', modeC1);
    db.setSetting('container_flip_C1', flipC1);

    for (let c = 2; c <= 7; c++) {
      const mode = req.body[`mode_C${c}`];
      const flip = req.body[`flip_C${c}`];
      if (!valid40.has(mode)) throw new Error(`Invalid mode for C${c}`);
      if (!validFlip.has(flip)) throw new Error(`Invalid flip for C${c}`);
      db.setSetting(`container_mode_C${c}`, mode);
      db.setSetting(`container_flip_C${c}`, flip);
    }

    const current = {};
    for (let c = 1; c <= 7; c++) {
      const label = db.getContainerDepths(c).label;
      current[`C${c}`] = {
        mode: db.getSetting(`container_mode_C${c}`),
        flip: db.getSetting(`container_flip_C${c}`),
        label
      };
    }

    res.render('inventory_container_settings', { current, msg: 'Saved successfully.' });
  } catch (e) {
    const current = {};
    for (let c = 1; c <= 7; c++) {
      const label = db.getContainerDepths(c).label;
      current[`C${c}`] = {
        mode: db.getSetting(`container_mode_C${c}`) || ((c === 1) ? '8-slot' : '18-slot'),
        flip: db.getSetting(`container_flip_C${c}`) || 'L_LONG',
        label
      };
    }
    res.status(400).render('inventory_container_settings', { current, msg: e?.message || String(e) });
  }
});

// ==========================================================
// Inventory: Add Pallet (manual receive)
// ==========================================================
app.get('/inventory/add-pallet', requireConnected, (req, res) => {
  const containerNo = Number(req.query.c || 1);
  const containers = db.listContainers();
  const slotOptions = db.listValidSlotCodes(containerNo);
  const skus = db.listSkusAllFiltered({ categoryId: 'all' });
  res.render('inventory_add_pallet', { msg: null, containers, containerNo, slotOptions, skus });
});

app.post('/inventory/add-pallet', requireConnected, (req, res) => {
  try {
    const containerNo = Number(req.body.container_no || 1);
    const skuId = Number(req.body.sku_id);
    const locationCode = String(req.body.location_code || '').trim();
    const qtyUnits = Number(req.body.qty_units);

    if (!skuId) throw new Error('SKU is required');
    if (!locationCode) throw new Error('Location is required');
    if (!Number.isFinite(qtyUnits) || qtyUnits <= 0) throw new Error('Qty must be > 0');

    // optional fields
    const lotId = req.body.lot_id ? Number(req.body.lot_id) : null;
    const palletConfigId = req.body.pallet_config_id ? Number(req.body.pallet_config_id) : null;
    const notes = req.body.notes ? String(req.body.notes) : null;

    db.createPallet({
      skuId,
      lotId,
      palletConfigId,
      locationCode,
      qtyUnits,
      notes
    });

    // go back to the container map automatically
    res.redirect(`/inventory/map?c=${containerNo}`);
  } catch (e) {
    const containerNo = Number(req.body.container_no || 1);
    const containers = db.listContainers();
    const slotOptions = db.listValidSlotCodes(containerNo);
    const skus = db.listSkusAllFiltered({ categoryId: 'all' });
    res.status(400).render('inventory_add_pallet', { msg: e?.message || String(e), containers, containerNo, slotOptions, skus });
  }
});


// Map
app.get('/inventory/map', requireConnected, (req, res) => {
  const containerNo = Number(req.query.c || 1);
  const containers = db.listContainers();

  const pallets = db.listPalletsInContainer(containerNo);
  const palletByLoc = new Map();
  for (const p of pallets) palletByLoc.set(p.location_code, p);

  const depths = db.getContainerDepths(containerNo);
  const left = [];
  const right = [];

  for (let d = 1; d <= depths.leftMax; d++) {
    const code = `C${containerNo}-L${String(d).padStart(2, '0')}`;
    left.push({ code, pallet: palletByLoc.get(code) || null });
  }
  for (let d = 1; d <= depths.rightMax; d++) {
    const code = `C${containerNo}-R${String(d).padStart(2, '0')}`;
    right.push({ code, pallet: palletByLoc.get(code) || null });
  }

  const slotOptions = [...db.listValidSlotCodes(containerNo), 'WALKIN', 'RETURNS'];
  const c1Mode = (containerNo === 1) ? (db.getSetting('container_mode_C1') || '8-slot') : null;

  res.render('inventory_map', {
    containerNo,
    containers,
    left,
    right,
    containerLabel: depths.label,
    slotOptions,
    c1Mode
  });
});

// ==========================================================
// Inventory Allocation (Preview + Apply)  <-- NO LOGIN for now
// ==========================================================
app.get('/inventory/allocate', async (req, res) => {
  const conn = db.getConnection();
  return res.render('inventory_allocate', { connected: !!conn, msg: null, plan: null });
});

app.post('/inventory/allocate/preview', async (req, res) => {
  try {
    const invoiceId = String(req.body.invoice_id || '').trim();
    if (!invoiceId) throw new Error('Missing invoice id');

    const conn = db.getConnectionOrThrow();
    const oauthClient = getOAuthClient(conn);

    const invResp = await qboReadInvoiceWithRetry(oauthClient, conn.realm_id, invoiceId);
    const invoice = invResp?.Invoice;
    if (!invoice) throw new Error(`Invoice not found: ${invoiceId}`);

    const plan = buildPlanFromInvoice(invoice);

    return res.render('inventory_allocate', { connected: true, msg: null, plan });
  } catch (e) {
    const conn = db.getConnection();
    return res.status(400).render('inventory_allocate', { connected: !!conn, msg: e?.message || String(e), plan: null });
  }
});

app.post('/inventory/allocate/apply', async (req, res) => {
  try {
    const invoiceId = String(req.body.invoice_id || '').trim();
    if (!invoiceId) throw new Error('Missing invoice id');

    const conn = db.getConnectionOrThrow();
    const oauthClient = getOAuthClient(conn);

    const invResp = await qboReadInvoiceWithRetry(oauthClient, conn.realm_id, invoiceId);
    const invoice = invResp?.Invoice;
    if (!invoice) throw new Error(`Invoice not found: ${invoiceId}`);

    const plan = buildPlanFromInvoice(invoice);
    applyPlan(plan);

    return res.render('inventory_allocate', { connected: true, msg: '✅ Allocation applied successfully.', plan });
  } catch (e) {
    const conn = db.getConnection();
    return res.status(400).render('inventory_allocate', { connected: !!conn, msg: e?.message || String(e), plan: null });
  }
});

// ==========================================================
// Webhook endpoint (invoice sorter/surcharge) + auto-allocation
// ==========================================================
app.post('/webhooks/qbo', verifyIntuitWebhook, async (req, res) => {
  res.status(200).send('OK');

  try {
    const conn = db.getConnection();
    if (!conn) {
      console.log('[webhook] skip: no connection');
      return;
    }

    const oauthClient = getOAuthClient(conn);
    const payload = req.body;

    const notifications = payload?.eventNotifications || [];
    const invoiceIds = [];

    for (const n of notifications) {
      const entities = n?.dataChangeEvent?.entities || [];
      for (const ent of entities) {
        if (ent?.name === 'Invoice' && ent?.id) invoiceIds.push(String(ent.id));
      }
    }

    console.log(`[webhook] received invoices=${invoiceIds.length} ids=${invoiceIds.slice(0, 20).join(',')}`);

    for (const invoiceId of invoiceIds) {
      processInvoiceWithRetry({ oauthClient, realmId: conn.realm_id, invoiceId, source: 'webhook', retries: 6 })
        .then(async () => {
          console.log(`[webhook] processed ok invoiceId=${invoiceId}`);

          if (db.getAutoAllocateEnabled()) {
            try {
              const invResp = await qboReadInvoiceWithRetry(oauthClient, conn.realm_id, invoiceId);
              const invoice = invResp?.Invoice;
              if (invoice) {
                await runAutoAllocateForInvoice({ invoice, invoiceId });
                console.log(`[inv_engine] ok invoiceId=${invoiceId}`);
              }
            } catch (e) {
              console.log(`[inv_engine] error invoiceId=${invoiceId} err=${e?.message || e}`);
            }
          }
        })
        .catch(err => console.log(`[webhook] processed error invoiceId=${invoiceId} err=${err?.message || err}`));
    }
  } catch (e) {
    console.log('[webhook] fatal error', e?.message || e);
  }
});

// ==========================================================
// Inventory Engine Settings (toggle)
// ==========================================================
app.get('/inventory/settings/engine', (req, res) => {
  const enabled = db.getAutoAllocateEnabled();
  const timezone = db.getSetting('inventory_timezone') || 'America/Los_Angeles';
  res.render('inventory_engine_settings', { enabled, timezone, msg: null });
});

app.post('/inventory/settings/engine', (req, res) => {
  const enabled = req.body.enabled === 'on';
  db.setAutoAllocateEnabled(enabled);
  const timezone = db.getSetting('inventory_timezone') || 'America/Los_Angeles';
  res.render('inventory_engine_settings', { enabled, timezone, msg: 'Saved.' });
});

// ==========================================================
// Inventory: SKU Settings (category filter)
// ==========================================================
app.get('/inventory/settings/skus', requireConnected, (req, res) => {
  const selectedCat = (req.query.cat || 'all').toString();
  const categories = db.listCategoriesOrdered();
  const skus = db.listSkusAllFiltered({ categoryId: selectedCat });
  res.render('inventory_sku_settings', { skus, msg: null, categories, selectedCat });
});

// Bulk save selected SKU rows (Active / Lot / Organic / Unit / Threshold)
app.post('/inventory/settings/skus/bulk-save', requireConnected, (req, res) => {
  try {
    const selected = req.body.selected_sku_ids;
    const selectedCat = (req.body.selectedCat || 'all').toString();
    const ids = Array.isArray(selected) ? selected.map(Number) : (selected ? [Number(selected)] : []);

    if (ids.length === 0) throw new Error('No SKUs selected.');

    for (const skuId of ids) {
      const active = req.body[`active_${skuId}`] === 'on' ? 1 : 0;
      const is_lot_tracked = req.body[`is_lot_tracked_${skuId}`] === 'on' ? 1 : 0;
      const is_organic = req.body[`is_organic_${skuId}`] === 'on' ? 1 : 0;
      const unit_type = (req.body[`unit_type_${skuId}`] || 'unit').toString();

      let threshold = req.body[`pallet_pick_threshold_${skuId}`];
      threshold = (threshold === undefined || threshold === null || String(threshold).trim() === '')
        ? null
        : Number(threshold);

      if (threshold !== null && (threshold < 0.1 || threshold > 1.0)) {
        throw new Error(`Pallet threshold must be between 0.10 and 1.00 (or blank). SKU ${skuId}`);
      }

      db.updateSkuSettings({
        sku_id: skuId,
        active,
        is_organic,
        is_lot_tracked,
        unit_type,
        pallet_pick_threshold: threshold
      });
    }

    const categories = db.listCategoriesOrdered();
    const skus = db.listSkusAllFiltered({ categoryId: selectedCat });

    res.render('inventory_sku_settings', {
      skus,
      msg: `Saved ${ids.length} selected SKU(s).`,
      categories,
      selectedCat
    });
  } catch (e) {
    const selectedCat = (req.body.selectedCat || 'all').toString();
    const categories = db.listCategoriesOrdered();
    const skus = db.listSkusAllFiltered({ categoryId: selectedCat });

    res.status(400).render('inventory_sku_settings', {
      skus,
      msg: e?.message || String(e),
      categories,
      selectedCat
    });
  }
});


// Bulk update SKUs (Active / Lot / Organic) for current filter
app.post('/inventory/settings/skus/bulk', requireConnected, (req, res) => {
  try {
    const selectedCat = (req.body.selectedCat || 'all').toString();
    const action = (req.body.action || '').toString();

    // action format:
    // active:on | active:off
    // lot:on | lot:off
    // organic:on | organic:off
    // active:off_all  (ignores selectedCat, applies to all)
    const [field, value] = action.split(':');

    if (!field || !value) throw new Error('Invalid bulk action');

    const applyAll = value === 'off_all';
    const cat = applyAll ? 'all' : selectedCat;

    const rows = db.listSkusAllFiltered({ categoryId: cat });

    let updates = 0;
    for (const sku of rows) {
      const patch = {
        sku_id: sku.id,
        active: sku.active,
        is_organic: sku.is_organic,
        is_lot_tracked: sku.is_lot_tracked,
        unit_type: sku.unit_type || 'unit',
        pallet_pick_threshold: sku.pallet_pick_threshold
      };

      if (field === 'active') patch.active = (value === 'on') ? 1 : 0;
      else if (field === 'lot') patch.is_lot_tracked = (value === 'on') ? 1 : 0;
      else if (field === 'organic') patch.is_organic = (value === 'on') ? 1 : 0;
      else throw new Error('Unknown bulk field');

      db.updateSkuSettings(patch);
      updates++;
    }

    const categories = db.listCategoriesOrdered();
    const skus = db.listSkusAllFiltered({ categoryId: selectedCat });

    res.render('inventory_sku_settings', {
      skus,
      msg: `Bulk update applied to ${updates} SKUs (${applyAll ? 'ALL categories' : selectedCat}).`,
      categories,
      selectedCat
    });
  } catch (e) {
    const selectedCat = (req.body.selectedCat || 'all').toString();
    const categories = db.listCategoriesOrdered();
    const skus = db.listSkusAllFiltered({ categoryId: selectedCat });

    res.status(400).render('inventory_sku_settings', {
      skus,
      msg: e?.message || String(e),
      categories,
      selectedCat
    });
  }
});


app.post('/inventory/settings/skus/sync', requireConnected, async (req, res) => {
  const conn = db.getConnectionOrThrow();
  const oauthClient = getOAuthClient(conn);

  try {
    let start = 1;
    const pageSize = 1000;

    while (true) {
      const q = `select Id, Name, Type, Active, ParentRef from Item startposition ${start} maxresults ${pageSize}`;
      const r = await qboQuery(oauthClient, conn.realm_id, q);
      const items = r?.QueryResponse?.Item || [];

      for (const it of items) {
        if (!it?.Id || !it?.Name) continue;
        if (String(it.Type || '').toLowerCase() === 'category') continue;

        const parentCatId = it?.ParentRef?.value ? String(it.ParentRef.value) : null;
        db.upsertSkuFromQbo({
          qbo_item_id: String(it.Id),
          name: String(it.Name),
          qbo_category_id: parentCatId
        });
      }

      if (items.length < pageSize) break;
      start += pageSize;
    }

    const categories = db.listCategoriesOrdered();
    const selectedCat = 'all';
    const skus = db.listSkusAllFiltered({ categoryId: selectedCat });
    res.render('inventory_sku_settings', { skus, msg: 'Synced items from QuickBooks.', categories, selectedCat });
  } catch (e) {
    const categories = db.listCategoriesOrdered();
    const selectedCat = 'all';
    const skus = db.listSkusAllFiltered({ categoryId: selectedCat });
    res.status(500).render('inventory_sku_settings', { skus, msg: `Sync failed: ${e?.message || e}`, categories, selectedCat });
  }
});

app.post('/inventory/settings/skus/update', requireConnected, (req, res) => {
  try {
    const sku_id = Number(req.body.sku_id);

    const active = req.body.active === 'on' ? 1 : 0;
    const is_lot_tracked = req.body.is_lot_tracked === 'on' ? 1 : 0;
    const is_organic = req.body.is_organic === 'on' ? 1 : 0;
    const unit_type = (req.body.unit_type || 'unit').toString();

    let threshold = req.body.pallet_pick_threshold;
    threshold = (threshold === undefined || threshold === null || String(threshold).trim() === '')
      ? null
      : Number(threshold);

    if (threshold !== null && (threshold < 0.1 || threshold > 1.0)) {
      throw new Error('Pallet threshold must be between 0.10 and 1.00 (or blank).');
    }

    db.updateSkuSettings({
      sku_id,
      active,
      is_organic,
      is_lot_tracked,
      unit_type,
      pallet_pick_threshold: threshold
    });

    const categories = db.listCategoriesOrdered();
    const selectedCat = (req.body.selectedCat || 'all').toString();
    const skus = db.listSkusAllFiltered({ categoryId: selectedCat });
    res.render('inventory_sku_settings', { skus, msg: 'Saved SKU settings.', categories, selectedCat });
  } catch (e) {
    const categories = db.listCategoriesOrdered();
    const selectedCat = (req.body.selectedCat || 'all').toString();
    const skus = db.listSkusAllFiltered({ categoryId: selectedCat });
    res.status(400).render('inventory_sku_settings', { skus, msg: e?.message || String(e), categories, selectedCat });
  }
});

// ==========================================================
// Inventory: Pallet Configs UI
// ==========================================================
app.get('/inventory/settings/pallet-configs', requireConnected, (req, res) => {
  const skus = db.listSkusAllFiltered({ categoryId: 'all' });
  const configs = db.listPalletConfigsAll();
  res.render('inventory_pallet_configs', { skus, configs, msg: null });
});

app.post('/inventory/settings/pallet-configs/add', requireConnected, (req, res) => {
  try {
    const sku_id = Number(req.body.sku_id);
    const name = String(req.body.name || '').trim();
    const units_per_pallet = Number(req.body.units_per_pallet);

    if (!sku_id) throw new Error('SKU is required');
    if (!name) throw new Error('Name is required');
    if (!Number.isFinite(units_per_pallet) || units_per_pallet <= 0) throw new Error('Units per pallet must be > 0');

    const ti = req.body.ti ? Number(req.body.ti) : null;
    const hi = req.body.hi ? Number(req.body.hi) : null;
    const is_default = req.body.is_default === 'on';
    const notes = req.body.notes ? String(req.body.notes) : null;

    db.addPalletConfig({ sku_id, name, ti, hi, units_per_pallet, is_default, notes });

    const skus = db.listSkusAllFiltered({ categoryId: 'all' });
    const configs = db.listPalletConfigsAll();
    res.render('inventory_pallet_configs', { skus, configs, msg: 'Added pallet config.' });
  } catch (e) {
    const skus = db.listSkusAllFiltered({ categoryId: 'all' });
    const configs = db.listPalletConfigsAll();
    res.status(400).render('inventory_pallet_configs', { skus, configs, msg: e?.message || String(e) });
  }
});

app.post('/inventory/settings/pallet-configs/update', requireConnected, (req, res) => {
  try {
    const id = Number(req.body.id);
    const sku_id = Number(req.body.sku_id);
    const name = String(req.body.name || '').trim();
    const units_per_pallet = Number(req.body.units_per_pallet);

    if (!id) throw new Error('Missing config id');
    if (!sku_id) throw new Error('SKU is required');
    if (!name) throw new Error('Name is required');
    if (!Number.isFinite(units_per_pallet) || units_per_pallet <= 0) throw new Error('Units per pallet must be > 0');

    const ti = req.body.ti ? Number(req.body.ti) : null;
    const hi = req.body.hi ? Number(req.body.hi) : null;
    const is_default = req.body.is_default === 'on';
    const notes = req.body.notes ? String(req.body.notes) : null;

    db.updatePalletConfig({ id, sku_id, name, ti, hi, units_per_pallet, is_default, notes });

    const skus = db.listSkusAllFiltered({ categoryId: 'all' });
    const configs = db.listPalletConfigsAll();
    res.render('inventory_pallet_configs', { skus, configs, msg: 'Updated pallet config.' });
  } catch (e) {
    const skus = db.listSkusAllFiltered({ categoryId: 'all' });
    const configs = db.listPalletConfigsAll();
    res.status(400).render('inventory_pallet_configs', { skus, configs, msg: e?.message || String(e) });
  }
});

app.post('/inventory/settings/pallet-configs/delete', requireConnected, (req, res) => {
  try {
    const id = Number(req.body.id);
    if (!id) throw new Error('Missing config id');
    db.deletePalletConfig(id);

    const skus = db.listSkusAllFiltered({ categoryId: 'all' });
    const configs = db.listPalletConfigsAll();
    res.render('inventory_pallet_configs', { skus, configs, msg: 'Deleted pallet config.' });
  } catch (e) {
    const skus = db.listSkusAllFiltered({ categoryId: 'all' });
    const configs = db.listPalletConfigsAll();
    res.status(400).render('inventory_pallet_configs', { skus, configs, msg: e?.message || String(e) });
  }
});

// ==========================================================
// Inventory: Yard view (if you want it back)
// ==========================================================
app.get('/inventory/yard', requireConnected, (req, res) => {
  const containers = db.listContainers();
  const yard = containers.map(containerNo => {
    const pallets = db.listPalletsInContainer(containerNo);
    const palletByLoc = new Map();
    for (const p of pallets) palletByLoc.set(p.location_code, p);

    const depths = db.getContainerDepths(containerNo);
    const left = [];
    const right = [];

    for (let d = 1; d <= depths.leftMax; d++) {
      const code = `C${containerNo}-L${String(d).padStart(2, '0')}`;
      left.push({ code, pallet: palletByLoc.get(code) || null });
    }
    for (let d = 1; d <= depths.rightMax; d++) {
      const code = `C${containerNo}-R${String(d).padStart(2, '0')}`;
      right.push({ code, pallet: palletByLoc.get(code) || null });
    }

    return { containerNo, label: depths.label, left, right };
  });

  res.render('inventory_yard', { yard });
});


const port = process.env.PORT || 3000;
app.listen(port, () => console.log(`App running on http://localhost:${port}`));

