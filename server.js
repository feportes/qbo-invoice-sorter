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
// ADMIN
// ==========================================================
app.get('/admin', requireConnected, (req, res) => res.redirect('/admin/categories'));

app.get('/admin/sync', requireConnected, async (req, res) => {
  const conn = db.getConnectionOrThrow();
  const oauthClient = getOAuthClient(conn);
  try {
    const customerCount = await syncCustomers(oauthClient, conn.realm_id);
    const categoryCount = await syncCategories(oauthClient, conn.realm_id);

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
// INVENTORY: Container Settings
// ==========================================================
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
// Inventory: Pallet Configs UI
// ==========================================================
app.get('/inventory/settings/pallet-configs', requireConnected, (req, res) => {
  try {
    const skus = db.listSkusAllFiltered({ categoryId: 'all' });
    const configs = db.listPalletConfigsAll();
    res.render('inventory_pallet_configs', { skus, configs, msg: null });
  } catch (e) {
    res.status(500).send(`Pallet configs failed: ${e?.message || e}`);
  }
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
// Inventory: Walk-in (pallets + loose + exact slot occupancy)
// ==========================================================
app.get('/inventory/walkin', requireConnected, (req, res) => {
  try {
    const rows = db.listWalkinLoose();
    const pallets = db.listPalletsInWalkin();

    // Build exact slot groups (based on current container settings)
    const slotGroups = [];
    for (let c = 1; c <= 7; c++) {
      const depths = db.getContainerDepths(c);
      const validSlots = db.listValidSlotCodes(c);

      const palletsInC = db.listPalletsInContainer(c);
      const occMap = new Map();
      for (const p of palletsInC) {
        occMap.set(p.location_code, p);
      }

      const slots = validSlots.map(code => {
        const occ = occMap.get(code);
        return {
          code,
          occupied: !!occ,
          pallet: occ || null
        };
      });

      const occupiedCount = slots.reduce((n, s) => n + (s.occupied ? 1 : 0), 0);

      slotGroups.push({
        containerNo: c,
        label: depths.label,
        occupiedCount,
        slots
      });
    }

    res.render('inventory_walkin', { rows, pallets, slotGroups });
  } catch (e) {
    res.status(500).send(`Walk-in failed: ${e?.message || e}`);
  }
});

// ==========================================================
// Inventory: Add Pallet (manual receive) — compatible with BOTH add-pallet EJS versions
// ==========================================================
app.get('/inventory/add-pallet', requireConnected, (req, res) => {
  try {
    const containerNo = Number(req.query.c || 1);
    const containers = db.listContainers();

    // Visual slot arrays (for the older UI)
    const depths = db.getContainerDepths(containerNo);
    const left = [];
    const right = [];
    for (let d = 1; d <= depths.leftMax; d++) left.push({ code: `C${containerNo}-L${String(d).padStart(2,'0')}` });
    for (let d = 1; d <= depths.rightMax; d++) right.push({ code: `C${containerNo}-R${String(d).padStart(2,'0')}` });

    // Dropdown options (for the newer UI)
    const slotOptions = db.listValidSlotCodes(containerNo);

    // SKUs (support both templates)
    const skus = (typeof db.listSkusAllFiltered === 'function')
      ? db.listSkusAllFiltered({ categoryId: 'all' })
      : db.listSkusActiveOnly();

    res.render('inventory_add_pallet', {
      msg: null,
      containers,
      containerNo,
      left,
      right,
      slotOptions,
      skus
    });
  } catch (e) {
    res.status(500).send(`Add pallet page failed: ${e?.message || e}`);
  }
});

// ==========================================================
// Inventory: Map  (✅ merged WALKIN/RETURNS panel data added)
// ==========================================================
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

  // ✅ NEW: for merged WALKIN/RETURNS panel inside the map page
  const walkinPallets = db.listPalletsInWalkin();
  const walkinLoose = db.listWalkinLoose();

  res.render('inventory_map', {
    containerNo,
    containers,
    left,
    right,
    containerLabel: depths.label,
    slotOptions,
    c1Mode,

    // ✅ pass to EJS
    walkinPallets,
    walkinLoose
  });
});

// ==========================================================
// Inventory: Yard view (all containers)
// ==========================================================
app.get('/inventory/yard', requireConnected, (req, res) => {
  try {
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
  } catch (e) {
    res.status(500).send(`Yard failed: ${e?.message || e}`);
  }
});

// ==========================================================
// Inventory: Move pallet (form + JSON)
// ==========================================================
app.post('/inventory/move', requireConnected, (req, res) => {
  try {
    const palletId = Number(req.body.pallet_id);
    const toSlot = String(req.body.to_slot || '').trim();
    const containerNo = Number(req.body.container_no || 1);

    if (!palletId) throw new Error('Missing pallet_id');
    if (!toSlot) throw new Error('Missing destination');

    const loc = db.getLocationByCode(toSlot);
    if (!loc) throw new Error(`Destination slot not found: ${toSlot}`);

    db.movePallet(palletId, loc.id, 'user');
    res.redirect(`/inventory/map?c=${containerNo}`);
  } catch (e) {
    res.status(500).send(`Move failed: ${e?.message || e}`);
  }
});

// JSON move endpoint (used by drag/drop)
app.post('/inventory/move-json', requireConnected, (req, res) => {
  try {
    const palletId = Number(req.body.pallet_id);
    const toSlot = String(req.body.to_slot || '').trim();

    if (!palletId) return res.status(400).json({ ok: false, error: 'Missing pallet_id' });
    if (!toSlot) return res.status(400).json({ ok: false, error: 'Missing destination' });

    const loc = db.getLocationByCode(toSlot);
    if (!loc) return res.status(400).json({ ok: false, error: `Destination slot not found: ${toSlot}` });

    db.movePallet(palletId, loc.id, 'user');
    return res.json({ ok: true });
  } catch (e) {
    // Prefer 400 so UI shows the message cleanly (occupied slot, etc.)
    return res.status(400).json({ ok: false, error: e?.message || String(e) });
  }
});

// Break pallet -> loose in walkin
app.post('/inventory/break-to-walkin', requireConnected, (req, res) => {
  try {
    const palletId = Number(req.body.pallet_id);
    const qty = Number(req.body.qty);
    const containerNo = Number(req.body.container_no || 1);

    if (!palletId) throw new Error('Missing pallet_id');
    if (!Number.isFinite(qty) || qty <= 0) throw new Error('Qty must be > 0');

    db.breakPalletToWalkin({ palletId, qty, userName: 'user' });
    res.redirect(`/inventory/map?c=${containerNo}`);
  } catch (e) {
    res.status(500).send(`Break failed: ${e?.message || e}`);
  }
});

// ==========================================================
// Inventory Allocation (Preview + Apply)
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
// Inventory: RETURNS page (pallets in RETURNS)
// ==========================================================
app.get('/inventory/returns', requireConnected, (req, res) => {
  try {
    const pallets = db.listPalletsInReturns();
    res.render('inventory_returns', { pallets });
  } catch (e) {
    res.status(500).send(`RETURNS failed: ${e?.message || e}`);
  }
});


const port = process.env.PORT || 3000;
app.listen(port, () => console.log(`App running on http://localhost:${port}`));
