import multer from 'multer';
import { createRequire } from 'module';

const require = createRequire(import.meta.url);
const pdfParse = require('pdf-parse'); // <-- ONLY ONE pdfParse definition





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

const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 15 * 1024 * 1024 }
});


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

// Force reprocess: clears processed lock for current SyncToken and runs again
app.post('/admin/process-invoice-force', requireConnected, async (req, res) => {
  const { invoice_id } = req.body;
  const conn = db.getConnectionOrThrow();
  const oauthClient = getOAuthClient(conn);

  try {
    if (!invoice_id) throw new Error('Missing invoice id');

    // ✅ key fix
    db.clearProcessed(String(invoice_id));

    const result = await processInvoiceWithRetry({
      oauthClient,
      realmId: conn.realm_id,
      invoiceId: String(invoice_id),
      source: 'manual_force',
      retries: 6
    });

    res.render('process_result', { result });
  } catch (e) {
    res.status(500).send(`Force process failed: ${e?.message || e}`);
  }
});

// ==========================================================
// Inbound Docs: upload pack/weight list (organic lot evidence)
// ==========================================================
app.get('/inventory/inbound', requireConnected, (req, res) => {
  const docs = db.listInboundDocs();
  res.render('inventory_inbound', { docs, msg: null });
});

function parseBrazilNumber(x) {
  // "6.480,00" -> 6480.00
  const s = String(x || '').trim();
  if (!s) return null;
  const norm = s.replace(/\./g, '').replace(',', '.');
  const n = Number(norm);
  return Number.isFinite(n) ? n : null;
}

function parsePackWeightListText(text) {
  const lines = String(text || '')
    .split('\n')
    .map(l => l.trim())
    .filter(Boolean);

  let doc_date = null;
  let container_no = null;

  // Buffer for wrapped product names (lines that appear BEFORE the numbered row)
  let pendingDesc = '';

  // A) Normal: "06 ORGANIC SWEETENED ACAI SORBET 9KG 2008.992140 BUCKET 282828-BB 720 6.480,00 6.901,20 25086017"
  const rowFull =
    /^(\d{2})\s+(.+?)\s+(\d{4}\.\d{4,})\s+([A-Z]+)\s+([A-Z0-9\-]+)\s+(\d+(?:[.,]\d+)?)\s+([\d\.,]+)\s+([\d\.,]+)\s+(\d+)$/i;

  // B) Wrapped-name: "05 2009.897065 BOX 232323 336 2.016,00 2.370,82 25013038"
  // (product name is in pendingDesc)
  const rowNoName =
    /^(\d{2})\s+(\d{4}\.\d{4,})\s+([A-Z]+)\s+([A-Z0-9\-]+)\s+(\d+(?:[.,]\d+)?)\s+([\d\.,]+)\s+([\d\.,]+)\s+(\d+)$/i;

  const rows = [];

  for (const l of lines) {
    // Header fields
    const mDate = l.match(/DATE:\s*(\d{2})\/(\d{2})\/(\d{4})/i);
    if (mDate) doc_date = `${mDate[3]}-${mDate[2]}-${mDate[1]}`;

    const mCont = l.match(/CONTAINER\s+([A-Z0-9]+)/i);
    if (mCont) container_no = mCont[1];

    // Skip obvious header-ish lines
    if (
      /^Pack and Weight List/i.test(l) ||
      /^Costumer:/i.test(l) ||
      /^Country:/i.test(l) ||
      /^Total Package:/i.test(l) ||
      /^Total Net Weight:/i.test(l) ||
      /^Gross Weight:/i.test(l) ||
      /^Net Weight/i.test(l) ||
      /^Product\s+NCM/i.test(l) ||
      /^\(Kg\)/i.test(l)
    ) {
      continue;
    }

    // Try normal full row
    let m = l.match(rowFull);
    if (m) {
      pendingDesc = '';
      rows.push({
        line_no: Number(m[1]),
        raw_product_name: m[2].trim(),
        ncm: m[3],
        package_type: m[4],
        package_code: m[5],
        qty_packages: Number(String(m[6]).replace(',', '.')),
        net_kg: parseBrazilNumber(m[7]),
        gross_kg: parseBrazilNumber(m[8]),
        lot_number: m[9]
      });
      continue;
    }

    // Try wrapped-name row
    m = l.match(rowNoName);
    if (m) {
      const name = pendingDesc.trim();
      pendingDesc = '';
      rows.push({
        line_no: Number(m[1]),
        raw_product_name: name || '(UNPARSED PRODUCT NAME)',
        ncm: m[2],
        package_type: m[3],
        package_code: m[4],
        qty_packages: Number(String(m[5]).replace(',', '.')),
        net_kg: parseBrazilNumber(m[6]),
        gross_kg: parseBrazilNumber(m[7]),
        lot_number: m[8]
      });
      continue;
    }

    // Otherwise it may be a product-name continuation line
    if (/[A-Z]/i.test(l) && !/^\d{2}$/.test(l)) {
      pendingDesc = pendingDesc ? `${pendingDesc} ${l}` : l;
    }
  }

  return { doc_date, container_no, rows };
}

 
 

app.post('/inventory/inbound/upload', requireConnected, upload.single('pdf'), async (req, res) => {
  try {
    if (!req.file) throw new Error('Missing PDF file');

    const parsed = await pdfParse(req.file.buffer);
    const { doc_date, container_no, rows } = parsePackWeightListText(parsed.text);

    const inboundDocId = db.createInboundDoc({
      doc_date,
      container_no,
      source_filename: req.file.originalname,
      notes: null
    });

    for (const r of rows) {
      const skuId = db.findSkuIdByAliasOrName(r.raw_product_name);
      db.addInboundDocLine(inboundDocId, { ...r, sku_id: skuId });
    }

    res.redirect(`/inventory/inbound/${inboundDocId}`);
  } catch (e) {
    const docs = db.listInboundDocs();
    res.status(400).render('inventory_inbound', { docs, msg: e?.message || String(e) });
  }
});

app.get('/inventory/inbound/:id', requireConnected, (req, res) => {
  const doc = db.getInboundDoc(req.params.id);
  const lines = db.listInboundDocLines(req.params.id);
  const skus = db.sqlite.prepare(`SELECT id, name FROM skus ORDER BY name COLLATE NOCASE`).all();
  res.render('inventory_inbound_review', { doc, lines, skus, msg: null });
});

// ✅ Save mappings + auto-create lots + auto-save aliases
app.post('/inventory/inbound/:id/apply', requireConnected, (req, res) => {
  try {
    const docId = Number(req.params.id);
    const doc = db.getInboundDoc(docId);
    if (!doc) throw new Error('Inbound doc not found');

    const lineIds = Array.isArray(req.body.line_id) ? req.body.line_id : [req.body.line_id].filter(Boolean);
    const skuIds  = Array.isArray(req.body.sku_id)  ? req.body.sku_id  : [req.body.sku_id].filter(Boolean);

    // 1) Save line->SKU mappings
    for (let i = 0; i < lineIds.length; i++) {
      const lineId = Number(lineIds[i]);
      const skuId = skuIds[i] ? Number(skuIds[i]) : null;
      db.setInboundLineSku(lineId, skuId);
    }

    // 2) Reload lines to get raw names + lot numbers
    const lines = db.listInboundDocLines(docId);

    // 3) Auto-save aliases for mapped lines
    for (const ln of lines) {
      if (!ln.sku_id) continue;
      if (!ln.raw_product_name) continue;
      db.addSkuAlias({ sku_id: ln.sku_id, alias: ln.raw_product_name });
    }

    // 4) Create lots for mapped lines
    for (const ln of lines) {
      if (!ln.sku_id) continue;
      if (!ln.lot_number) continue;
      db.upsertLotForSku({ sku_id: ln.sku_id, lot_number: ln.lot_number });
    }

    const updatedLines = db.listInboundDocLines(docId);
    const skus = db.sqlite.prepare(`SELECT id, name FROM skus ORDER BY name COLLATE NOCASE`).all();
    res.render('inventory_inbound_review', { doc, lines: updatedLines, skus, msg: 'Saved mappings + aliases + lots created.' });

  } catch (e) {
    res.status(500).send(`Apply inbound failed: ${e?.message || e}`);
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

  // For merged panels inside map
  const walkinPallets = db.listPalletsInWalkin();
  const walkinLoose = db.listWalkinLoose();
  const returnsPallets = db.listPalletsInReturns();

  res.render('inventory_map', {
    containerNo,
    containers,
    left,
    right,
    containerLabel: depths.label,
    slotOptions,
    c1Mode,
    walkinPallets,
    walkinLoose,
    returnsPallets
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

// ==========================================================
// Inventory: Engine Settings (toggle + timezone)
// ==========================================================
app.get('/inventory/settings/engine', requireConnected, (req, res) => {
  try {
    const enabled = db.getAutoAllocateEnabled();
    const timezone = db.getSetting('inventory_timezone') || 'America/Los_Angeles';
    res.render('inventory_engine_settings', { enabled, timezone, msg: null });
  } catch (e) {
    res.status(500).send(`Engine settings failed: ${e?.message || e}`);
  }
});

app.post('/inventory/settings/engine', requireConnected, (req, res) => {
  try {
    const enabled = req.body.enabled === 'on';
    db.setAutoAllocateEnabled(enabled);

    const tz = String(req.body.timezone || '').trim() || 'America/Los_Angeles';
    db.setSetting('inventory_timezone', tz);

    const timezone = db.getSetting('inventory_timezone') || 'America/Los_Angeles';
    res.render('inventory_engine_settings', { enabled, timezone, msg: 'Saved.' });
  } catch (e) {
    const enabled = db.getAutoAllocateEnabled();
    const timezone = db.getSetting('inventory_timezone') || 'America/Los_Angeles';
    res.status(400).render('inventory_engine_settings', { enabled, timezone, msg: e?.message || String(e) });
  }
});

// ==========================================================
// Inventory: SKU Settings (category filter + bulk actions)
// ==========================================================
app.get('/inventory/settings/skus', requireConnected, (req, res) => {
  try {
    const selectedCat = (req.query.cat || 'all').toString();
    const categories = db.listCategoriesOrdered();
    const skus = db.listSkusAllFiltered({ categoryId: selectedCat });
    res.render('inventory_sku_settings', { skus, msg: null, categories, selectedCat });
  } catch (e) {
    res.status(500).send(`SKU settings failed: ${e?.message || e}`);
  }
});

// Save selected SKU rows (Active / Lot / Organic / Unit / Threshold)
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

// Bulk update current filter (Active / Lot / Organic)
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

// Sync SKUs from QBO (Items)
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

// ==========================================================
// Inventory Audit: Assign lot numbers to previous sales (Organic only)
// ==========================================================
app.get('/inventory/audit', requireConnected, (req, res) => {
  res.render('inventory_audit', {
    msg: null,
    invoiceId: '',
    invoice: null,
    organicLines: [],
    lotsBySku: {},
    existing: []
  });
});

app.post('/inventory/audit/preview', requireConnected, async (req, res) => {
  try {
    const invoiceId = String(req.body.invoice_id || '').trim();
    if (!invoiceId) throw new Error('Missing invoice id');

    const conn = db.getConnectionOrThrow();
    const oauthClient = getOAuthClient(conn);

    const invResp = await qboReadInvoiceWithRetry(oauthClient, conn.realm_id, invoiceId);
    const invoice = invResp?.Invoice;
    if (!invoice) throw new Error(`Invoice not found: ${invoiceId}`);

    // Build organic-only lines (based on SKU settings)
    const lines = (invoice?.Line || [])
      .filter(l => l?.DetailType === 'SalesItemLineDetail')
      .map(l => {
        const det = l.SalesItemLineDetail;
        const qboItemId = det?.ItemRef?.value || null;
        const qboName = det?.ItemRef?.name || null;
        const qty = Number(det?.Qty || 0);
        return { qboItemId, qboName, qty };
      })
      .filter(x => x.qboItemId && x.qty > 0);

    const organicLines = [];
    const lotsBySku = {};

    for (const ln of lines) {
      const sku = db.getSkuByQboItemId(ln.qboItemId);
      if (!sku) continue;
      if (!sku.is_organic) continue; // ✅ only organic SKUs

      organicLines.push({
        sku_id: sku.id,
        sku_name: sku.name,
        unit_type: sku.unit_type,
        qboItemId: ln.qboItemId,
        qboName: ln.qboName,
        qty: ln.qty
      });

      if (!lotsBySku[String(sku.id)]) {
        lotsBySku[String(sku.id)] = db.listLotsForSku(sku.id);
      }
    }

    const existing = db.listAuditAllocations(invoiceId);

    res.render('inventory_audit', {
      msg: organicLines.length ? null : 'No organic SKUs found on this invoice (based on SKU settings).',
      invoiceId,
      invoice,
      organicLines,
      lotsBySku,
      existing
    });
  } catch (e) {
    res.status(400).render('inventory_audit', {
      msg: e?.message || String(e),
      invoiceId: '',
      invoice: null,
      organicLines: [],
      lotsBySku: {},
      existing: []
    });
  }
});

app.post('/inventory/audit/save', requireConnected, (req, res) => {
  try {
    const invoiceId = String(req.body.invoice_id || '').trim();
    if (!invoiceId) throw new Error('Missing invoice id');

    const txnDate = req.body.txn_date ? String(req.body.txn_date) : null;
    const customerName = req.body.customer_name ? String(req.body.customer_name) : null;

    const toArr = (x) => Array.isArray(x) ? x : (x !== undefined ? [x] : []);
    const skuArr = toArr(req.body.sku_id);
    const lotArr = toArr(req.body.lot_id);
    const qtyArr = toArr(req.body.qty_units);
    const noteArr = toArr(req.body.note);

    if (skuArr.length === 0) throw new Error('No rows submitted.');

    const rows = [];
    for (let i = 0; i < skuArr.length; i++) {
      const sku_id = Number(skuArr[i]);
      const lot_id_raw = lotArr[i];
      const lot_id = (lot_id_raw === '' || lot_id_raw === null || lot_id_raw === undefined) ? null : Number(lot_id_raw);
      const qty_units = Number(qtyArr[i]);

      if (!sku_id) continue;
      if (!Number.isFinite(qty_units) || qty_units <= 0) continue;

      // ✅ safety: only allow organic SKUs
      const sku = db.sqlite.prepare(`SELECT * FROM skus WHERE id=?`).get(sku_id);
      if (!sku || !sku.is_organic) continue;

      rows.push({
        sku_id,
        lot_id,
        qty_units,
        method: 'MANUAL',
        note: noteArr[i] ? String(noteArr[i]) : null
      });
    }

    db.replaceAuditAllocations({ invoiceId, txnDate, customerName, rows });

    // Redirect back to the page
    res.redirect('/inventory/audit');
  } catch (e) {
    res.status(500).send(`Audit save failed: ${e?.message || e}`);
  }
});

// ==========================================================
// Audit: Search invoices by Organic SKU (fast via local index)
// ==========================================================
app.get('/inventory/audit/search', requireConnected, (req, res) => {
  const organicSkus = db.sqlite.prepare(`
    SELECT id, name, unit_type
    FROM skus
    WHERE is_organic=1 AND active=1
    ORDER BY name COLLATE NOCASE
  `).all();

  res.render('inventory_audit_search', {
    msg: null,
    organicSkus,
    selectedSkuId: '',
    startDate: '2025-01-01',
    endDate: '',
    invoices: [],
    lots: []
  });
});

// ==========================================================
// Audit: Scan QBO invoices in date range and index organic SKU lines
// ==========================================================
app.post('/inventory/audit/scan', requireConnected, async (req, res) => {
  const conn = db.getConnectionOrThrow();
  const oauthClient = getOAuthClient(conn);

  try {
    const skuId = Number(req.body.sku_id);
    const startDate = String(req.body.start_date || '').trim() || '2025-01-01';
    const endDate = String(req.body.end_date || '').trim() || null;

    if (!skuId) throw new Error('Missing sku_id');

    const sku = db.sqlite.prepare(`SELECT * FROM skus WHERE id=?`).get(skuId);
    if (!sku || !sku.is_organic) throw new Error('Selected SKU is not marked organic.');

    // 1) Query invoice list from QBO in date range
    // QBO Query API supports Invoice query w/ TxnDate filters.
    // We page through results.
    let start = 1;
    const pageSize = 100;
    const invoicesToRead = [];

    while (true) {
      const where = endDate
        ? `TxnDate >= '${startDate}' AND TxnDate <= '${endDate}'`
        : `TxnDate >= '${startDate}'`;

      const q = `select Id, DocNumber, TxnDate, CustomerRef from Invoice where ${where} startposition ${start} maxresults ${pageSize}`;
      const r = await qboQuery(oauthClient, conn.realm_id, q);
      const invs = r?.QueryResponse?.Invoice || [];

      for (const inv of invs) {
        if (inv?.Id) invoicesToRead.push({
          id: String(inv.Id),
          txnDate: inv.TxnDate ? String(inv.TxnDate) : null,
          docNumber: inv.DocNumber ? String(inv.DocNumber) : null,
          customerName: inv?.CustomerRef?.name ? String(inv.CustomerRef.name) : null
        });
      }

      if (invs.length < pageSize) break;
      start += pageSize;
    }

    // 2) Read each invoice detail and index only lines matching this SKU’s qbo_item_id
    // (We do invoice detail read because QBO query won’t return Line items.)
    const targetQboItemId = String(sku.qbo_item_id || '');
    if (!targetQboItemId) throw new Error('SKU has no qbo_item_id mapped. Sync SKUs first.');

    for (const meta of invoicesToRead) {
      const invResp = await qboReadInvoiceWithRetry(oauthClient, conn.realm_id, meta.id);
      const invoice = invResp?.Invoice;
      if (!invoice?.Id) continue;

      const lines = invoice.Line || [];
      for (const line of lines) {
        if (line?.DetailType !== 'SalesItemLineDetail') continue;
        const det = line.SalesItemLineDetail;
        const itemId = det?.ItemRef?.value ? String(det.ItemRef.value) : null;
        if (!itemId || itemId !== targetQboItemId) continue;

        const qty = Number(det?.Qty || 0);
        if (!Number.isFinite(qty) || qty <= 0) continue;

        db.upsertInvoiceSkuLine({
          qbo_invoice_id: String(invoice.Id),
          txn_date: invoice.TxnDate ? String(invoice.TxnDate) : meta.txnDate,
          doc_number: invoice.DocNumber ? String(invoice.DocNumber) : meta.docNumber,
          customer_name: invoice?.CustomerRef?.name ? String(invoice.CustomerRef.name) : meta.customerName,
          sku_id: skuId,
          qbo_item_id: itemId,
          qty_units: qty,
          amount: Number(line.Amount || 0)
        });
      }
    }

    res.redirect(`/inventory/audit/search?sku=${skuId}&start=${encodeURIComponent(startDate)}${endDate ? `&end=${encodeURIComponent(endDate)}` : ''}`);
  } catch (e) {
    const organicSkus = db.sqlite.prepare(`SELECT id, name, unit_type FROM skus WHERE is_organic=1 AND active=1 ORDER BY name COLLATE NOCASE`).all();
    res.status(400).render('inventory_audit_search', {
      msg: e?.message || String(e),
      organicSkus,
      selectedSkuId: String(req.body.sku_id || ''),
      startDate: String(req.body.start_date || '2025-01-01'),
      endDate: String(req.body.end_date || ''),
      invoices: [],
      lots: []
    });
  }
});

// ==========================================================
// Audit search results (uses local index)
// ==========================================================
app.get('/inventory/audit/search', requireConnected, (req, res) => {
  const organicSkus = db.sqlite.prepare(`
    SELECT id, name, unit_type
    FROM skus
    WHERE is_organic=1 AND active=1
    ORDER BY name COLLATE NOCASE
  `).all();

  const selectedSkuId = String(req.query.sku || '').trim();
  const startDate = String(req.query.start || '2025-01-01').trim();
  const endDate = String(req.query.end || '').trim() || null;

  let invoices = [];
  let lots = [];
  if (selectedSkuId) {
    invoices = db.listInvoicesContainingSku({ skuId: Number(selectedSkuId), startDate, endDate });
    lots = db.listLotsForSku(Number(selectedSkuId));
  }

  res.render('inventory_audit_search', {
    msg: null,
    organicSkus,
    selectedSkuId,
    startDate,
    endDate: endDate || '',
    invoices,
    lots
  });
});

// ==========================================================
// Bulk assign a LOT to selected invoices (audit-only)
// ==========================================================
app.post('/inventory/audit/assign-lot', requireConnected, (req, res) => {
  try {
    const skuId = Number(req.body.sku_id);
    const lotId = req.body.lot_id ? Number(req.body.lot_id) : null;
    const startDate = String(req.body.start_date || '2025-01-01').trim();
    const endDate = String(req.body.end_date || '').trim() || null;

    const ids = Array.isArray(req.body.invoice_ids) ? req.body.invoice_ids : (req.body.invoice_ids ? [req.body.invoice_ids] : []);
    if (!skuId) throw new Error('Missing sku_id');
    if (ids.length === 0) throw new Error('No invoices selected.');

    // Save one audit allocation row per selected invoice, qty = invoice total qty for that SKU
    for (const invoiceId of ids) {
      const inv = db.sqlite.prepare(`
        SELECT qbo_invoice_id, MAX(txn_date) AS txn_date, MAX(customer_name) AS customer_name, SUM(qty_units) AS qty_units
        FROM invoice_sku_lines
        WHERE qbo_invoice_id=? AND sku_id=?
      `).get(String(invoiceId), Number(skuId));

      if (!inv) continue;

      db.replaceAuditAllocations({
        invoiceId: String(invoiceId),
        txnDate: inv.txn_date || null,
        customerName: inv.customer_name || null,
        rows: [{
          sku_id: skuId,
          lot_id: lotId,
          qty_units: Number(inv.qty_units || 0),
          method: 'MANUAL',
          note: null
        }]
      });
    }

    // Return to results
    res.redirect(`/inventory/audit/search?sku=${skuId}&start=${encodeURIComponent(startDate)}${endDate ? `&end=${encodeURIComponent(endDate)}` : ''}`);
  } catch (e) {
    res.status(500).send(`Assign lot failed: ${e?.message || e}`);
  }
});

// ==========================================================
// Printable lot report (Lot -> invoice list)
// ==========================================================
app.get('/inventory/audit/lot-report', requireConnected, (req, res) => {
  try {
    const skuId = Number(req.query.sku);
    const lotId = req.query.lot ? Number(req.query.lot) : null;
    const startDate = String(req.query.start || '2025-01-01').trim();
    const endDate = String(req.query.end || '').trim() || null;

    if (!skuId) throw new Error('Missing sku');

    const sku = db.sqlite.prepare(`SELECT * FROM skus WHERE id=?`).get(skuId);
    const lot = lotId ? db.sqlite.prepare(`SELECT * FROM lots WHERE id=?`).get(lotId) : null;

    const rows = db.listAuditLotReport({ skuId, lotId, startDate, endDate });
    const total = rows.reduce((s, r) => s + Number(r.allocated_qty || 0), 0);

    res.render('inventory_audit_lot_report', { sku, lot, rows, total, startDate, endDate: endDate || '' });
  } catch (e) {
    res.status(500).send(`Lot report failed: ${e?.message || e}`);
  }
});

// ==========================================================
// Organic Audit Search (SKU -> invoices -> lot assignment)
// ==========================================================
app.get('/inventory/audit/search', requireConnected, (req, res) => {
  const organicSkus = db.sqlite.prepare(`
    SELECT id, name, unit_type, qbo_item_id
    FROM skus
    WHERE is_organic=1 AND active=1
    ORDER BY name COLLATE NOCASE
  `).all();

  const selectedSkuId = String(req.query.sku || '').trim();
  const startDate = String(req.query.start || (db.getSetting('organic_tracking_start') || '2025-01-01')).trim();
  const endDate = String(req.query.end || '').trim() || null;
  const msg = String(req.query.msg || '') || null;

  let invoices = [];
  let lots = [];
  if (selectedSkuId) {
    invoices = db.listInvoicesContainingSku({ skuId: Number(selectedSkuId), startDate, endDate });
    lots = db.listLotsForSku(Number(selectedSkuId));
  }

  res.render('inventory_audit_search', {
    msg,
    organicSkus,
    selectedSkuId,
    startDate,
    endDate: endDate || '',
    invoices,
    lots
  });
});

// ==========================================================
// Scan QBO invoices in date range and index lines for selected organic SKU
// ==========================================================
app.post('/inventory/audit/scan', requireConnected, async (req, res) => {
  const conn = db.getConnectionOrThrow();
  const oauthClient = getOAuthClient(conn);

  try {
    const skuId = Number(req.body.sku_id);
    const startDate = String(req.body.start_date || (db.getSetting('organic_tracking_start') || '2025-01-01')).trim();
    const endDate = String(req.body.end_date || '').trim() || null;

    if (!skuId) throw new Error('Missing sku_id');

    const sku = db.sqlite.prepare(`SELECT * FROM skus WHERE id=?`).get(skuId);
    if (!sku || !sku.is_organic) throw new Error('Selected SKU is not marked organic.');
    if (!sku.qbo_item_id) throw new Error('SKU has no qbo_item_id. Sync SKUs from QBO first.');

    // Pull invoice headers (paged)
    let start = 1;
    const pageSize = 100;
    const metas = [];

    while (true) {
      const where = endDate
        ? `TxnDate >= '${startDate}' AND TxnDate <= '${endDate}'`
        : `TxnDate >= '${startDate}'`;

      const q = `select Id, DocNumber, TxnDate, CustomerRef from Invoice where ${where} startposition ${start} maxresults ${pageSize}`;
      const r = await qboQuery(oauthClient, conn.realm_id, q);
      const invs = r?.QueryResponse?.Invoice || [];

      for (const inv of invs) {
        if (inv?.Id) metas.push({
          id: String(inv.Id),
          txnDate: inv.TxnDate ? String(inv.TxnDate) : null,
          docNumber: inv.DocNumber ? String(inv.DocNumber) : null,
          customerName: inv?.CustomerRef?.name ? String(inv.CustomerRef.name) : null
        });
      }

      if (invs.length < pageSize) break;
      start += pageSize;
    }

    const targetItemId = String(sku.qbo_item_id);
    let indexed = 0;

    for (const meta of metas) {
      const invResp = await qboReadInvoiceWithRetry(oauthClient, conn.realm_id, meta.id);
      const invoice = invResp?.Invoice;
      if (!invoice?.Id) continue;

      const lines = invoice.Line || [];
      for (const line of lines) {
        if (line?.DetailType !== 'SalesItemLineDetail') continue;
        const det = line.SalesItemLineDetail;
        const itemId = det?.ItemRef?.value ? String(det.ItemRef.value) : null;
        if (!itemId || itemId !== targetItemId) continue;

        const qty = Number(det?.Qty || 0);
        if (!Number.isFinite(qty) || qty <= 0) continue;

        db.upsertInvoiceSkuLine({
          qbo_invoice_id: String(invoice.Id),
          txn_date: invoice.TxnDate ? String(invoice.TxnDate) : meta.txnDate,
          doc_number: invoice.DocNumber ? String(invoice.DocNumber) : meta.docNumber,
          customer_name: invoice?.CustomerRef?.name ? String(invoice.CustomerRef.name) : meta.customerName,
          sku_id: skuId,
          qbo_item_id: itemId,
          qty_units: qty,
          amount: Number(line.Amount || 0)
        });
        indexed++;
      }
    }

    res.redirect(`/inventory/audit/search?sku=${skuId}&start=${encodeURIComponent(startDate)}${endDate ? `&end=${encodeURIComponent(endDate)}` : ''}&msg=${encodeURIComponent(`Scan complete. Indexed lines: ${indexed}`)}`);
  } catch (e) {
    res.status(500).send(`Audit scan failed: ${e?.message || e}`);
  }
});

// ==========================================================
// Assign a LOT to selected invoices (audit-only)
// ==========================================================
app.post('/inventory/audit/assign-lot', requireConnected, (req, res) => {
  try {
    const skuId = Number(req.body.sku_id);
    const lotId = req.body.lot_id ? Number(req.body.lot_id) : null;
    const startDate = String(req.body.start_date || (db.getSetting('organic_tracking_start') || '2025-01-01')).trim();
    const endDate = String(req.body.end_date || '').trim() || null;

    const ids = Array.isArray(req.body.invoice_ids) ? req.body.invoice_ids : (req.body.invoice_ids ? [req.body.invoice_ids] : []);
    if (!skuId) throw new Error('Missing sku_id');
    if (ids.length === 0) throw new Error('No invoices selected.');

    // safety: only organic
    const sku = db.sqlite.prepare(`SELECT * FROM skus WHERE id=?`).get(skuId);
    if (!sku || !sku.is_organic) throw new Error('Selected SKU is not organic.');

    for (const invoiceId of ids) {
      const inv = db.sqlite.prepare(`
        SELECT qbo_invoice_id, MAX(txn_date) AS txn_date, MAX(customer_name) AS customer_name, SUM(qty_units) AS qty_units
        FROM invoice_sku_lines
        WHERE qbo_invoice_id=? AND sku_id=?
      `).get(String(invoiceId), Number(skuId));
      if (!inv) continue;

      db.replaceAuditAllocations({
        invoiceId: String(invoiceId),
        txnDate: inv.txn_date || null,
        customerName: inv.customer_name || null,
        rows: [{
          sku_id: skuId,
          lot_id: lotId,
          qty_units: Number(inv.qty_units || 0),
          method: 'MANUAL',
          note: lotId ? null : 'UNKNOWN LOT'
        }]
      });
    }

    res.redirect(`/inventory/audit/search?sku=${skuId}&start=${encodeURIComponent(startDate)}${endDate ? `&end=${encodeURIComponent(endDate)}` : ''}&msg=${encodeURIComponent('Assigned lot to selected invoices.')}`);
  } catch (e) {
    res.status(500).send(`Assign lot failed: ${e?.message || e}`);
  }
});

// ==========================================================
// Auto-assign lots (unassigned only) using date-based suggestion
// ==========================================================
app.post('/inventory/audit/auto-assign', requireConnected, (req, res) => {
  try {
    const skuId = Number(req.body.sku_id);
    const startDate = String(req.body.start_date || (db.getSetting('organic_tracking_start') || '2025-01-01')).trim();
    const endDate = String(req.body.end_date || '').trim() || null;

    if (!skuId) throw new Error('Missing sku_id');

    const sku = db.sqlite.prepare(`SELECT * FROM skus WHERE id=?`).get(skuId);
    if (!sku || !sku.is_organic) throw new Error('Selected SKU is not organic.');

    const invs = db.sqlite.prepare(`
      SELECT
        qbo_invoice_id,
        MAX(txn_date) AS txn_date,
        MAX(customer_name) AS customer_name,
        SUM(qty_units) AS qty_units
      FROM invoice_sku_lines
      WHERE sku_id=?
        AND (? IS NULL OR txn_date >= ?)
        AND (? IS NULL OR txn_date <= ?)
      GROUP BY qbo_invoice_id
      ORDER BY txn_date ASC
    `).all(
      Number(skuId),
      startDate || null, startDate || null,
      endDate || null, endDate || null
    );

    let assigned = 0;
    let unknown = 0;
    let skipped = 0;

    for (const inv of invs) {
      const invoiceId = String(inv.qbo_invoice_id);
      const txnDate = inv.txn_date ? String(inv.txn_date) : null;

      if (db.hasAuditAllocationForInvoiceSku(invoiceId, skuId)) {
        skipped++;
        continue;
      }

      const lotId = txnDate ? db.getSuggestedLotForSkuOnDate(skuId, txnDate) : null;
      if (!lotId) unknown++;

      db.replaceAuditAllocations({
        invoiceId,
        txnDate,
        customerName: inv.customer_name || null,
        rows: [{
          sku_id: skuId,
          lot_id: lotId,
          qty_units: Number(inv.qty_units || 0),
          method: 'AUTO_SUGGEST',
          note: lotId ? 'Auto-suggested by date' : 'Auto-suggest failed: UNKNOWN LOT'
        }]
      });

      assigned++;
    }

    res.redirect(`/inventory/audit/search?sku=${skuId}&start=${encodeURIComponent(startDate)}${endDate ? `&end=${encodeURIComponent(endDate)}` : ''}&msg=${encodeURIComponent(`Auto-assign complete: assigned=${assigned}, unknown=${unknown}, skipped(existing)=${skipped}`)}`);
  } catch (e) {
    res.status(500).send(`Auto-assign failed: ${e?.message || e}`);
  }
});

// ==========================================================
// Auto-assign lots for ALL organic SKUs (unassigned only)
// ==========================================================
app.post('/inventory/audit/auto-assign-all', requireConnected, (req, res) => {
  try {
    const startDate = String(req.body.start_date || (db.getSetting('organic_tracking_start') || '2025-01-01')).trim();
    const endDate = String(req.body.end_date || '').trim() || null;

    const skus = db.sqlite.prepare(`
      SELECT id, name
      FROM skus
      WHERE is_organic=1 AND active=1
      ORDER BY name COLLATE NOCASE
    `).all();

    let totalSkus = skus.length;
    let totalAssigned = 0;
    let totalUnknown = 0;
    let totalSkipped = 0;

    for (const sku of skus) {
      const skuId = Number(sku.id);

      const invs = db.sqlite.prepare(`
        SELECT
          qbo_invoice_id,
          MAX(txn_date) AS txn_date,
          MAX(customer_name) AS customer_name,
          SUM(qty_units) AS qty_units
        FROM invoice_sku_lines
        WHERE sku_id=?
          AND (? IS NULL OR txn_date >= ?)
          AND (? IS NULL OR txn_date <= ?)
        GROUP BY qbo_invoice_id
        ORDER BY txn_date ASC
      `).all(
        skuId,
        startDate || null, startDate || null,
        endDate || null, endDate || null
      );

      for (const inv of invs) {
        const invoiceId = String(inv.qbo_invoice_id);
        const txnDate = inv.txn_date ? String(inv.txn_date) : null;

        // If already assigned for this invoice+SKU, skip.
        if (db.hasAuditAllocationForInvoiceSku(invoiceId, skuId)) {
          totalSkipped++;
          continue;
        }

        const lotId = txnDate ? db.getSuggestedLotForSkuOnDate(skuId, txnDate) : null;
        if (!lotId) totalUnknown++;

        db.replaceAuditAllocations({
          invoiceId,
          txnDate,
          customerName: inv.customer_name || null,
          rows: [{
            sku_id: skuId,
            lot_id: lotId,
            qty_units: Number(inv.qty_units || 0),
            method: 'AUTO_SUGGEST',
            note: lotId ? 'Auto-suggested by date' : 'Auto-suggest failed: UNKNOWN LOT'
          }]
        });

        totalAssigned++;
      }
    }

    // Send user back to search with a status message
    const msg = `Auto-assign ALL complete: skus=${totalSkus}, assigned=${totalAssigned}, unknown=${totalUnknown}, skipped(existing)=${totalSkipped}.`;
    res.redirect(`/inventory/audit/search?start=${encodeURIComponent(startDate)}${endDate ? `&end=${encodeURIComponent(endDate)}` : ''}&msg=${encodeURIComponent(msg)}`);

  } catch (e) {
    res.status(500).send(`Auto-assign ALL failed: ${e?.message || e}`);
  }
});


// ==========================================================
// Master printable report (ALL organic SKUs + lots)
// ==========================================================
app.get('/inventory/audit/master-report', requireConnected, (req, res) => {
  try {
    const startDate = String(req.query.start || (db.getSetting('organic_tracking_start') || '2025-01-01')).trim();
    const endDate = String(req.query.end || '').trim() || null;

    const rows = db.listAuditMasterReport({ startDate, endDate });

    const bySku = new Map();
    let grandTotal = 0;

    for (const r of rows) {
      const qty = Number(r.qty_units || 0);
      grandTotal += qty;

      if (!bySku.has(r.sku_id)) {
        bySku.set(r.sku_id, {
          sku_id: r.sku_id,
          sku_name: r.sku_name,
          unit_type: r.unit_type,
          skuTotal: 0,
          lots: new Map()
        });
      }

      const skuGroup = bySku.get(r.sku_id);
      skuGroup.skuTotal += qty;

      const lotKey = r.lot_number ? `LOT:${r.lot_number}` : 'LOT:UNKNOWN';

      if (!skuGroup.lots.has(lotKey)) {
        skuGroup.lots.set(lotKey, {
          lot_number: r.lot_number || 'UNKNOWN LOT',
          lotTotal: 0,
          invoices: []
        });
      }

      const lotGroup = skuGroup.lots.get(lotKey);
      lotGroup.lotTotal += qty;

      lotGroup.invoices.push({
        txn_date: r.txn_date || '',
        qbo_invoice_id: r.qbo_invoice_id,
        customer_name: r.customer_name || '',
        qty_units: qty
      });
    }

    const skuGroups = [...bySku.values()].map(g => ({
      ...g,
      lotsArr: [...g.lots.values()]
    }));

    res.render('inventory_audit_master_report', {
      startDate,
      endDate: endDate || '',
      skuGroups,
      grandTotal
    });

  } catch (e) {
    res.status(500).send(`Master report failed: ${e?.message || e}`);
  }
});


const port = process.env.PORT || 3000;
app.listen(port, () => console.log(`App running on http://localhost:${port}`));

