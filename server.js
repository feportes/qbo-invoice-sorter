import 'dotenv/config';
import express from 'express';
import morgan from 'morgan';
import path from 'path';
import { fileURLToPath } from 'url';
import XLSX from 'xlsx';

import multer from 'multer';
import { createRequire } from 'module';

import { db } from './src/db.js';
import { ensureSchema, seedDefaults } from './src/schema.js';
import { getOAuthClient, authStart, authCallback, requireConnected, withFreshClient } from './src/oauth.js';
import { qboReadItemByName, qboQuery, qboReadInvoiceWithRetry } from './src/qbo.js';
import { syncCustomers, syncCategories } from './src/sync.js';
import { verifyIntuitWebhook, rawBodySaver } from './src/webhooks.js';
import { processInvoice } from './src/processor.js';
import { runAutoAllocateForInvoice } from './src/inventory_engine.js';
import { buildPlanFromInvoice, applyPlan } from './src/inventory_allocate.js';

const require = createRequire(import.meta.url);
const pdfParse = require('pdf-parse'); // pdf-parse@1.1.1

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
// Inbound Docs
// ==========================================================
app.get('/inventory/inbound', requireConnected, (req, res) => {
  const docs = db.listInboundDocs();
  res.render('inventory_inbound', { docs, msg: null });
});

app.get('/inventory/inbound/:id', requireConnected, (req, res) => {
  const docId = Number(req.params.id);
  const doc = db.getInboundDoc(docId);
  const lines = db.listInboundDocLines(docId);

  const skus = db.sqlite
    .prepare(`SELECT id, name FROM skus ORDER BY name COLLATE NOCASE`)
    .all();

  res.render('inventory_inbound_review', {
    doc,
    lines,
    skus,
    msg: String(req.query.msg || '') || null
  });
});

app.post('/inventory/inbound/:id/save-only', requireConnected, (req, res) => {
  try {
    const docId = Number(req.params.id);

    const toArr = (x) => Array.isArray(x) ? x : (x !== undefined ? [x] : []);
    const lineIds = toArr(req.body.line_id);
    const skuIds  = toArr(req.body.sku_id);

    const nameArr  = toArr(req.body.raw_product_name);
    const qtyArr   = toArr(req.body.qty_packages);
    const netArr   = toArr(req.body.net_kg);
    const grossArr = toArr(req.body.gross_kg);
    const lotArr   = toArr(req.body.lot_number);

    for (let i = 0; i < lineIds.length; i++) {
      const lineId = Number(lineIds[i]);
      if (!lineId) continue;

      const skuId = skuIds[i] ? Number(skuIds[i]) : null;
      db.setInboundLineSku(lineId, skuId);

      db.updateInboundLineAllFields({
        line_id: lineId,
        raw_product_name: nameArr[i],
        ncm: null,
        package_type: null,
        package_code: null,
        qty_packages: qtyArr[i],
        net_kg: netArr[i],
        gross_kg: grossArr[i],
        lot_number: lotArr[i]
      });
    }

    return res.redirect(`/inventory/inbound/${docId}?msg=${encodeURIComponent('Saved changes (no lots created).')}`);
  } catch (e) {
    return res.status(500).send(`Save-only failed: ${e?.message || e}`);
  }
});

app.post('/inventory/inbound/:id/create-lots', requireConnected, (req, res) => {
  try {
    const docId = Number(req.params.id);
    const lines = db.listInboundDocLines(docId);

    let created = 0;
    let skipped = 0;

    for (const ln of lines) {
      if (!ln.sku_id || !ln.lot_number) { skipped++; continue; }

      const sku = db.sqlite.prepare(`SELECT is_organic FROM skus WHERE id=?`).get(Number(ln.sku_id));
      if (!sku || !sku.is_organic) { skipped++; continue; }

      db.upsertLotForSku({ sku_id: ln.sku_id, lot_number: ln.lot_number });
      created++;
    }

    return res.redirect(`/inventory/inbound/${docId}?msg=${encodeURIComponent('Saved edits + created organic lots.')}`);
  } catch (e) {
    return res.status(500).send(`Create lots failed: ${e?.message || e}`);
  }
});

app.post('/inventory/inbound/:id/delete', requireConnected, (req, res) => {
  try {
    db.deleteInboundDoc(req.params.id);
    res.redirect('/inventory/inbound');
  } catch (e) {
    res.status(500).send(`Delete inbound doc failed: ${e?.message || e}`);
  }
});

// XLSX Import (inspection-safe)
app.post('/inventory/inbound/upload-xlsx', requireConnected, upload.single('xlsx'), async (req, res) => {
  try {
    if (!req.file) throw new Error('Missing XLSX file');

    const wb = XLSX.read(req.file.buffer, { type: 'buffer' });
    const firstSheetName = wb.SheetNames?.[0];
    if (!firstSheetName) throw new Error('XLSX has no sheets');

    const ws = wb.Sheets[firstSheetName];
    const rawRows = XLSX.utils.sheet_to_json(ws, { defval: '', raw: false });
    if (!rawRows.length) throw new Error('XLSX sheet has no data rows');

    function normalizeHeader(h) {
      return String(h || '')
        .trim()
        .toLowerCase()
        .replace(/\s+/g, '_')
        .replace(/[^\w]/g, '');
    }

    function numOrNull(x) {
      if (x === '' || x === null || x === undefined) return null;
      const n = Number(String(x).replace(/,/g, '').trim());
      return Number.isFinite(n) ? n : null;
    }

    const rows = rawRows.map(r => {
      const out = {};
      for (const k of Object.keys(r)) out[normalizeHeader(k)] = r[k];
      return out;
    });

    const firstRow = rows[0] || {};

    const sheetDocDate = firstRow.doc_date || firstRow.docdate || null;
    const sheetContainer = firstRow.container_no || firstRow.containerno || null;

    const finalDocDate =
      (req.body.doc_date && req.body.doc_date.trim()) ||
      sheetDocDate ||
      null;

    const finalContainerNo =
      (req.body.container_no && req.body.container_no.trim()) ||
      sheetContainer ||
      null;

    const parsedRows = [];
    for (const r of rows) {
      const product = String(r.product || r.raw_product_name || '').trim();
      if (!product) continue;

      parsedRows.push({
        line_no: numOrNull(r.line ?? r.line_no),
        raw_product_name: product,
        ncm: String(r.ncm || '').trim() || null,
        package_type: String(r.package || r.package_type || '').trim() || null,
        package_code: String(r.code || r.package_code || '').trim() || null,
        qty_packages: numOrNull(r.packages ?? r.qty_packages),
        net_kg: numOrNull(r.net_kg ?? r.netkg ?? r.net),
        gross_kg: numOrNull(r.gross_kg ?? r.grosskg ?? r.gross),
        lot_number: String(r.batch || r.lot_number || r.lot || '').trim() || null
      });
    }

    if (!parsedRows.length) throw new Error('No usable rows found.');

    const inboundDocId = db.createInboundDoc({
      doc_date: finalDocDate,
      container_no: finalContainerNo,
      source_filename: req.file.originalname,
      notes: `xlsx_rows=${parsedRows.length}`
    });

    if (typeof db.updateInboundDocRawText === 'function') {
      db.updateInboundDocRawText(
        inboundDocId,
        JSON.stringify({ source: 'XLSX_IMPORT', sheet: firstSheetName, rows: parsedRows })
      );
    }

    for (const r of parsedRows) {
      const skuId = db.findSkuIdByAliasOrName(r.raw_product_name);
      db.addInboundDocLine(inboundDocId, { ...r, sku_id: skuId });
    }

    return res.redirect(`/inventory/inbound/${inboundDocId}?msg=${encodeURIComponent('Imported from XLSX (auto header detected).')}`);
  } catch (e) {
    const docs = db.listInboundDocs();
    return res.status(400).render('inventory_inbound', { docs, msg: e?.message || String(e) });
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
// Webhook endpoint
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
// Inventory: SKU Settings sync + updates (unchanged)
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
// Organic Audit Search (single canonical set)
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
  let lotAvailability = [];
  if (selectedSkuId) {
  invoices = db.listInvoicesContainingSku({ skuId: Number(selectedSkuId), startDate, endDate });
  lots = db.listLotsForSku(Number(selectedSkuId));
  lotAvailability = db.listLotAvailabilityForSku(Number(selectedSkuId));
  }

  res.render('inventory_audit_search', {
    msg,
    organicSkus,
    selectedSkuId,
    startDate,
    endDate: endDate || '',
    invoices,
    lots,
    lotAvailability
  });
});

app.post('/inventory/audit/scan', requireConnected, async (req, res) => {
const fresh = await withFreshClient();
const conn = fresh.conn;
const oauthClient = fresh.oauthClient;

  try {
    const skuId = Number(req.body.sku_id);
    const startDate = String(req.body.start_date || (db.getSetting('organic_tracking_start') || '2025-01-01')).trim();
    const endDate = String(req.body.end_date || '').trim() || null;

    if (!skuId) throw new Error('Missing sku_id');

    const sku = db.sqlite.prepare(`SELECT * FROM skus WHERE id=?`).get(skuId);
    if (!sku || !sku.is_organic) throw new Error('Selected SKU is not marked organic.');
    if (!sku.qbo_item_id) throw new Error('SKU has no qbo_item_id. Sync SKUs from QBO first.');

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

// 🔒 Prevent timeouts / 502s on massive ranges
if (metas.length > 2000) {
  throw new Error(
    `Too many invoices in range (${metas.length}). Narrow the date range (try 30–60 days).`
  );
}

    const targetItemId = String(sku.qbo_item_id);
    let indexed = 0;

    console.log('[audit-scan]', { skuId, targetItemId, startDate, endDate, invoicesToRead: metas.length });

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

    console.log('[audit-scan] indexedLines=', indexed);

    return res.redirect(
      `/inventory/audit/search?sku=${skuId}&start=${encodeURIComponent(startDate)}${endDate ? `&end=${encodeURIComponent(endDate)}` : ''}&msg=${encodeURIComponent(`Scan complete. Invoices checked: ${metas.length}. Indexed lines: ${indexed}.`)}`
    );
  } catch (e) {
    return res.redirect(
      `/inventory/audit/search?sku=${encodeURIComponent(req.body.sku_id || '')}&start=${encodeURIComponent(req.body.start_date || '')}&end=${encodeURIComponent(req.body.end_date || '')}&msg=${encodeURIComponent(`Scan failed: ${e?.message || e}`)}`
    );
  }
});

app.post('/inventory/audit/assign-lot', requireConnected, (req, res) => {
  try {
    const skuId = Number(req.body.sku_id);
    const lotId = req.body.lot_id ? Number(req.body.lot_id) : null;
    const startDate = String(req.body.start_date || (db.getSetting('organic_tracking_start') || '2025-01-01')).trim();
    const endDate = String(req.body.end_date || '').trim() || null;

    const ids = Array.isArray(req.body.invoice_ids) ? req.body.invoice_ids : (req.body.invoice_ids ? [req.body.invoice_ids] : []);
    if (!skuId) throw new Error('Missing sku_id');
    if (ids.length === 0) throw new Error('No invoices selected.');

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

    return res.redirect(`/inventory/audit/search?sku=${skuId}&start=${encodeURIComponent(startDate)}${endDate ? `&end=${encodeURIComponent(endDate)}` : ''}&msg=${encodeURIComponent('Assigned lot to selected invoices.')}`);
  } catch (e) {
    return res.redirect(`/inventory/audit/search?sku=${encodeURIComponent(req.body.sku_id || '')}&start=${encodeURIComponent(req.body.start_date || '')}&end=${encodeURIComponent(req.body.end_date || '')}&msg=${encodeURIComponent(`Assign failed: ${e?.message || e}`)}`);
  }
});

// Lot report
app.get('/inventory/audit/lot-report', requireConnected, (req, res) => {
  try {
    const skuId = Number(req.query.sku);
    const lotId = req.query.lot ? Number(req.query.lot) : null;
    const startDate = String(req.query.start || (db.getSetting('organic_tracking_start') || '2025-01-01')).trim();
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

// Auto-assign per SKU
app.post('/inventory/audit/auto-assign', requireConnected, (req, res) => {
  try {
    const skuId = Number(req.body.sku_id);
    const startDate = String(req.body.start_date || (db.getSetting('organic_tracking_start') || '2025-01-01')).trim();
    const endDate = String(req.body.end_date || '').trim() || null;

    if (!skuId) throw new Error('Missing sku_id');

    const sku = db.sqlite.prepare(`SELECT * FROM skus WHERE id=?`).get(skuId);
    if (!sku || !sku.is_organic) throw new Error('Selected SKU is not organic.');

    // 1) Load lots with remaining
    const lots = db.listLotAvailabilityForSku(skuId)
      .filter(l => Number(l.remaining_units || 0) > 0);

    // 2) Load unassigned invoices for this SKU
    const invs = db.listUnassignedInvoicesForSku({ skuId, startDate, endDate });

    let lotIdx = 0;
    let assignedInvoices = 0;
    let splitAllocations = 0;
    let unknownQty = 0;

    for (const inv of invs) {
      let need = Number(inv.qty_units || 0);
      if (!need || need <= 0) continue;

      const rows = [];

      while (need > 0 && lotIdx < lots.length) {
        const cur = lots[lotIdx];
        const remaining = Number(cur.remaining_units || 0);

        if (remaining <= 0) { lotIdx++; continue; }

        const take = Math.min(need, remaining);

        rows.push({
          sku_id: skuId,
          lot_id: Number(cur.lot_id),
          qty_units: take,
          method: 'AUTO_SUGGEST',
          note: `Auto FIFO by inbound date (${cur.inbound_date || 'unknown'})`
        });

        cur.remaining_units = remaining - take;
        need -= take;

        if (cur.remaining_units <= 0) lotIdx++;
      }

      // If we ran out of lots, any leftover becomes UNKNOWN LOT
      if (need > 0) {
        unknownQty += need;
        rows.push({
          sku_id: skuId,
          lot_id: null,
          qty_units: need,
          method: 'AUTO_SUGGEST',
          note: 'Auto-assign: insufficient inbound lot balance (UNKNOWN LOT)'
        });
      }

      // Write allocations (can include multiple rows => split across lots)
      db.replaceAuditAllocations({
        invoiceId: String(inv.qbo_invoice_id),
        txnDate: inv.txn_date || null,
        customerName: inv.customer_name || null,
        rows
      });

      assignedInvoices++;
      if (rows.length > 1) splitAllocations++;
    }

    const msg = `Auto-assign (qty-safe) done. Invoices assigned=${assignedInvoices}, splitAcrossLots=${splitAllocations}, unknownQty=${unknownQty.toFixed(2).replace(/\.00$/,'')}`;
    return res.redirect(`/inventory/audit/search?sku=${skuId}&start=${encodeURIComponent(startDate)}${endDate ? `&end=${encodeURIComponent(endDate)}` : ''}&msg=${encodeURIComponent(msg)}`);

  } catch (e) {
    return res.redirect(`/inventory/audit/search?msg=${encodeURIComponent(`Auto-assign failed: ${e?.message || e}`)}`);
  }
});

// Auto-assign ALL
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

    const msg = `Auto-assign ALL complete: skus=${totalSkus}, assigned=${totalAssigned}, unknown=${totalUnknown}, skipped(existing)=${totalSkipped}.`;
    return res.redirect(`/inventory/audit/search?start=${encodeURIComponent(startDate)}${endDate ? `&end=${encodeURIComponent(endDate)}` : ''}&msg=${encodeURIComponent(msg)}`);
  } catch (e) {
    return res.redirect(`/inventory/audit/search?msg=${encodeURIComponent(`Auto-assign ALL failed: ${e?.message || e}`)}`);
  }
});

// Master report
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