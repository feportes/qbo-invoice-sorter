
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

async function resolveInvoiceId(oauthClient, realmId, invoiceIdOrDocNumber) {
  const raw = String(invoiceIdOrDocNumber || '').trim();
  if (!raw) throw new Error('Missing invoice id');

  // 1) Try as QBO Id first
  try {
    await qboReadInvoiceWithRetry(oauthClient, realmId, raw, 2);
    return raw;
  } catch {}

  // 2) Try as DocNumber
  const safe = raw.replace(/'/g, "\\'");
  const q = `select Id, DocNumber from Invoice where DocNumber='${safe}' maxresults 1`;
  const r = await qboQuery(oauthClient, realmId, q);
  const inv = r?.QueryResponse?.Invoice?.[0];
  if (inv?.Id) return String(inv.Id);

  throw new Error(`Invoice not found by Id or DocNumber: ${raw}`);
}

// ==========================================================
// Helper: retry processInvoice on "Invoice not found"
// ==========================================================
async function processInvoiceWithRetry({ oauthClient, realmId, invoiceId, source, retries = 12 }) {
  let lastErr = null;

  // ~2–3 minutes total worst-case. Good for bursts.
  const delays = [1500, 2500, 4000, 6500, 10000, 15000, 20000, 25000, 30000, 30000, 30000, 30000];

  for (let i = 0; i < retries; i++) {
    try {
      return await processInvoice({ oauthClient, realmId, invoiceId, source });
    } catch (e) {
      lastErr = e;
      const msg = String(e?.message || e).toLowerCase();

      // Only retry the “eventual consistency” / availability cases
      const isNotFound =
        msg.includes('invoice not found') ||
        msg.includes('not found') ||
        msg.includes('404');

      if (!isNotFound) throw e;

      const delay = delays[Math.min(i, delays.length - 1)];
      console.log(`[webhook] invoiceId=${invoiceId} not readable yet (attempt ${i + 1}/${retries}), retrying in ${delay}ms`);
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

  const custMap = new Map(customers.map(c => [String(c.id), c.display_name]));

  // Add display label and sort alphabetically (enabled first)
  const rulesSorted = rules.map(r => {
    const label = (r.match_type === 'exact')
      ? (custMap.get(String(r.customer_id || '')) || '')
      : (r.prefix || '');
    return { ...r, customer_label: label };
  }).sort((a, b) => {
    // enabled first
    const ea = a.enabled ? 0 : 1;
    const eb = b.enabled ? 0 : 1;
    if (ea !== eb) return ea - eb;

    // exact before prefix (optional)
    const ma = a.match_type === 'exact' ? 0 : 1;
    const mb = b.match_type === 'exact' ? 0 : 1;
    if (ma !== mb) return ma - mb;

    // alphabetical label
    return String(a.customer_label || '').localeCompare(String(b.customer_label || ''), undefined, { sensitivity: 'base' });
  });

  res.render('rules', { customers, rules: rulesSorted });
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

// Force reprocess: clears processed lock and runs again (accepts QBO Id OR DocNumber)
app.post('/admin/process-invoice-force', requireConnected, async (req, res) => {
  try {
    const invoice_id = String(req.body.invoice_id || '').trim();
    if (!invoice_id) throw new Error('Missing invoice id');

    const { conn, oauthClient } = await withFreshClient();

    // ✅ Resolve DocNumber -> Id (or keep Id if already valid)
    const resolvedId = await resolveInvoiceId(oauthClient, conn.realm_id, invoice_id);

    // ✅ Clear processed lock for the REAL QBO Id
    db.clearProcessed(String(resolvedId));

    const result = await processInvoiceWithRetry({
      oauthClient,
      realmId: conn.realm_id,
      invoiceId: String(resolvedId),
      source: 'manual_force',
      retries: 12
    });

    res.render('process_result', { result });
  } catch (e) {
    res.status(500).send(`Force process failed: ${e?.message || e}`);
  }
});

function requireJobKey(req, res, next) {
  const expected = process.env.JOB_KEY;
  if (!expected) return res.status(500).send('JOB_KEY not configured');
  if (req.header('X-JOB-KEY') !== expected) return res.status(403).send('Forbidden');
  next();
}

function daysBetween(a, b) {
  // a,b are YYYY-MM-DD
  const da = new Date(a + 'T00:00:00Z');
  const dbb = new Date(b + 'T00:00:00Z');
  return Math.round((dbb - da) / (1000 * 60 * 60 * 24));
}

function todayISO() {
  const now = new Date();
  // Use UTC date for consistency (cron runs UTC)
  return now.toISOString().slice(0, 10);
}

async function runQboEmailJob({ dry = true, startDate = null, maxInvoices = 2000 } = {}) {
  const { conn, oauthClient } = await withFreshClient();
  const tdy = todayISO();

  // Default: last 120 days (prevents hammering QBO)
  if (!startDate) {
    const d = new Date(Date.now() - 120 * 24 * 60 * 60 * 1000);
    startDate = d.toISOString().slice(0, 10);
  }

  const pageSize = 100;
  let start = 1;

  let scanned = 0;
  let eligibleSendInvoice = 0;
  let eligibleReminderPre = 0;
  let eligibleReminderPost = 0;

  let sentInvoices = 0;
  let sentRemindersPre = 0;
  let sentRemindersPost = 0;
  let failed = 0;

  console.log(`[email-job] start dry=${dry} startDate=${startDate} maxInvoices=${maxInvoices}`);

  while (true) {
    const q = `select Id, DocNumber, TxnDate, DueDate, Balance, CustomerRef, EmailStatus from Invoice where TxnDate >= '${startDate}' startposition ${start} maxresults ${pageSize}`;
    const r = await qboQuery(oauthClient, conn.realm_id, q);
    const invs = r?.QueryResponse?.Invoice || [];
    scanned += invs.length;

    for (const inv of invs) {
      if (scanned >= maxInvoices) {
        console.log(`[email-job] cap reached scanned=${scanned} maxInvoices=${maxInvoices} (stopping early)`);
        return {
          scanned, eligibleSendInvoice, eligibleReminderPre, eligibleReminderPost,
          sentInvoices, sentRemindersPre, sentRemindersPost, failed,
          capped: true, startDate
        };
      }

      const invoiceId = String(inv.Id);
      const customerId = String(inv?.CustomerRef?.value || '');

      const settings = db.getEmailCustomerSettings(customerId) || {
        enabled_send_invoice: 0,
        enabled_reminder: 0,
        reminder_days_before_due: 3,
        enabled_post_due_reminder: 0,
        post_due_days_after_due: 3
      };

      const balance = Number(inv.Balance || 0);
      if (!(balance > 0)) continue;

      const emailStatus = String(inv.EmailStatus || '').toLowerCase();
      const emailAlreadySent = (emailStatus === 'emailsent');

      // A) Auto-send invoice (STRICT EmailStatus)
      const shouldSendInvoice =
        settings.enabled_send_invoice &&
        !emailAlreadySent;

      if (shouldSendInvoice) {
        eligibleSendInvoice++;
        if (!dry) {
          try {
            await qboSendInvoice(oauthClient, conn.realm_id, invoiceId);
            sentInvoices++;
          } catch (e) {
            failed++;
            console.log(`[email-job] send invoice failed id=${invoiceId} err=${e?.message || e}`);
          }
        }
      }

      // Need due date for reminders
      const due = inv.DueDate ? String(inv.DueDate) : null;
      if (!due) continue;

      // PRE-DUE reminder (one-time)
      if (settings.enabled_reminder) {
        if (daysBetween(tdy, due) > 0) { // only before due
          if (emailAlreadySent) { // only if invoice was already emailed once
            const daysBefore = Number(settings.reminder_days_before_due || 3);
            const delta = daysBetween(tdy, due);

            if (delta === daysBefore) {
              if (!db.hasReminderBeenSent(invoiceId, 'REMINDER_PRE')) {
                eligibleReminderPre++;
                if (!dry) {
                  try {
                    await qboSendInvoice(oauthClient, conn.realm_id, invoiceId);
                    db.logReminderSent({ invoiceId, type: 'REMINDER_PRE', status: 'SENT' });
                    sentRemindersPre++;
                  } catch (e) {
                    db.logReminderSent({ invoiceId, type: 'REMINDER_PRE', status: 'FAILED', error: e?.message || String(e) });
                    failed++;
                    console.log(`[email-job] pre-due reminder failed id=${invoiceId} err=${e?.message || e}`);
                  }
                }
              }
            }
          }
        }
      }

      // POST-DUE reminder (one-time, N days after due)
      if (settings.enabled_post_due_reminder) {
        if (emailAlreadySent) { // only if invoice was already emailed once
          const daysAfterDue = daysBetween(due, tdy); // positive means overdue by X days
          const targetAfter = Number(settings.post_due_days_after_due || 3);

          if (daysAfterDue === targetAfter) {
            if (!db.hasReminderBeenSent(invoiceId, 'REMINDER_POST')) {
              eligibleReminderPost++;
              if (!dry) {
                try {
                  await qboSendInvoice(oauthClient, conn.realm_id, invoiceId);
                  db.logReminderSent({ invoiceId, type: 'REMINDER_POST', status: 'SENT' });
                  sentRemindersPost++;
                } catch (e) {
                  db.logReminderSent({ invoiceId, type: 'REMINDER_POST', status: 'FAILED', error: e?.message || String(e) });
                  failed++;
                  console.log(`[email-job] post-due reminder failed id=${invoiceId} err=${e?.message || e}`);
                }
              }
            }
          }
        }
      }
    }

    if (invs.length < pageSize) break;
    start += pageSize;
  }

  console.log(`[email-job] done scanned=${scanned} eligibleSend=${eligibleSendInvoice} eligiblePre=${eligibleReminderPre} eligiblePost=${eligibleReminderPost} sentInvoices=${sentInvoices} sentPre=${sentRemindersPre} sentPost=${sentRemindersPost} failed=${failed}`);

  return {
    scanned, eligibleSendInvoice, eligibleReminderPre, eligibleReminderPost,
    sentInvoices, sentRemindersPre, sentRemindersPost, failed,
    capped: false, startDate
  };
}

app.post('/jobs/qbo-email', requireJobKey, async (req, res) => {
  res.status(200).send('OK');

  try {
    const startDate = req.body?.startDate || null;
    const maxInvoices = req.body?.maxInvoices ? Number(req.body.maxInvoices) : 2000;

    await runQboEmailJob({ dry: false, startDate, maxInvoices });
  } catch (e) {
    console.log('[email-job] fatal', e?.message || e);
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


async function runQboEmailJob({ dry = true, startDate = null, maxInvoices = 2000 } = {}) {
  const { conn, oauthClient } = await withFreshClient();

  const tdy = todayISO();

  // Default: last 120 days (prevents hammering QBO)
  if (!startDate) {
    const d = new Date(Date.now() - 120 * 24 * 60 * 60 * 1000);
    startDate = d.toISOString().slice(0, 10);
  }

  const pageSize = 100;
  let start = 1;

  let scanned = 0;
  let eligibleSendInvoice = 0;
  let eligibleReminder = 0;

  let sentInvoices = 0;
  let sentReminders = 0;
  let failed = 0;

  console.log(`[email-job] start dry=${dry} startDate=${startDate} maxInvoices=${maxInvoices}`);

  while (true) {
    const q = `select Id, DocNumber, TxnDate, DueDate, Balance, CustomerRef, EmailStatus from Invoice where TxnDate >= '${startDate}' startposition ${start} maxresults ${pageSize}`;
    const r = await qboQuery(oauthClient, conn.realm_id, q);
    const invs = r?.QueryResponse?.Invoice || [];
    scanned += invs.length;

    for (const inv of invs) {
      // Safety cap (stops huge runs)
      if (scanned >= maxInvoices) {
        console.log(`[email-job] cap reached scanned=${scanned} maxInvoices=${maxInvoices} (stopping early)`);
        return { scanned, eligibleSendInvoice, eligibleReminder, sentInvoices, sentReminders, failed, capped: true, startDate };
      }

      const invoiceId = String(inv.Id);
      const customerId = String(inv?.CustomerRef?.value || '');

      const settings = db.getEmailCustomerSettings(customerId) || {
        enabled_send_invoice: 0,
        enabled_reminder: 0,
        reminder_days_before_due: 3
      };

      const balance = Number(inv.Balance || 0);
      if (!(balance > 0)) continue; // paid/zero balance → skip

      const emailStatus = String(inv.EmailStatus || '').toLowerCase();
      const emailAlreadySent = (emailStatus === 'emailsent');

      // A) Auto-send invoice (STRICT QBO EmailStatus)
      const shouldSendInvoice =
        settings.enabled_send_invoice &&
        !emailAlreadySent;

      if (shouldSendInvoice) {
        eligibleSendInvoice++;
        if (!dry) {
          try {
            await qboSendInvoice(oauthClient, conn.realm_id, invoiceId);
            sentInvoices++;
          } catch (e) {
            failed++;
            console.log(`[email-job] send invoice failed id=${invoiceId} err=${e?.message || e}`);
          }
        }
      }

      // B) Reminder (no overdue, no repeats)
      if (settings.enabled_reminder) {
        const due = inv.DueDate ? String(inv.DueDate) : null;
        if (!due) continue;

        // Never after due date
        if (daysBetween(tdy, due) <= 0) continue;

        // Optional: only remind if invoice was already emailed at least once
        // (reduces confusion and matches real workflow)
        if (!emailAlreadySent) continue;

        const daysBefore = Number(settings.reminder_days_before_due || 3);
        const delta = daysBetween(tdy, due);

        if (delta === daysBefore) {
          if (db.hasReminderBeenSent(invoiceId)) continue;

          eligibleReminder++;

          if (!dry) {
            try {
              await qboSendInvoice(oauthClient, conn.realm_id, invoiceId);
              db.logReminderSent({ invoiceId, status: 'SENT' });
              sentReminders++;
            } catch (e) {
              db.logReminderSent({ invoiceId, status: 'FAILED', error: e?.message || String(e) });
              failed++;
              console.log(`[email-job] reminder failed id=${invoiceId} err=${e?.message || e}`);
            }
          }
        }
      }
    }

    if (invs.length < pageSize) break;
    start += pageSize;
  }

  console.log(`[email-job] done scanned=${scanned} eligibleSend=${eligibleSendInvoice} eligibleReminder=${eligibleReminder} sentInvoices=${sentInvoices} sentReminders=${sentReminders} failed=${failed}`);

  return { scanned, eligibleSendInvoice, eligibleReminder, sentInvoices, sentReminders, failed, capped: false, startDate };
}

// ==========================================================
// Webhook endpoint
// ==========================================================
app.post('/webhooks/qbo', verifyIntuitWebhook, async (req, res) => {
  res.status(200).send('OK');

  try {
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

    // ✅ Sequential processing = fewer rate limit / propagation issues
    for (const invoiceId of invoiceIds) {
      try {
        const { conn, oauthClient } = await withFreshClient();

        await processInvoiceWithRetry({
          oauthClient,
          realmId: conn.realm_id,
          invoiceId,
          source: 'webhook',
          retries: 10 // 👈 give QBO time to “settle”
        });

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

      } catch (err) {
        console.log(`[webhook] processed error invoiceId=${invoiceId} err=${err?.message || err}`);
      }
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
  invoices = db.listUnassignedInvoicesContainingSku({ skuId: Number(selectedSkuId), startDate, endDate });
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

app.get('/inventory/audit/assigned', requireConnected, (req, res) => {
  const skuId = Number(req.query.sku);
  const startDate = String(req.query.start || (db.getSetting('organic_tracking_start') || '2025-01-01')).trim();
  const endDate = String(req.query.end || '').trim() || null;

  if (!skuId) {
    return res.redirect('/inventory/audit/search?msg=' + encodeURIComponent('Select a SKU first.'));
  }

  const sku = db.sqlite.prepare(`SELECT id, name FROM skus WHERE id=?`).get(skuId);
  const rows = db.listAssignedAllocationsForSku({ skuId, startDate, endDate });
  const lots = db.listLotsForSku(skuId);

  res.render('inventory_audit_assigned', {
    sku,
    rows,
    lots,
    startDate,
    endDate: endDate || '',
    msg: String(req.query.msg || '') || null
  });
});

app.post('/inventory/audit/assigned/delete', requireConnected, (req, res) => {
  try {
    const skuId = Number(req.body.sku_id);
    const startDate = String(req.body.start_date || '').trim();
    const endDate = String(req.body.end_date || '').trim() || '';

    const ids = Array.isArray(req.body.allocation_ids)
      ? req.body.allocation_ids
      : (req.body.allocation_ids ? [req.body.allocation_ids] : []);

    const deleted = db.deleteAuditAllocationsByIds(ids);

    return res.redirect(
      `/inventory/audit/assigned?sku=${skuId}&start=${encodeURIComponent(startDate)}&end=${encodeURIComponent(endDate)}&msg=${encodeURIComponent(`Deleted ${deleted} allocation(s).`)}`
    );
  } catch (e) {
    return res.status(500).send(`Delete allocations failed: ${e?.message || e}`);
  }
});

app.post('/inventory/audit/assigned/reassign', requireConnected, (req, res) => {
  try {
    const skuId = Number(req.body.sku_id);
    const startDate = String(req.body.start_date || '').trim();
    const endDate = String(req.body.end_date || '').trim() || '';

    const ids = Array.isArray(req.body.allocation_ids)
      ? req.body.allocation_ids
      : (req.body.allocation_ids ? [req.body.allocation_ids] : []);

    const newLotIdRaw = String(req.body.new_lot_id ?? '').trim();
    const newLotId = newLotIdRaw ? Number(newLotIdRaw) : null; // null = UNKNOWN LOT

    if (!skuId) throw new Error('Missing sku_id');
    if (ids.length === 0) throw new Error('No allocations selected.');

    // Guard only if assigning to a real lot
    if (newLotId) {
      const allocs = db.getAuditAllocationsByIds(ids);
      const movingQty = allocs
        .filter(a => Number(a.sku_id) === skuId)
        .reduce((s, a) => s + Number(a.qty_units || 0), 0);

      const inbound = db.getInboundUnitsForSkuLotId({ skuId, lotId: newLotId });
      const allocatedExcluding = db.getAllocatedUnitsForSkuLotId({
        skuId,
        lotId: newLotId,
        excludeAllocationIds: ids
      });

      if (allocatedExcluding + movingQty > inbound) {
        throw new Error(`Over-allocation blocked. Inbound=${inbound}, allocated=${allocatedExcluding}, trying to add=${movingQty}.`);
      }
    }

    const changed = db.updateAllocationLotByIds({ ids, newLotId });

    return res.redirect(
      `/inventory/audit/assigned?sku=${skuId}&start=${encodeURIComponent(startDate)}&end=${encodeURIComponent(endDate)}&msg=${encodeURIComponent(`Reassigned ${changed} allocation(s).`)}`
    );
  } catch (e) {
    const skuId = Number(req.body.sku_id || 0);
    const startDate = String(req.body.start_date || '').trim();
    const endDate = String(req.body.end_date || '').trim() || '';
    return res.redirect(
      `/inventory/audit/assigned?sku=${skuId}&start=${encodeURIComponent(startDate)}&end=${encodeURIComponent(endDate)}&msg=${encodeURIComponent(`Reassign failed: ${e?.message || e}`)}`
    );
  }
});

app.post('/inventory/audit/assigned/split', requireConnected, (req, res) => {
  try {
    const skuId = Number(req.body.sku_id);
    const startDate = String(req.body.start_date || '').trim();
    const endDate = String(req.body.end_date || '').trim() || '';

    const allocationId = Number(req.body.allocation_id);
    if (!skuId) throw new Error('Missing sku_id');
    if (!allocationId) throw new Error('Missing allocation_id');

    const lotAIdRaw = String(req.body.lot_a_id ?? '').trim();
    const lotBIdRaw = String(req.body.lot_b_id ?? '').trim();
    const lotAId = lotAIdRaw ? Number(lotAIdRaw) : null; // null = UNKNOWN LOT
    const lotBId = lotBIdRaw ? Number(lotBIdRaw) : null;

    const qtyA = Number(req.body.qty_a);
    const qtyB = Number(req.body.qty_b);

    if (!Number.isFinite(qtyA) || qtyA <= 0) throw new Error('qty_a must be > 0');
    if (!Number.isFinite(qtyB) || qtyB <= 0) throw new Error('qty_b must be > 0');

    const original = db.getAuditAllocationById(allocationId);
    if (!original) throw new Error('Allocation not found');
    if (Number(original.sku_id) !== skuId) throw new Error('Allocation sku_id mismatch');

    const originalQty = Number(original.qty_units || 0);
    const sum = qtyA + qtyB;
    if (Math.abs(sum - originalQty) > 0.000001) {
      throw new Error(`Split qty mismatch. Original=${originalQty}, qty_a+qty_b=${sum}`);
    }

    // Guard each target lot (if not UNKNOWN)
    const excludeIds = [allocationId];

    function guardLot(lotId, addQty) {
      if (!lotId) return; // UNKNOWN lot has no capacity check
      const inbound = db.getInboundUnitsForSkuLotId({ skuId, lotId });
      const allocatedExcluding = db.getAllocatedUnitsForSkuLotId({
        skuId,
        lotId,
        excludeAllocationIds: excludeIds
      });
      if (allocatedExcluding + addQty > inbound) {
        throw new Error(`Over-allocation blocked for lotId=${lotId}. Inbound=${inbound}, allocated=${allocatedExcluding}, trying to add=${addQty}`);
      }
    }

    guardLot(lotAId, qtyA);
    guardLot(lotBId, qtyB);

    // Apply split atomically
    const tx = db.sqlite.transaction(() => {
      db.deleteAuditAllocationById(allocationId);

      db.insertAuditAllocationRow({
        invoiceId: original.qbo_invoice_id,
        txnDate: original.txn_date,
        customerName: original.customer_name,
        skuId,
        lotId: lotAId,
        qtyUnits: qtyA,
        method: 'MANUAL',
        note: `Split from allocation #${allocationId}`
      });

      db.insertAuditAllocationRow({
        invoiceId: original.qbo_invoice_id,
        txnDate: original.txn_date,
        customerName: original.customer_name,
        skuId,
        lotId: lotBId,
        qtyUnits: qtyB,
        method: 'MANUAL',
        note: `Split from allocation #${allocationId}`
      });
    });

    tx();

    return res.redirect(
      `/inventory/audit/assigned?sku=${skuId}&start=${encodeURIComponent(startDate)}&end=${encodeURIComponent(endDate)}&msg=${encodeURIComponent('Split applied.')}`
    );
  } catch (e) {
    const skuId = Number(req.body.sku_id || 0);
    const startDate = String(req.body.start_date || '').trim();
    const endDate = String(req.body.end_date || '').trim() || '';
    return res.redirect(
      `/inventory/audit/assigned?sku=${skuId}&start=${encodeURIComponent(startDate)}&end=${encodeURIComponent(endDate)}&msg=${encodeURIComponent(`Split failed: ${e?.message || e}`)}`
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
// Guard: block overfill when lot_id is provided
if (lotId) {
  const inbound = db.getInboundUnitsForSkuLotId({ skuId, lotId });
  const alreadyAllocated = db.getAllocatedUnitsForSkuLotId({ skuId, lotId });

  // sum qty on selected invoices for this sku
  let selectedQty = 0;
  for (const invoiceId of ids) {
    const inv = db.sqlite.prepare(`
      SELECT SUM(qty_units) AS qty_units
      FROM invoice_sku_lines
      WHERE qbo_invoice_id=? AND sku_id=?
    `).get(String(invoiceId), Number(skuId));
    selectedQty += Number(inv?.qty_units || 0);
  }

  if (alreadyAllocated + selectedQty > inbound) {
    throw new Error(`Over-allocation blocked. Inbound=${inbound}, allocated=${alreadyAllocated}, trying to add=${selectedQty}.`);
  }
}
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

    // Lots with remaining (FIFO by inbound date)
    const lots = db.listLotAvailabilityForSku(skuId)
      .map(l => ({ ...l, remaining_units: Number(l.remaining_units || 0) }))
      .filter(l => l.remaining_units > 0);

    // Unassigned invoices only (so we don't keep redoing old work)
    const invs = db.listUnassignedInvoicesForSku({ skuId, startDate, endDate });

    let lotIdx = 0;
    let assignedInvoices = 0;
    let splitCount = 0;
    let unknownQty = 0;

    for (const inv of invs) {
      let need = Number(inv.qty_units || 0);
      if (need <= 0) continue;

      const rows = [];

      while (need > 0 && lotIdx < lots.length) {
        const cur = lots[lotIdx];
        const avail = cur.remaining_units;

        if (avail <= 0) { lotIdx++; continue; }

        const take = Math.min(need, avail);

        rows.push({
          sku_id: skuId,
          lot_id: Number(cur.lot_id),
          qty_units: take,
          method: 'AUTO_SUGGEST',
          note: `Auto FIFO (${cur.inbound_date || 'unknown'})`
        });

        cur.remaining_units -= take;
        need -= take;

        if (cur.remaining_units <= 0) lotIdx++;
      }

      if (need > 0) {
        unknownQty += need;
        rows.push({
          sku_id: skuId,
          lot_id: null,
          qty_units: need,
          method: 'AUTO_SUGGEST',
          note: 'Auto FIFO: insufficient inbound balance (Local Production / Non-Organic)'
        });
      }

      // Write allocations (can be multiple rows => split)
      db.replaceAuditAllocations({
        invoiceId: String(inv.qbo_invoice_id),
        txnDate: inv.txn_date || null,
        customerName: inv.customer_name || null,
        rows
      });

      assignedInvoices++;
      if (rows.length > 1) splitCount++;
    }

    return res.redirect(
      `/inventory/audit/search?sku=${skuId}&start=${encodeURIComponent(startDate)}${endDate ? `&end=${encodeURIComponent(endDate)}` : ''}&msg=${encodeURIComponent(`Auto-assign done. invoices=${assignedInvoices}, split=${splitCount}, unknownQty=${unknownQty}`)}`
    );
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
    let totalInvoicesAssigned = 0;
    let totalSplitInvoices = 0;
    let totalUnknownQty = 0;
    let totalSkipped = 0;

    for (const sku of skus) {
      const skuId = Number(sku.id);

      // Lots with remaining (FIFO by inbound date)
      const lots = db.listLotAvailabilityForSku(skuId)
        .map(l => ({ ...l, remaining_units: Number(l.remaining_units || 0) }))
        .filter(l => l.remaining_units > 0);

      // Only invoices that are NOT already allocated for this SKU
      const invs = db.listUnassignedInvoicesForSku({ skuId, startDate, endDate });

      if (!invs.length) {
        totalSkipped++;
        continue;
      }

      let lotIdx = 0;

      for (const inv of invs) {
        let need = Number(inv.qty_units || 0);
        if (need <= 0) continue;

        const rows = [];

        while (need > 0 && lotIdx < lots.length) {
          const cur = lots[lotIdx];
          const avail = cur.remaining_units;

          if (avail <= 0) { lotIdx++; continue; }

          const take = Math.min(need, avail);

          rows.push({
            sku_id: skuId,
            lot_id: Number(cur.lot_id),
            qty_units: take,
            method: 'AUTO_SUGGEST',
            note: `Auto FIFO (${cur.inbound_date || 'unknown'})`
          });

          cur.remaining_units -= take;
          need -= take;

          if (cur.remaining_units <= 0) lotIdx++;
        }

        if (need > 0) {
          totalUnknownQty += need;
          rows.push({
            sku_id: skuId,
            lot_id: null,
            qty_units: need,
            method: 'AUTO_SUGGEST',
            note: 'Auto FIFO: insufficient inbound balance (Local Production / Non-Organic)'
          });
        }

        db.replaceAuditAllocations({
          invoiceId: String(inv.qbo_invoice_id),
          txnDate: inv.txn_date || null,
          customerName: inv.customer_name || null,
          rows
        });

        totalInvoicesAssigned++;
        if (rows.length > 1) totalSplitInvoices++;
      }
    }

    const msg =
      `Auto-assign ALL (qty-safe) complete: skus=${totalSkus}, ` +
      `invoices_assigned=${totalInvoicesAssigned}, split_invoices=${totalSplitInvoices}, ` +
      `unknownQty=${totalUnknownQty}, skus_skipped(no_unassigned)=${totalSkipped}.`;

    return res.redirect(
      `/inventory/audit/search?start=${encodeURIComponent(startDate)}` +
      `${endDate ? `&end=${encodeURIComponent(endDate)}` : ''}` +
      `&msg=${encodeURIComponent(msg)}`
    );

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

app.get('/admin/email-automation', requireConnected, (req, res) => {
  const customers = db.listCustomers(); // already alphabetical in your db.js
  const settings = db.listEmailCustomerSettings();

  const map = new Map();
  for (const s of settings) map.set(String(s.customer_id), s);

  // Merge rows
  const rows = customers.map(c => {
    const s = map.get(String(c.id)) || {
      enabled_send_invoice: 0,
      enabled_reminder: 0,
      reminder_days_before_due: 3
    };
    return {
      customer_id: c.id,
      display_name: c.display_name,
      enabled_send_invoice: Number(s.enabled_send_invoice || 0),
      enabled_reminder: Number(s.enabled_reminder || 0),
      reminder_days_before_due: Number(s.reminder_days_before_due || 3)
    };
  });

  // Active setup list (enabled either invoice or reminder)
  const active = rows.filter(r => r.enabled_send_invoice || r.enabled_reminder);

  res.render('admin_email_automation', { rows, active, msg: String(req.query.msg || '') || null });
});

app.post('/admin/email-automation/save', requireConnected, (req, res) => {
  try {
    const toArr = (x) => Array.isArray(x) ? x : (x !== undefined ? [x] : []);
    const customerIds = toArr(req.body.customer_id);

    for (const id of customerIds) {
      const enabled_send_invoice = req.body[`enabled_send_invoice_${id}`] === 'on';
      const enabled_reminder = req.body[`enabled_reminder_${id}`] === 'on';
      const days = Number(req.body[`reminder_days_before_due_${id}`] || 3);

      db.upsertEmailCustomerSettings({
        customer_id: id,
        enabled_send_invoice,
        enabled_reminder,
        reminder_days_before_due: days
      });
    }

    return res.redirect('/admin/email-automation?msg=' + encodeURIComponent('Saved email automation settings.'));
  } catch (e) {
    return res.redirect('/admin/email-automation?msg=' + encodeURIComponent('Save failed: ' + (e?.message || e)));
  }
});

app.post('/admin/email-automation/run-now', requireConnected, async (req, res) => {
  try {
    const dry = String(req.body.dry || '1') === '1';
    const startDate = String(req.body.startDate || '').trim() || null;
    const maxInvoices = req.body.maxInvoices ? Number(req.body.maxInvoices) : 2000;

    const summary = await runQboEmailJob({ dry, startDate, maxInvoices });

    const msg =
      `${dry ? 'Dry run' : 'Sent'}: scanned=${summary.scanned}, eligibleSend=${summary.eligibleSendInvoice}, ` +
      `eligiblePre=${summary.eligibleReminderPre}, eligiblePost=${summary.eligibleReminderPost}, ` +
      `sentInvoices=${summary.sentInvoices}, sentPre=${summary.sentRemindersPre}, sentPost=${summary.sentRemindersPost}, failed=${summary.failed}` +
      (summary.capped ? ' (CAP HIT)' : '');

    return res.redirect('/admin/email-automation?msg=' + encodeURIComponent(msg));
  } catch (e) {
    return res.redirect('/admin/email-automation?msg=' + encodeURIComponent('Run failed: ' + (e?.message || e)));
  }
});

// Admin-only: run email job now (dry run or send)
app.post('/admin/email-automation/run-now', requireConnected, async (req, res) => {
  try {
    const dry = String(req.body.dry || '1') === '1';
    const startDate = String(req.body.startDate || '2025-01-01').trim();

    // Call the same internal function you use in /jobs/qbo-email
    const summary = await runQboEmailJob({ dry, startDate });

    return res.redirect('/admin/email-automation?msg=' + encodeURIComponent(
      `${dry ? 'Dry run' : 'Sent'}: scanned=${summary.scanned}, sendInvoice=${summary.sentInvoices}, reminders=${summary.sentReminders}, failed=${summary.failed}`
    ));
  } catch (e) {
    return res.redirect('/admin/email-automation?msg=' + encodeURIComponent('Run failed: ' + (e?.message || e)));
  }
});

const port = process.env.PORT || 3000;
app.listen(port, () => console.log(`App running on http://localhost:${port}`));