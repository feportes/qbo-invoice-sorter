
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
import { qboReadItemByName, qboQuery, qboReadInvoiceWithRetry, qboSendInvoice } from './src/qbo.js';
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

  // ~2ÃÂ¢ÃÂÃÂ3 minutes total worst-case. Good for bursts.
  const delays = [1500, 2500, 4000, 6500, 10000, 15000, 20000, 25000, 30000, 30000, 30000, 30000];

  for (let i = 0; i < retries; i++) {
    try {
      return await processInvoice({ oauthClient, realmId, invoiceId, source });
    } catch (e) {
      lastErr = e;
      const msg = String(e?.message || e).toLowerCase();

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
    const categoryCount = await syncCategories(oauthClient, conn.realm_id,);

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

  const rulesSorted = rules.map(r => {
    const label = (r.match_type === 'exact')
      ? (custMap.get(String(r.customer_id || '')) || '')
      : (r.prefix || '');
    return { ...r, customer_label: label };
  }).sort((a, b) => {
    const ea = a.enabled ? 0 : 1;
    const eb = b.enabled ? 0 : 1;
    if (ea !== eb) return ea - eb;

    const ma = a.match_type === 'exact' ? 0 : 1;
    const mb = b.match_type === 'exact' ? 0 : 1;
    if (ma !== mb) return ma - mb;

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
    const resolvedId = await resolveInvoiceId(oauthClient, conn.realm_id, invoice_id);

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

// ==========================================================
// EMAIL JOB AUTH + DATE HELPERS
// ==========================================================
function requireJobKey(req, res, next) {
  const expected = process.env.JOB_KEY;
  if (!expected) return res.status(500).send('JOB_KEY not configured');
  if (req.header('X-JOB-KEY') !== expected) return res.status(403).send('Forbidden');
  next();
}

function daysBetween(a, b) {
  const da = new Date(a + 'T00:00:00Z');
  const dbb = new Date(b + 'T00:00:00Z');
  return Math.round((dbb - da) / (1000 * 60 * 60 * 24));
}

function todayISO() {
  const now = new Date();
  return now.toISOString().slice(0, 10);
}

// ==========================================================
// Shared Email Job Runner (used by cron + admin run-now)
// ==========================================================
async function runQboEmailJob({ dry = true, startDate = null, maxInvoices = 2000 } = {}) {
  const { conn, oauthClient } = await withFreshClient();
  const tdy = todayISO();

  // Default: last 120 days
  if (!startDate) {
    const d = new Date(Date.now() - 120 * 24 * 60 * 60 * 1000);
    startDate = d.toISOString().slice(0, 10);
  }

  // customer id -> name map (from your synced customers table)
  const customerNameMap = new Map(db.listCustomers().map(c => [String(c.id), c.display_name]));

  // previews (dry run only)
  const previewSend = [];
  const previewPre = [];
  const previewPost = [];

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
          scanned,
          eligibleSendInvoice,
          eligibleReminderPre,
          eligibleReminderPost,
          sentInvoices,
          sentRemindersPre,
          sentRemindersPost,
          failed,
          capped: true,
          startDate,
          previewSend,
          previewPre,
          previewPost
        };
      }

      const invoiceId = String(inv.Id);
      const docNumber = String(inv.DocNumber || invoiceId);
      const customerId = String(inv?.CustomerRef?.value || '');
      const customerName = customerNameMap.get(customerId) || String(inv?.CustomerRef?.name || '');

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
      const txnDate = String(inv.TxnDate || '');
        const shouldSendInvoice = settings.enabled_send_invoice && !emailAlreadySent && txnDate <= tdy;

      if (shouldSendInvoice) {
        eligibleSendInvoice++;

        if (dry && previewSend.length < 50) {
          previewSend.push({
            docNumber,
            customerName,
            dueDate: inv.DueDate || '',
            balance,
            emailStatus: inv.EmailStatus || ''
          });
        }

        if (!dry) {
          try {
            await qboSendInvoice(oauthClient, conn.realm_id, invoiceId);
            sentInvoices++;
          } catch (e) {
              failed++;
              const _qboErr = e?.response?.data?.Fault?.Error?.[0]?.Detail
                || e?.response?.data?.Fault?.Error?.[0]?.Message
                || e?.response?.data
                || e?.message || e;
              console.log(`[email-job] send invoice failed id=${invoiceId} doc=${docNumber} err=${JSON.stringify(_qboErr)}`);
          }
        }
      }

      const due = inv.DueDate ? String(inv.DueDate) : null;
      if (!due) continue;

      // PRE-DUE reminder (one-time, only before due, only if already emailed once)
      if (settings.enabled_reminder) {
        if (daysBetween(tdy, due) > 0 && emailAlreadySent) {
          const daysBefore = Number(settings.reminder_days_before_due || 3);
          const delta = daysBetween(tdy, due);

          if (delta === daysBefore) {
            if (!db.hasReminderBeenSent(invoiceId, 'REMINDER_PRE')) {
              eligibleReminderPre++;

              if (dry && previewPre.length < 50) {
                previewPre.push({
                  docNumber,
                  customerName,
                  dueDate: due,
                  balance
                });
              }

              if (!dry) {
                try {
                  await qboSendInvoice(oauthClient, conn.realm_id, invoiceId);
                  db.logReminderSent({ invoiceId, type: 'REMINDER_PRE', status: 'SENT' });
                  sentRemindersPre++;
                } catch (e) {
                  db.logReminderSent({ invoiceId, type: 'REMINDER_PRE', status: 'FAILED', error: e?.message || String(e) });
                  failed++;
                  console.log(`[email-job] pre-due reminder failed id=${invoiceId} doc=${docNumber} err=${e?.message || e}`);
                }
              }
            }
          }
        }
      }

      // POST-DUE reminder (one-time, exact N days after due, only if already emailed once)
      if (settings.enabled_post_due_reminder) {
        if (emailAlreadySent) {
          const daysAfterDue = daysBetween(due, tdy);
          const targetAfter = Number(settings.post_due_days_after_due || 3);

          if (daysAfterDue === targetAfter) {
            if (!db.hasReminderBeenSent(invoiceId, 'REMINDER_POST')) {
              eligibleReminderPost++;

              if (dry && previewPost.length < 50) {
                previewPost.push({
                  docNumber,
                  customerName,
                  dueDate: due,
                  balance
                });
              }

              if (!dry) {
                try {
                  await qboSendInvoice(oauthClient, conn.realm_id, invoiceId);
                  db.logReminderSent({ invoiceId, type: 'REMINDER_POST', status: 'SENT' });
                  sentRemindersPost++;
                } catch (e) {
                  db.logReminderSent({ invoiceId, type: 'REMINDER_POST', status: 'FAILED', error: e?.message || String(e) });
                  failed++;
                  console.log(`[email-job] post-due reminder failed id=${invoiceId} doc=${docNumber} err=${e?.message || e}`);
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
    scanned,
    eligibleSendInvoice,
    eligibleReminderPre,
    eligibleReminderPost,
    sentInvoices,
    sentRemindersPre,
    sentRemindersPost,
    failed,
    capped: false,
    startDate,
    previewSend,
    previewPre,
    previewPost
  };
}

// Cron endpoint
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

    return res.render('inventory_allocate', { connected: true, msg: 'ÃÂ¢ÃÂÃÂ Allocation applied successfully.', plan });
  } catch (e) {
    const conn = db.getConnection();
    return res.status(400).render('inventory_allocate', { connected: !!conn, msg: e?.message || String(e), plan: null });
  }
});

// ==========================================================
// INVENTORY ROUTES
// ==========================================================

// Helper: build left/right slot arrays for a container
function buildContainerSlots(containerNo, db) {
  const { leftMax, rightMax, flip } = db.getContainerDepths(containerNo);
  const pallets = db.listPalletsInContainer(containerNo);
  const palletMap = {};
  for (const p of pallets) palletMap[p.location_code] = p;
  const left = [];
  for (let d = 1; d <= leftMax; d++) {
    const code = `C${containerNo}-L-${String(d).padStart(2,'0')}`;
    left.push({ code, depth: d, pallet: palletMap[code] || null });
  }
  const right = [];
  for (let d = 1; d <= rightMax; d++) {
    const code = `C${containerNo}-R-${String(d).padStart(2,'0')}`;
    right.push({ code, depth: d, pallet: palletMap[code] || null });
  }
  return { left, right, flip, leftMax, rightMax };
}

// GET /inventory/map
app.get('/inventory/map', requireConnected, (req, res) => {
  try {
    const containers = db.listContainers();
    let defaultC = 1;
    for (const c of containers) {
      const pallets = db.listPalletsInContainer(c.containerNo);
      if (pallets.length > 0) { defaultC = c.containerNo; break; }
    }
    const containerNo = parseInt(req.query.c || defaultC, 10);
    const { left, right, flip } = buildContainerSlots(containerNo, db);
    const c1Mode = db.getSetting('container_mode_C1') || 'S';
    const slotOptions = db.listValidSlotCodes(containerNo);
    const walkinPallets = db.listPalletsInWalkin();
    const walkinLoose   = db.listWalkinLoose();
    const returnsPallets = db.listPalletsInReturns();
    res.render('inventory_map', {
      containerNo, c1Mode, containers, left, right, flip,
      walkinPallets, walkinLoose, returnsPallets, slotOptions,
      msg: String(req.query.msg || '') || null
    });
  } catch (e) {
    res.status(500).send(`Inventory map error: ${e?.message || e}`);
  }
});

// GET /inventory/walkin
app.get('/inventory/walkin', requireConnected, (req, res) => {
  try {
    const pallets = db.listPalletsInWalkin();
    const groupMap = {};
    for (const p of pallets) {
      const key = p.sku_id;
      if (!groupMap[key]) groupMap[key] = { sku_name: p.sku_name, sku_id: p.sku_id, slots: [] };
      groupMap[key].slots.push(p);
    }
    const slotGroups = Object.values(groupMap);
    const rows = pallets;
    res.render('inventory_walkin', { pallets, slotGroups, rows });
  } catch (e) {
    res.status(500).send(`Walk-in error: ${e?.message || e}`);
  }
});

// GET /inventory/returns
app.get('/inventory/returns', requireConnected, (req, res) => {
  try {
    const pallets = db.listPalletsInReturns();
    res.render('inventory_returns', { pallets });
  } catch (e) {
    res.status(500).send(`Returns error: ${e?.message || e}`);
  }
});

// GET /inventory/yard
app.get('/inventory/yard', requireConnected, (req, res) => {
  try {
    const yard = db.listPalletsByLocationCode('YARD') || [];
    const modeLabel = 'Grid View';
    const rangeLabel = `${yard.length} pallet(s)`;
    res.render('inventory_yard', { yard, modeLabel, rangeLabel });
  } catch (e) {
    res.status(500).send(`Yard error: ${e?.message || e}`);
  }
});

// GET /inventory/add-pallet
app.get('/inventory/add-pallet', requireConnected, (req, res) => {
  try {
    const containerNo = parseInt(req.query.c || 1, 10);
    const containers = db.listContainers();
    const { left, right } = buildContainerSlots(containerNo, db);
    const skus = db.listSkusActiveOnly();
    const configs = db.listPalletConfigsAll();
    res.render('inventory_add_pallet', {
      containerNo, containers, left, right, skus, configs,
      msg: String(req.query.msg || '') || null
    });
  } catch (e) {
    res.status(500).send(`Add pallet error: ${e?.message || e}`);
  }
});

// POST /inventory/add-pallet
app.post('/inventory/add-pallet', requireConnected, async (req, res) => {
  try {
    const { sku_id, lot_id, pallet_config_id, location_code, qty_units, pallet_tag, notes, containerNo } = req.body;
    if (!sku_id || !location_code || !qty_units) throw new Error('Missing required fields');
    db.createPallet({
      skuId: Number(sku_id),
      lotId: lot_id ? Number(lot_id) : null,
      palletConfigId: pallet_config_id ? Number(pallet_config_id) : null,
      locationCode: String(location_code),
      qtyUnits: Number(qty_units),
      palletTag: pallet_tag || null,
      notes: notes || null,
      userName: 'admin'
    });
    const c = containerNo || 1;
    res.redirect(`/inventory/add-pallet?c=${c}&msg=${encodeURIComponent('Pallet added.')}`);
  } catch (e) {
    res.redirect(`/inventory/add-pallet?msg=${encodeURIComponent('Error: ' + (e?.message || e))}`);
  }
});

// POST /inventory/move (form-based)
app.post('/inventory/move', requireConnected, (req, res) => {
  try {
    const { pallet_id, to_location_code, redirect_to } = req.body;
    if (!pallet_id || !to_location_code) throw new Error('Missing pallet_id or to_location_code');
    const loc = db.getLocationByCode(String(to_location_code));
    if (!loc) throw new Error(`Unknown location: ${to_location_code}`);
    db.movePallet(Number(pallet_id), loc.id, 'user');
    const back = redirect_to || '/inventory/map';
    res.redirect(`${back}?msg=${encodeURIComponent('Pallet moved.')}`);
  } catch (e) {
    const back = req.body.redirect_to || '/inventory/map';
    res.redirect(`${back}?msg=${encodeURIComponent('Move failed: ' + (e?.message || e))}`);
  }
});

// POST /inventory/move-json (drag-drop AJAX)
app.post('/inventory/move-json', requireConnected, (req, res) => {
  try {
    const { pallet_id, to_location_code } = req.body;
    if (!pallet_id || !to_location_code) throw new Error('Missing pallet_id or to_location_code');
    const loc = db.getLocationByCode(String(to_location_code));
    if (!loc) throw new Error(`Unknown location: ${to_location_code}`);
    db.movePallet(Number(pallet_id), loc.id, 'user');
    res.json({ ok: true });
  } catch (e) {
    res.status(400).json({ ok: false, error: e?.message || String(e) });
  }
});

// POST /inventory/break-to-walkin
app.post('/inventory/break-to-walkin', requireConnected, (req, res) => {
  try {
    const { pallet_id, qty, redirect_to } = req.body;
    if (!pallet_id) throw new Error('Missing pallet_id');
    db.breakPalletToWalkin(Number(pallet_id), qty ? Number(qty) : null, 'user');
    const back = redirect_to || '/inventory/map';
    res.redirect(`${back}?msg=${encodeURIComponent('Pallet broken to walk-in.')}`);
  } catch (e) {
    const back = req.body.redirect_to || '/inventory/map';
    res.redirect(`${back}?msg=${encodeURIComponent('Break failed: ' + (e?.message || e))}`);
  }
});

// GET /inventory/settings/containers
app.get('/inventory/settings/containers', requireConnected, (req, res) => {
  try {
    const c = {};
    for (let n = 1; n <= 7; n++) {
      c[`C${n}`] = {
        mode:  db.getSetting(`container_mode_C${n}`)  || (n === 1 ? 'S' : 'L'),
        flip:  db.getSetting(`container_flip_C${n}`)  || '0',
        label: n === 1 ? '20ft' : '40ft'
      };
    }
    res.render('inventory_container_settings', { c, msg: String(req.query.msg || '') || null });
  } catch (e) {
    res.status(500).send(`Container settings error: ${e?.message || e}`);
  }
});

// POST /inventory/settings/containers
app.post('/inventory/settings/containers', requireConnected, (req, res) => {
  try {
    for (let n = 1; n <= 7; n++) {
      const mode = req.body[`mode_C${n}`] || (n === 1 ? 'S' : 'L');
      const flip = req.body[`flip_C${n}`] === '1' ? '1' : '0';
      db.setSetting(`container_mode_C${n}`, mode);
      db.setSetting(`container_flip_C${n}`, flip);
    }
    res.redirect('/inventory/settings/containers?msg=' + encodeURIComponent('Container settings saved.'));
  } catch (e) {
    res.redirect('/inventory/settings/containers?msg=' + encodeURIComponent('Save failed: ' + (e?.message || e)));
  }
});

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

    for (const invoiceId of invoiceIds) {
      try {
        const { conn, oauthClient } = await withFreshClient();

        await processInvoiceWithRetry({
          oauthClient,
          realmId: conn.realm_id,
          invoiceId,
          source: 'webhook',
          retries: 10
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
// Inventory: SKU Settings sync + updates
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
// Email Automation UI (GET/POST/Run-Now)
// ==========================================================

app.get('/admin/email-automation', requireConnected, (req, res) => {
  const customers = db.listCustomers();
  const settings = db.listEmailCustomerSettings();

  const map = new Map();
  for (const s of settings) map.set(String(s.customer_id), s);

  const rows = customers.map(c => {
    const s = map.get(String(c.id)) || {
      enabled_send_invoice: 0,
      enabled_reminder: 0,
      reminder_days_before_due: 3,
      enabled_post_due_reminder: 0,
      post_due_days_after_due: 3
    };

    return {
      customer_id: c.id,
      display_name: c.display_name,
      enabled_send_invoice: Number(s.enabled_send_invoice || 0),
      enabled_reminder: Number(s.enabled_reminder || 0),
      reminder_days_before_due: Number(s.reminder_days_before_due || 3),
      enabled_post_due_reminder: Number(s.enabled_post_due_reminder || 0),
      post_due_days_after_due: Number(s.post_due_days_after_due || 3)
    };
  });

  const active = rows.filter(r =>
    r.enabled_send_invoice || r.enabled_reminder || r.enabled_post_due_reminder
  );

  // IMPORTANT: always pass preview arrays so EJS never throws
  res.render('admin_email_automation', {
    rows,
    active,
    msg: String(req.query.msg || '') || null,
    previewSend: [],
    previewPre: [],
    previewPost: []
  });
});

app.post('/admin/email-automation/save', requireConnected, (req, res) => {
  try {
    const toArr = (x) => Array.isArray(x) ? x : (x !== undefined ? [x] : []);
    const customerIds = toArr(req.body.customer_id);

    for (const id of customerIds) {
      const enabled_send_invoice = req.body[`enabled_send_invoice_${id}`] === 'on';
      const enabled_reminder = req.body[`enabled_reminder_${id}`] === 'on';
      const reminder_days_before_due = Number(req.body[`reminder_days_before_due_${id}`] || 3);

      const enabled_post_due_reminder = req.body[`enabled_post_due_reminder_${id}`] === 'on';
      const post_due_days_after_due = Number(req.body[`post_due_days_after_due_${id}`] || 3);

      db.upsertEmailCustomerSettings({
        customer_id: id,
        enabled_send_invoice,
        enabled_reminder,
        reminder_days_before_due,
        enabled_post_due_reminder,
        post_due_days_after_due
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

    // rebuild rows/active exactly like GET page
    const customers = db.listCustomers();
    const settings = db.listEmailCustomerSettings();
    const map = new Map(settings.map(s => [String(s.customer_id), s]));

    const rows = customers.map(c => {
      const s = map.get(String(c.id)) || {
        enabled_send_invoice: 0,
        enabled_reminder: 0,
        reminder_days_before_due: 3,
        enabled_post_due_reminder: 0,
        post_due_days_after_due: 3
      };

      return {
        customer_id: c.id,
        display_name: c.display_name,
        enabled_send_invoice: Number(s.enabled_send_invoice || 0),
        enabled_reminder: Number(s.enabled_reminder || 0),
        reminder_days_before_due: Number(s.reminder_days_before_due || 3),
        enabled_post_due_reminder: Number(s.enabled_post_due_reminder || 0),
        post_due_days_after_due: Number(s.post_due_days_after_due || 3)
      };
    });

    const active = rows.filter(r =>
      r.enabled_send_invoice || r.enabled_reminder || r.enabled_post_due_reminder
    );

    const msg =
      `${dry ? 'Dry run' : 'Sent'}: scanned=${summary.scanned}, eligibleSend=${summary.eligibleSendInvoice}, ` +
      `eligiblePre=${summary.eligibleReminderPre}, eligiblePost=${summary.eligibleReminderPost}, ` +
      `sentInvoices=${summary.sentInvoices}, sentPre=${summary.sentRemindersPre}, sentPost=${summary.sentRemindersPost}, failed=${summary.failed}` +
      (summary.capped ? ' (CAP HIT)' : '');

    return res.render('admin_email_automation', {
      rows,
      active,
      msg,
      previewSend: summary.previewSend || [],
      previewPre: summary.previewPre || [],
      previewPost: summary.previewPost || []
    });
  } catch (e) {
    return res.redirect('/admin/email-automation?msg=' + encodeURIComponent('Run failed: ' + (e?.message || e)));
  }
});

// ==========================================================
// Server start
// ==========================================================
const port = process.env.PORT || 3000;
app.listen(port, () => console.log(`App running on http://localhost:${port}`));