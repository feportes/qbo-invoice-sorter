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

ensureSchema();
seedDefaults();

app.set('views', path.join(__dirname, 'src', 'views'));
app.set('view engine', 'ejs');

app.use(morgan('dev'));
app.use(express.json({ verify: rawBodySaver, limit: '2mb' }));
app.use(express.urlencoded({ extended: true }));

app.get('/', (req, res) => {
  const conn = db.getConnection();
  res.render('index', {
    connected: !!conn,
    realmId: conn?.realm_id || null,
    companyName: conn?.company_name || null,
  });
});

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

// ===============================
// Inventory: Container Settings (mode + flip)
// ===============================
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

// ===============================
// Inventory: Yard + Map (asymmetric)
// ===============================
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

  res.render('inventory_map', {
    containerNo,
    containers,
    left,
    right,
    containerLabel: depths.label,
    slotOptions
  });
});

const port = process.env.PORT || 3000;
app.listen(port, () => console.log(`App running on http://localhost:${port}`));
