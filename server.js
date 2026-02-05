import 'dotenv/config';
import express from 'express';
import morgan from 'morgan';
import path from 'path';
import { fileURLToPath } from 'url';

import { db } from './src/db.js';
import { ensureSchema, seedDefaults } from './src/schema.js';
import { getOAuthClient, authStart, authCallback, requireConnected } from './src/oauth.js';
import { qboReadItemByName, qboQuery } from './src/qbo.js';
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

// ===============================
// Inventory: SKU Settings (filter by category)
// ===============================
app.get('/inventory/settings/skus', requireConnected, (req, res) => {
  const selectedCat = (req.query.cat || 'all').toString();
  const categories = db.listCategoriesOrdered();
  const skus = db.listSkusAllFiltered({ categoryId: selectedCat });
  res.render('inventory_sku_settings', { skus, msg: null, categories, selectedCat });
});

app.post('/inventory/settings/skus/sync', requireConnected, async (req, res) => {
  const conn = db.getConnectionOrThrow();
  const oauthClient = getOAuthClient(conn);

  try {
    let start = 1;
    const pageSize = 1000;
    let total = 0;

    while (true) {
      const q = `select Id, Name, Type, Active, ParentRef from Item startposition ${start} maxresults ${pageSize}`;
      const r = await qboQuery(oauthClient, conn.realm_id, q);
      const items = r?.QueryResponse?.Item || [];

      for (const it of items) {
        if (!it?.Id || !it?.Name) continue;

        // Skip category rows (headers)
        if (String(it.Type || '').toLowerCase() === 'category') continue;

        const parentCatId = it?.ParentRef?.value ? String(it.ParentRef.value) : null;

        db.upsertSkuFromQbo({
          qbo_item_id: String(it.Id),
          name: String(it.Name),
          qbo_category_id: parentCatId
        });
      }

      total += items.length;
      if (items.length < pageSize) break;
      start += pageSize;
    }

    const categories = db.listCategoriesOrdered();
    const selectedCat = 'all';
    const skus = db.listSkusAllFiltered({ categoryId: selectedCat });
    res.render('inventory_sku_settings', { skus, msg: `Synced items from QuickBooks.`, categories, selectedCat });
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

const port = process.env.PORT || 3000;
app.listen(port, () => console.log(`App running on http://localhost:${port}`));
