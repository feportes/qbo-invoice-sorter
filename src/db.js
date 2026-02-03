import Database from 'better-sqlite3';
import fs from 'fs';
import path from 'path';

const dbPath = process.env.DB_PATH || './data/app.db';
const dir = path.dirname(dbPath);
if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });

const sqlite = new Database(dbPath);
sqlite.pragma('journal_mode = WAL');

export const db = {
  sqlite,

  // Connection (single company)
  getConnection() {
    return sqlite.prepare('SELECT * FROM connections WHERE id=1').get();
  },
  getConnectionOrThrow() {
    const c = this.getConnection();
    if (!c) throw new Error('Not connected. Go to /auth/start.');
    return c;
  },
  upsertConnection(row) {
    sqlite.prepare(`
      INSERT INTO connections (id, realm_id, company_name, access_token, refresh_token, expires_at, refresh_expires_at)
      VALUES (1, @realm_id, @company_name, @access_token, @refresh_token, @expires_at, @refresh_expires_at)
      ON CONFLICT(id) DO UPDATE SET
        realm_id=excluded.realm_id,
        company_name=excluded.company_name,
        access_token=excluded.access_token,
        refresh_token=excluded.refresh_token,
        expires_at=excluded.expires_at,
        refresh_expires_at=excluded.refresh_expires_at
    `).run(row);
  },

  // Settings
  getSetting(key) {
    const row = sqlite.prepare('SELECT value FROM settings WHERE key=?').get(key);
    return row?.value ?? null;
  },
  setSetting(key, value) {
    sqlite.prepare(`
      INSERT INTO settings (key, value) VALUES (?, ?)
      ON CONFLICT(key) DO UPDATE SET value=excluded.value
    `).run(key, String(value));
  },

  // Customers
  upsertCustomer({ id, display_name }) {
    sqlite.prepare(`
      INSERT INTO customers (id, display_name)
      VALUES (?, ?)
      ON CONFLICT(id) DO UPDATE SET display_name=excluded.display_name
    `).run(id, display_name);
  },
  listCustomers() {
    return sqlite.prepare('SELECT * FROM customers ORDER BY display_name COLLATE NOCASE').all();
  },
  getCustomer(id) {
    return sqlite.prepare('SELECT * FROM customers WHERE id=?').get(id);
  },

  // Categories
  upsertCategory({ id, name }) {
    sqlite.prepare(`
      INSERT INTO categories (id, name)
      VALUES (?, ?)
      ON CONFLICT(id) DO UPDATE SET name=excluded.name
    `).run(id, name);
  },
  listCategoriesOrdered() {
    return sqlite.prepare(`
      SELECT c.*, COALESCE(o.sort_index, 999999) AS sort_index
      FROM categories c
      LEFT JOIN category_order o ON o.category_id = c.id
      ORDER BY sort_index ASC, c.name COLLATE NOCASE ASC
    `).all();
  },
  getCategorySortIndex(categoryId) {
    const row = sqlite.prepare('SELECT sort_index FROM category_order WHERE category_id=?').get(categoryId);
    return row?.sort_index ?? null;
  },
  saveCategoryOrder(categoryIdsInOrder) {
    const tx = sqlite.transaction((arr) => {
      sqlite.prepare('DELETE FROM category_order').run();
      const ins = sqlite.prepare('INSERT INTO category_order (category_id, sort_index) VALUES (?, ?)');
      arr.forEach((id, idx) => ins.run(id, idx));
    });
    tx(categoryIdsInOrder);
  },

  // Rules
  listRules() {
    return sqlite.prepare(`
      SELECT * FROM customer_rules
      ORDER BY enabled DESC, match_type ASC, rule_type ASC
    `).all();
  },
  upsertRule(r) {
    if (r.id) {
      sqlite.prepare(`
        UPDATE customer_rules
        SET match_type=@match_type,
            customer_id=@customer_id,
            prefix=@prefix,
            rule_type=@rule_type,
            threshold=@threshold,
            amount=@amount,
            enabled=@enabled
        WHERE id=@id
      `).run(r);
      return;
    }
    sqlite.prepare(`
      INSERT INTO customer_rules (match_type, customer_id, prefix, rule_type, threshold, amount, enabled)
      VALUES (@match_type, @customer_id, @prefix, @rule_type, @threshold, @amount, @enabled)
    `).run(r);
  },
  deleteRule(id) {
    sqlite.prepare('DELETE FROM customer_rules WHERE id=?').run(id);
  },

  // Logs
  addLog({ invoice_id, customer_name, action, detail, source }) {
    sqlite.prepare(`
      INSERT INTO logs (invoice_id, customer_name, action, detail, source)
      VALUES (?, ?, ?, ?, ?)
    `).run(invoice_id, customer_name, action, detail, source);
  },
  listLogs(limit = 50) {
    return sqlite.prepare('SELECT * FROM logs ORDER BY created_at DESC LIMIT ?').all(limit);
  },

  // Invoice processing lock / idempotency
  hasProcessed(invoiceId, syncToken) {
    const row = sqlite.prepare('SELECT 1 FROM processed WHERE invoice_id=? AND sync_token=?').get(invoiceId, syncToken);
    return !!row;
  },
  markProcessed(invoiceId, syncToken) {
    sqlite.prepare('INSERT INTO processed (invoice_id, sync_token) VALUES (?, ?)').run(invoiceId, syncToken);
  },

  // ==========================================================
  // Inventory: Containers / Pallets / Loose (Map + Walk-in)
  // ==========================================================

  listContainers() {
    return [1, 2, 3, 4, 5, 6, 7];
  },

  getLocationByCode(code) {
    return sqlite.prepare('SELECT * FROM locations WHERE code=?').get(code);
  },

  listContainerSlots(containerNo) {
    return sqlite.prepare(`
      SELECT * FROM locations
      WHERE type='CONTAINER' AND container_no=?
      ORDER BY depth ASC, side DESC
    `).all(containerNo);
  },

  listPalletsInContainer(containerNo) {
    return sqlite.prepare(`
      SELECT p.*,
             l.code AS location_code,
             s.name AS sku_name,
             s.unit_type AS unit_type,
             lo.lot_number AS lot_number
      FROM pallets p
      JOIN locations l ON l.id = p.location_id
      JOIN skus s ON s.id = p.sku_id
      LEFT JOIN lots lo ON lo.id = p.lot_id
      WHERE l.type='CONTAINER' AND l.container_no=?
      ORDER BY l.depth ASC, l.side DESC
    `).all(containerNo);
  },

  getPalletById(palletId) {
    return sqlite.prepare(`
      SELECT p.*, l.code AS location_code,
             s.name AS sku_name, s.unit_type AS unit_type,
             lo.lot_number AS lot_number
      FROM pallets p
      JOIN locations l ON l.id = p.location_id
      JOIN skus s ON s.id = p.sku_id
      LEFT JOIN lots lo ON lo.id = p.lot_id
      WHERE p.id=?
    `).get(palletId);
  },

  movePallet(palletId, toLocationId, userName = 'system') {
    const tx = sqlite.transaction(() => {
      const pallet = sqlite.prepare('SELECT * FROM pallets WHERE id=?').get(palletId);
      if (!pallet) throw new Error('Pallet not found');

      const fromLoc = sqlite.prepare('SELECT * FROM locations WHERE id=?').get(pallet.location_id);
      const toLoc = sqlite.prepare('SELECT * FROM locations WHERE id=?').get(toLocationId);
      if (!toLoc) throw new Error('Destination location not found');

      sqlite.prepare('UPDATE pallets SET location_id=? WHERE id=?').run(toLocationId, palletId);

      const unitTypeRow = sqlite.prepare('SELECT unit_type FROM skus WHERE id=?').get(pallet.sku_id);

      sqlite.prepare(`
        INSERT INTO inventory_movements
          (user_name, sku_id, lot_id, qty_units, unit_type, from_location_id, to_location_id, from_pallet_id, to_pallet_id,
           type, reference_type, reference_id, note)
        VALUES
          (?, ?, ?, 0, ?, ?, ?, ?, ?, 'MOVE_PALLET', 'MANUAL', NULL, ?)
      `).run(
        userName,
        pallet.sku_id,
        pallet.lot_id,
        unitTypeRow?.unit_type || 'unit',
        fromLoc?.id ?? null,
        toLoc.id,
        palletId,
        palletId,
        `Moved pallet from ${fromLoc?.code ?? 'UNKNOWN'} to ${toLoc.code}`
      );
    });
    tx();
  },

  addLooseQty({ skuId, lotId, locationId, qtyDelta }) {
    const tx = sqlite.transaction(() => {
      const row = sqlite.prepare(`
        SELECT * FROM loose_inventory
        WHERE sku_id=? AND COALESCE(lot_id,0)=COALESCE(?,0) AND location_id=?
      `).get(skuId, lotId, locationId);

      if (!row) {
        sqlite.prepare(`
          INSERT INTO loose_inventory (sku_id, lot_id, location_id, qty_units)
          VALUES (?, ?, ?, ?)
        `).run(skuId, lotId || null, locationId, qtyDelta);
      } else {
        sqlite.prepare(`
          UPDATE loose_inventory
          SET qty_units = qty_units + ?, updated_at=CURRENT_TIMESTAMP
          WHERE id=?
        `).run(qtyDelta, row.id);
      }
    });
    tx();
  },

  breakPalletToWalkin({ palletId, qty, userName = 'system' }) {
    const tx = sqlite.transaction(() => {
      const pallet = sqlite.prepare('SELECT * FROM pallets WHERE id=?').get(palletId);
      if (!pallet) throw new Error('Pallet not found');

      if (Number(pallet.qty_units) < Number(qty)) {
        throw new Error(`Not enough qty on pallet. On pallet: ${pallet.qty_units}`);
      }

      const walkin = sqlite.prepare(`SELECT * FROM locations WHERE code='WALKIN'`).get();
      if (!walkin) throw new Error('WALKIN location missing');

      const fromLoc = sqlite.prepare('SELECT * FROM locations WHERE id=?').get(pallet.location_id);
      const unitTypeRow = sqlite.prepare('SELECT unit_type FROM skus WHERE id=?').get(pallet.sku_id);

      // Reduce pallet qty and update status
      const newQty = Number(pallet.qty_units) - Number(qty);
      const newStatus = newQty <= 0 ? 'DEPLETED' : 'OPEN';

      sqlite.prepare('UPDATE pallets SET qty_units=?, status=? WHERE id=?')
        .run(newQty, newStatus, palletId);

      // Add to WALKIN loose
      this.addLooseQty({
        skuId: pallet.sku_id,
        lotId: pallet.lot_id,
        locationId: walkin.id,
        qtyDelta: Number(qty)
      });

      sqlite.prepare(`
        INSERT INTO inventory_movements
          (user_name, sku_id, lot_id, qty_units, unit_type, from_location_id, to_location_id, from_pallet_id,
           type, reference_type, reference_id, note)
        VALUES
          (?, ?, ?, ?, ?, ?, ?, ?, 'BREAK_TO_LOOSE', 'MANUAL', NULL, ?)
      `).run(
        userName,
        pallet.sku_id,
        pallet.lot_id,
        Number(qty),
        unitTypeRow?.unit_type || 'unit',
        fromLoc?.id ?? null,
        walkin.id,
        palletId,
        `Broke pallet ${palletId} from ${fromLoc?.code ?? 'UNKNOWN'} to WALKIN (+${qty})`
      );
    });
    tx.call(this);
  },

  listWalkinLoose() {
    return sqlite.prepare(`
      SELECT li.*,
             s.name AS sku_name,
             s.unit_type AS unit_type,
             lo.lot_number AS lot_number
      FROM loose_inventory li
      JOIN skus s ON s.id = li.sku_id
      LEFT JOIN lots lo ON lo.id = li.lot_id
      JOIN locations l ON l.id = li.location_id
      WHERE l.code='WALKIN'
      ORDER BY sku_name COLLATE NOCASE, lot_number COLLATE NOCASE
    `).all();
  },

  // ==========================================================
  // Quick Add Pallet helpers
  // ==========================================================

  createPallet({ skuId, lotId, palletConfigId, locationCode, qtyUnits, notes }) {
    const tx = sqlite.transaction(() => {
      const loc = sqlite.prepare('SELECT * FROM locations WHERE code=?').get(locationCode);
      if (!loc) throw new Error(`Location not found: ${locationCode}`);

      const skuRow = sqlite.prepare('SELECT unit_type, is_lot_tracked FROM skus WHERE id=?').get(skuId);
      if (!skuRow) throw new Error('SKU not found');

      // If SKU is lot-tracked, lotId is recommended, but allow null when you run out and buy local.
      // We'll treat lotId NULL as "NO TRACE" for that SKU batch.
      const insert = sqlite.prepare(`
        INSERT INTO pallets
          (sku_id, lot_id, pallet_config_id, location_id, qty_units, status, notes)
        VALUES
          (?, ?, ?, ?, ?, 'SEALED', ?)
      `);

      const result = insert.run(
        skuId,
        lotId || null,
        palletConfigId || null,
        loc.id,
        Number(qtyUnits),
        notes || null
      );

      sqlite.prepare(`
        INSERT INTO inventory_movements
          (user_name, sku_id, lot_id, qty_units, unit_type, to_location_id, to_pallet_id,
           type, reference_type, reference_id, note)
        VALUES
          (?, ?, ?, ?, ?, ?, ?, 'RECEIVE', 'MANUAL', NULL, ?)
      `).run(
        'user',
        skuId,
        lotId || null,
        Number(qtyUnits),
        skuRow.unit_type || 'unit',
        loc.id,
        result.lastInsertRowid,
        'Quick add pallet'
      );

      return result.lastInsertRowid;
    });

    return tx();
  },

  listSkus() {
    return sqlite.prepare(`
      SELECT * FROM skus
      WHERE active=1
      ORDER BY name COLLATE NOCASE
    `).all();
  },

  listLotsForSku(skuId) {
    return sqlite.prepare(`
      SELECT * FROM lots
      WHERE sku_id=?
      ORDER BY created_at DESC
    `).all(skuId);
  },

  listPalletConfigsForSku(skuId) {
    return sqlite.prepare(`
      SELECT * FROM pallet_configs
      WHERE sku_id=?
      ORDER BY is_default DESC, name COLLATE NOCASE
    `).all(skuId);
  }
};
