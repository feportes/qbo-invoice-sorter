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

  // Connection
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

  // Idempotency
  hasProcessed(invoiceId, syncToken) {
    const row = sqlite.prepare('SELECT 1 FROM processed WHERE invoice_id=? AND sync_token=?').get(invoiceId, syncToken);
    return !!row;
  },
  markProcessed(invoiceId, syncToken) {
    sqlite.prepare('INSERT INTO processed (invoice_id, sync_token) VALUES (?, ?)').run(invoiceId, syncToken);
  },

  // Containers
  listContainers() {
    return [1, 2, 3, 4, 5, 6, 7];
  },

  // Asymmetric per-container depths based on mode + flip
  getContainerDepths(containerNo) {
    if (containerNo === 1) {
      const mode = this.getSetting('container_mode_C1') || '8-slot';
      const flip = this.getSetting('container_flip_C1') || 'L_LONG';

      if (mode === '8-slot') return { leftMax: 4, rightMax: 4, label: '8-slot (4/4)' };

      // 9-slot = 5/4 with flip
      if (flip === 'R_LONG') return { leftMax: 4, rightMax: 5, label: '9-slot (4/5)' };
      return { leftMax: 5, rightMax: 4, label: '9-slot (5/4)' };
    }

    const mode = this.getSetting(`container_mode_C${containerNo}`) || '18-slot';
    const flip = this.getSetting(`container_flip_C${containerNo}`) || 'L_LONG';

    if (mode === '18-slot') return { leftMax: 9, rightMax: 9, label: '18-slot (9/9)' };

    // 20-slot = 11/9 with flip
    if (flip === 'R_LONG') return { leftMax: 9, rightMax: 11, label: '20-slot (9/11)' };
    return { leftMax: 11, rightMax: 9, label: '20-slot (11/9)' };
  },

  // Slot options
  listValidSlotCodes(containerNo) {
    const { leftMax, rightMax } = this.getContainerDepths(containerNo);
    const codes = [];
    for (let d = 1; d <= leftMax; d++) codes.push(`C${containerNo}-L${String(d).padStart(2, '0')}`);
    for (let d = 1; d <= rightMax; d++) codes.push(`C${containerNo}-R${String(d).padStart(2, '0')}`);
    return codes;
  },

  getLocationByCode(code) {
    return sqlite.prepare('SELECT * FROM locations WHERE code=?').get(code);
  },

  // Inventory: pallets in container
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

  movePallet(palletId, toLocationId, userName='system') {
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

  // ============================
  // SKU Sync / Settings helpers  ✅ NEW
  // ============================

  upsertSkuFromQbo({ qbo_item_id, name }) {
    sqlite.prepare(`
      INSERT INTO skus (qbo_item_id, name, unit_type, is_organic, is_lot_tracked, active)
      VALUES (?, ?, 'unit', 0, 0, 1)
      ON CONFLICT(qbo_item_id) DO UPDATE SET
        name = excluded.name
    `).run(String(qbo_item_id), String(name));
  },

  listSkusAll() {
    return sqlite.prepare(`
      SELECT *
      FROM skus
      ORDER BY name COLLATE NOCASE
    `).all();
  },

  updateSkuSettings({ sku_id, active, is_organic, is_lot_tracked, unit_type, pallet_pick_threshold }) {
    sqlite.prepare(`
      UPDATE skus
      SET active = ?,
          is_organic = ?,
          is_lot_tracked = ?,
          unit_type = ?,
          pallet_pick_threshold = ?
      WHERE id = ?
    `).run(
      active,
      is_organic,
      is_lot_tracked,
      unit_type,
      pallet_pick_threshold,
      sku_id
    );
  },
};
