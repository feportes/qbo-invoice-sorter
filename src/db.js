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

  // Active SKUs only (for dropdowns like Add Pallet)
  listSkusActiveOnly() {
    return sqlite.prepare(`
      SELECT *
      FROM skus
      WHERE active=1
      ORDER BY name COLLATE NOCASE
    `).all();
  },

  clearProcessed(invoiceId) {
    sqlite.prepare(`DELETE FROM processed WHERE invoice_id=?`).run(String(invoiceId));
  },


  // ==========================================================
  // Allocation helpers (walk-in + pallets)
  // ==========================================================
  getSkuByQboItemId(qboItemId) {
    return sqlite.prepare(`SELECT * FROM skus WHERE qbo_item_id=? LIMIT 1`).get(String(qboItemId));
  },

  getDefaultUnitsPerPalletForSku(skuId) {
    const row = sqlite.prepare(`
      SELECT units_per_pallet
      FROM pallet_configs
      WHERE sku_id=? AND is_default=1
      LIMIT 1
    `).get(skuId);
    return row ? Number(row.units_per_pallet) : null;
  },

  getWalkinLocation() {
    return sqlite.prepare(`SELECT * FROM locations WHERE code='WALKIN' LIMIT 1`).get();
  },

  getWalkinQtyBySkuLot(skuId, lotId) {
    const walkin = this.getWalkinLocation();
    if (!walkin) return 0;
    const row = sqlite.prepare(`
      SELECT qty_units
      FROM loose_inventory
      WHERE sku_id=? AND COALESCE(lot_id,0)=COALESCE(?,0) AND location_id=?
    `).get(skuId, lotId ?? null, walkin.id);
    return row ? Number(row.qty_units) : 0;
  },

  listWalkinLotsForSku(skuId) {
    const walkin = this.getWalkinLocation();
    if (!walkin) return [];
    return sqlite.prepare(`
      SELECT li.lot_id, li.qty_units
      FROM loose_inventory li
      WHERE li.sku_id=? AND li.location_id=? AND li.qty_units > 0
      ORDER BY CASE WHEN li.lot_id IS NOT NULL THEN 0 ELSE 1 END, li.updated_at DESC
    `).all(skuId, walkin.id);
  },

  // Pallets ordered by "closest to door" with lane priority
  listPalletsForSkuDoorFirst(skuId) {
    const lane = this.getSetting('lane_priority') || 'R_FIRST';
    const sideCase = (lane === 'R_FIRST')
      ? "CASE l.side WHEN 'R' THEN 0 WHEN 'L' THEN 1 ELSE 9 END"
      : "CASE l.side WHEN 'L' THEN 0 WHEN 'R' THEN 1 ELSE 9 END";

    return sqlite.prepare(`
      SELECT p.id, p.sku_id, p.lot_id, p.qty_units, p.status,
             l.code AS location_code, l.container_no, l.side, l.depth, l.id AS location_id
      FROM pallets p
      JOIN locations l ON l.id = p.location_id
      WHERE p.sku_id=? AND l.type='CONTAINER' AND p.qty_units > 0
      ORDER BY
        CASE WHEN p.lot_id IS NOT NULL THEN 0 ELSE 1 END,
        l.container_no ASC,
        l.depth ASC,
        ${sideCase} ASC,
        p.id ASC
    `).all(skuId);
  },

  addInvoiceAllocation(row) {
    sqlite.prepare(`
      INSERT INTO invoice_allocations
        (qbo_invoice_id, sku_id, lot_id, source_type, source_location_code, source_pallet_id, qty_units)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `).run(
      String(row.qbo_invoice_id),
      row.sku_id,
      row.lot_id ?? null,
      row.source_type,
      row.source_location_code,
      row.source_pallet_id ?? null,
      Number(row.qty_units)
    );
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
  // ✅ Pallets listing helpers for WALKIN/RETURNS/etc
  // ==========================================================
  listPalletsInLocationId(locationId) {
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
      WHERE p.location_id = ?
      ORDER BY s.name COLLATE NOCASE, lo.lot_number COLLATE NOCASE, p.id DESC
    `).all(Number(locationId));
  },

  listPalletsByLocationCode(code) {
    const loc = this.getLocationByCode(String(code));
    if (!loc) return [];
    return this.listPalletsInLocationId(loc.id);
  },

  listPalletsInWalkin() {
    return this.listPalletsByLocationCode('WALKIN');
  },

  // ==========================================================
  // RETURNS helpers
  // ==========================================================
  listPalletsInReturns() {
    return this.listPalletsByLocationCode('RETURNS');
  },


  // ==========================================================
  // Inventory Engine: toggle + invoice state/totals + reversals
  // ==========================================================
  getAutoAllocateEnabled() {
    return String(this.getSetting('auto_allocate_enabled') || '0') === '1';
  },

  setAutoAllocateEnabled(on) {
    this.setSetting('auto_allocate_enabled', on ? '1' : '0');
  },

  getInvoiceState(invoiceId) {
    return sqlite.prepare(`
      SELECT * FROM invoice_state WHERE qbo_invoice_id=?
    `).get(String(invoiceId));
  },

  upsertInvoiceState({ invoiceId, hash, txnDate }) {
    sqlite.prepare(`
      INSERT INTO invoice_state (qbo_invoice_id, last_hash, last_txn_date, last_seen_at)
      VALUES (?, ?, ?, CURRENT_TIMESTAMP)
      ON CONFLICT(qbo_invoice_id) DO UPDATE SET
        last_hash=excluded.last_hash,
        last_txn_date=excluded.last_txn_date,
        last_seen_at=CURRENT_TIMESTAMP
    `).run(String(invoiceId), String(hash), txnDate ? String(txnDate) : null);
  },

  getInvoiceTotals(invoiceId) {
    return sqlite.prepare(`
      SELECT sku_id, qty_units
      FROM invoice_line_totals
      WHERE qbo_invoice_id=?
    `).all(String(invoiceId));
  },

  replaceInvoiceTotals(invoiceId, totals) {
    const tx = sqlite.transaction(() => {
      sqlite.prepare(`DELETE FROM invoice_line_totals WHERE qbo_invoice_id=?`).run(String(invoiceId));
      const ins = sqlite.prepare(`
        INSERT INTO invoice_line_totals (qbo_invoice_id, sku_id, qty_units, updated_at)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP)
      `);
      for (const t of totals) {
        ins.run(String(invoiceId), Number(t.sku_id), Number(t.qty_units));
      }
    });
    tx();
  },

  getAllocationsForInvoice(invoiceId) {
    return sqlite.prepare(`
      SELECT *
      FROM invoice_allocations
      WHERE qbo_invoice_id=?
      ORDER BY id ASC
    `).all(String(invoiceId));
  },

  deleteAllocationsForInvoice(invoiceId) {
    sqlite.prepare(`DELETE FROM invoice_allocations WHERE qbo_invoice_id=?`).run(String(invoiceId));
  },

  reverseInvoiceAllocations(invoiceId) {
    const s = sqlite;
    const walkin = this.getWalkinLocation();
    if (!walkin) throw new Error('WALKIN location missing');

    const allocs = this.getAllocationsForInvoice(invoiceId);

    const tx = s.transaction(() => {
      for (const a of allocs) {
        const qty = Number(a.qty_units);

        if (a.source_type === 'WALKIN') {
          const row = s.prepare(`
            SELECT * FROM loose_inventory
            WHERE sku_id=? AND COALESCE(lot_id,0)=COALESCE(?,0) AND location_id=?
          `).get(a.sku_id, a.lot_id ?? null, walkin.id);

          if (!row) {
            s.prepare(`
              INSERT INTO loose_inventory (sku_id, lot_id, location_id, qty_units)
              VALUES (?, ?, ?, ?)
            `).run(a.sku_id, a.lot_id ?? null, walkin.id, qty);
          } else {
            s.prepare(`UPDATE loose_inventory SET qty_units = qty_units + ?, updated_at=CURRENT_TIMESTAMP WHERE id=?`)
              .run(qty, row.id);
          }
        } else if (a.source_type === 'PALLET') {
          const pallet = s.prepare(`SELECT * FROM pallets WHERE id=?`).get(a.source_pallet_id);
          if (pallet) {
            const newQty = Number(pallet.qty_units) + qty;
            s.prepare(`UPDATE pallets SET qty_units=?, status='OPEN' WHERE id=?`)
              .run(newQty, a.source_pallet_id);
          }
        }
      }

      this.deleteAllocationsForInvoice(invoiceId);
    });

    tx.call(this);
  },

  // ==========================================================
  // Pallet Configs (UI + default per SKU)
  // ==========================================================
  listPalletConfigsAll() {
    return sqlite.prepare(`
      SELECT pc.*,
             s.name AS sku_name,
             s.unit_type AS unit_type
      FROM pallet_configs pc
      JOIN skus s ON s.id = pc.sku_id
      ORDER BY s.name COLLATE NOCASE, pc.is_default DESC, pc.name COLLATE NOCASE
    `).all();
  },

  listPalletConfigsForSku(skuId) {
    return sqlite.prepare(`
      SELECT *
      FROM pallet_configs
      WHERE sku_id=?
      ORDER BY is_default DESC, name COLLATE NOCASE
    `).all(skuId);
  },

  addPalletConfig({ sku_id, name, ti, hi, units_per_pallet, is_default, notes }) {
    const tx = sqlite.transaction(() => {
      if (is_default) sqlite.prepare(`UPDATE pallet_configs SET is_default=0 WHERE sku_id=?`).run(sku_id);

      sqlite.prepare(`
        INSERT INTO pallet_configs
          (sku_id, name, ti, hi, units_per_pallet, is_default, notes)
        VALUES
          (?, ?, ?, ?, ?, ?, ?)
      `).run(
        sku_id,
        name,
        ti ?? null,
        hi ?? null,
        units_per_pallet,
        is_default ? 1 : 0,
        notes ?? null
      );
    });
    tx();
  },

  updatePalletConfig({ id, sku_id, name, ti, hi, units_per_pallet, is_default, notes }) {
    const tx = sqlite.transaction(() => {
      if (is_default) sqlite.prepare(`UPDATE pallet_configs SET is_default=0 WHERE sku_id=?`).run(sku_id);

      sqlite.prepare(`
        UPDATE pallet_configs
        SET name=?,
            ti=?,
            hi=?,
            units_per_pallet=?,
            is_default=?,
            notes=?
        WHERE id=?
      `).run(
        name,
        ti ?? null,
        hi ?? null,
        units_per_pallet,
        is_default ? 1 : 0,
        notes ?? null,
        id
      );
    });
    tx();
  },

  deletePalletConfig(id) {
    sqlite.prepare(`DELETE FROM pallet_configs WHERE id=?`).run(id);
  },

  getDefaultPalletConfigForSku(skuId) {
    return sqlite.prepare(`
      SELECT *
      FROM pallet_configs
      WHERE sku_id=? AND is_default=1
      LIMIT 1
    `).get(skuId);
  },

  // ==========================================================
  // ✅ ADD PALLET (manual receive)
  // ==========================================================
  createPallet({ skuId, lotId, palletConfigId, locationCode, qtyUnits, notes, userName = 'user' }) {
    const tx = sqlite.transaction(() => {
      const loc = sqlite.prepare(`SELECT * FROM locations WHERE code=? LIMIT 1`).get(String(locationCode));
      if (!loc) throw new Error(`Location not found: ${locationCode}`);

      const sku = sqlite.prepare(`SELECT * FROM skus WHERE id=? LIMIT 1`).get(Number(skuId));
      if (!sku) throw new Error(`SKU not found: ${skuId}`);

      const qty = Number(qtyUnits);
      if (!Number.isFinite(qty) || qty <= 0) throw new Error('qtyUnits must be > 0');

      const ins = sqlite.prepare(`
        INSERT INTO pallets
          (sku_id, lot_id, pallet_config_id, location_id, qty_units, status, notes)
        VALUES
          (?, ?, ?, ?, ?, 'SEALED', ?)
      `);

      const result = ins.run(
        Number(skuId),
        lotId ? Number(lotId) : null,
        palletConfigId ? Number(palletConfigId) : null,
        Number(loc.id),
        qty,
        notes ? String(notes) : null
      );

      const palletId = Number(result.lastInsertRowid);

      sqlite.prepare(`
        INSERT INTO inventory_movements
          (user_name, sku_id, lot_id, qty_units, unit_type, to_location_id, to_pallet_id, type, reference_type, reference_id, note)
        VALUES
          (?, ?, ?, ?, ?, ?, ?, 'RECEIVE', 'MANUAL', NULL, ?)
      `).run(
        String(userName || 'user'),
        Number(skuId),
        lotId ? Number(lotId) : null,
        qty,
        String(sku.unit_type || 'unit'),
        Number(loc.id),
        palletId,
        `Manual receive pallet into ${loc.code}`
      );

      return palletId;
    });

    return tx();
  },

  // ==========================================================
  // Move pallet between locations (including WALKIN/RETURNS)
  // ✅ NOW prevents moving into an occupied CONTAINER slot
  // ==========================================================
  movePallet(palletId, toLocationId, userName = 'user') {
    const tx = sqlite.transaction(() => {
      const pallet = sqlite.prepare('SELECT * FROM pallets WHERE id=?').get(Number(palletId));
      if (!pallet) throw new Error('Pallet not found');

      const fromLoc = sqlite.prepare('SELECT * FROM locations WHERE id=?').get(pallet.location_id);
      const toLoc = sqlite.prepare('SELECT * FROM locations WHERE id=?').get(Number(toLocationId));
      if (!toLoc) throw new Error('Destination location not found');

      // Enforce 1 pallet per container slot
      if (String(toLoc.type) === 'CONTAINER') {
        const existing = sqlite.prepare(`
          SELECT p.id, s.name AS sku_name, p.qty_units, l.code AS location_code
          FROM pallets p
          JOIN skus s ON s.id = p.sku_id
          JOIN locations l ON l.id = p.location_id
          WHERE p.location_id=? AND p.id <> ?
          LIMIT 1
        `).get(Number(toLoc.id), Number(palletId));

        if (existing) {
          throw new Error(`Destination ${toLoc.code} is occupied by pallet #${existing.id} (${existing.sku_name}, ${existing.qty_units}).`);
        }
      }

      sqlite.prepare('UPDATE pallets SET location_id=? WHERE id=?')
        .run(Number(toLocationId), Number(palletId));

      const sku = sqlite.prepare('SELECT unit_type FROM skus WHERE id=?').get(pallet.sku_id);

      sqlite.prepare(`
        INSERT INTO inventory_movements
          (user_name, sku_id, lot_id, qty_units, unit_type,
           from_location_id, to_location_id,
           from_pallet_id, to_pallet_id,
           type, reference_type, reference_id, note)
        VALUES
          (?, ?, ?, 0, ?,
           ?, ?,
           ?, ?,
           'MOVE_PALLET', 'MANUAL', NULL, ?)
      `).run(
        String(userName),
        Number(pallet.sku_id),
        pallet.lot_id ?? null,
        String(sku?.unit_type || 'unit'),
        fromLoc?.id ?? null,
        Number(toLoc.id),
        Number(palletId),
        Number(palletId),
        `Moved pallet ${palletId} from ${fromLoc?.code || 'UNKNOWN'} to ${toLoc.code}`
      );
    });

    tx();
  },

  // ==========================================================
  // Break pallet qty into WALKIN loose inventory
  // ==========================================================
  breakPalletToWalkin({ palletId, qty, userName = 'user' }) {
    const tx = sqlite.transaction(() => {
      const pallet = sqlite.prepare('SELECT * FROM pallets WHERE id=?').get(Number(palletId));
      if (!pallet) throw new Error('Pallet not found');

      const q = Number(qty);
      if (!Number.isFinite(q) || q <= 0) throw new Error('Qty must be > 0');
      if (Number(pallet.qty_units) < q) throw new Error(`Not enough qty on pallet. On pallet: ${pallet.qty_units}`);

      const walkin = sqlite.prepare(`SELECT * FROM locations WHERE code='WALKIN' LIMIT 1`).get();
      if (!walkin) throw new Error('WALKIN location missing');

      const newQty = Number(pallet.qty_units) - q;
      const newStatus = newQty <= 0 ? 'DEPLETED' : 'OPEN';

      sqlite.prepare('UPDATE pallets SET qty_units=?, status=? WHERE id=?')
        .run(newQty, newStatus, Number(palletId));

      const row = sqlite.prepare(`
        SELECT * FROM loose_inventory
        WHERE sku_id=? AND COALESCE(lot_id,0)=COALESCE(?,0) AND location_id=?
      `).get(pallet.sku_id, pallet.lot_id ?? null, walkin.id);

      if (!row) {
        sqlite.prepare(`
          INSERT INTO loose_inventory (sku_id, lot_id, location_id, qty_units)
          VALUES (?, ?, ?, ?)
        `).run(
          pallet.sku_id,
          pallet.lot_id ?? null,
          walkin.id,
          q
        );
      } else {
        sqlite.prepare(`
          UPDATE loose_inventory
          SET qty_units = qty_units + ?, updated_at=CURRENT_TIMESTAMP
          WHERE id=?
        `).run(q, row.id);
      }

      const fromLoc = sqlite.prepare('SELECT * FROM locations WHERE id=?').get(pallet.location_id);
      const sku = sqlite.prepare('SELECT unit_type FROM skus WHERE id=?').get(pallet.sku_id);

      sqlite.prepare(`
        INSERT INTO inventory_movements
          (user_name, sku_id, lot_id, qty_units, unit_type,
           from_location_id, to_location_id,
           from_pallet_id,
           type, reference_type, reference_id, note)
        VALUES
          (?, ?, ?, ?, ?,
           ?, ?,
           ?,
           'BREAK_TO_LOOSE', 'MANUAL', NULL, ?)
      `).run(
        String(userName),
        Number(pallet.sku_id),
        pallet.lot_id ?? null,
        q,
        String(sku?.unit_type || 'unit'),
        fromLoc?.id ?? null,
        walkin.id,
        Number(palletId),
        `Broke ${q} from pallet ${palletId} into WALKIN`
      );
    });

    tx();
  },

  // ==========================================================
  // Invoice processing lock / idempotency (needed by sorter)
  // ==========================================================
  hasProcessed(invoiceId, syncToken) {
    const row = sqlite
      .prepare('SELECT 1 FROM processed WHERE invoice_id=? AND sync_token=?')
      .get(String(invoiceId), String(syncToken));
    return !!row;
  },

  markProcessed(invoiceId, syncToken) {
    sqlite
      .prepare('INSERT OR IGNORE INTO processed (invoice_id, sync_token) VALUES (?, ?)')
      .run(String(invoiceId), String(syncToken));
  },

  // ==========================================================
  // Connection
  // ==========================================================
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

  // ==========================================================
  // Settings
  // ==========================================================
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

  // ==========================================================
  // Customers
  // ==========================================================
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

  // ==========================================================
  // Categories (QBO Item Category)
  // ==========================================================
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

  saveCategoryOrder(categoryIdsInOrder) {
    const tx = sqlite.transaction((arr) => {
      sqlite.prepare('DELETE FROM category_order').run();
      const ins = sqlite.prepare('INSERT INTO category_order (category_id, sort_index) VALUES (?, ?)');
      arr.forEach((id, idx) => ins.run(String(id), idx));
    });
    tx(categoryIdsInOrder);
  },

  // ==========================================================
  // Rules (surcharge)
  // ==========================================================
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

  // ==========================================================
  // Logs
  // ==========================================================
  addLog({ invoice_id, customer_name, action, detail, source }) {
    sqlite.prepare(`
      INSERT INTO logs (invoice_id, customer_name, action, detail, source)
      VALUES (?, ?, ?, ?, ?)
    `).run(invoice_id, customer_name, action, detail, source);
  },

  // ==========================================================
  // Containers
  // ==========================================================
  listContainers() { return [1,2,3,4,5,6,7]; },

  getContainerDepths(containerNo) {
    if (containerNo === 1) {
      const mode = this.getSetting('container_mode_C1') || '8-slot';
      const flip = this.getSetting('container_flip_C1') || 'L_LONG';
      if (mode === '8-slot') return { leftMax: 4, rightMax: 4, label: '8-slot (4/4)' };
      if (flip === 'R_LONG') return { leftMax: 4, rightMax: 5, label: '9-slot (4/5)' };
      return { leftMax: 5, rightMax: 4, label: '9-slot (5/4)' };
    }
    const mode = this.getSetting(`container_mode_C${containerNo}`) || '18-slot';
    const flip = this.getSetting(`container_flip_C${containerNo}`) || 'L_LONG';
    if (mode === '18-slot') return { leftMax: 9, rightMax: 9, label: '18-slot (9/9)' };
    if (flip === 'R_LONG') return { leftMax: 9, rightMax: 11, label: '20-slot (9/11)' };
    return { leftMax: 11, rightMax: 9, label: '20-slot (11/9)' };
  },

  listValidSlotCodes(containerNo) {
    const { leftMax, rightMax } = this.getContainerDepths(containerNo);
    const codes = [];
    for (let d = 1; d <= leftMax; d++) codes.push(`C${containerNo}-L${String(d).padStart(2,'0')}`);
    for (let d = 1; d <= rightMax; d++) codes.push(`C${containerNo}-R${String(d).padStart(2,'0')}`);
    return codes;
  },

  getLocationByCode(code) {
    return sqlite.prepare('SELECT * FROM locations WHERE code=?').get(code);
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

  // ==========================================================
  // SKU Sync / Filter
  // ==========================================================
  upsertSkuFromQbo({ qbo_item_id, name, qbo_category_id }) {
    sqlite.prepare(`
      INSERT INTO skus (qbo_item_id, name, unit_type, is_organic, is_lot_tracked, active, qbo_category_id)
      VALUES (?, ?, 'unit', 0, 0, 1, ?)
      ON CONFLICT(qbo_item_id) DO UPDATE SET
        name = excluded.name,
        qbo_category_id = excluded.qbo_category_id
    `).run(String(qbo_item_id), String(name), qbo_category_id ? String(qbo_category_id) : null);
  },

  listSkusAllFiltered({ categoryId = null } = {}) {
    if (!categoryId || categoryId === 'all') {
      return sqlite.prepare(`SELECT * FROM skus ORDER BY name COLLATE NOCASE`).all();
    }
    if (categoryId === 'uncategorized') {
      return sqlite.prepare(`SELECT * FROM skus WHERE qbo_category_id IS NULL ORDER BY name COLLATE NOCASE`).all();
    }
    return sqlite.prepare(`SELECT * FROM skus WHERE qbo_category_id=? ORDER BY name COLLATE NOCASE`).all(String(categoryId));
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


