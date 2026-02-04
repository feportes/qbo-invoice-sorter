import { db } from './db.js';

export function ensureSchema() {
  const s = db.sqlite;
  s.exec(`
    CREATE TABLE IF NOT EXISTS connections (
      id INTEGER PRIMARY KEY CHECK (id=1),
      realm_id TEXT NOT NULL,
      company_name TEXT,
      access_token TEXT NOT NULL,
      refresh_token TEXT NOT NULL,
      expires_at INTEGER NOT NULL,
      refresh_expires_at INTEGER
    );

    CREATE TABLE IF NOT EXISTS settings (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS customers (
      id TEXT PRIMARY KEY,
      display_name TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS categories (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS category_order (
      category_id TEXT PRIMARY KEY,
      sort_index INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS customer_rules (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      match_type TEXT NOT NULL,
      customer_id TEXT,
      prefix TEXT,
      rule_type TEXT NOT NULL,
      threshold REAL,
      amount REAL,
      enabled INTEGER NOT NULL DEFAULT 1
    );

    CREATE TABLE IF NOT EXISTS logs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      invoice_id TEXT,
      customer_name TEXT,
      action TEXT NOT NULL,
      detail TEXT,
      source TEXT
    );

    CREATE TABLE IF NOT EXISTS processed (
      invoice_id TEXT NOT NULL,
      sync_token TEXT NOT NULL,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (invoice_id, sync_token)
    );

    -- =========================
    -- Inventory / Lot Tracking
    -- =========================

    CREATE TABLE IF NOT EXISTS skus (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      qbo_item_id TEXT UNIQUE,
      name TEXT NOT NULL,
      unit_type TEXT NOT NULL,
      is_organic INTEGER NOT NULL DEFAULT 0,
      is_lot_tracked INTEGER NOT NULL DEFAULT 0,
      active INTEGER NOT NULL DEFAULT 1,
      pallet_pick_threshold REAL NULL,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );

    CREATE INDEX IF NOT EXISTS idx_skus_active ON skus(active);

    CREATE TABLE IF NOT EXISTS pallet_configs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      sku_id INTEGER NOT NULL REFERENCES skus(id),
      name TEXT NOT NULL,
      ti INTEGER,
      hi INTEGER,
      units_per_pallet INTEGER NOT NULL,
      board_between_layers INTEGER NOT NULL DEFAULT 0,
      double_stack_allowed INTEGER NOT NULL DEFAULT 0,
      max_stack INTEGER NOT NULL DEFAULT 1,
      is_default INTEGER NOT NULL DEFAULT 0,
      notes TEXT,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );

    CREATE INDEX IF NOT EXISTS idx_pallet_configs_sku ON pallet_configs(sku_id);
    CREATE UNIQUE INDEX IF NOT EXISTS ux_pallet_configs_default
    ON pallet_configs(sku_id) WHERE is_default = 1;

    CREATE TABLE IF NOT EXISTS locations (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      type TEXT NOT NULL,
      code TEXT NOT NULL UNIQUE,
      container_no INTEGER NULL,
      side TEXT NULL,
      depth INTEGER NULL,
      enabled INTEGER NOT NULL DEFAULT 1,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );

    CREATE INDEX IF NOT EXISTS idx_locations_type ON locations(type);
    CREATE INDEX IF NOT EXISTS idx_locations_container ON locations(container_no, side, depth);

    CREATE TABLE IF NOT EXISTS lots (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      sku_id INTEGER NOT NULL REFERENCES skus(id),
      receipt_id INTEGER,
      lot_number TEXT NOT NULL,
      supplier_lot TEXT,
      production_date DATE,
      expiration_date DATE,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );

    CREATE INDEX IF NOT EXISTS idx_lots_sku ON lots(sku_id);
    CREATE UNIQUE INDEX IF NOT EXISTS ux_lots_sku_lot
    ON lots(sku_id, lot_number);

    CREATE TABLE IF NOT EXISTS pallets (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      pallet_tag TEXT UNIQUE,
      sku_id INTEGER NOT NULL REFERENCES skus(id),
      lot_id INTEGER REFERENCES lots(id),
      pallet_config_id INTEGER REFERENCES pallet_configs(id),
      location_id INTEGER NOT NULL REFERENCES locations(id),
      qty_units REAL NOT NULL,
      status TEXT NOT NULL DEFAULT 'SEALED',
      received_receipt_id INTEGER,
      notes TEXT,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );

    CREATE INDEX IF NOT EXISTS idx_pallets_loc ON pallets(location_id);
    CREATE INDEX IF NOT EXISTS idx_pallets_sku_lot ON pallets(sku_id, lot_id);

    CREATE TABLE IF NOT EXISTS loose_inventory (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      sku_id INTEGER NOT NULL REFERENCES skus(id),
      lot_id INTEGER REFERENCES lots(id),
      location_id INTEGER NOT NULL REFERENCES locations(id),
      qty_units REAL NOT NULL DEFAULT 0,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );

    CREATE UNIQUE INDEX IF NOT EXISTS ux_loose_unique
    ON loose_inventory(sku_id, COALESCE(lot_id, 0), location_id);

    CREATE TABLE IF NOT EXISTS inventory_movements (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      user_name TEXT,
      sku_id INTEGER NOT NULL REFERENCES skus(id),
      lot_id INTEGER REFERENCES lots(id),
      qty_units REAL NOT NULL,
      unit_type TEXT NOT NULL,
      from_location_id INTEGER REFERENCES locations(id),
      to_location_id INTEGER REFERENCES locations(id),
      from_pallet_id INTEGER REFERENCES pallets(id),
      to_pallet_id INTEGER REFERENCES pallets(id),
      type TEXT NOT NULL,
      reference_type TEXT,
      reference_id TEXT,
      note TEXT
    );

    CREATE INDEX IF NOT EXISTS idx_movements_ref ON inventory_movements(reference_type, reference_id);
    CREATE INDEX IF NOT EXISTS idx_movements_sku_lot ON inventory_movements(sku_id, lot_id);
    CREATE INDEX IF NOT EXISTS idx_movements_time ON inventory_movements(created_at);
  `);
}

export function seedDefaults() {
  // Existing settings
  if (!db.getSetting('default_surcharge_amount')) db.setSetting('default_surcharge_amount', '15');
  if (!db.getSetting('surcharge_item_name')) db.setSetting('surcharge_item_name', 'Operating Cost Surcharge');
  if (!db.getSetting('uncategorized_position')) db.setSetting('uncategorized_position', 'bottom');

  // Inventory defaults
  if (!db.getSetting('default_pallet_pick_threshold')) db.setSetting('default_pallet_pick_threshold', '0.80');
  if (!db.getSetting('lane_priority')) db.setSetting('lane_priority', 'R_FIRST');
  if (!db.getSetting('walkin_first_default')) db.setSetting('walkin_first_default', '1');

  // ✅ Asymmetric container defaults
  // C1 default: 8-slot (4/4)
  if (!db.getSetting('container_mode_C1')) db.setSetting('container_mode_C1', '8-slot');
  if (!db.getSetting('container_flip_C1')) db.setSetting('container_flip_C1', 'L_LONG'); // ignored for 8-slot

  // C2–C7 default: 18-slot (9/9)
  for (let c = 2; c <= 7; c++) {
    const mk = `container_mode_C${c}`;
    const fk = `container_flip_C${c}`;
    if (!db.getSetting(mk)) db.setSetting(mk, '18-slot');
    if (!db.getSetting(fk)) db.setSetting(fk, 'L_LONG'); // ignored for 18-slot
  }

  seedLocations();
}

function seedLocations() {
  const s = db.sqlite;

  const upsertLoc = s.prepare(`
    INSERT INTO locations (type, code, container_no, side, depth, enabled)
    VALUES (@type, @code, @container_no, @side, @depth, @enabled)
    ON CONFLICT(code) DO UPDATE SET enabled = excluded.enabled
  `);

  upsertLoc.run({ type: 'WALKIN',  code: 'WALKIN',  container_no: null, side: null, depth: null, enabled: 1 });
  upsertLoc.run({ type: 'RETURNS', code: 'RETURNS', container_no: null, side: null, depth: null, enabled: 1 });

  // Seed max possible slot depth up to 11 (needed for 20-slot 11/9)
  for (let containerNo = 1; containerNo <= 7; containerNo++) {
    for (const side of ['L', 'R']) {
      for (let depth = 1; depth <= 11; depth++) {
        const code = `C${containerNo}-${side}${String(depth).padStart(2, '0')}`;
        upsertLoc.run({
          type: 'CONTAINER',
          code,
          container_no: containerNo,
          side,
          depth,
          enabled: 1
        });
      }
    }
  }
}
