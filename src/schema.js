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

    CREATE TABLE IF NOT EXISTS container_templates (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      container_no INTEGER NOT NULL,
      mode_name TEXT NOT NULL,
      max_depth INTEGER NOT NULL,
      enabled INTEGER NOT NULL DEFAULT 1,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );

    CREATE UNIQUE INDEX IF NOT EXISTS ux_container_templates
    ON container_templates(container_no, mode_name);

    CREATE TABLE IF NOT EXISTS documents (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      type TEXT NOT NULL,
      filename TEXT NOT NULL,
      storage_path TEXT NOT NULL,
      parsed_json TEXT,
      uploaded_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS receipts (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      received_date DATE NOT NULL,
      supplier_name TEXT,
      container_ref TEXT,
      reference_no TEXT,
      document_id INTEGER REFERENCES documents(id),
      notes TEXT,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS lots (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      sku_id INTEGER NOT NULL REFERENCES skus(id),
      receipt_id INTEGER REFERENCES receipts(id),
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
      received_receipt_id INTEGER REFERENCES receipts(id),
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

  // Inventory global defaults
  if (!db.getSetting('default_pallet_pick_threshold')) db.setSetting('default_pallet_pick_threshold', '0.80');
  if (!db.getSetting('lane_priority')) db.setSetting('lane_priority', 'R_FIRST');
  if (!db.getSetting('walkin_first_default')) db.setSetting('walkin_first_default', '1');

  // Container mode settings
  if (!db.getSetting('container_mode_C1')) db.setSetting('container_mode_C1', '10-slot');
  for (let c = 2; c <= 7; c++) {
    const key = `container_mode_C${c}`;
    if (!db.getSetting(key)) db.setSetting(key, '20-slot'); // default 40ft = 20 slot
  }

  // Seed customer rules only once
  const rules = db.listRules();
  if (rules.length === 0) {
    db.upsertRule({
      id: null,
      match_type: 'prefix',
      customer_id: null,
      prefix: 'Alohana',
      rule_type: 'always_0',
      threshold: null,
      amount: 0,
      enabled: 1
    });
    db.upsertRule({
      id: null,
      match_type: 'exact',
      customer_id: null,
      prefix: null,
      rule_type: 'always_15',
      threshold: null,
      amount: 15,
      enabled: 0
    });
    db.upsertRule({
      id: null,
      match_type: 'exact',
      customer_id: null,
      prefix: null,
      rule_type: 'exclude',
      threshold: null,
      amount: null,
      enabled: 0
    });
    db.upsertRule({
      id: null,
      match_type: 'exact',
      customer_id: null,
      prefix: null,
      rule_type: 'conditional',
      threshold: 1000,
      amount: 15,
      enabled: 0
    });
  }

  seedLocationsAndDefaults();
}

function seedLocationsAndDefaults() {
  const s = db.sqlite;

  const upsertLoc = s.prepare(`
    INSERT INTO locations (type, code, container_no, side, depth, enabled)
    VALUES (@type, @code, @container_no, @side, @depth, @enabled)
    ON CONFLICT(code) DO UPDATE SET enabled = excluded.enabled
  `);

  upsertLoc.run({ type: 'WALKIN',  code: 'WALKIN',  container_no: null, side: null, depth: null, enabled: 1 });
  upsertLoc.run({ type: 'RETURNS', code: 'RETURNS', container_no: null, side: null, depth: null, enabled: 1 });

  const upsertTemplate = s.prepare(`
    INSERT INTO container_templates (container_no, mode_name, max_depth, enabled)
    VALUES (?, ?, ?, 1)
    ON CONFLICT(container_no, mode_name)
    DO UPDATE SET max_depth=excluded.max_depth, enabled=1
  `);

  upsertTemplate.run(1, '8-slot', 4);
  upsertTemplate.run(1, '10-slot', 5);

  for (let c = 2; c <= 7; c++) {
    upsertTemplate.run(c, '18-slot', 9);
    upsertTemplate.run(c, '20-slot', 10);
  }

  for (let containerNo = 1; containerNo <= 7; containerNo++) {
    for (const side of ['L', 'R']) {
      for (let depth = 1; depth <= 10; depth++) {
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
