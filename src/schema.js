import { db } from './db.js';

export function ensureSchema() {
  const s = db.sqlite;
s.exec(`
CREATE TABLE IF NOT EXISTS inbound_docs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  doc_type TEXT NOT NULL DEFAULT 'PACK_WEIGHT_LIST',
  doc_date TEXT NULL,              -- YYYY-MM-DD (parsed)
  container_no TEXT NULL,
  source_filename TEXT NULL,
  uploaded_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  notes TEXT NULL
);

CREATE TABLE IF NOT EXISTS inbound_doc_lines (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  inbound_doc_id INTEGER NOT NULL,
  line_no INTEGER NULL,

  raw_product_name TEXT NULL,      -- exactly as PDF
  package_type TEXT NULL,          -- BUCKET / BOX etc
  package_code TEXT NULL,
  ncm TEXT NULL,

  qty_packages REAL NULL,
  net_kg REAL NULL,
  gross_kg REAL NULL,

  lot_number TEXT NULL,            -- Batch N°
  sku_id INTEGER NULL,             -- mapped later

  FOREIGN KEY (inbound_doc_id) REFERENCES inbound_docs(id)
);

CREATE TABLE IF NOT EXISTS sku_aliases (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  sku_id INTEGER NOT NULL,
  alias TEXT NOT NULL,
  UNIQUE(sku_id, alias),
  FOREIGN KEY (sku_id) REFERENCES skus(id)
);
`);


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

// ✅ Email automation tables
try {
  s.exec(`
    CREATE TABLE IF NOT EXISTS email_customer_settings (
      customer_id TEXT PRIMARY KEY,
      enabled_send_invoice INTEGER NOT NULL DEFAULT 0,
      enabled_reminder INTEGER NOT NULL DEFAULT 0,
      reminder_days_before_due INTEGER NOT NULL DEFAULT 3,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS invoice_email_log (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      qbo_invoice_id TEXT NOT NULL,
      type TEXT NOT NULL, -- REMINDER
      sent_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      status TEXT NOT NULL, -- SENT | FAILED
      error TEXT
    );

    CREATE INDEX IF NOT EXISTS idx_invoice_email_log_invoice ON invoice_email_log(qbo_invoice_id, type);
  `);
} catch {}

  // ✅ Safe migrations
try { s.exec(`ALTER TABLE email_customer_settings ADD COLUMN enabled_post_due_reminder INTEGER NOT NULL DEFAULT 0;`); } catch {}
try { s.exec(`ALTER TABLE email_customer_settings ADD COLUMN post_due_days_after_due INTEGER NOT NULL DEFAULT 3;`); } catch {}

  try { s.exec(`ALTER TABLE skus ADD COLUMN qbo_category_id TEXT;`); } catch {}
  try { s.exec(`CREATE INDEX IF NOT EXISTS idx_skus_qbo_category_id ON skus(qbo_category_id);`); } catch {}
try {
  s.exec(`
    CREATE TABLE IF NOT EXISTS email_customer_settings (
      customer_id TEXT PRIMARY KEY,
      enabled_send_invoice INTEGER NOT NULL DEFAULT 0,
      enabled_reminder INTEGER NOT NULL DEFAULT 0,
      reminder_days_before_due INTEGER NOT NULL DEFAULT 3,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS invoice_email_log (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      qbo_invoice_id TEXT NOT NULL,
      type TEXT NOT NULL, -- REMINDER
      sent_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      status TEXT NOT NULL, -- SENT | FAILED
      error TEXT
    );

    CREATE INDEX IF NOT EXISTS idx_invoice_email_log_invoice ON invoice_email_log(qbo_invoice_id, type);
  `);
} catch {}

  // ✅ Inbound docs: store extracted PDF text for reload/debug/inspection
  try { s.exec(`ALTER TABLE inbound_docs ADD COLUMN raw_text TEXT;`); } catch {}

  // ✅ Allocation tracking
  try {
    s.exec(`
      CREATE TABLE IF NOT EXISTS invoice_allocations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        qbo_invoice_id TEXT NOT NULL,
        sku_id INTEGER NOT NULL,
        lot_id INTEGER,
        source_type TEXT NOT NULL,         -- WALKIN | PALLET
        source_location_code TEXT NOT NULL,
        source_pallet_id INTEGER,
        qty_units REAL NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
      );
      CREATE INDEX IF NOT EXISTS idx_invoice_allocations_invoice ON invoice_allocations(qbo_invoice_id);
    `);
  } catch {}

  // ✅ Engine state tables
  try {
    s.exec(`
      CREATE TABLE IF NOT EXISTS invoice_state (
        qbo_invoice_id TEXT PRIMARY KEY,
        last_hash TEXT NOT NULL,
        last_txn_date TEXT,
        last_seen_at DATETIME DEFAULT CURRENT_TIMESTAMP
      );

      CREATE TABLE IF NOT EXISTS invoice_line_totals (
        qbo_invoice_id TEXT NOT NULL,
        sku_id INTEGER NOT NULL,
        qty_units REAL NOT NULL,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (qbo_invoice_id, sku_id)
      );

      CREATE INDEX IF NOT EXISTS idx_invoice_line_totals_invoice ON invoice_line_totals(qbo_invoice_id);
    `);
  } catch {}

  // ✅ Lot Audit Allocations (does NOT change inventory)
  try {
    s.exec(`
      CREATE TABLE IF NOT EXISTS invoice_lot_audit_allocations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        qbo_invoice_id TEXT NOT NULL,
        txn_date TEXT,
        customer_name TEXT,
        sku_id INTEGER NOT NULL,
        lot_id INTEGER,
        qty_units REAL NOT NULL,
        method TEXT NOT NULL DEFAULT 'MANUAL',  -- MANUAL | AUTO_SUGGEST
        note TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
      );

      CREATE INDEX IF NOT EXISTS idx_audit_alloc_invoice ON invoice_lot_audit_allocations(qbo_invoice_id);
      CREATE INDEX IF NOT EXISTS idx_audit_alloc_sku ON invoice_lot_audit_allocations(sku_id, lot_id);
    `);
  } catch {}

  // ✅ Invoice SKU Line Index (audit search/reporting)
  try {
    s.exec(`
      CREATE TABLE IF NOT EXISTS invoice_sku_lines (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        qbo_invoice_id TEXT NOT NULL,
        txn_date TEXT,
        doc_number TEXT,
        customer_name TEXT,
        sku_id INTEGER NOT NULL,
        qbo_item_id TEXT,
        qty_units REAL NOT NULL,
        amount REAL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(qbo_invoice_id, sku_id, qbo_item_id, qty_units, amount)
      );

      CREATE INDEX IF NOT EXISTS idx_invoice_sku_lines_sku_date ON invoice_sku_lines(sku_id, txn_date);
      CREATE INDEX IF NOT EXISTS idx_invoice_sku_lines_invoice ON invoice_sku_lines(qbo_invoice_id);
    `);
  } catch {}
}

export function seedDefaults() {
  if (!db.getSetting('default_surcharge_amount')) db.setSetting('default_surcharge_amount', '15');
  if (!db.getSetting('surcharge_item_name')) db.setSetting('surcharge_item_name', 'Operating Cost Surcharge');
  if (!db.getSetting('uncategorized_position')) db.setSetting('uncategorized_position', 'bottom');

  if (!db.getSetting('default_pallet_pick_threshold')) db.setSetting('default_pallet_pick_threshold', '0.80');
  if (!db.getSetting('lane_priority')) db.setSetting('lane_priority', 'R_FIRST');
  if (!db.getSetting('walkin_first_default')) db.setSetting('walkin_first_default', '1');

  // Engine defaults
  if (!db.getSetting('auto_allocate_enabled')) db.setSetting('auto_allocate_enabled', '0');
  if (!db.getSetting('inventory_timezone')) db.setSetting('inventory_timezone', 'America/Los_Angeles');

  // Audit defaults
  if (!db.getSetting('organic_tracking_start')) db.setSetting('organic_tracking_start', '2025-01-01');
}