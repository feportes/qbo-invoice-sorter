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
      match_type TEXT NOT NULL,              -- exact | prefix
      customer_id TEXT,                      -- for exact
      prefix TEXT,                           -- for prefix
      rule_type TEXT NOT NULL,               -- always_0 | always_15 | conditional | exclude
      threshold REAL,                        -- for conditional
      amount REAL,                           -- for always/conditional (default 15)
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
  `);
}

export function seedDefaults() {
  // Settings
  if (!db.getSetting('default_surcharge_amount')) db.setSetting('default_surcharge_amount', '15');
  if (!db.getSetting('surcharge_item_name')) db.setSetting('surcharge_item_name', 'Operating Cost Surcharge');
  if (!db.getSetting('uncategorized_position')) db.setSetting('uncategorized_position', 'bottom');

  // Seed some example rules only if empty
  const rules = db.listRules();
  if (rules.length > 0) return;

  // Prefix: Alohana* always 0
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

  // Exact: Amazonia Bowls always 15 (will require selecting customer id later in UI; we leave as disabled placeholder)
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

  // Exact: Amafruits exclude (disabled placeholder)
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

  // Exact: Dear Acai conditional waive >=1000 (disabled placeholder)
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
