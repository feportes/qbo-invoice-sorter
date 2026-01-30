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
  listLogs(limit=50) {
    return sqlite.prepare('SELECT * FROM logs ORDER BY created_at DESC LIMIT ?').all(limit);
  },

  // Invoice processing lock / idempotency
  hasProcessed(invoiceId, syncToken) {
    const row = sqlite.prepare('SELECT 1 FROM processed WHERE invoice_id=? AND sync_token=?').get(invoiceId, syncToken);
    return !!row;
  },
  markProcessed(invoiceId, syncToken) {
    sqlite.prepare('INSERT INTO processed (invoice_id, sync_token) VALUES (?, ?)').run(invoiceId, syncToken);
  }
};
