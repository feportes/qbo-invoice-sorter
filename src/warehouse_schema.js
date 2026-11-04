// Additive, isolated tables: existing inventory and QuickBooks records are retained.
export function ensureWarehouseSchema(sql) {
  sql.exec(`
    CREATE TABLE IF NOT EXISTS wh_users (
      id TEXT PRIMARY KEY, username TEXT NOT NULL UNIQUE, display_name TEXT NOT NULL,
      password_hash TEXT NOT NULL, role TEXT NOT NULL, team_id TEXT,
      enabled INTEGER NOT NULL DEFAULT 1, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
      functions TEXT NOT NULL DEFAULT '[]', must_change_password INTEGER NOT NULL DEFAULT 1
    );
    CREATE TABLE IF NOT EXISTS wh_sessions (
      token_hash TEXT PRIMARY KEY, user_id TEXT NOT NULL REFERENCES wh_users(id),
      csrf TEXT NOT NULL, device_label TEXT NOT NULL, created_at TEXT NOT NULL, expires_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS wh_sessions_user ON wh_sessions(user_id);
    CREATE TABLE IF NOT EXISTS wh_login_attempts (key TEXT PRIMARY KEY, failures INTEGER NOT NULL, reset_at INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS wh_audit (
      id TEXT PRIMARY KEY, at TEXT NOT NULL, user_id TEXT NOT NULL, action TEXT NOT NULL,
      entity_type TEXT NOT NULL, entity_id TEXT NOT NULL, detail TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS wh_audit_entity ON wh_audit(entity_type,entity_id,at);
    CREATE TABLE IF NOT EXISTS wh_purchase_orders (
      id TEXT PRIMARY KEY, vendor_id TEXT NOT NULL, vendor_name TEXT NOT NULL,
      qbo_po_id TEXT, po_number TEXT NOT NULL, expected_date TEXT, status TEXT NOT NULL,
      revision INTEGER NOT NULL, lines TEXT NOT NULL, created_by TEXT NOT NULL,
      created_at TEXT NOT NULL, updated_at TEXT NOT NULL
    );
    CREATE UNIQUE INDEX IF NOT EXISTS wh_po_qbo_id ON wh_purchase_orders(qbo_po_id) WHERE qbo_po_id IS NOT NULL;
    CREATE TABLE IF NOT EXISTS wh_receipts (
      id TEXT PRIMARY KEY, po_id TEXT NOT NULL REFERENCES wh_purchase_orders(id),
      mode TEXT NOT NULL, status TEXT NOT NULL, revision INTEGER NOT NULL,
      lines TEXT NOT NULL, notes TEXT NOT NULL, received_by TEXT NOT NULL,
      received_at TEXT NOT NULL, approved_by TEXT, approved_at TEXT, putaway_at TEXT
    );
    CREATE INDEX IF NOT EXISTS wh_receipts_po ON wh_receipts(po_id,status);
    CREATE TABLE IF NOT EXISTS wh_putaway (
      receipt_id TEXT PRIMARY KEY REFERENCES wh_receipts(id), plan TEXT NOT NULL, applied_by TEXT NOT NULL, applied_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS wh_idempotency (
      id TEXT PRIMARY KEY, user_id TEXT NOT NULL, digest TEXT NOT NULL, result TEXT NOT NULL, at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS wh_routes (
      id TEXT PRIMARY KEY, route_date TEXT NOT NULL, driver_id TEXT NOT NULL,
      status TEXT NOT NULL, revision INTEGER NOT NULL, snapshot TEXT NOT NULL,
      source_revision TEXT NOT NULL, updated_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS wh_routes_driver ON wh_routes(driver_id,route_date,status);
    CREATE TABLE IF NOT EXISTS wh_driver_events (
      sequence INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL UNIQUE,
      route_id TEXT NOT NULL REFERENCES wh_routes(id), user_id TEXT NOT NULL,
      device_id TEXT NOT NULL, device_at TEXT NOT NULL, received_at TEXT NOT NULL,
      kind TEXT NOT NULL, payload TEXT NOT NULL, status TEXT NOT NULL,
      reviewed_by TEXT, reviewed_at TEXT
    );
    CREATE INDEX IF NOT EXISTS wh_driver_events_route ON wh_driver_events(route_id,sequence);
    CREATE TABLE IF NOT EXISTS wh_media (
      id TEXT PRIMARY KEY, user_id TEXT NOT NULL, route_id TEXT, receipt_id TEXT,
      stop_id TEXT, mime_type TEXT NOT NULL, bytes BLOB NOT NULL,
      device_at TEXT NOT NULL, received_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS wh_office_issue_reviews (event_id TEXT PRIMARY KEY, resolution TEXT NOT NULL, reviewed_by TEXT NOT NULL, reviewed_at TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS wh_source_jobs (
      event_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, route_id TEXT NOT NULL,
      operation TEXT, status TEXT NOT NULL DEFAULT 'pending', error TEXT,
      updated_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS wh_source_team (id TEXT PRIMARY KEY,name TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS wh_source_access (user_id TEXT NOT NULL,route_id TEXT NOT NULL,PRIMARY KEY(user_id,route_id));
    CREATE TABLE IF NOT EXISTS wh_source_access_cache (user_id TEXT PRIMARY KEY,refreshed_at TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS wh_driver_catalog (user_id TEXT PRIMARY KEY,version TEXT NOT NULL,barcode_version TEXT NOT NULL,products TEXT NOT NULL,checked_at TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS wh_barcode_versions (version TEXT PRIMARY KEY,products TEXT NOT NULL,created_at TEXT NOT NULL);
  `);
  const columns=new Set(sql.prepare('PRAGMA table_info(wh_users)').all().map(c=>c.name));
  if(!columns.has('functions'))sql.exec("ALTER TABLE wh_users ADD COLUMN functions TEXT NOT NULL DEFAULT '[]'");
  if(!columns.has('must_change_password'))sql.exec('ALTER TABLE wh_users ADD COLUMN must_change_password INTEGER NOT NULL DEFAULT 1');
  if(!sql.prepare('PRAGMA table_info(wh_routes)').all().some(c=>c.name==='source_stop_revisions'))sql.exec("ALTER TABLE wh_routes ADD COLUMN source_stop_revisions TEXT NOT NULL DEFAULT '{}'");
  if(!sql.prepare('PRAGMA table_info(wh_driver_catalog)').all().some(c=>c.name==='source_revision'))sql.exec('ALTER TABLE wh_driver_catalog ADD COLUMN source_revision INTEGER NOT NULL DEFAULT 0');
}
