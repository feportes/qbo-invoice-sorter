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

getInboundUnitsForSkuLotId({ skuId, lotId }) {
  const lot = sqlite.prepare(`SELECT lot_number FROM lots WHERE id=? AND sku_id=?`).get(Number(lotId), Number(skuId));
  if (!lot?.lot_number) return 0;

  const r = sqlite.prepare(`
    SELECT SUM(COALESCE(il.qty_packages,0)) AS inbound_units
    FROM inbound_doc_lines il
    WHERE il.sku_id = ?
      AND il.lot_number = ?
  `).get(Number(skuId), String(lot.lot_number));

  return Number(r?.inbound_units || 0);
},

getAllocatedUnitsForSkuLotId({ skuId, lotId, excludeAllocationIds = [] }) {
  const exclude = (excludeAllocationIds || []).map(Number).filter(Boolean);

  if (exclude.length === 0) {
    const r = sqlite.prepare(`
      SELECT SUM(COALESCE(qty_units,0)) AS allocated
      FROM invoice_lot_audit_allocations
      WHERE sku_id=? AND lot_id=?
    `).get(Number(skuId), Number(lotId));
    return Number(r?.allocated || 0);
  }

  const placeholders = exclude.map(() => '?').join(',');
  const r = sqlite.prepare(`
    SELECT SUM(COALESCE(qty_units,0)) AS allocated
    FROM invoice_lot_audit_allocations
    WHERE sku_id=? AND lot_id=?
      AND id NOT IN (${placeholders})
  `).get(Number(skuId), Number(lotId), ...exclude);

  return Number(r?.allocated || 0);
},

getAuditAllocationsByIds(ids) {
  const arr = (ids || []).map(Number).filter(Boolean);
  if (arr.length === 0) return [];
  const placeholders = arr.map(() => '?').join(',');
  return sqlite.prepare(`
    SELECT *
    FROM invoice_lot_audit_allocations
    WHERE id IN (${placeholders})
  `).all(...arr);
},

deleteAuditAllocationsByIds(ids) {
  const arr = (ids || []).map(Number).filter(Boolean);
  if (arr.length === 0) return 0;
  const placeholders = arr.map(() => '?').join(',');
  const r = sqlite.prepare(`DELETE FROM invoice_lot_audit_allocations WHERE id IN (${placeholders})`).run(...arr);
  return r.changes || 0;
},

reassignAuditAllocationsByIds({ ids, newLotId }) {
  const arr = (ids || []).map(Number).filter(Boolean);
  if (arr.length === 0) return 0;
  const placeholders = arr.map(() => '?').join(',');

  const r = sqlite.prepare(`
    UPDATE invoice_lot_audit_allocations
    SET lot_id = ?, method='MANUAL'
    WHERE id IN (${placeholders})
  `).run(newLotId ? Number(newLotId) : null, ...arr);

  return r.changes || 0;
},

listAssignedAllocationsForSku({ skuId, startDate, endDate }) {
  return sqlite.prepare(`
    SELECT
      a.id,
      a.qbo_invoice_id,
      a.txn_date,
      a.customer_name,
      a.qty_units,
      a.method,
      a.note,
      lo.lot_number,
      lo.id AS lot_id,
      MAX(l.doc_number) AS doc_number
    FROM invoice_lot_audit_allocations a
    LEFT JOIN lots lo ON lo.id = a.lot_id
    LEFT JOIN invoice_sku_lines l
      ON l.qbo_invoice_id = a.qbo_invoice_id
     AND l.sku_id = a.sku_id
    WHERE a.sku_id = ?
      AND (? IS NULL OR a.txn_date >= ?)
      AND (? IS NULL OR a.txn_date <= ?)
    GROUP BY a.id
    ORDER BY a.txn_date ASC, a.qbo_invoice_id ASC, a.id ASC
  `).all(
    Number(skuId),
    startDate || null, startDate || null,
    endDate || null, endDate || null
  );
},


listUnassignedInvoicesContainingSku({ skuId, startDate, endDate }) {
  return sqlite.prepare(`
    SELECT
      l.qbo_invoice_id,
      MAX(l.txn_date) AS txn_date,
      MAX(l.doc_number) AS doc_number,
      MAX(l.customer_name) AS customer_name,
      SUM(l.qty_units) AS qty_units,
      SUM(COALESCE(l.amount, 0)) AS amount
    FROM invoice_sku_lines l
    LEFT JOIN invoice_lot_audit_allocations a
      ON a.qbo_invoice_id = l.qbo_invoice_id
     AND a.sku_id = l.sku_id
    WHERE l.sku_id = ?
      AND a.id IS NULL
      AND (? IS NULL OR l.txn_date >= ?)
      AND (? IS NULL OR l.txn_date <= ?)
    GROUP BY l.qbo_invoice_id
    ORDER BY txn_date ASC
  `).all(
    Number(skuId),
    startDate || null, startDate || null,
    endDate || null, endDate || null
  );
},

listUnassignedInvoicesForSku({ skuId, startDate, endDate }) {
  return sqlite.prepare(`
    SELECT
      l.qbo_invoice_id,
      MAX(l.txn_date) AS txn_date,
      MAX(l.doc_number) AS doc_number,
      MAX(l.customer_name) AS customer_name,
      SUM(l.qty_units) AS qty_units,
      SUM(COALESCE(l.amount, 0)) AS amount
    FROM invoice_sku_lines l
    LEFT JOIN invoice_lot_audit_allocations a
      ON a.qbo_invoice_id = l.qbo_invoice_id
     AND a.sku_id = l.sku_id
    WHERE l.sku_id = ?
      AND a.id IS NULL
      AND (? IS NULL OR l.txn_date >= ?)
      AND (? IS NULL OR l.txn_date <= ?)
    GROUP BY l.qbo_invoice_id
    ORDER BY txn_date ASC, l.qbo_invoice_id ASC
  `).all(
    Number(skuId),
    startDate || null, startDate || null,
    endDate || null, endDate || null
  ).map(r => ({
    ...r,
    qty_units: Number(r.qty_units || 0),
    amount: Number(r.amount || 0)
  }));
},

listLotAvailabilityForSku(skuId) {
  return sqlite.prepare(`
    SELECT
      lo.id AS lot_id,
      lo.lot_number,
      MIN(d.doc_date) AS inbound_date,
      SUM(COALESCE(il.qty_packages, 0)) AS inbound_units,
      SUM(COALESCE(il.net_kg, 0)) AS inbound_net_kg,
      COALESCE((
        SELECT SUM(a.qty_units)
        FROM invoice_lot_audit_allocations a
        WHERE a.sku_id = ?
          AND a.lot_id = lo.id
      ), 0) AS allocated_units
    FROM lots lo
    JOIN inbound_doc_lines il
      ON il.sku_id = lo.sku_id
     AND il.lot_number = lo.lot_number
    JOIN inbound_docs d
      ON d.id = il.inbound_doc_id
    WHERE lo.sku_id = ?
    GROUP BY lo.id, lo.lot_number
    ORDER BY inbound_date ASC, lo.lot_number COLLATE NOCASE ASC
  `).all(Number(skuId), Number(skuId))
  .map(r => ({
    ...r,
    inbound_units: Number(r.inbound_units || 0),
    allocated_units: Number(r.allocated_units || 0),
    remaining_units: Number(r.inbound_units || 0) - Number(r.allocated_units || 0)
  }));
},

updateInboundDocHeader({ id, doc_date, container_no }) {
  sqlite.prepare(`
    UPDATE inbound_docs
    SET doc_date = COALESCE(?, doc_date),
        container_no = COALESCE(?, container_no)
    WHERE id = ?
  `).run(
    doc_date || null,
    container_no || null,
    Number(id)
  );
},

updateInboundDocRawText(docId, rawText) {
  sqlite.prepare(`UPDATE inbound_docs SET raw_text=? WHERE id=?`)
    .run(rawText ? String(rawText) : null, Number(docId));
},

getInboundDocRawText(docId) {
  const r = sqlite.prepare(`SELECT raw_text FROM inbound_docs WHERE id=?`).get(Number(docId));
  return r?.raw_text || null;
},

replaceInboundDocLines(docId, rows) {
  const id = Number(docId);
  const tx = sqlite.transaction(() => {
    sqlite.prepare(`DELETE FROM inbound_doc_lines WHERE inbound_doc_id=?`).run(id);

    const ins = sqlite.prepare(`
      INSERT INTO inbound_doc_lines
        (inbound_doc_id, line_no, raw_product_name, ncm, package_type, package_code,
         qty_packages, net_kg, gross_kg, lot_number, sku_id)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);

    for (const r of rows) {
      ins.run(
        id,
        r.line_no ?? null,
        r.raw_product_name ?? null,
        r.ncm ?? null,
        r.package_type ?? null,
        r.package_code ?? null,
        r.qty_packages ?? null,
        r.net_kg ?? null,
        r.gross_kg ?? null,
        r.lot_number ?? null,
        r.sku_id ?? null
      );
    }
  });
  tx();
},

updateInboundLineAllFields({
  line_id,
  raw_product_name,
  ncm,
  package_type,
  package_code,
  qty_packages,
  net_kg,
  gross_kg,
  lot_number
}) {
  sqlite.prepare(`
    UPDATE inbound_doc_lines
    SET raw_product_name = ?,
        ncm = ?,
        package_type = ?,
        package_code = ?,
        qty_packages = ?,
        net_kg = ?,
        gross_kg = ?,
        lot_number = ?
    WHERE id = ?
  `).run(
    raw_product_name ? String(raw_product_name).trim() : null,
    ncm ? String(ncm).trim() : null,
    package_type ? String(package_type).trim() : null,
    package_code ? String(package_code).trim() : null,
    (qty_packages === '' || qty_packages === null || qty_packages === undefined) ? null : Number(qty_packages),
    (net_kg === '' || net_kg === null || net_kg === undefined) ? null : Number(net_kg),
    (gross_kg === '' || gross_kg === null || gross_kg === undefined) ? null : Number(gross_kg),
    lot_number ? String(lot_number).trim() : null,
    Number(line_id)
  );
},

deleteInboundDoc(docId) {
  const id = Number(docId);
  const tx = sqlite.transaction(() => {
    sqlite.prepare(`DELETE FROM inbound_doc_lines WHERE inbound_doc_id=?`).run(id);
    sqlite.prepare(`DELETE FROM inbound_docs WHERE id=?`).run(id);
  });
  tx();
},


createInboundDoc({ doc_date, container_no, source_filename, notes }) {
  const r = sqlite.prepare(`
    INSERT INTO inbound_docs (doc_date, container_no, source_filename, notes)
    VALUES (?, ?, ?, ?)
  `).run(doc_date || null, container_no || null, source_filename || null, notes || null);
  return Number(r.lastInsertRowid);
},

addInboundDocLine(inbound_doc_id, line) {
  sqlite.prepare(`
    INSERT INTO inbound_doc_lines
      (inbound_doc_id, line_no, raw_product_name, package_type, package_code, ncm,
       qty_packages, net_kg, gross_kg, lot_number, sku_id)
    VALUES
      (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(
    Number(inbound_doc_id),
    line.line_no ?? null,
    line.raw_product_name ?? null,
    line.package_type ?? null,
    line.package_code ?? null,
    line.ncm ?? null,
    line.qty_packages ?? null,
    line.net_kg ?? null,
    line.gross_kg ?? null,
    line.lot_number ?? null,
    line.sku_id ?? null
  );
},

getInboundDoc(id) {
  return sqlite.prepare(`SELECT * FROM inbound_docs WHERE id=?`).get(Number(id));
},

listInboundDocs() {
  return sqlite.prepare(`
    SELECT * FROM inbound_docs
    ORDER BY uploaded_at DESC, id DESC
  `).all();
},

listInboundDocLines(docId) {
  return sqlite.prepare(`
    SELECT l.*,
           s.name AS sku_name
    FROM inbound_doc_lines l
    LEFT JOIN skus s ON s.id = l.sku_id
    WHERE l.inbound_doc_id=?
    ORDER BY COALESCE(l.line_no, l.id) ASC
  `).all(Number(docId));
},

addSkuAlias({ sku_id, alias }) {
  const a = String(alias || '').trim();
  if (!a) return;
  sqlite.prepare(`
    INSERT OR IGNORE INTO sku_aliases (sku_id, alias)
    VALUES (?, ?)
  `).run(Number(sku_id), a);
},

findSkuIdByAliasOrName(rawName) {
  const nm = String(rawName || '').trim();
  if (!nm) return null;

  const a = sqlite.prepare(`
    SELECT sku_id FROM sku_aliases
    WHERE lower(alias)=lower(?)
    LIMIT 1
  `).get(nm);
  if (a?.sku_id) return Number(a.sku_id);

  const s = sqlite.prepare(`
    SELECT id FROM skus
    WHERE lower(name)=lower(?)
    LIMIT 1
  `).get(nm);
  if (s?.id) return Number(s.id);

  return null;
},

setInboundLineSku(lineId, skuId) {
  sqlite.prepare(`UPDATE inbound_doc_lines SET sku_id=? WHERE id=?`)
    .run(skuId ? Number(skuId) : null, Number(lineId));
},

upsertLotForSku({ sku_id, lot_number, production_date = null, expiration_date = null }) {
  const lotNum = String(lot_number || '').trim();
  if (!lotNum) return null;

  const lot = sqlite.prepare(`
    SELECT * FROM lots
    WHERE sku_id=? AND lot_number=?
    LIMIT 1
  `).get(Number(sku_id), lotNum);

  if (lot?.id) return Number(lot.id);

  const r = sqlite.prepare(`
    INSERT INTO lots (sku_id, lot_number, production_date, expiration_date)
    VALUES (?, ?, ?, ?)
  `).run(Number(sku_id), lotNum, production_date, expiration_date);

  return Number(r.lastInsertRowid);
},


// ==========================================================
// Invoice SKU line index (audit search)
// ==========================================================
upsertInvoiceSkuLine(row) {
  sqlite.prepare(`
    INSERT OR IGNORE INTO invoice_sku_lines
      (qbo_invoice_id, txn_date, doc_number, customer_name, sku_id, qbo_item_id, qty_units, amount)
    VALUES
      (?, ?, ?, ?, ?, ?, ?, ?)
  `).run(
    String(row.qbo_invoice_id),
    row.txn_date ? String(row.txn_date) : null,
    row.doc_number ? String(row.doc_number) : null,
    row.customer_name ? String(row.customer_name) : null,
    Number(row.sku_id),
    row.qbo_item_id ? String(row.qbo_item_id) : null,
    Number(row.qty_units),
    row.amount === null || row.amount === undefined ? null : Number(row.amount)
  );
},

listInvoicesContainingSku({ skuId, startDate, endDate }) {
  return sqlite.prepare(`
    SELECT
      l.qbo_invoice_id,
      MAX(l.txn_date) AS txn_date,
      MAX(l.doc_number) AS doc_number,
      MAX(l.customer_name) AS customer_name,
      SUM(l.qty_units) AS qty_units,
      SUM(COALESCE(l.amount, 0)) AS amount
    FROM invoice_sku_lines l
    WHERE l.sku_id = ?
      AND (? IS NULL OR l.txn_date >= ?)
      AND (? IS NULL OR l.txn_date <= ?)
    GROUP BY l.qbo_invoice_id
    ORDER BY txn_date ASC
  `).all(Number(skuId),
    startDate || null, startDate || null,
    endDate || null, endDate || null
  );
},

listLotsForSku(skuId) {
  return sqlite.prepare(`
    SELECT l.*
    FROM lots l
    WHERE l.sku_id=?
    ORDER BY
      CASE WHEN l.expiration_date IS NULL THEN 1 ELSE 0 END,
      l.expiration_date ASC,
      CASE WHEN l.production_date IS NULL THEN 1 ELSE 0 END,
      l.production_date ASC,
      l.lot_number COLLATE NOCASE ASC
  `).all(Number(skuId));
},

// ==========================================================
// Audit allocations (do NOT change inventory)
// ==========================================================
listAuditAllocations(invoiceId) {
  return sqlite.prepare(`
    SELECT a.*,
           s.name AS sku_name,
           s.unit_type AS unit_type,
           lo.lot_number AS lot_number
    FROM invoice_lot_audit_allocations a
    JOIN skus s ON s.id = a.sku_id
    LEFT JOIN lots lo ON lo.id = a.lot_id
    WHERE a.qbo_invoice_id=?
    ORDER BY a.id ASC
  `).all(String(invoiceId));
},

hasAuditAllocationForInvoiceSku(invoiceId, skuId) {
  const row = sqlite.prepare(`
    SELECT 1 FROM invoice_lot_audit_allocations
    WHERE qbo_invoice_id=? AND sku_id=?
    LIMIT 1
  `).get(String(invoiceId), Number(skuId));
  return !!row;
},

replaceAuditAllocations({ invoiceId, txnDate, customerName, rows }) {
  const tx = sqlite.transaction(() => {
    sqlite.prepare(`DELETE FROM invoice_lot_audit_allocations WHERE qbo_invoice_id=?`)
      .run(String(invoiceId));

    const ins = sqlite.prepare(`
      INSERT INTO invoice_lot_audit_allocations
        (qbo_invoice_id, txn_date, customer_name, sku_id, lot_id, qty_units, method, note)
      VALUES
        (?, ?, ?, ?, ?, ?, ?, ?)
    `);

    for (const r of rows) {
      ins.run(
        String(invoiceId),
        txnDate ? String(txnDate) : null,
        customerName ? String(customerName) : null,
        Number(r.sku_id),
        r.lot_id ? Number(r.lot_id) : null,
        Number(r.qty_units),
        String(r.method || 'MANUAL'),
        r.note ? String(r.note) : null
      );
    }
  });
  tx();
},

// ==========================================================
// Auto-suggest lot by txn_date
// ==========================================================
getSuggestedLotForSkuOnDate(skuId, txnDate) {
  const d = String(txnDate || '').trim();
  if (!d) return null;

  const r1 = sqlite.prepare(`
    SELECT id
    FROM lots
    WHERE sku_id=?
      AND (production_date IS NULL OR production_date <= ?)
      AND (expiration_date IS NULL OR ? <= expiration_date)
    ORDER BY
      CASE WHEN expiration_date IS NULL THEN 1 ELSE 0 END,
      expiration_date ASC,
      CASE WHEN production_date IS NULL THEN 1 ELSE 0 END,
      production_date DESC,
      lot_number COLLATE NOCASE ASC
    LIMIT 1
  `).get(Number(skuId), d, d);
  if (r1?.id) return Number(r1.id);

  const r2 = sqlite.prepare(`
    SELECT id
    FROM lots
    WHERE sku_id=? AND production_date IS NOT NULL AND production_date <= ?
    ORDER BY production_date DESC, lot_number COLLATE NOCASE ASC
    LIMIT 1
  `).get(Number(skuId), d);
  if (r2?.id) return Number(r2.id);

  const r3 = sqlite.prepare(`
    SELECT id
    FROM lots
    WHERE sku_id=? AND expiration_date IS NOT NULL AND expiration_date >= ?
    ORDER BY expiration_date ASC, lot_number COLLATE NOCASE ASC
    LIMIT 1
  `).get(Number(skuId), d);
  if (r3?.id) return Number(r3.id);

  const r4 = sqlite.prepare(`
    SELECT id
    FROM lots
    WHERE sku_id=?
    ORDER BY created_at DESC, lot_number COLLATE NOCASE DESC
    LIMIT 1
  `).get(Number(skuId));
  return r4?.id ? Number(r4.id) : null;
},

listAuditMasterReport({ startDate, endDate }) {
  return sqlite.prepare(`
    SELECT
      s.id AS sku_id,
      s.name AS sku_name,
      s.unit_type,
      lo.id AS lot_id,
      lo.lot_number,
      a.qbo_invoice_id,
      a.txn_date,
      a.customer_name,
      SUM(a.qty_units) AS qty_units
    FROM invoice_lot_audit_allocations a
    JOIN skus s ON s.id = a.sku_id
    LEFT JOIN lots lo ON lo.id = a.lot_id
    WHERE s.is_organic=1
      AND (? IS NULL OR a.txn_date >= ?)
      AND (? IS NULL OR a.txn_date <= ?)
    GROUP BY
      s.id, s.name, s.unit_type,
      lo.id, lo.lot_number,
      a.qbo_invoice_id, a.txn_date, a.customer_name
    ORDER BY
      s.name COLLATE NOCASE ASC,
      (lo.lot_number IS NULL) ASC,
      lo.lot_number COLLATE NOCASE ASC,
      a.txn_date ASC,
      a.qbo_invoice_id ASC
  `).all(
    startDate || null, startDate || null,
    endDate || null, endDate || null
  );
},



// ==========================================================
// Invoice SKU line index (audit search)
// ==========================================================
upsertInvoiceSkuLine(row) {
  sqlite.prepare(`
    INSERT OR IGNORE INTO invoice_sku_lines
      (qbo_invoice_id, txn_date, doc_number, customer_name, sku_id, qbo_item_id, qty_units, amount)
    VALUES
      (?, ?, ?, ?, ?, ?, ?, ?)
  `).run(
    String(row.qbo_invoice_id),
    row.txn_date ? String(row.txn_date) : null,
    row.doc_number ? String(row.doc_number) : null,
    row.customer_name ? String(row.customer_name) : null,
    Number(row.sku_id),
    row.qbo_item_id ? String(row.qbo_item_id) : null,
    Number(row.qty_units),
    row.amount === null || row.amount === undefined ? null : Number(row.amount)
  );
},

listInvoicesContainingSku({ skuId, startDate, endDate }) {
  return sqlite.prepare(`
    SELECT
      l.qbo_invoice_id,
      MAX(l.txn_date) AS txn_date,
      MAX(l.doc_number) AS doc_number,
      MAX(l.customer_name) AS customer_name,
      SUM(l.qty_units) AS qty_units,
      SUM(COALESCE(l.amount, 0)) AS amount
    FROM invoice_sku_lines l
    WHERE l.sku_id = ?
      AND (? IS NULL OR l.txn_date >= ?)
      AND (? IS NULL OR l.txn_date <= ?)
    GROUP BY l.qbo_invoice_id
    ORDER BY txn_date ASC
  `).all(Number(skuId),
    startDate || null, startDate || null,
    endDate || null, endDate || null
  );
},

// Report: Lot -> invoices (based on saved audit allocations)
listAuditLotReport({ skuId, lotId, startDate, endDate }) {
  return sqlite.prepare(`
    SELECT
      a.qbo_invoice_id,
      a.txn_date,
      a.customer_name,
      a.qty_units AS allocated_qty,
      a.method,
      a.note,
      lo.lot_number
    FROM invoice_lot_audit_allocations a
    LEFT JOIN lots lo ON lo.id = a.lot_id
    WHERE a.sku_id = ?
      AND COALESCE(a.lot_id, 0) = COALESCE(?, 0)
      AND (? IS NULL OR a.txn_date >= ?)
      AND (? IS NULL OR a.txn_date <= ?)
    ORDER BY a.txn_date ASC, a.qbo_invoice_id ASC
  `).all(
    Number(skuId),
    lotId ? Number(lotId) : null,
    startDate || null, startDate || null,
    endDate || null, endDate || null
  );
},


  // ==========================================================
  // Lots helpers (for audit dropdowns)
  // ==========================================================
  listLotsForSku(skuId) {
    return sqlite.prepare(`
      SELECT l.*,
             s.name AS sku_name
      FROM lots l
      JOIN skus s ON s.id = l.sku_id
      WHERE l.sku_id=?
      ORDER BY
        CASE WHEN l.expiration_date IS NULL THEN 1 ELSE 0 END,
        l.expiration_date ASC,
        CASE WHEN l.production_date IS NULL THEN 1 ELSE 0 END,
        l.production_date ASC,
        l.lot_number COLLATE NOCASE ASC
    `).all(Number(skuId));
  },

  // ==========================================================
  // Audit allocations (DO NOT change inventory)
  // ==========================================================
  listAuditAllocations(invoiceId) {
    return sqlite.prepare(`
      SELECT a.*,
             s.name AS sku_name,
             s.unit_type AS unit_type,
             lo.lot_number AS lot_number
      FROM invoice_lot_audit_allocations a
      JOIN skus s ON s.id = a.sku_id
      LEFT JOIN lots lo ON lo.id = a.lot_id
      WHERE a.qbo_invoice_id=?
      ORDER BY a.id ASC
    `).all(String(invoiceId));
  },

  replaceAuditAllocations({ invoiceId, txnDate, customerName, rows }) {
  const tx = sqlite.transaction(() => {
    // ✅ Only delete allocations for the SKU(s) being replaced,
    // NOT the entire invoice (prevents wiping other SKU allocations).
    const skuIds = [...new Set((rows || []).map(r => Number(r.sku_id)).filter(Boolean))];

    if (skuIds.length === 0) {
      return;
    }

    const placeholders = skuIds.map(() => '?').join(',');
    sqlite.prepare(`
      DELETE FROM invoice_lot_audit_allocations
      WHERE qbo_invoice_id=?
        AND sku_id IN (${placeholders})
    `).run(String(invoiceId), ...skuIds);

    const ins = sqlite.prepare(`
      INSERT INTO invoice_lot_audit_allocations
        (qbo_invoice_id, txn_date, customer_name, sku_id, lot_id, qty_units, method, note)
      VALUES
        (?, ?, ?, ?, ?, ?, ?, ?)
    `);

    for (const r of rows) {
      ins.run(
        String(invoiceId),
        txnDate ? String(txnDate) : null,
        customerName ? String(customerName) : null,
        Number(r.sku_id),
        r.lot_id ? Number(r.lot_id) : null,
        Number(r.qty_units),
        String(r.method || 'MANUAL'),
        r.note ? String(r.note) : null
      );
    }
  });

  tx();
},



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


