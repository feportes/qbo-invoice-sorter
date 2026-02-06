import crypto from 'crypto';
import { db } from './db.js';
import { buildPlanFromInvoice, applyPlan } from './inventory_allocate.js';

function todayInTZ(tz = 'America/Los_Angeles') {
  const dtf = new Intl.DateTimeFormat('en-CA', {
    timeZone: tz, year: 'numeric', month: '2-digit', day: '2-digit'
  });
  return dtf.format(new Date()); // YYYY-MM-DD
}

function stableInvoiceHash(invoice) {
  // hash only what affects allocation: TxnDate + itemId + qty
  const txnDate = String(invoice?.TxnDate || '');
  const lines = (invoice?.Line || [])
    .filter(l => l?.DetailType === 'SalesItemLineDetail')
    .map(l => ({
      itemId: l?.SalesItemLineDetail?.ItemRef?.value || null,
      qty: Number(l?.SalesItemLineDetail?.Qty || 0)
    }))
    .filter(x => x.itemId && x.qty > 0)
    .sort((a, b) => String(a.itemId).localeCompare(String(b.itemId)));

  return crypto
    .createHash('sha256')
    .update(JSON.stringify({ txnDate, lines }))
    .digest('hex');
}

/**
 * Engine v1:
 * - blocks future invoices (TxnDate > today in tz)
 * - if invoice hash unchanged => noop
 * - if changed and previously allocated => reverse all allocations then apply again (simple + safe)
 * - stores invoice_state + invoice_line_totals
 */
export async function runAutoAllocateForInvoice({ invoice, invoiceId }) {
  const tz = db.getSetting('inventory_timezone') || 'America/Los_Angeles';
  const today = todayInTZ(tz);
  const txnDate = String(invoice?.TxnDate || '');

  if (!txnDate) {
    db.addLog({ invoice_id: String(invoiceId), customer_name: null, action: 'inv_engine_skip', detail: 'Missing TxnDate', source: 'inv_engine' });
    return { status: 'skip', reason: 'missing TxnDate' };
  }

  if (txnDate > today) {
    db.addLog({
      invoice_id: String(invoiceId),
      customer_name: null,
      action: 'inv_engine_skip_future',
      detail: `Blocked future invoice TxnDate=${txnDate} today=${today}`,
      source: 'inv_engine'
    });
    return { status: 'skip', reason: 'future invoice blocked', txnDate, today };
  }

  const hash = stableInvoiceHash(invoice);
  const prev = db.getInvoiceState(invoiceId);

  if (prev && prev.last_hash === hash) {
    return { status: 'noop', reason: 'hash unchanged' };
  }

  // If previously allocated, reverse first (safe v1)
  if (prev) {
    try {
      db.reverseInvoiceAllocations(invoiceId);
      db.addLog({
        invoice_id: String(invoiceId),
        customer_name: null,
        action: 'inv_engine_reversed_prior',
        detail: 'Reversed prior allocations due to invoice change',
        source: 'inv_engine'
      });
    } catch (e) {
      db.addLog({
        invoice_id: String(invoiceId),
        customer_name: null,
        action: 'inv_engine_reverse_error',
        detail: e?.message || String(e),
        source: 'inv_engine'
      });
      throw e;
    }
  }

  // Apply current plan
  const plan = buildPlanFromInvoice(invoice);
  applyPlan(plan);

  // Store totals for reference (optional use later)
  const totalsMap = new Map();
  for (const it of plan.items) {
    if (it.skipped) continue;
    totalsMap.set(it.sku_id, (totalsMap.get(it.sku_id) || 0) + Number(it.qtyRequested || 0));
  }
  const totalsArr = [...totalsMap.entries()].map(([sku_id, qty_units]) => ({ sku_id, qty_units }));

  db.replaceInvoiceTotals(invoiceId, totalsArr);
  db.upsertInvoiceState({ invoiceId, hash, txnDate });

  db.addLog({
    invoice_id: String(invoiceId),
    customer_name: null,
    action: 'inv_engine_applied',
    detail: `Auto-allocated invoice. TxnDate=${txnDate} today=${today}`,
    source: 'inv_engine'
  });

  return { status: 'applied', invoiceId: String(invoiceId), txnDate, today };
}
