import { db } from './db.js';
import {
  qboReadInvoiceWithRetry,
  qboUpdateInvoice,
  qboBatchReadItems,
  qboReadCustomer,
  qboReadItemByName
} from './qbo.js';

const AUTO_MARKER = '[AUTO_SORTED_V1]';

function isMovableItemLine(line) {
  return line?.DetailType === 'SalesItemLineDetail' && line?.SalesItemLineDetail?.ItemRef?.value;
}

function getItemId(line) {
  return line?.SalesItemLineDetail?.ItemRef?.value || null;
}

function normalize(s) {
  return (s || '').trim().toLowerCase();
}

function buildSurchargeLine({ surchargeItemId, amount }) {
  return {
    DetailType: 'SalesItemLineDetail',
    Amount: Number(amount),
    SalesItemLineDetail: {
      ItemRef: { value: String(surchargeItemId) }
    }
  };
}

function ensureAutoMarkerInPrivateNote(note) {
  const cur = (note || '').toString();
  if (cur.includes(AUTO_MARKER)) return cur;
  return (cur + ' ' + AUTO_MARKER).trim();
}

function shouldSkipBecauseLocked(invoice, source) {
  if (source !== 'webhook') return false;
  const note = (invoice?.PrivateNote || '').toString();
  return note.includes(AUTO_MARKER);
}

function findRuleForCustomer({ customerId, customerName }) {
  const rules = db.listRules().filter(r => r.enabled === 1);

  const exact = rules.find(r => r.match_type === 'exact' && r.customer_id && r.customer_id === String(customerId));
  if (exact) return exact;

  const name = customerName || '';
  const lower = normalize(name);
  const prefixRules = rules
    .filter(r => r.match_type === 'prefix' && r.prefix)
    .sort((a, b) => (b.prefix.length - a.prefix.length));

  for (const r of prefixRules) {
    if (lower.startsWith(normalize(r.prefix))) return r;
  }

  return { rule_type: 'always_15', amount: Number(db.getSetting('default_surcharge_amount') || 15), threshold: null };
}

function computeSubtotalIgnoreDiscounts(lines, surchargeItemId) {
  let sum = 0;
  for (const line of lines) {
    if (!isMovableItemLine(line)) continue;
    const itemId = getItemId(line);
    if (surchargeItemId && itemId === String(surchargeItemId)) continue;
    sum += Number(line.Amount || 0);
  }
  return sum;
}

function findSurchargeLineIndex(lines, surchargeItemId) {
  if (!surchargeItemId) return -1;
  return lines.findIndex(l => isMovableItemLine(l) && getItemId(l) === String(surchargeItemId));
}

/**
 * Extract a category order number from an item name like:
 * "01. Sorbets: ..." or "5 - Beverage: ..." etc.
 */
function extractPrefixNumber(name) {
  const n = (name || '').trim();
  const m = n.match(/^(\d{1,2})\s*[.\-:]/);
  return m ? Number(m[1]) : null;
}

export async function processInvoice({ oauthClient, realmId, invoiceId, source }) {
  // Ensure surcharge item id cached
  let surchargeItemId = db.getSetting('surcharge_item_id');
  if (!surchargeItemId) {
    const surchargeItemName = db.getSetting('surcharge_item_name');
    const surcharge = await qboReadItemByName(oauthClient, realmId, surchargeItemName);
    if (!surcharge?.Id) throw new Error(`Could not find Item named "${surchargeItemName}" in QBO.`);
    surchargeItemId = surcharge.Id;
    db.setSetting('surcharge_item_id', surchargeItemId);
  }

  // Read invoice (retry)
  const invResp = await qboReadInvoiceWithRetry(oauthClient, realmId, invoiceId, 6);
  const invoice = invResp?.Invoice;
  if (!invoice) throw new Error(`Invoice not found: ${invoiceId}`);

  // LOCK for webhooks
  if (shouldSkipBecauseLocked(invoice, source)) {
    db.addLog({
      invoice_id: invoice.Id,
      customer_name: invoice?.CustomerRef?.name || null,
      action: 'skip_locked',
      detail: `Skipped because invoice contains ${AUTO_MARKER}`,
      source
    });
    return { invoiceId: invoice.Id, status: 'noop', reason: 'invoice locked (manual edits preserved)' };
  }

  // Idempotency
  if (db.hasProcessed(invoice.Id, invoice.SyncToken)) {
    return { invoiceId: invoice.Id, status: 'noop', reason: 'already processed for this SyncToken' };
  }

  const customerId = invoice?.CustomerRef?.value;
  let customerName = invoice?.CustomerRef?.name || null;

  if (!customerName && customerId) {
    try {
      const c = await qboReadCustomer(oauthClient, realmId, customerId);
      customerName = c?.Customer?.DisplayName || null;
    } catch {}
  }

  const originalLines = invoice.Line || [];
  const rule = findRuleForCustomer({ customerId, customerName });

  // Determine desired surcharge amount
  const subtotal = computeSubtotalIgnoreDiscounts(originalLines, surchargeItemId);

  let desiredAmount = Number(db.getSetting('default_surcharge_amount') || 15);
  let shouldHaveSurchargeLine = true;

  if (rule.rule_type === 'exclude') {
    shouldHaveSurchargeLine = false;
  } else if (rule.rule_type === 'always_0') {
    desiredAmount = 0;
  } else if (rule.rule_type === 'always_15') {
    desiredAmount = Number(rule.amount ?? desiredAmount);
  } else if (rule.rule_type === 'conditional') {
    const threshold = Number(rule.threshold ?? 0);
    const amount = Number(rule.amount ?? desiredAmount);
    desiredAmount = subtotal >= threshold ? 0 : amount;
  } else {
    desiredAmount = Number(rule.amount ?? desiredAmount);
  }

  let lines = [...originalLines];

  const surchargeIdx = findSurchargeLineIndex(lines, surchargeItemId);
  const hasSurchargeLine = surchargeIdx >= 0;
  const currentSurchargeAmount = hasSurchargeLine ? Number(lines[surchargeIdx].Amount || 0) : null;

  // Manual override detection (no Description tag):
  // If a surcharge line exists but doesn't match our computed rule, treat as manual override.
  const manualSurchargeOverride =
    hasSurchargeLine &&
    (
      // rule says exclude but user kept a line
      (!shouldHaveSurchargeLine) ||
      // amount differs from what rule would set
      (Math.abs(currentSurchargeAmount - Number(desiredAmount)) > 0.0001)
    );

  // Apply surcharge only if NOT manual override
  if (!manualSurchargeOverride) {
    if (!shouldHaveSurchargeLine) {
      if (surchargeIdx >= 0) {
        lines.splice(surchargeIdx, 1);
        db.addLog({
          invoice_id: invoice.Id,
          customer_name: customerName,
          action: 'surcharge_removed',
          detail: `Excluded customer; removed surcharge line.`,
          source
        });
      }
    } else {
      if (surchargeIdx >= 0) {
        const existing = lines[surchargeIdx];
        lines[surchargeIdx] = {
          ...existing,
          Amount: Number(desiredAmount),
          // ✅ no Description modification
          SalesItemLineDetail: {
            ...existing.SalesItemLineDetail,
            ItemRef: { value: String(surchargeItemId) }
          }
        };
        db.addLog({
          invoice_id: invoice.Id,
          customer_name: customerName,
          action: 'surcharge_corrected',
          detail: `Set surcharge to ${desiredAmount}. Subtotal=${subtotal}.`,
          source
        });
      } else {
        lines.push(buildSurchargeLine({ surchargeItemId, amount: desiredAmount }));
        db.addLog({
          invoice_id: invoice.Id,
          customer_name: customerName,
          action: 'surcharge_added',
          detail: `Added surcharge ${desiredAmount}. Subtotal=${subtotal}.`,
          source
        });
      }
    }
  } else {
    db.addLog({
      invoice_id: invoice.Id,
      customer_name: customerName,
      action: 'manual_surcharge_override',
      detail: `Detected manual surcharge override; app will not modify/remove/reposition surcharge line.`,
      source
    });
  }

  // Sort item lines
  const movableIdxs = [];
  const movableLines = [];
  let surchargeLineForBottom = null;

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    if (!isMovableItemLine(line)) continue;

    const itemId = getItemId(line);

    // If manual override, leave surcharge wherever it is (do not reposition)
    if (manualSurchargeOverride && itemId === String(surchargeItemId)) continue;

    // If auto-managed surcharge, pull it out and put it at bottom later
    if (!manualSurchargeOverride && itemId === String(surchargeItemId)) {
      surchargeLineForBottom = line;
      continue;
    }

    movableIdxs.push(i);
    movableLines.push(line);
  }

  const itemIds = [...new Set(movableLines.map(l => getItemId(l)).filter(Boolean))];
  const itemMap = await qboBatchReadItems(oauthClient, realmId, itemIds);

  const categorySort = new Map();
  for (const c of db.listCategoriesOrdered()) {
    categorySort.set(String(c.id), Number(c.sort_index));
  }

  function sortKey(line) {
    const itemId = getItemId(line);
    const item = itemMap.get(String(itemId));
    const itemName =
      line?.SalesItemLineDetail?.ItemRef?.name ||
      item?.Name ||
      '';

    const catId = item?.ParentRef?.value ? String(item.ParentRef.value) : null;
    if (catId && categorySort.has(catId)) {
      return { catIdx: categorySort.get(catId), itemName: itemName.toLowerCase() };
    }

    const prefixNum = extractPrefixNumber(itemName);
    if (prefixNum !== null) {
      return { catIdx: prefixNum * 1000, itemName: itemName.toLowerCase() };
    }

    return { catIdx: 999999, itemName: itemName.toLowerCase() };
  }

  const sortedMovable = movableLines.slice().sort((a, b) => {
    const ka = sortKey(a);
    const kb = sortKey(b);
    if (ka.catIdx !== kb.catIdx) return ka.catIdx - kb.catIdx;
    if (ka.itemName < kb.itemName) return -1;
    if (ka.itemName > kb.itemName) return 1;
    return 0;
  });

  let cursor = 0;
  for (const idx of movableIdxs) {
    lines[idx] = sortedMovable[cursor++];
  }

  // Force surcharge to bottom only if auto-managed (not manual override)
  if (!manualSurchargeOverride && shouldHaveSurchargeLine) {
    lines = lines.filter(l => !(isMovableItemLine(l) && getItemId(l) === String(surchargeItemId)));
    if (surchargeLineForBottom) lines.push(surchargeLineForBottom);
  }

  // Lock invoice after first processing
  const nextPrivateNote = ensureAutoMarkerInPrivateNote(invoice.PrivateNote);

  const linesChanged = JSON.stringify(originalLines) !== JSON.stringify(lines);
  const noteChanged = (invoice.PrivateNote || '').toString() !== nextPrivateNote;

  if (!linesChanged && !noteChanged) {
    db.markProcessed(invoice.Id, invoice.SyncToken);
    return { invoiceId: invoice.Id, status: 'noop', reason: 'no changes needed' };
  }

  const payload = {
    Id: invoice.Id,
    SyncToken: invoice.SyncToken,
    sparse: true,
    Line: lines,
    PrivateNote: nextPrivateNote
  };

  await qboUpdateInvoice(oauthClient, realmId, payload);

  db.markProcessed(invoice.Id, invoice.SyncToken);
  db.addLog({
    invoice_id: invoice.Id,
    customer_name: customerName,
    action: 'invoice_updated',
    detail: `Processed + locked invoice with ${AUTO_MARKER}.`,
    source
  });

  return {
    invoiceId: invoice.Id,
    status: 'updated',
    customerName,
    subtotal,
    desiredAmount,
    ruleApplied: rule.rule_type,
    locked: true,
    manualSurchargeOverride
  };
}
