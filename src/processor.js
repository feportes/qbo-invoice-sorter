import { db } from './db.js';
import {
  qboReadInvoice,
  qboUpdateInvoice,
  qboBatchReadItems,
  qboReadCustomer,
  qboReadItemByName
} from './qbo.js';

const AUTO_MARKER = '[AUTO_SORTED_V1]';
const AUTO_SURCHARGE_TAG = '(AUTO)';

function isMovableItemLine(line) {
  return line?.DetailType === 'SalesItemLineDetail' && line?.SalesItemLineDetail?.ItemRef?.value;
}

function getItemId(line) {
  return line?.SalesItemLineDetail?.ItemRef?.value || null;
}

function normalize(s) {
  return (s || '').trim().toLowerCase();
}

function findRuleForCustomer({ customerId, customerName }) {
  const rules = db.listRules().filter(r => r.enabled === 1);

  // 1) exact match by customer_id
  const exact = rules.find(r => r.match_type === 'exact' && r.customer_id && r.customer_id === String(customerId));
  if (exact) return exact;

  // 2) prefix match on name
  const name = customerName || '';
  const lower = normalize(name);
  const prefixRules = rules
    .filter(r => r.match_type === 'prefix' && r.prefix)
    .sort((a, b) => (b.prefix.length - a.prefix.length)); // longest prefix wins

  for (const r of prefixRules) {
    if (lower.startsWith(normalize(r.prefix))) return r;
  }

  // 3) default
  return { rule_type: 'always_15', amount: Number(db.getSetting('default_surcharge_amount') || 15), threshold: null };
}

function computeSubtotalIgnoreDiscounts(lines, surchargeItemId) {
  let sum = 0;
  for (const line of lines) {
    if (!isMovableItemLine(line)) continue;
    const itemId = getItemId(line);
    if (surchargeItemId && itemId === String(surchargeItemId)) continue; // threshold is before surcharge
    const amt = Number(line.Amount || 0);
    sum += amt;
  }
  return sum;
}

function findSurchargeLineIndex(lines, surchargeItemId) {
  if (!surchargeItemId) return -1;
  return lines.findIndex(l => isMovableItemLine(l) && getItemId(l) === String(surchargeItemId));
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
  // Only skip on webhooks. Manual processing can still re-run if you want.
  if (source !== 'webhook') return false;
  const note = (invoice?.PrivateNote || '').toString();
  return note.includes(AUTO_MARKER);
}

/**
 * Extract a category order number from an item name like:
 * "01. Sorbets: ..." or "5 - Beverage: ..." etc.
 */
function extractPrefixNumber(name) {
  const n = (name || '').trim();
  // Matches: "01.", "1.", "05 -", "5 -", "05:", etc
  const m = n.match(/^(\d{1,2})\s*[.\-:]/);
  return m ? Number(m[1]) : null;
}

export async function processInvoice({ oauthClient, realmId, invoiceId, source }) {
  // Ensure we have surcharge item id cached
  let surchargeItemId = db.getSetting('surcharge_item_id');
  if (!surchargeItemId) {
    const surchargeItemName = db.getSetting('surcharge_item_name');
    const surcharge = await qboReadItemByName(oauthClient, realmId, surchargeItemName);
    if (!surcharge?.Id) throw new Error(`Could not find Item named "${surchargeItemName}" in QBO.`);
    surchargeItemId = surcharge.Id;
    db.setSetting('surcharge_item_id', surchargeItemId);
  }

  // 1) Read invoice
  const invResp = await qboReadInvoice(oauthClient, realmId, invoiceId);
  const invoice = invResp?.Invoice;
  if (!invoice) throw new Error(`Invoice not found: ${invoiceId}`);

  // LOCK: if already processed + locked, never overwrite later edits (webhooks)
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

  // Idempotency (per SyncToken)
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

  // 2) Determine desired surcharge amount from rules
  const subtotal = computeSubtotalIgnoreDiscounts(originalLines, surchargeItemId);

  let desiredAmount = Number(db.getSetting('default_surcharge_amount') || 15);
  let shouldHaveSurchargeLine = true;

  if (rule.rule_type === 'exclude') {
    shouldHaveSurchargeLine = false; // remove/never add (unless manual override exists)
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

  // Detect existing surcharge line
  const surchargeIdx = findSurchargeLineIndex(lines, surchargeItemId);

  // Manual override logic:
  // If a surcharge line exists and it is NOT tagged (AUTO), treat it as manual override:
  // - do not change its amount
  // - do not remove it
  // - do not force it to the bottom
  const hasSurchargeLine = surchargeIdx >= 0;
  const manualSurchargeOverride = hasSurchargeLine && !lineHasAutoTag(lines[surchargeIdx]);

  // Apply surcharge rule only if NOT manual override
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
        // correct amount on AUTO line
        const existing = lines[surchargeIdx];
        lines[surchargeIdx] = {
          ...existing,
          Amount: Number(desiredAmount),
          Description: existing?.Description || `Operating Cost Surcharge ${AUTO_SURCHARGE_TAG}`,
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
        // add AUTO line (even if amount 0, per your preference)
        lines.push(buildSurchargeLine({ surchargeItemId, amount: desiredAmount, autoTag: true }));
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
      detail: `Detected manual surcharge line; app will not modify/remove/reposition it.`,
      source
    });
  }

  // 3) Sort item lines by category order
  // NOTE: QBO isn't returning ParentRef (catId is null), so we sort by numeric prefix in item name.
  // If a real ParentRef exists and matches your saved order, we will still use it.
  const movableIdxs = [];
  const movableLines = [];
  let autoSurchargeLine = null;

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    if (!isMovableItemLine(line)) continue;

    const itemId = getItemId(line);

    // Exclude AUTO surcharge from sorting and handle placement later
    if (!manualSurchargeOverride && itemId === String(surchargeItemId)) {
      autoSurchargeLine = line;
      continue;
    }

    // Manual override surcharge line stays where it is
    if (manualSurchargeOverride && itemId === String(surchargeItemId)) {
      continue;
    }

    movableIdxs.push(i);
    movableLines.push(line);
  }

  // Fetch Items for mapping
  const itemIds = [...new Set(movableLines.map(l => getItemId(l)).filter(Boolean))];
  const itemMap = await qboBatchReadItems(oauthClient, realmId, itemIds);

  // Build category sort index mapping from your saved order page
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

    // 1) Try real QBO category first (if present)
    const catId = item?.ParentRef?.value ? String(item.ParentRef.value) : null;
    if (catId && categorySort.has(catId)) {
      return { catIdx: categorySort.get(catId), itemName: itemName.toLowerCase() };
    }

    // 2) Fallback: numeric prefix in item name (your system)
    const prefixNum = extractPrefixNumber(itemName);
    if (prefixNum !== null) {
      // multiply to keep room for future in-between ordering
      return { catIdx: prefixNum * 1000, itemName: itemName.toLowerCase() };
    }

    // 3) Last fallback
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

  // Rebuild in-place (keep non-movable lines where they are)
  let cursor = 0;
  for (const idx of movableIdxs) {
    lines[idx] = sortedMovable[cursor++];
  }

  // 4) Force AUTO surcharge to bottom (below last item)
  // Only do this for AUTO surcharge. If manual override, leave its placement untouched.
  if (!manualSurchargeOverride && shouldHaveSurchargeLine) {
    lines = lines.filter(l => !(isMovableItemLine(l) && getItemId(l) === String(surchargeItemId)));
    if (autoSurchargeLine) {
      lines.push(autoSurchargeLine);
    } else {
      // Should exist by now; add as AUTO if missing
      lines.push(buildSurchargeLine({ surchargeItemId, amount: desiredAmount, autoTag: true }));
    }
  }

  // 5) Lock invoice after first processing (prevents overwriting later manual edits)
  const nextPrivateNote = ensureAutoMarkerInPrivateNote(invoice.PrivateNote);

  // Determine if anything changed
  const linesChanged = JSON.stringify(originalLines) !== JSON.stringify(lines);
  const noteChanged = (invoice.PrivateNote || '').toString() !== nextPrivateNote;

  if (!linesChanged && !noteChanged) {
    db.markProcessed(invoice.Id, invoice.SyncToken);
    return { invoiceId: invoice.Id, status: 'noop', reason: 'no changes needed' };
  }

  // 6) Update invoice
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
