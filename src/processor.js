import { db } from './db.js';
import { qboReadInvoice, qboUpdateInvoice, qboBatchReadItems, qboReadCustomer, qboReadItemByName } from './qbo.js';

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
    .sort((a,b) => (b.prefix.length - a.prefix.length)); // longest prefix wins
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
      // Taxability: rely on the Item being taxable in QBO (recommended).
      // You can also set TaxCodeRef here if your company uses consistent codes.
    }
  };
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

  // 2) Apply surcharge rule
  const subtotal = computeSubtotalIgnoreDiscounts(originalLines, surchargeItemId);

  let desiredAmount = Number(db.getSetting('default_surcharge_amount') || 15);
  let shouldHaveSurchargeLine = true;

  if (rule.rule_type === 'exclude') {
    shouldHaveSurchargeLine = false; // remove/never add
  } else if (rule.rule_type === 'always_0') {
    desiredAmount = 0;
  } else if (rule.rule_type === 'always_15') {
    desiredAmount = Number(rule.amount ?? desiredAmount);
  } else if (rule.rule_type === 'conditional') {
    const threshold = Number(rule.threshold ?? 0);
    const amount = Number(rule.amount ?? desiredAmount);
    desiredAmount = subtotal >= threshold ? 0 : amount;
  } else {
    // default
    desiredAmount = Number(rule.amount ?? desiredAmount);
  }

  let lines = [...originalLines];

  const surchargeIdx = findSurchargeLineIndex(lines, surchargeItemId);

  if (!shouldHaveSurchargeLine) {
    if (surchargeIdx >= 0) {
      lines.splice(surchargeIdx, 1);
      db.addLog({ invoice_id: invoice.Id, customer_name: customerName, action: 'surcharge_removed', detail: `Excluded customer; removed surcharge line.`, source });
    }
  } else {
    if (surchargeIdx >= 0) {
      // correct amount
      lines[surchargeIdx] = {
        ...lines[surchargeIdx],
        Amount: Number(desiredAmount),
        SalesItemLineDetail: {
          ...lines[surchargeIdx].SalesItemLineDetail,
          ItemRef: { value: String(surchargeItemId) }
        }
      };
      db.addLog({ invoice_id: invoice.Id, customer_name: customerName, action: 'surcharge_corrected', detail: `Set surcharge to ${desiredAmount}. Subtotal=${subtotal}.`, source });
    } else {
      // add line (even if amount 0, per your preference)
      lines.push(buildSurchargeLine({ surchargeItemId, amount: desiredAmount }));
      db.addLog({ invoice_id: invoice.Id, customer_name: customerName, action: 'surcharge_added', detail: `Added surcharge ${desiredAmount}. Subtotal=${subtotal}.`, source });
    }
  }

  // 3) Sort item lines by category order (excluding surcharge)
  // Movable product lines (excluding surcharge) are what we sort.
  const movableIdxs = [];
  const movableLines = [];
  let surchargeLine = null;

  for (let i=0; i<lines.length; i++) {
    const line = lines[i];
    if (!isMovableItemLine(line)) continue;

    const itemId = getItemId(line);
    if (shouldHaveSurchargeLine && itemId === String(surchargeItemId)) {
      surchargeLine = line; // we will place it at the bottom later
      continue;
    }
    movableIdxs.push(i);
    movableLines.push(line);
  }

  // Fetch Items for category mapping
  const itemIds = [...new Set(movableLines.map(l => getItemId(l)).filter(Boolean))];
  const itemMap = await qboBatchReadItems(oauthClient, realmId, itemIds);

  // Build category sort index mapping
  const categorySort = new Map();
  for (const c of db.listCategoriesOrdered()) {
    categorySort.set(String(c.id), Number(c.sort_index));
  }

  function sortKey(line) {
    const itemId = getItemId(line);
    const item = itemMap.get(String(itemId));
    const catId = item?.ParentRef?.value ? String(item.ParentRef.value) : null;
    const catIdx = catId && categorySort.has(catId) ? categorySort.get(catId) : 999999;
    const itemName = line?.SalesItemLineDetail?.ItemRef?.name || item?.Name || '';
    return { catIdx, itemName: itemName.toLowerCase() };
  }

  const sortedMovable = movableLines.slice().sort((a,b) => {
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

  // 4) Force surcharge to bottom (below last item)
  // If present, remove it from wherever and push to end
  if (shouldHaveSurchargeLine) {
    lines = lines.filter(l => !(isMovableItemLine(l) && getItemId(l) === String(surchargeItemId)));
    // Re-add last
    if (surchargeLine) lines.push(surchargeLine);
    else {
      // should exist by now
      lines.push(buildSurchargeLine({ surchargeItemId, amount: desiredAmount }));
    }
  }

  // Check if anything changed (simple compare by JSON string of lines + surcharge amount)
  const changed = JSON.stringify(originalLines) !== JSON.stringify(lines);

  if (!changed) {
    db.markProcessed(invoice.Id, invoice.SyncToken);
    return { invoiceId: invoice.Id, status: 'noop', reason: 'no changes needed' };
  }

  // 5) Update invoice
  const payload = {
    Id: invoice.Id,
    SyncToken: invoice.SyncToken,
    sparse: true,
    Line: lines
  };

  const upd = await qboUpdateInvoice(oauthClient, realmId, payload);

  db.markProcessed(invoice.Id, invoice.SyncToken);
  db.addLog({ invoice_id: invoice.Id, customer_name: customerName, action: 'invoice_updated', detail: `Sorted lines + surcharge processed.`, source });

  return { invoiceId: invoice.Id, status: 'updated', customerName, subtotal, desiredAmount, ruleApplied: rule.rule_type };
}
