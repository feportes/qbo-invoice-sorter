import { db } from './db.js';

function todayInTZ(tz = 'America/Los_Angeles') {
  const dtf = new Intl.DateTimeFormat('en-CA', {
    timeZone: tz, year: 'numeric', month: '2-digit', day: '2-digit'
  });
  return dtf.format(new Date());
}

function num(x) {
  const n = Number(x);
  return Number.isFinite(n) ? n : 0;
}

function getThresholdForSku(sku) {
  const override = (sku.pallet_pick_threshold === null || typeof sku.pallet_pick_threshold === 'undefined')
    ? null
    : Number(sku.pallet_pick_threshold);
  if (override !== null && Number.isFinite(override)) return override;
  const global = Number(db.getSetting('default_pallet_pick_threshold') || 0.80);
  return Number.isFinite(global) ? global : 0.80;
}

export function buildPlanFromInvoice(invoice) {
  const tz = db.getSetting('inventory_timezone') || 'America/Los_Angeles';
  const today = todayInTZ(tz);
  const txnDate = String(invoice?.TxnDate || '');

  const eligible = txnDate <= today; // allow today or past, block future

  const lines = invoice?.Line || [];
  const items = [];

  for (const line of lines) {
    if (line?.DetailType !== 'SalesItemLineDetail') continue;
    const det = line?.SalesItemLineDetail;
    const qboItemId = det?.ItemRef?.value;
    if (!qboItemId) continue;
    const qtyRequested = num(det?.Qty);
    if (qtyRequested <= 0) continue;

    const sku = db.getSkuByQboItemId(qboItemId);
    if (!sku || !sku.active) {
      items.push({
        qboItemId,
        qboName: det?.ItemRef?.name || null,
        qtyRequested,
        skipped: true,
        reason: 'SKU not mapped or inactive'
      });
      continue;
    }

    const threshold = getThresholdForSku(sku);
    const unitsPerPallet = db.getDefaultUnitsPerPalletForSku(sku.id);
    const palletMode = (unitsPerPallet && unitsPerPallet > 0)
      ? ((qtyRequested / unitsPerPallet) >= threshold)
      : false;

    // Build pick list
    let remaining = qtyRequested;
    const picks = [];

    const takeWalkin = () => {
      const lots = db.listWalkinLotsForSku(sku.id);
      for (const l of lots) {
        if (remaining <= 0) break;
        const avail = Number(l.qty_units || 0);
        if (avail <= 0) continue;
        const take = Math.min(remaining, avail);
        picks.push({
          source: 'WALKIN',
          location: 'WALKIN',
          lot_id: l.lot_id ?? null,
          qty: take
        });
        remaining -= take;
      }
    };

    const takePallets = () => {
      const pallets = db.listPalletsForSkuDoorFirst(sku.id);
      for (const p of pallets) {
        if (remaining <= 0) break;
        const avail = Number(p.qty_units || 0);
        if (avail <= 0) continue;
        const take = Math.min(remaining, avail);
        picks.push({
          source: 'PALLET',
          pallet_id: p.id,
          location: p.location_code,
          lot_id: p.lot_id ?? null,
          qty: take
        });
        remaining -= take;
      }
    };

    // Pallet mode => container first; otherwise walk-in first
    if (palletMode) {
      takePallets();
      takeWalkin();
    } else {
      takeWalkin();
      takePallets();
    }

    items.push({
      sku_id: sku.id,
      sku_name: sku.name,
      unit_type: sku.unit_type,
      qboItemId,
      qboName: det?.ItemRef?.name || sku.name,
      qtyRequested,
      threshold,
      unitsPerPallet,
      palletMode,
      remainingUnfilled: remaining,
      picks
    });
  }

  return {
    invoiceId: String(invoice?.Id || ''),
    docNumber: invoice?.DocNumber || null,
    customerName: invoice?.CustomerRef?.name || null,
    txnDate,
    today,
    eligibleToApply: eligible,
    items
  };
}

export function applyPlan(plan) {
  if (!plan.eligibleToApply) {
    throw new Error(`Invoice TxnDate ${plan.txnDate} is in the future (today is ${plan.today}). Allocation blocked.`);
  }

  const s = db.sqlite;
  const walkin = db.getWalkinLocation();
  if (!walkin) throw new Error('WALKIN location not found in locations table.');

  const tx = s.transaction(() => {
    for (const it of plan.items) {
      if (it.skipped) continue;
      for (const p of it.picks) {
        if (p.qty <= 0) continue;

        if (p.source === 'WALKIN') {
          // decrement loose inventory
          const row = s.prepare(`
            SELECT * FROM loose_inventory
            WHERE sku_id=? AND COALESCE(lot_id,0)=COALESCE(?,0) AND location_id=?
          `).get(it.sku_id, p.lot_id ?? null, walkin.id);

          if (!row || Number(row.qty_units) < p.qty - 1e-9) {
            throw new Error(`Walk-in short for ${it.sku_name}. Need ${p.qty}.`);
          }

          s.prepare(`UPDATE loose_inventory SET qty_units = qty_units - ?, updated_at=CURRENT_TIMESTAMP WHERE id=?`)
            .run(p.qty, row.id);

          db.addInvoiceAllocation({
            qbo_invoice_id: plan.invoiceId,
            sku_id: it.sku_id,
            lot_id: p.lot_id ?? null,
            source_type: 'WALKIN',
            source_location_code: 'WALKIN',
            source_pallet_id: null,
            qty_units: p.qty
          });
        } else {
          // decrement pallet
          const pallet = s.prepare(`SELECT * FROM pallets WHERE id=?`).get(p.pallet_id);
          if (!pallet || Number(pallet.qty_units) < p.qty - 1e-9) {
            throw new Error(`Pallet short for ${it.sku_name}. Pallet ${p.pallet_id} need ${p.qty}.`);
          }
          const newQty = Number(pallet.qty_units) - p.qty;
          const newStatus = newQty <= 0 ? 'DEPLETED' : 'OPEN';
          s.prepare(`UPDATE pallets SET qty_units=?, status=? WHERE id=?`)
            .run(newQty, newStatus, p.pallet_id);

          db.addInvoiceAllocation({
            qbo_invoice_id: plan.invoiceId,
            sku_id: it.sku_id,
            lot_id: pallet.lot_id ?? null,
            source_type: 'PALLET',
            source_location_code: p.location,
            source_pallet_id: p.pallet_id,
            qty_units: p.qty
          });
        }
      }
    }
  });

  tx();
}
