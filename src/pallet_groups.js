// Each product remains its own stock/lot row; a group occupies one physical slot.
export function copyFingerprint(items) {
  return JSON.stringify([...items].sort((a,b)=>a.id-b.id).map(p=>[p.id,p.sku_id,p.lot_id,p.pallet_config_id,p.location_id,p.qty_units]));
}
export function groupPallets(rows) {
  const groups = new Map();
  for (const p of rows) {
    const key = p.pallet_group || `single:${p.id}`;
    if (!groups.has(key)) groups.set(key, { ...p, items: [] });
    groups.get(key).items.push(p);
  }
  return [...groups.values()].map(p=>({ ...p, copy_fingerprint: copyFingerprint(p.items) }));
}
