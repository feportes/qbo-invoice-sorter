// Display metadata only. Preserve quantity, position and concurrency fingerprints.
export function decoratePalletLayout(sql,layout){
 const copy=structuredClone(layout),pallets=[...(copy.walkin||[]),...(copy.loose||[]),...(copy.returns||[])];
 for(const c of copy.yard||[])pallets.push(...[...(c.left||[]),...(c.right||[])].map(s=>s.pallet).filter(Boolean),...(c.unmappedPallets||[]));
 if(!pallets.length)return copy;
 const categories=new Map(sql.prepare('SELECT s.id,c.name AS category FROM skus s LEFT JOIN categories c ON c.id=s.qbo_category_id').all().map(i=>[String(i.id),i.category||'']));
 for(const p of pallets)for(const item of p.items||[p])item.category=categories.get(String(item.sku_id))||item.category||'';
 return copy;
}
