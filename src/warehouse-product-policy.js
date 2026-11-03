/** Physical supplies may use a Service item in QuickBooks without being a fee. */
export function physicalServiceSupply(p){const name=String(p.name||p.Name||p.fullName||p.FullyQualifiedName||'').split(':').pop();return /^sip\s*n\s*sea\b/i.test(name)&&/\b(cups?|lids?|bowls?|containers?)\b/i.test(name);}
export function pickableProduct(p){return !p.archived&&p.active!==false&&p.Active!==false&&(String(p.type||p.Type||'').toUpperCase()!=='SERVICE'||physicalServiceSupply(p));}
