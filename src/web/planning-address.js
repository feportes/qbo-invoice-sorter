export function planningStreetAddress(value){
 const text=String(value||'').replace(/\s*\n\s*/g,', ').replace(/\b([A-Z]{2})\s+(\d{5})(\d{4})\b/gi,'$1 $2-$3');
 if(/\bP\.?\s*O\.?\s*Box\b/i.test(text))throw Error('A street delivery address is needed; a PO box cannot be mapped');
 const parts=text.split(/,\s*/),street=parts.find(p=>/^\s*\d+[A-Z-]*\s+(?:[A-Z]|\d+(?:st|nd|rd|th)\b)/i.test(p));let start=street?text.indexOf(street):-1;
 if(start<0){const match=/\b\d{1,6}\s+(?:[A-Z]|\d+(?:st|nd|rd|th)\b)/i.exec(text);if(match)start=match.index;}
 const rest=start<0?'':text.slice(start),zip=/\b[A-Z]{2}\s+\d{5}(?:-\d{4})?\b/i.exec(rest);
 if(!zip)throw Error('A full street address, state and ZIP code are needed');
 return rest.slice(0,zip.index+zip[0].length).trim();
}
