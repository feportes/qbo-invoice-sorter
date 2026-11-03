let googleReady;
export function googleMaps(key){if(window.google?.maps)return Promise.resolve(window.google.maps);if(googleReady)return googleReady;googleReady=new Promise((resolve,reject)=>{const script=document.createElement('script');script.src='https://maps.googleapis.com/maps/api/js?key='+encodeURIComponent(key)+'&loading=async&callback=drawPlanningGoogleMapsReady';window.drawPlanningGoogleMapsReady=()=>resolve(window.google.maps);script.onerror=()=>{googleReady=null;reject(Error('Satellite imagery could not load.'));};document.head.append(script);});return googleReady;}

export function planningStreetAddress(value){
 const text=String(value||'').replace(/\s*\n\s*/g,', ').replace(/\b([A-Z]{2})\s+(\d{5})(\d{4})\b/gi,'$1 $2-$3');
 if(/\bP\.?\s*O\.?\s*Box\b/i.test(text))throw Error('A street delivery address is needed; a PO box cannot be mapped');
 const parts=text.split(/,\s*/),street=parts.find(p=>/^\s*\d+[A-Z-]*\s+(?:[A-Z]|\d+(?:st|nd|rd|th)\b)/i.test(p));let start=street?text.indexOf(street):-1;
 if(start<0){const match=/\b\d{1,6}\s+(?:[A-Z]|\d+(?:st|nd|rd|th)\b)/i.exec(text);if(match)start=match.index;}
 const rest=start<0?'':text.slice(start),zip=/\b[A-Z]{2}\s+\d{5}(?:-\d{4})?\b/i.exec(rest);
 if(!zip)throw Error('A full street address, state and ZIP code are needed');
 return rest.slice(0,zip.index+zip[0].length).trim();
}
export async function locatePlanningAddress(address,key){
 address=planningStreetAddress(address);const G=await googleMaps(key),library=G.importLibrary?await G.importLibrary('geocoding'):G;
 let response;try{response=await new library.Geocoder().geocode({address,region:'us'});}catch(e){if(e.code==='ZERO_RESULTS')return {status:'not-found',message:'No street-address match'};throw Error('Google address lookup failed. Check the Geocoding API setup or retry.');}
 const matches=(response.results||[]).filter(r=>!r.partial_match&&r.geometry?.location&&['ROOFTOP','RANGE_INTERPOLATED'].includes(r.geometry.location_type));
 if(matches.length!==1){const candidate=(response.results||[]).length===1&&response.results[0],valid=candidate?.geometry?.location&&['ROOFTOP','RANGE_INTERPOLATED'].includes(candidate.geometry.location_type);return {status:'review',message:valid?'Confirm the suggested street address':'No exact street-address match; review the address',...(valid?{candidate:{lat:candidate.geometry.location.lat(),lng:candidate.geometry.location.lng(),matchedAddress:candidate.formatted_address,locationType:candidate.geometry.location_type}}:{})};}
 const r=matches[0];return {status:'located',lat:r.geometry.location.lat(),lng:r.geometry.location.lng(),matchedAddress:r.formatted_address,locationType:r.geometry.location_type};
}
