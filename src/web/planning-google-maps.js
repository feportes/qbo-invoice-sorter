let googleReady;
export function googleMaps(key){if(window.google?.maps)return Promise.resolve(window.google.maps);if(googleReady)return googleReady;googleReady=new Promise((resolve,reject)=>{const script=document.createElement('script');script.src='https://maps.googleapis.com/maps/api/js?key='+encodeURIComponent(key)+'&loading=async&callback=drawPlanningGoogleMapsReady';window.drawPlanningGoogleMapsReady=()=>resolve(window.google.maps);script.onerror=()=>{googleReady=null;reject(Error('Satellite imagery could not load.'));};document.head.append(script);});return googleReady;}

export async function locatePlanningAddress(address,key){
 const G=await googleMaps(key),library=G.importLibrary?await G.importLibrary('geocoding'):G;
 let response;try{response=await new library.Geocoder().geocode({address,region:'us'});}catch(e){if(e.code==='ZERO_RESULTS')return {status:'not-found',message:'No street-address match'};throw Error('Google address lookup failed. Check the Geocoding API setup or retry.');}
 const matches=(response.results||[]).filter(r=>!r.partial_match&&r.geometry?.location&&['ROOFTOP','RANGE_INTERPOLATED'].includes(r.geometry.location_type));
 if(matches.length!==1)return {status:'review',message:matches.length?'More than one address match; review the address':'No exact street-address match; review the address'};
 const r=matches[0];return {status:'located',lat:r.geometry.location.lat(),lng:r.geometry.location.lng(),matchedAddress:r.formatted_address,locationType:r.geometry.location_type};
}
