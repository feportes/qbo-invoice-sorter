import {randomBytes,randomUUID,scrypt as scryptCallback,timingSafeEqual,createHash} from 'node:crypto';
import {promisify} from 'node:util';
const scrypt=promisify(scryptCallback);
export const roles=new Set(['admin','manager','office','warehouse','driver']);
export const functionNames=['deliveries','warehouse','receiving','planning','reports','products','vendors'];
export const defaultFunctions=role=>['admin','manager','office'].includes(role)?[...functionNames]:role==='warehouse'?['warehouse','receiving','products']:['deliveries'];
export const digest=value=>createHash('sha256').update(value).digest('hex');
export const problem=(message,status=400)=>{throw Object.assign(new Error(message),{status});};
export const equal=(a,b)=>{const x=Buffer.from(String(a)),y=Buffer.from(String(b));return x.length===y.length&&timingSafeEqual(x,y);};
const now=()=>new Date().toISOString();
export function audit(sql,user,action,entityType,entityId,detail={}) {
  sql.prepare('INSERT INTO wh_audit VALUES (?,?,?,?,?,?,?)').run(randomUUID(),now(),user.id,action,entityType,String(entityId),JSON.stringify(detail));
}
export function safeUser(user){return {id:user.id,username:user.username,displayName:user.display_name,role:user.role,teamId:user.team_id,enabled:!!user.enabled,functions:JSON.parse(user.functions||'[]'),mustChangePassword:!!user.must_change_password};}
function selectedFunctions(input,role){const f=input??defaultFunctions(role);if(!Array.isArray(f)||f.some(x=>!functionNames.includes(x)))problem('Choose valid work functions.');return role==='admin'?[...functionNames]:[...new Set(f)];}
export async function hashPassword(password){
  if(typeof password!=='string'||password.length<12||password.length>200)problem('Use a password between 12 and 200 characters.');
  const salt=randomBytes(16).toString('hex');
  return 'scrypt:'+salt+':'+Buffer.from(await scrypt(password,salt,64)).toString('hex');
}
async function matches(password,encoded){
  const [method,salt,expected]=String(encoded).split(':');if(method!=='scrypt'||!salt||!expected)return false;
  const actual=Buffer.from(await scrypt(String(password),salt,64)).toString('hex');return equal(actual,expected);
}
export async function addUser(sql,actor,input){
  permit(actor,['admin']);
  const username=String(input.username||'').trim().toLowerCase(),displayName=String(input.displayName||'').trim(),role=input.role;
  if(!/^[a-z0-9][a-z0-9_.@+-]{2,99}$/.test(username)||!displayName||displayName.length>100||!roles.has(role))problem('Review username, display name and role.');
  const teamId=String(input.teamId||'').trim();if(role==='driver'&&(!teamId||teamId.length>100))problem('Link the driver to a route-team identity.');
  const enabled=input.enabled??true;if(typeof enabled!=='boolean'||teamId.length>100)problem('Review account details.');
  const passwordHash=await hashPassword(input.password),id=randomUUID(),at=now();
  return sql.transaction(()=>{
    if(sql.prepare('SELECT id FROM wh_users WHERE username=?').get(username))problem('That username is already in use.',409);
    sql.prepare('INSERT INTO wh_users (id,username,display_name,password_hash,role,team_id,enabled,created_at,updated_at,functions,must_change_password) VALUES (?,?,?,?,?,?,?,?,?,?,1)').run(id,username,displayName,passwordHash,role,teamId||null,enabled?1:0,at,at,JSON.stringify(selectedFunctions(input.functions,role)));
    const user=sql.prepare('SELECT * FROM wh_users WHERE id=?').get(id);audit(sql,actor||user,'user-created','user',id,{username,role,teamId:teamId||null});return safeUser(user);
  })();
}
export async function bootstrap(sql,env,input){
  const secret=env.WAREHOUSE_BOOTSTRAP_TOKEN;
  if(!secret||secret.length<32||!equal(secret,input.token||''))problem('Setup is not available.',403);
  if(sql.prepare('SELECT id FROM wh_users LIMIT 1').get())problem('The first administrator is already configured.',409);
  const passwordHash=await hashPassword(input.password),username=String(input.username||'').trim().toLowerCase(),name=String(input.displayName||'').trim();
  if(!/^[a-z0-9][a-z0-9_.@+-]{2,99}$/.test(username)||!name||name.length>100)problem('Review administrator name and username.');
  return sql.transaction(()=>{
    if(sql.prepare('SELECT id FROM wh_users LIMIT 1').get())problem('The first administrator is already configured.',409);
    const id=randomUUID(),at=now();sql.prepare('INSERT INTO wh_users (id,username,display_name,password_hash,role,team_id,enabled,created_at,updated_at,functions,must_change_password) VALUES (?,?,?,?,?,?,1,?,?,?,0)').run(id,username,name,passwordHash,'admin',input.teamId||null,at,at,JSON.stringify(defaultFunctions('admin')));
    const user=sql.prepare('SELECT * FROM wh_users WHERE id=?').get(id);audit(sql,user,'administrator-bootstrap','user',id);return safeUser(user);
  })();
}
export async function login(sql,input,address='unknown'){
  const username=String(input.username||'').trim().toLowerCase(),password=input.password;
  if(username.length>100||typeof password!=='string'||password.length>200)problem('Check username and password.',401);
  const key=digest(username+'\0'+address),time=Date.now(),attempt=sql.prepare('SELECT * FROM wh_login_attempts WHERE key=?').get(key);
  if(attempt&&attempt.reset_at>time&&attempt.failures>=8)problem('Too many attempts. Try again in 15 minutes.',429);
  const user=sql.prepare('SELECT * FROM wh_users WHERE username=?').get(username);
  // Unknown and disabled accounts still perform password work.
  const encoded=user?.password_hash||'scrypt:00000000000000000000000000000000:'+ '0'.repeat(128);
  const valid=await matches(password,encoded);
  if(!valid||!user?.enabled){
    sql.prepare('INSERT INTO wh_login_attempts VALUES (?,?,?) ON CONFLICT(key) DO UPDATE SET failures=CASE WHEN reset_at>? THEN failures+1 ELSE 1 END,reset_at=CASE WHEN reset_at>? THEN reset_at ELSE ? END').run(key,1,time+900000,time,time,time+900000);
    problem('Check username and password.',401);
  }
  return sql.transaction(()=>{
    sql.prepare('DELETE FROM wh_login_attempts WHERE key=?').run(key);
    sql.prepare('DELETE FROM wh_sessions WHERE expires_at<?').run(now());
    const token=randomBytes(32).toString('base64url'),csrf=randomBytes(24).toString('base64url'),at=now(),expires=new Date(time+14*86400000).toISOString();
    sql.prepare('INSERT INTO wh_sessions VALUES (?,?,?,?,?,?)').run(digest(token),user.id,csrf,String(input.deviceLabel||'Warehouse device').slice(0,100),at,expires);
    audit(sql,user,'signed-in','user',user.id,{deviceLabel:String(input.deviceLabel||'Warehouse device').slice(0,100)});
    return {token,csrf,user:safeUser(user),expiresAt:expires};
  })();
}
export function authenticate(sql,request){
  const header=String(request.headers.authorization||''),bearer=/^Bearer ([A-Za-z0-9_-]{43})$/.exec(header)?.[1];
  const cookies=String(request.headers.cookie||'').split(';').map(x=>x.trim()),cookie=cookies.find(x=>x.startsWith('__Host-wh_session='))?.slice(18);
  const token=bearer||cookie;if(!token)problem('Sign in to continue.',401);
  const row=sql.prepare('SELECT s.*,u.username,u.display_name,u.role,u.team_id,u.enabled,u.functions,u.must_change_password FROM wh_sessions s JOIN wh_users u ON u.id=s.user_id WHERE token_hash=?').get(digest(token));
  if(!row||!row.enabled||row.expires_at<=now())problem('Sign in to continue.',401);
  if(!bearer&&!['GET','HEAD'].includes(request.method)&&!equal(row.csrf,request.headers['x-warehouse-csrf']||''))problem('Refresh this page before saving.',403);
  return {...safeUser({...row,id:row.user_id}),sessionHash:row.token_hash,csrf:row.csrf,expiresAt:row.expires_at};
}
export function permit(user,allowed,feature){if(user.mustChangePassword)problem('Change your temporary password before starting work.',403);if(user.role==='admin')return;if(feature&&!user.functions?.includes(feature))problem('Your account does not have this work function.',403);const extraWorkFunction=feature&&(allowed.includes('warehouse')||allowed.includes('driver'))&&user.functions?.includes(feature);if(!allowed.includes(user.role)&&!(user.role==='manager'&&allowed.includes('office'))&&!extraWorkFunction)problem('Your account cannot perform this action.',403);}
export function disableUser(sql,actor,id){permit(actor,['admin']);if(id===actor.id)problem('Use another administrator to disable your own account.');return sql.transaction(()=>{const old=sql.prepare('SELECT * FROM wh_users WHERE id=?').get(id);if(!old)problem('User not found.',404);sql.prepare('UPDATE wh_users SET enabled=0,updated_at=? WHERE id=?').run(now(),id);sql.prepare('DELETE FROM wh_sessions WHERE user_id=?').run(id);audit(sql,actor,'user-disabled','user',id);return {disabled:true};})();}
export async function changePassword(sql,actor,input){const user=sql.prepare('SELECT * FROM wh_users WHERE id=?').get(actor.id);if(!user||!await matches(input.currentPassword,user.password_hash))problem('Check your current password.',403);const hash=await hashPassword(input.newPassword);return sql.transaction(()=>{sql.prepare('UPDATE wh_users SET password_hash=?,must_change_password=0,updated_at=? WHERE id=?').run(hash,now(),actor.id);sql.prepare('DELETE FROM wh_sessions WHERE user_id=? AND token_hash<>?').run(actor.id,actor.sessionHash);audit(sql,actor,'own-password-changed','user',actor.id);return {user:safeUser(sql.prepare('SELECT * FROM wh_users WHERE id=?').get(actor.id))};})();}
export async function updateUser(sql,actor,id,input){
  permit(actor,['admin']);const old=sql.prepare('SELECT * FROM wh_users WHERE id=?').get(id);if(!old)problem('User not found.',404);
  const username=String(input.username??old.username).trim().toLowerCase(),role=input.role??old.role,enabled=input.enabled===undefined?!!old.enabled:input.enabled,teamId=String(input.teamId??old.team_id??'').trim(),displayName=String(input.displayName??old.display_name).trim();
  if(!roles.has(role)||!username.match(/^[a-z0-9][a-z0-9_.@+-]{2,99}$/)||typeof enabled!=='boolean'||!displayName||displayName.length>100||teamId.length>100)problem('Review account details.');
  if(id===actor.id&&(role!=='admin'||!enabled))problem('Keep your main administrator account enabled with full access.');
  if(role==='driver'&&!teamId)problem('Link the driver to a route-team identity.');
  const functions=selectedFunctions(input.functions??JSON.parse(old.functions),role),passwordHash=input.newPassword?await hashPassword(input.newPassword):old.password_hash;
  return sql.transaction(()=>{if(sql.prepare('SELECT id FROM wh_users WHERE username=? AND id<>?').get(username,id))problem('Username is already in use.',409);sql.prepare('UPDATE wh_users SET username=?,display_name=?,password_hash=?,role=?,team_id=?,enabled=?,updated_at=?,functions=?,must_change_password=? WHERE id=?').run(username,displayName,passwordHash,role,teamId||null,enabled?1:0,now(),JSON.stringify(functions),input.newPassword?1:old.must_change_password,id);sql.prepare('DELETE FROM wh_sessions WHERE user_id=?').run(id);audit(sql,actor,'user-access-updated','user',id,{username,role,teamId,enabled,functions,passwordReset:!!input.newPassword});return {user:safeUser(sql.prepare('SELECT * FROM wh_users WHERE id=?').get(id)),signInAgain:id===actor.id};})();
}
