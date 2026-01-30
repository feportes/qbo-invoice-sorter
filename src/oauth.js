import OAuthClient from 'intuit-oauth';
import { db } from './db.js';

export function getOAuthClient(conn) {
  const oauthClient = new OAuthClient({
    clientId: process.env.INTUIT_CLIENT_ID,
    clientSecret: process.env.INTUIT_CLIENT_SECRET,
    environment: 'production', // use 'sandbox' if you are testing in sandbox
    redirectUri: process.env.INTUIT_REDIRECT_URI
  });

  // Inject token
  oauthClient.setToken({
    token_type: 'bearer',
    access_token: conn.access_token,
    refresh_token: conn.refresh_token,
    expires_in: Math.max(1, Math.floor((conn.expires_at - Date.now()) / 1000)),
    x_refresh_token_expires_in: conn.refresh_expires_at ? Math.max(1, Math.floor((conn.refresh_expires_at - Date.now()) / 1000)) : undefined
  });

  return oauthClient;
}

async function refreshIfNeeded(oauthClient, conn) {
  const now = Date.now();
  // refresh 2 minutes before expiry
  if (conn.expires_at - now > 2 * 60 * 1000) return { oauthClient, conn };

  const token = await oauthClient.refresh();
  const t = token.getJson();

  db.upsertConnection({
    realm_id: conn.realm_id,
    company_name: conn.company_name,
    access_token: t.access_token,
    refresh_token: t.refresh_token || conn.refresh_token,
    expires_at: Date.now() + (t.expires_in * 1000),
    refresh_expires_at: t.x_refresh_token_expires_in ? Date.now() + (t.x_refresh_token_expires_in * 1000) : conn.refresh_expires_at
  });

  const updated = db.getConnectionOrThrow();
  return { oauthClient: getOAuthClient(updated), conn: updated };
}

export function requireConnected(req, res, next) {
  const conn = db.getConnection();
  if (!conn) return res.redirect('/');
  next();
}

export async function authStart(req, res) {
  const oauthClient = new OAuthClient({
    clientId: process.env.INTUIT_CLIENT_ID,
    clientSecret: process.env.INTUIT_CLIENT_SECRET,
    environment: 'production',
    redirectUri: process.env.INTUIT_REDIRECT_URI
  });

  const authUri = oauthClient.authorizeUri({
    scope: [OAuthClient.scopes.Accounting],
    state: 'qbo-sorter'
  });

  res.redirect(authUri);
}

export async function authCallback(req, res) {
  const oauthClient = new OAuthClient({
    clientId: process.env.INTUIT_CLIENT_ID,
    clientSecret: process.env.INTUIT_CLIENT_SECRET,
    environment: 'production',
    redirectUri: process.env.INTUIT_REDIRECT_URI
  });

  try {
    const tokenResp = await oauthClient.createToken(req.url);
    const t = tokenResp.getJson();
   // HARD-FIX: force Trans-Portes, Inc. realm (Intuit sandbox lock workaround)
const realmId = req.query.realmId;



    // Try to fetch company name (optional)
    let companyName = null;
    try {
      oauthClient.setToken(t);
      // companyinfo: /companyinfo/{realmId}/companyinfo/{realmId}
      const url = `https://quickbooks.api.intuit.com/v3/company/${realmId}/companyinfo/${realmId}`;
      const resp = await oauthClient.makeApiCall({ url });
const json = (typeof resp?.getJson === 'function')
  ? resp.getJson()
  : (resp?.json ?? (resp?.body ? JSON.parse(resp.body) : null));

companyName = json?.CompanyInfo?.CompanyName || null;

    } catch {}

    db.upsertConnection({
      realm_id: String(realmId),
      company_name: companyName,
      access_token: t.access_token,
      refresh_token: t.refresh_token,
      expires_at: Date.now() + (t.expires_in * 1000),
      refresh_expires_at: t.x_refresh_token_expires_in ? Date.now() + (t.x_refresh_token_expires_in * 1000) : null
    });

    res.redirect('/admin/sync');
  } catch (e) {
    res.status(500).send(`OAuth error: ${e?.message || e}`);
  }
}

// Helper for other modules
export async function withFreshClient() {
  const conn = db.getConnectionOrThrow();
  let oauthClient = getOAuthClient(conn);
  const refreshed = await refreshIfNeeded(oauthClient, conn);
  return refreshed;
}
