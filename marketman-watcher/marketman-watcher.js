/**
 * marketman-watcher.js
 * 
 * Polls orders.sft.2026@gmail.com for MarketMan order emails,
 * scrapes the order details from the View Order link,
 * and forwards a clean plain-text order to Cut+Dry.
 *
 * Setup:
 *   npm install googleapis cheerio node-fetch
 *
 * Usage:
 *   node marketman-watcher.js            ← runs once (good for cron)
 *   node marketman-watcher.js --watch    ← polls every 5 minutes
 */

const { google } = require('googleapis');
const cheerio = require('cheerio');
const fetch = require('node-fetch');
const fs = require('fs');
const path = require('path');

// ─── CONFIG ────────────────────────────────────────────────────────────────
const CONFIG = {
  // Gmail account that receives MarketMan orders
  ORDERS_EMAIL: 'orders.sft.2026@gmail.com',

  // Cut+Dry order desk email
  CUTDRY_EMAIL: 'superfruits-orderdesk@mg.cutanddry.com',

  // Only process emails from MarketMan
  MARKETMAN_SENDER: 'sales@marketman.com',

  // How often to poll in --watch mode (ms)
  POLL_INTERVAL_MS: 5 * 60 * 1000, // 5 minutes

  // File to track last-processed email ID (prevents duplicates)
  STATE_FILE: path.join(__dirname, '.watcher-state.json'),

  // OAuth credentials file (downloaded from Google Cloud Console)
  CREDENTIALS_FILE: path.join(__dirname, 'credentials.json'),

  // OAuth token file (generated on first run)
  TOKEN_FILE: path.join(__dirname, 'token.json'),
};
// ───────────────────────────────────────────────────────────────────────────


// ─── GMAIL AUTH ────────────────────────────────────────────────────────────
const SCOPES = [
  'https://www.googleapis.com/auth/gmail.readonly',
  'https://www.googleapis.com/auth/gmail.send',
  'https://www.googleapis.com/auth/gmail.modify', // to mark as read
];

async function getGmailClient() {
  let credentials;

  // On Render: read from environment variables (base64 encoded)
  if (process.env.CREDENTIALS_JSON_B64) {
    credentials = JSON.parse(Buffer.from(process.env.CREDENTIALS_JSON_B64, 'base64').toString('utf-8'));
  } else if (fs.existsSync(CONFIG.CREDENTIALS_FILE)) {
    credentials = JSON.parse(fs.readFileSync(CONFIG.CREDENTIALS_FILE));
  } else {
    throw new Error('Missing credentials — set CREDENTIALS_JSON_B64 env var or provide credentials.json');
  }

  const { client_secret, client_id, redirect_uris } = credentials.installed || credentials.web;
  const oAuth2Client = new google.auth.OAuth2(client_id, client_secret, redirect_uris[0]);

  // On Render: read token from environment variable (base64 encoded)
  if (process.env.TOKEN_JSON_B64) {
    const token = JSON.parse(Buffer.from(process.env.TOKEN_JSON_B64, 'base64').toString('utf-8'));
    oAuth2Client.setCredentials(token);
  } else if (fs.existsSync(CONFIG.TOKEN_FILE)) {
    const token = JSON.parse(fs.readFileSync(CONFIG.TOKEN_FILE));
    oAuth2Client.setCredentials(token);
  } else {
    const authUrl = oAuth2Client.generateAuthUrl({ access_type: 'offline', scope: SCOPES });
    console.log('\n🔐 Authorize this app by visiting:\n');
    console.log('  ' + authUrl);
    console.log('\nThen run: node marketman-watcher.js --auth <CODE>\n');
    process.exit(0);
  }

  return google.gmail({ version: 'v1', auth: oAuth2Client });
}

// Run once to save the OAuth token
async function saveToken(code) {
  const credentials = JSON.parse(fs.readFileSync(CONFIG.CREDENTIALS_FILE));
  const { client_secret, client_id, redirect_uris } = credentials.installed || credentials.web;
  const oAuth2Client = new google.auth.OAuth2(client_id, client_secret, redirect_uris[0]);
  const { tokens } = await oAuth2Client.getToken(code);
  fs.writeFileSync(CONFIG.TOKEN_FILE, JSON.stringify(tokens));
  console.log('✅ Token saved to', CONFIG.TOKEN_FILE);
}
// ───────────────────────────────────────────────────────────────────────────


// ─── STATE (track processed emails) ────────────────────────────────────────
function loadState() {
  if (fs.existsSync(CONFIG.STATE_FILE)) {
    return JSON.parse(fs.readFileSync(CONFIG.STATE_FILE));
  }
  return { processedIds: [] };
}

function saveState(state) {
  fs.writeFileSync(CONFIG.STATE_FILE, JSON.stringify(state, null, 2));
}
// ───────────────────────────────────────────────────────────────────────────


// ─── GMAIL HELPERS ─────────────────────────────────────────────────────────
async function getUnprocessedMarketManEmails(gmail, state) {
  // Search for unread emails from MarketMan
  const res = await gmail.users.messages.list({
    userId: 'me',
    q: `from:${CONFIG.MARKETMAN_SENDER} is:unread`,
    maxResults: 20,
  });

  const messages = res.data.messages || [];
  // Filter out already-processed ones
  return messages.filter(m => !state.processedIds.includes(m.id));
}

async function getEmailDetails(gmail, messageId) {
  const res = await gmail.users.messages.get({
    userId: 'me',
    id: messageId,
    format: 'full',
  });
  return res.data;
}

function extractEmailBody(message) {
  // Gmail stores body in parts (multipart) or directly
  const parts = message.payload.parts || [message.payload];
  let html = '';
  let text = '';

  function walkParts(parts) {
    for (const part of parts) {
      if (part.parts) walkParts(part.parts);
      if (part.mimeType === 'text/html' && part.body?.data) {
        html = Buffer.from(part.body.data, 'base64').toString('utf-8');
      }
      if (part.mimeType === 'text/plain' && part.body?.data) {
        text = Buffer.from(part.body.data, 'base64').toString('utf-8');
      }
    }
  }
  walkParts(parts);
  return { html, text };
}

function extractHeader(message, name) {
  const headers = message.payload.headers || [];
  const h = headers.find(h => h.name.toLowerCase() === name.toLowerCase());
  return h ? h.value : '';
}
// ───────────────────────────────────────────────────────────────────────────


// ─── LINK EXTRACTION ───────────────────────────────────────────────────────
function extractMarketManLink(html, text) {
  // Try HTML first — look for the "View Order" button href
  if (html) {
    const $ = cheerio.load(html);
    let link = null;

    // Look for anchor tags containing "View Order" text
    $('a').each((_, el) => {
      const linkText = $(el).text().trim().toLowerCase();
      const href = $(el).attr('href') || '';
      if (linkText.includes('view order') || href.includes('marketman.com')) {
        link = href;
        return false; // break
      }
    });

    if (link) return resolveMarketManLink(link);
  }

  // Fallback: regex on plain text
  if (text) {
    // Match ProofPoint-wrapped or direct MarketMan links
    const proofpointMatch = text.match(/https?:\/\/urldefense\.proofpoint\.com[^\s]+/);
    if (proofpointMatch) return resolveMarketManLink(proofpointMatch[0]);

    const directMatch = text.match(/https?:\/\/vendor\.marketman\.com[^\s]+/);
    if (directMatch) return directMatch[0];
  }

  return null;
}

function resolveMarketManLink(url) {
  // If it's a ProofPoint URL, decode it to get the real MarketMan URL
  if (url.includes('urldefense.proofpoint.com')) {
    try {
      const uParam = new URL(url).searchParams.get('u');
      if (uParam) {
        // ProofPoint encodes hyphens as -3A etc.
        return decodeURIComponent(uParam.replace(/-3A/g, ':').replace(/-2F/g, '/').replace(/-3F/g, '?').replace(/-3D/g, '=').replace(/-26/g, '&'));
      }
    } catch (e) {
      console.warn('Could not decode ProofPoint URL:', e.message);
    }
  }
  return url;
}
// ───────────────────────────────────────────────────────────────────────────


// ─── MARKETMAN ORDER SCRAPER ───────────────────────────────────────────────
//
// Based on confirmed MarketMan DirectOrder.aspx page structure:
//
//   Header area:
//     "Purchase Order from {location}"  ← page title / h1
//     Supplier    | Order number | Customer number | Delivery date
//     Sent by     | Sent date    | Ship to
//
//   Items table columns (in order):
//     Item code | Product | Quantity | Total Qty | Price | Total price
//
//   Footer:
//     Comments: {text}   ← maps to Special Instructions in Cut+Dry
//
async function scrapeMarketManOrder(orderUrl) {
  console.log('  🔗 Fetching order from:', orderUrl);

  const response = await fetch(orderUrl, {
    headers: {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36',
    },
    timeout: 15000,
  });

  if (!response.ok) {
    throw new Error(`Failed to fetch MarketMan order page: ${response.status} ${response.statusText}`);
  }

  const html = await response.text();
  const $ = cheerio.load(html);

  // Always save HTML for audit (overwritten each run)
  fs.writeFileSync(path.join(__dirname, 'debug-last-order.html'), html);

  const order = {
    orderNumber: '',
    customer: '',        // the restaurant/location name (e.g. "Carlsbad")
    customerCode: '',    // Cut+Dry customer code — populated by customer in MarketMan
    deliveryDate: '',
    shipTo: '',
    sentBy: '',
    items: [],
    comments: '',        // → Special Instructions in Cut+Dry
    rawUrl: orderUrl,
  };

  // ── Page title gives us "Purchase Order from {location}" ──
  const pageTitle = $('h1, h2, .title, [class*="title"]').first().text().trim();
  if (pageTitle.includes('from')) {
    order.customer = pageTitle.replace(/purchase order from/i, '').trim();
  }

  // ── Scan all label→value pairs in the header ──
  // MarketMan uses a simple table/grid: label cell followed by value cell
  // Works for both table-based and div-based layouts
  $('table td, table th').each((_, el) => {
    const label = $(el).text().trim().toLowerCase();
    const value = $(el).next('td, th').text().trim();

    if (!value) return;

    if (label.includes('order number'))    order.orderNumber  = value;
    if (label.includes('customer number') || label.includes('customer code')) order.customerCode = value;
    if (label.includes('delivery date'))   order.deliveryDate = value;
    if (label.includes('ship to'))         order.shipTo       = value;
    if (label.includes('sent by'))         order.sentBy       = value;
    if (label.includes('supplier') && !order.customer) order.customer = value;
  });

  // Also try span/div label patterns
  $('[class*="label"], [class*="key"], strong, b').each((_, el) => {
    const label = $(el).text().trim().toLowerCase().replace(':', '');
    const value = $(el).parent().text().replace($(el).text(), '').trim()
                  || $(el).next().text().trim();

    if (!value) return;
    if (label.includes('order number') && !order.orderNumber)    order.orderNumber  = value;
    if (label.includes('delivery date') && !order.deliveryDate)  order.deliveryDate = value;
    if (label.includes('ship to') && !order.shipTo)              order.shipTo       = value;
    if (label.includes('sent by') && !order.sentBy)              order.sentBy       = value;
  });

  // ── Extract line items ──
  // Confirmed columns: Item code(0) | Product(1) | Quantity(2) | Total Qty(3) | Price(4) | Total price(5)
  let headerFound = false;

  $('table tr').each((_, row) => {
    const cells = $(row).find('td, th');
    if (cells.length < 3) return;

    const col0 = $(cells[0]).text().trim();
    const col1 = $(cells[1]).text().trim();
    const col2 = $(cells[2]).text().trim();
    const col3 = cells.length > 3 ? $(cells[3]).text().trim() : '';
    const col4 = cells.length > 4 ? $(cells[4]).text().trim() : '';
    const col5 = cells.length > 5 ? $(cells[5]).text().trim() : '';

    // Detect and skip the header row
    if (col1.toLowerCase().includes('product') || col0.toLowerCase().includes('item code')) {
      headerFound = true;
      return;
    }

    // Skip rows before header, empty rows, and total/summary rows
    if (!headerFound) return;
    if (!col1 || !col2) return;
    if (col1.startsWith('$') || col0 === '') return; // total row

    const qty = parseFloat(col2);
    if (isNaN(qty)) return; // not a data row

    order.items.push({
      itemCode:   col0,           // e.g. "24991"
      product:    col1,           // e.g. "Peanuts 5 lb"
      quantity:   col2,           // e.g. "1"
      totalQty:   col3,           // e.g. "5 lb"
      price:      col4,           // e.g. "$16.25"
      totalPrice: col5,           // e.g. "$16.25"
    });
  });

  // ── Extract Comments → Special Instructions ──
  // MarketMan shows: "Comments  Teste 2" then "Rows  1" then "items  1"
  // We want only the Comments value, not Rows/items counts
  $('table td, td').each((_, el) => {
    const label = $(el).text().trim().toLowerCase();
    if (label === 'comments') {
      const value = $(el).next('td').text().trim();
      if (value && !value.match(/^\d+$/)) { // ignore if it's just a number
        order.comments = value;
      }
    }
  });

  // Fallback: scan all text for "Comments" label
  if (!order.comments) {
    $('*').each((_, el) => {
      const text = $(el).clone().children().remove().end().text().trim();
      if (text.toLowerCase() === 'comments') {
        const sibling = $(el).next().text().trim();
        const parent  = $(el).parent().text().replace(text, '').trim();
        const candidate = sibling || parent;
        if (candidate && !candidate.match(/^\d+$/) && candidate.length < 500) {
          order.comments = candidate;
          return false; // break
        }
      }
    });
  }

  console.log(`  ✅ Scraped: ${order.items.length} item(s), comments: "${order.comments || 'none'}"`);

  if (order.items.length === 0) {
    console.warn('  ⚠️  No items found — check debug-last-order.html');
  }

  return order;
}

function extractText($, selectors) {
  for (const selector of selectors) {
    try {
      const text = $(selector).first().text().trim();
      if (text) return text;
    } catch (e) { /* try next */ }
  }
  return '';
}
// ───────────────────────────────────────────────────────────────────────────


// ─── FORMAT ORDER FOR CUT+DRY ──────────────────────────────────────────────
//
// Cut+Dry's AI reads plain-text emails and maps fields like:
//   - Customer name
//   - Delivery date
//   - Line items: "Product Name: Qty Unit"
//   - Special Instructions (from our Comments line)
//
function formatOrderForCutDry(order) {
  const lines = [];

  // Header — Cut+Dry uses these to identify customer + date
  lines.push(`New Order`);
  if (order.customer)     lines.push(`Customer: ${order.customer}`);
  if (order.customerCode && order.customerCode.toLowerCase() !== 'none') {
                          lines.push(`Customer Code: ${order.customerCode}`);
  }
  if (order.shipTo)       lines.push(`Deliver to: ${order.shipTo}`);
  if (order.deliveryDate) lines.push(`Delivery Date: ${order.deliveryDate}`);
  if (order.orderNumber)  lines.push(`Order #: ${order.orderNumber}`);
  if (order.sentBy)       lines.push(`Ordered by: ${order.sentBy}`);
  lines.push('');

  // Line items — written clearly so Cut+Dry AI can parse each one
  lines.push('Items:');
  if (order.items.length > 0) {
    for (const item of order.items) {
      // Format: "Product Name: Qty TotalQty"
      // e.g.  "Peanuts 5 lb: 1 (5 lb)"
      let line = `- ${item.product}: ${item.quantity}`;
      if (item.totalQty && item.totalQty !== item.quantity) {
        line += ` (${item.totalQty})`;
      }
      if (item.price) line += ` @ ${item.price}`;
      lines.push(line);
    }
  } else {
    lines.push('(Unable to parse items — see original order link below)');
  }

  // Special Instructions — mapped from MarketMan Comments field
  if (order.comments) {
    lines.push('');
    lines.push(`Special Instructions: ${order.comments}`);
  }

  lines.push('');
  lines.push(`Original Order: ${order.rawUrl}`);
  lines.push('(Auto-forwarded from MarketMan by SFT Order Watcher)');

  // Subject line helps Cut+Dry identify customer
  const subject = `Order from ${order.customer || 'MarketMan Customer'} — Deliver ${order.deliveryDate || 'see details'}`;

  return { subject, body: lines.join('\n') };
}
// ───────────────────────────────────────────────────────────────────────────


// ─── SEND EMAIL VIA GMAIL ──────────────────────────────────────────────────
async function sendEmail(gmail, to, subject, body) {
  const message = [
    `To: ${to}`,
    `From: ${CONFIG.ORDERS_EMAIL}`,
    `Subject: ${subject}`,
    `Content-Type: text/plain; charset=utf-8`,
    '',
    body,
  ].join('\n');

  const encoded = Buffer.from(message).toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');

  await gmail.users.messages.send({
    userId: 'me',
    requestBody: { raw: encoded },
  });

  console.log(`  📤 Forwarded to Cut+Dry: ${subject}`);
}
// ───────────────────────────────────────────────────────────────────────────


// ─── MARK EMAIL AS READ ────────────────────────────────────────────────────
async function markAsRead(gmail, messageId) {
  await gmail.users.messages.modify({
    userId: 'me',
    id: messageId,
    requestBody: { removeLabelIds: ['UNREAD'] },
  });
}
// ───────────────────────────────────────────────────────────────────────────


// ─── MAIN PROCESS LOOP ─────────────────────────────────────────────────────
async function processOrders() {
  console.log(`\n[${new Date().toLocaleTimeString()}] Checking for new MarketMan orders...`);

  const gmail = await getGmailClient();
  const state = loadState();

  const emails = await getUnprocessedMarketManEmails(gmail, state);
  console.log(`  Found ${emails.length} new email(s)`);

  for (const emailRef of emails) {
    try {
      console.log(`\n  📧 Processing email ID: ${emailRef.id}`);

      const message = await getEmailDetails(gmail, emailRef.id);
      const subject = extractHeader(message, 'subject');
      const { html, text } = extractEmailBody(message);

      console.log(`  Subject: ${subject}`);

      // Extract the MarketMan order link
      const orderLink = extractMarketManLink(html, text);
      if (!orderLink) {
        console.warn('  ⚠️  No MarketMan order link found in email — skipping');
        state.processedIds.push(emailRef.id);
        continue;
      }

      // Scrape the order page
      const order = await scrapeMarketManOrder(orderLink);

      // Format and forward to Cut+Dry
      const { subject: fwdSubject, body: fwdBody } = formatOrderForCutDry(order);
      await sendEmail(gmail, CONFIG.CUTDRY_EMAIL, fwdSubject, fwdBody);

      // Mark original as read and record as processed
      await markAsRead(gmail, emailRef.id);
      state.processedIds.push(emailRef.id);

      // Keep state file from growing forever (keep last 500)
      if (state.processedIds.length > 500) {
        state.processedIds = state.processedIds.slice(-500);
      }

      console.log(`  ✅ Done with email ${emailRef.id}`);

    } catch (err) {
      console.error(`  ❌ Error processing email ${emailRef.id}:`, err.message);
      // Don't mark as processed so it retries next cycle
    }
  }

  saveState(state);
}
// ───────────────────────────────────────────────────────────────────────────


// ─── ENTRY POINT ───────────────────────────────────────────────────────────
(async () => {
  const args = process.argv.slice(2);

  // Handle first-time OAuth token save
  if (args[0] === '--auth' && args[1]) {
    await saveToken(args[1]);
    return;
  }

  // Run once or watch mode
  await processOrders();

  if (args.includes('--watch')) {
    console.log(`\n👀 Watch mode: polling every ${CONFIG.POLL_INTERVAL_MS / 1000 / 60} minutes...`);
    setInterval(processOrders, CONFIG.POLL_INTERVAL_MS);
  }
})();
