# QBO Invoice Sorter + Operating Cost Surcharge (Post-Processor)

This app:
1) Listens for QuickBooks Online invoice events (webhooks)
2) Reorders invoice item lines by **your saved category order**
3) Adds/corrects "Operating Cost Surcharge" per customer rules and forces it to the bottom

## Your confirmed rules baked in
- Default surcharge: **$15**
- Threshold checks: **pre-tax subtotal; ignore discounts; before surcharge**
- Waivers: keep the line and show **$0**
- Excluded customers: **never add; remove if present**
- Surcharge line always goes at the very bottom

---

## 1) Prereqs
- Node.js 18+
- An Intuit Developer account and an Intuit app with:
  - QuickBooks Online scope: `com.intuit.quickbooks.accounting`
  - Redirect URI set to: `https://YOUR_DOMAIN/auth/callback`
  - Webhooks enabled for: `Invoice` (Create + Update)

---

## 2) Run locally (fastest way to test)
1. Copy `.env.example` to `.env` and fill it in
2. Install deps:
   ```bash
   npm install
   ```
3. Start:
   ```bash
   npm start
   ```
4. Open:
   - http://localhost:3000

### Local webhook testing
QuickBooks webhooks require a public URL. The easiest is **ngrok**:
```bash
ngrok http 3000
```
Then set:
- `APP_BASE_URL` = ngrok https URL
- `INTUIT_REDIRECT_URI` = `${APP_BASE_URL}/auth/callback`
- Webhook URL in Intuit app = `${APP_BASE_URL}/webhooks/qbo`

---

## 3) First-time setup inside the app
1) Visit `/auth/start` and connect QBO (pick your company)
2) Go to `/admin/sync` to pull customers & categories into the UI
3) Go to `/admin/categories` to set category order
4) Go to `/admin/rules` and set surcharge rules (we ship with sample seeds)

---

## 4) Production hosting (easy mode)
You can deploy this to any Node host.
Important: use a **persistent disk** for the SQLite DB (or swap to Postgres later).

Minimum environment variables:
- INTUIT_CLIENT_ID
- INTUIT_CLIENT_SECRET
- APP_BASE_URL (your public URL)
- INTUIT_REDIRECT_URI (APP_BASE_URL + /auth/callback)
- INTUIT_WEBHOOK_VERIFIER_TOKEN
- DB_PATH (e.g. /var/data/app.db)

---

## Notes
- The app is idempotent: webhooks can arrive multiple times; it won’t double-add surcharge.
- Taxability for surcharge: the safest way is to set the "Operating Cost Surcharge" Item as **taxable in QBO**.
