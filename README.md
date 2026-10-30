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

## Shared container map

Open `/inventory/yard` for the overhead team view. Existing detailed container,
walk-in, returns, pallet configuration, SKU settings, inbound, lot tracking and
invoice processing routes are retained. The new view uses the same SQLite stock
records as those existing screens; updates are saved on the server and other
open yard screens refresh every 30 seconds while idle.

- C1 defaults to 8 spaces (4/4); its existing optional 9-space mode is retained.
- C2-C7 default to 18 spaces (9/9). Container Settings also supports 20 spaces:
  11 sideways on the left and 9 straight on the right, or the reverse.
- All pallets are 40 x 48 inches. The diagram is schematic, not a measured plan.
- **Stored slot 01 remains nearest the door.** The overhead view renders the back
  at the top and adds separate positions counted from the back. No stored pallet
  locations are renumbered. Both historical spellings of slot codes are accepted.
- Layout saves are atomic and reject changes that would hide stock. Old stock
  outside a configured layout or sharing a physical slot appears under
  **Location review needed**, rather than disappearing from view.
- Drag pallets between slots, WALKIN and RETURNS, or use **Move** then tap an
  unoccupied destination on a phone. Use the detailed map to break quantities
  into walk-in loose stock.
- **Count / notes** records a physical quantity correction and entered operator
  name in the existing movement ledger. Stale quantity, location or notes are
  rejected. The entered name is self-reported, not an authenticated identity.
- Product entry uses the existing active SKU catalog, with name/category filters
  and a frozen/gelato/yogurt/sorbet shortcut. Existing SKU sync remains available
  in Product Settings. The shortcut uses category/product text; use the actual
  category selector or all products if a product's naming does not match it.
- **Print container maps** prints one container per US Letter page.
- `/inventory/loading-notes` retains the October 30 physical observations for
  C2, C5 and C6. These notes are deliberately not automatically received into
  stock: match the existing pallet or review product, unit, quantity, lot and
  slot first. Unknown quantities and four unlocated door-area pallets in C2
  remain unresolved. "Acai bricks" covers both brick labels from the notes.

### Validation and rollout

Run `npm ci` and `npm test`. Tests use temporary databases and fake local
connection records; they do not contact QuickBooks or send invoices/emails.
No production credentials, database exports or production inventory are included.

Review the branch before deploying through the existing Render service. Use its
existing persistent `DB_PATH`; back up that SQLite database before deployment.
No database schema migration or automatic stock import is required. Existing
access behavior is retained; this change does not add team accounts or roles.
