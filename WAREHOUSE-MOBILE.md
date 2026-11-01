# Warehouse mobile service: staged pilot

This branch adds an isolated employee API under `/api/warehouse` and an office screen at `/warehouse`. It is disabled until `WAREHOUSE_ENABLED=true` is configured. Existing inventory, yard and invoice processing retain their current routes.

## Accounts

The first administrator is created once using `POST /api/warehouse/bootstrap`, with the server's `WAREHOUSE_BOOTSTRAP_TOKEN` (at least 32 characters), a display name, username and password. Never put the setup token in a URL, repository or APK. Remove the environment variable after setup.

Subsequent accounts are managed by an administrator at `/warehouse`. Suggested usernames use the person's name and role, such as `alex.driver` or `sam.manager`; usernames can be changed. Passwords require 12–200 characters and are stored as salted scrypt hashes. New accounts and administrator password resets require the employee to choose a password at first sign-in. Temporary passwords are not written to the activity log.

- Administrator: all work functions and user/credential administration.
- Manager: office operations and enabled work functions; no user/security administration.
- Office: office operations and enabled work functions; no user/security administration.
- Warehouse and driver: enabled work functions; delivery access is restricted to their assigned team identity, including explicit helper assignments.

Changing role, functions, username, password or enabled state revokes the affected user's sessions. The administrator cannot disable or demote their own account. Browser sessions use a Secure, HttpOnly, SameSite=Strict cookie and CSRF protection; handhelds use opaque bearer tokens. The current expiry is 14 days. The Android app stores the bearer token with Android Keystore encryption, and keeps offline work independently from the session.

## Driver routes and office handoff

The server bridge accepts route packets with stable route, stop, product and route-team identifiers. Protect it with a random `WAREHOUSE_BRIDGE_KEY` of at least 32 characters; this is a server-only secret, never a handheld token. The bridge key grants access to route data, driver events, photos and stock; provision only to the trusted office server.

Route packets include `{id,name,date,driverId,helperId?,helperIds?,status,stops}`. Each stop contains `{id,customerName,address?,status,products}`; each product contains `{id,name,sku,barcodes,qty,scanLimit?}`. Barcode strings preserve leading zeroes. Scanning matches case-insensitively. Imported route status must be `loaded`, `delivering` or `delivered`.

Employee event batches use a stable `requestId`, `deviceId`, `expectedRevision` and ordered `events`. Each event has a UUID, kind, payload and device timestamp. The service validates the entire batch in one transaction. Exact retries return the previous receipt; a reused identifier with changed contents is rejected. Route conflicts retain the device's saved work for office review. Photos are private and must be uploaded before their journal events.

The office screen shows date-filtered route progress, stops, event timestamps and photos. Completed stops and cash/check records are pending office review. **This branch does not send invoices, apply payments or write QuickBooks bills.** Existing office approvals remain responsible for accounting changes.

### Not yet connected

Automatic pulling from the private planning Site and forwarding accepted native events into its existing approval workflow still need the server-to-server adapter and runtime configuration. No private Site authentication tokens belong in the APK. Route import and bridge event acknowledgment are integration endpoints, not a manual download step for drivers. Do not deploy the native build as the live delivery replacement until automatic preparation, restart/offline replay, photographs and office handoff have been exercised together.

The Android 0.2.0 pilot source has sign-in, assigned-route caching, a durable local event journal, photo capture and upload, guided stops, maps and server sync. It remains a pilot pending end-to-end hardware checks. The earlier training APK remains separate.

## Frozen receiving foundation

Local receiving records link to QuickBooks vendors and optionally read-only QuickBooks purchase orders. Frozen putaway suggestions use existing C1–C7 slot settings, stock and physical-slot aliases. Walk-in restock targets account for both pallet and loose stock. Dry and refrigerated receipts do not require container slots. Suggestions create no stock; explicit confirmation rechecks current slot occupancy and receipt revision, then creates pallets and movement records atomically. Damage/rejected quantities are excluded. No QuickBooks bills are created.

## Deployment and validation

Use the existing persistent inventory SQLite database; back it up before deployment. Schema additions are idempotent and isolated in `wh_*` tables. Configure the feature flag and independent random setup/bridge secrets through the hosting provider, never committed files. HTTPS is required for the cookie and Android network client.

Run `npm ci` and `npm test` with Node 22. Tests use in-memory or temporary databases and synthetic records. They cover existing yard behavior, account/session boundaries, manager security restrictions, required password changes, route/helper scope, CSRF, replay safety, transactional receiving and stock preservation. Staff names, actual credentials, photos and route packets must remain outside this public repository.
