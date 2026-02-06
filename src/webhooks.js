import crypto from 'crypto';

/**
 * Express body parser "verify" hook.
 * We store the raw bytes so signature verification matches Intuit's HMAC exactly.
 */
export function rawBodySaver(req, res, buf) {
  if (buf && buf.length) {
    req.rawBodyBuffer = Buffer.from(buf); // exact bytes
  }
}

/**
 * Verify Intuit webhook signature.
 * - Uses HMAC-SHA256 with your "verifier token" as the key
 * - Base64 output compared to `intuit-signature` header
 * - Calls next() only when valid
 */
export function verifyIntuitWebhook(req, res, next) {
  const verifierToken = process.env.INTUIT_WEBHOOK_VERIFIER_TOKEN;
  if (!verifierToken) {
    console.log('[webhook] Missing INTUIT_WEBHOOK_VERIFIER_TOKEN');
    return res.status(500).send('Missing INTUIT_WEBHOOK_VERIFIER_TOKEN');
  }

  // Intuit uses `intuit-signature` (some proxies might forward as x-intuit-signature)
  const signatureHeader = req.get('intuit-signature') || req.get('x-intuit-signature');
  if (!signatureHeader) {
    console.log('[webhook] Missing intuit-signature header');
    return res.status(401).send('Missing intuit-signature header');
  }

  // Use raw bytes if available; fallback to JSON string (should not be needed)
  const rawBytes = req.rawBodyBuffer
    ? req.rawBodyBuffer
    : Buffer.from(JSON.stringify(req.body || {}), 'utf8');

  const computed = crypto.createHmac('sha256', verifierToken).update(rawBytes).digest('base64');

  // Timing-safe compare (handles subtle whitespace differences too)
  const a = Buffer.from(computed, 'utf8');
  const b = Buffer.from(String(signatureHeader).trim(), 'utf8');

  const ok = a.length === b.length && crypto.timingSafeEqual(a, b);

  if (!ok) {
    console.log('[webhook] Invalid signature', {
      computed: computed.slice(0, 12) + '...',
      header: String(signatureHeader).trim().slice(0, 12) + '...',
      rawLen: rawBytes.length
    });
    return res.status(401).send('Invalid signature');
  }

  return next();
}
