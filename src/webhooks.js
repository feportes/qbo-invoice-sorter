import crypto from 'crypto';

export function rawBodySaver(req, res, buf) {
  // store rawBody for signature verification
  if (buf && buf.length) {
    req.rawBody = buf.toString('utf8');
  }
}

export function verifyIntuitWebhook(req, res, next) {
  const verifierToken = process.env.INTUIT_WEBHOOK_VERIFIER_TOKEN;
  if (!verifierToken) return res.status(500).send('Missing INTUIT_WEBHOOK_VERIFIER_TOKEN');

  const signature = req.get('intuit-signature') || req.get('x-intuit-signature');
  if (!signature) return res.status(401).send('Missing intuit-signature header');

  const raw = req.rawBody || JSON.stringify(req.body || {});
  const hmac = crypto.createHmac('sha256', verifierToken).update(raw).digest('base64');

  if (hmac !== signature) {
    return res.status(401).send('Invalid signature');
  }
  next();
}
