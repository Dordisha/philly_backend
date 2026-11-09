import { resolvePaidAccess } from './paidAccess.js';

// Server-owned grant registry. Never accept a tier or identity from the client.
// Subscription entries must be written by a trusted purchase verifier, not the app.
export function resolveOwnerAccess(authorization, options) {
  return resolvePaidAccess(authorization, 'pro', options);
}
