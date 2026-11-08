import { timingSafeEqual } from 'crypto';
import jwt from 'jsonwebtoken';
import { HttpError } from '../../../packages/shared/src';

export type Role = 'CUSTOMER' | 'AGENT';
export interface DemoUser { id: string; username: string; role: Role }

/** Demo identities (no user service in scope). Passwords come from DEMO_PASSWORD. */
export const DEMO_USERS: DemoUser[] = [
  { id: '11111111-1111-4111-8111-111111111111', username: 'alice', role: 'CUSTOMER' },
  { id: '22222222-2222-4222-8222-222222222222', username: 'bob', role: 'CUSTOMER' },
  { id: '33333333-3333-4333-8333-333333333333', username: 'agent', role: 'AGENT' },
];

function safeEqual(a: string, b: string) {
  const ab = Buffer.from(a);
  const bb = Buffer.from(b);
  return ab.length === bb.length && timingSafeEqual(ab, bb);
}

export function login(username: string, password: string, demoPassword: string): DemoUser {
  const user = DEMO_USERS.find((u) => u.username === username);
  if (!user || !safeEqual(password, demoPassword)) throw new HttpError(401, 'invalid username or password');
  return user;
}

export function issueToken(user: DemoUser, secret: string, ttlSeconds = 3600): string {
  return jwt.sign({ role: user.role, username: user.username }, secret, { subject: user.id, expiresIn: ttlSeconds, issuer: 'skybook-gateway', algorithm: 'HS256' });
}

export function verifyToken(header: string | undefined, secret: string): { id: string; role: Role } | null {
  if (!header) return null;
  const [scheme, token] = header.split(' ');
  if (scheme?.toLowerCase() !== 'bearer' || !token) throw new HttpError(401, 'Authorization header must be "Bearer <token>"');
  try {
    const claims = jwt.verify(token, secret, { issuer: 'skybook-gateway', algorithms: ['HS256'] }) as jwt.JwtPayload;
    if (!claims.sub || (claims.role !== 'CUSTOMER' && claims.role !== 'AGENT')) throw new Error('bad claims');
    return { id: claims.sub, role: claims.role };
  } catch {
    throw new HttpError(401, 'invalid or expired token');
  }
}
