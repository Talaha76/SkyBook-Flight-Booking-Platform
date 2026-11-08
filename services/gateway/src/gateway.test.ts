import { DEMO_USERS, issueToken, login, verifyToken } from './auth';
import { cacheKey, SearchQuery } from '../../flight-search/src/search';

const SECRET = 'test-secret-test-secret-test-secret';

describe('JWT auth', () => {
  test('login + verify round trip carries id and role', () => {
    const user = login('agent', 'pw', 'pw');
    const token = issueToken(user, SECRET);
    expect(verifyToken(`Bearer ${token}`, SECRET)).toEqual({ id: DEMO_USERS[2].id, role: 'AGENT' });
  });
  test('wrong password is rejected', () => {
    expect(() => login('alice', 'nope', 'pw')).toThrow('invalid username or password');
  });
  test('tampered or foreign-secret tokens are rejected', () => {
    const token = issueToken(DEMO_USERS[0], 'another-secret-another-secret-123');
    expect(() => verifyToken(`Bearer ${token}`, SECRET)).toThrow('invalid or expired token');
    expect(() => verifyToken('Basic abc', SECRET)).toThrow();
  });
  test('no header means anonymous', () => {
    expect(verifyToken(undefined, SECRET)).toBeNull();
  });
});

describe('search validation + cache key', () => {
  test('normalises IATA codes', () => {
    const q = SearchQuery.parse({ origin: 'yyz', destination: ' yvr ', date: '2026-12-01' });
    expect(cacheKey(q)).toBe('search:YYZ:YVR:2026-12-01');
  });
  test('rejects bad input', () => {
    expect(SearchQuery.safeParse({ origin: 'TORONTO', destination: 'YVR', date: '2026-12-01' }).success).toBe(false);
    expect(SearchQuery.safeParse({ origin: 'YYZ', destination: 'YVR', date: '12/01/2026' }).success).toBe(false);
  });
});
