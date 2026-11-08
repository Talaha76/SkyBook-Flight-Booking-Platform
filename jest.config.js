/** Two projects: fast unit tests (mocks only) and integration tests (real PostgreSQL). */
const base = { preset: 'ts-jest', testEnvironment: 'node', transform: { '^.+\\.ts$': ['ts-jest', { diagnostics: false }] } };
module.exports = {
  testTimeout: 120000,
  projects: [
    { ...base, displayName: 'unit', testMatch: ['<rootDir>/{packages,services}/**/*.test.ts'] },
    { ...base, displayName: 'integration', testMatch: ['<rootDir>/tests/integration/**/*.itest.ts'] },
  ],
};
