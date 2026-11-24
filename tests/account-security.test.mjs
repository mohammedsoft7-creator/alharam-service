import test from 'node:test';
import assert from 'node:assert/strict';
import { changeOwnPassword, checkNewPassword, createPasswordResetSender, accountErrorMessage } from '../account-security.mjs';

test('password validation respects confirmation, length and server policy', async () => {
  await assert.rejects(checkNewPassword({}, 'secret', 'different', async () => ({ isValid: true })), /غير متطابقتين/);
  await assert.rejects(checkNewPassword({}, '123', '123', async () => ({ isValid: true })), /6 إلى/);
  await assert.rejects(checkNewPassword({}, 'abcdef', 'abcdef', async () => ({ isValid: false, containsNumericCharacter: false })), /رقم/);
  await assert.doesNotReject(checkNewPassword({}, 'a long passphrase 123', 'a long passphrase 123', async () => ({ isValid: true })));
});

test('password change stops if the signed-in identity changes during reauthentication', async () => {
  const auth = { currentUser: { uid: 'first', email: 'first@example.test' } }; let updated = false;
  await assert.rejects(changeOwnPassword({ auth, currentPassword: 'old-password', newPassword: 'new-password', confirmation: 'new-password',
    validatePassword: async () => ({ isValid: true }), credential: () => ({}),
    reauthenticate: async () => { auth.currentUser = { uid: 'second', email: 'second@example.test' }; },
    updatePassword: async () => { updated = true; } }), /تغيّر الحساب/);
  assert.equal(updated, false);
});

test('same password and signed-out requests are rejected before any credential mutation', async () => {
  const options = { currentPassword: 'same-password', newPassword: 'same-password', confirmation: 'same-password' };
  await assert.rejects(changeOwnPassword({ ...options, auth: { currentUser: null } }), /سجّل الدخول/);
  await assert.rejects(changeOwnPassword({ ...options, auth: { currentUser: { uid: 'u', email: 'u@example.test' } } }), /مختلفة/);
});

test('reset sender shares in-flight requests and allows retry after transient failure', async () => {
  let count = 0, finish;
  const send = createPasswordResetSender(() => { count++; return new Promise(resolve => { finish = resolve; }); });
  const first = send('User@Example.test'), second = send('user@example.test');
  assert.equal(first, second); await Promise.resolve(); assert.equal(count, 1); finish(); await first;
  let attempts = 0;
  const retry = createPasswordResetSender(async () => { if (++attempts === 1) throw { code: 'auth/network-request-failed' }; });
  await assert.rejects(retry('user@example.test')); await retry('user@example.test'); assert.equal(attempts, 2);
});

test('reset cooldown expires and unexpected SDK details never enter UI messages', async () => {
  let now = 0, count = 0;
  const send = createPasswordResetSender(async () => { count++; }, () => now);
  await send('user@example.test'); now = 59999; await send('user@example.test'); assert.equal(count, 1);
  now = 60000; await send('user@example.test'); assert.equal(count, 2);
  assert.throws(() => send('not an email'), /بريدًا إلكترونيًا صحيحًا/);
  assert.equal(accountErrorMessage({ message: 'internal-password-sensitive-detail' }).includes('sensitive'), false);
});
