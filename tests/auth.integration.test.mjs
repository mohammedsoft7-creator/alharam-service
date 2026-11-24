import { before, beforeEach, after, test } from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { changeOwnPassword, checkNewPassword } from '../account-security.mjs';
let require = createRequire(new URL('../package.json', import.meta.url));
try { require.resolve('firebase/auth'); }
catch { require = createRequire(new URL('../.tooling/package.json', import.meta.url)); }
const { initializeApp, deleteApp } = require('firebase/app');
const { initializeAuth, inMemoryPersistence, connectAuthEmulator, createUserWithEmailAndPassword,
  signInWithEmailAndPassword, signOut, reauthenticateWithCredential, EmailAuthProvider, updatePassword,
  sendPasswordResetEmail, verifyPasswordResetCode, confirmPasswordReset } = require('firebase/auth');
const host = process.env.FIREBASE_AUTH_EMULATOR_HOST || '';
assert.match(host, /^(127\.0\.0\.1|localhost):\d+$/, 'Only a local Auth emulator is permitted');
const projectId = 'demo-alhram-rules';
let app, auth;
const email = 'password-tests@example.test';
const oldPassword = 'Initial-password-123';
const newPassword = 'Replacement-password-456';
before(() => {
  app = initializeApp({ projectId, apiKey: 'emulator-only-key', authDomain: 'localhost' }, 'password-integration');
  auth = initializeAuth(app, { persistence: inMemoryPersistence });
  connectAuthEmulator(auth, 'http://' + host, { disableWarnings: true });
});
beforeEach(async () => {
  await signOut(auth);
  const response = await fetch(`http://${host}/emulator/v1/projects/${projectId}/accounts`, { method: 'DELETE' });
  assert.equal(response.ok, true);
});
after(async () => { await signOut(auth); await deleteApp(app); });

// Auth emulator does not implement getPasswordPolicy. Policy failure paths are covered
// by account-security.test.mjs; use its fixed default policy only in this local suite.
const emulatorPolicy = async (auth, password) => ({ isValid: password.length >= 6 });
const change = (currentPassword, password = newPassword, confirmation = password) => changeOwnPassword({ auth,
  currentPassword, newPassword: password, confirmation, validatePassword: emulatorPolicy,
  credential: EmailAuthProvider.credential, reauthenticate: reauthenticateWithCredential, updatePassword });

test('real Firebase SDK changes an authenticated password while preserving the UID', async () => {
  const created = await createUserWithEmailAndPassword(auth, email, oldPassword);
  const result = await change(oldPassword);
  assert.equal(result.uid, created.user.uid);
  await signOut(auth);
  await assert.rejects(signInWithEmailAndPassword(auth, email, oldPassword));
  const next = await signInWithEmailAndPassword(auth, email, newPassword);
  assert.equal(next.user.uid, created.user.uid);
});

test('wrong current password cannot change the actual account password', async () => {
  await createUserWithEmailAndPassword(auth, email, oldPassword);
  await assert.rejects(change('Incorrect-password-789'));
  await signOut(auth);
  await assert.doesNotReject(signInWithEmailAndPassword(auth, email, oldPassword));
});

test('confirmation mismatch rejects before credential mutation', async () => {
  await createUserWithEmailAndPassword(auth, email, oldPassword);
  await assert.rejects(change(oldPassword, newPassword, 'Not-the-same-password'), /غير متطابقتين/);
  await checkNewPassword(auth, newPassword, newPassword, emulatorPolicy);
  await signOut(auth);
  await assert.doesNotReject(signInWithEmailAndPassword(auth, email, oldPassword));
});

test('reset email requires redeeming its one-time code and preserves the account UID', async () => {
  const created = await createUserWithEmailAndPassword(auth, email, oldPassword);
  await signOut(auth);
  await sendPasswordResetEmail(auth, email);
  const response = await fetch(`http://${host}/emulator/v1/projects/${projectId}/oobCodes`);
  const outbox = await response.json();
  const action = outbox.oobCodes.filter(item => item.email === email && item.requestType === 'PASSWORD_RESET').at(-1);
  assert.ok(action?.oobCode);
  await assert.doesNotReject(signInWithEmailAndPassword(auth, email, oldPassword));
  await signOut(auth);
  assert.equal(await verifyPasswordResetCode(auth, action.oobCode), email);
  await confirmPasswordReset(auth, action.oobCode, newPassword);
  await assert.rejects(confirmPasswordReset(auth, action.oobCode, 'Cannot-reuse-code-789'));
  await assert.rejects(signInWithEmailAndPassword(auth, email, oldPassword));
  const next = await signInWithEmailAndPassword(auth, email, newPassword);
  assert.equal(next.user.uid, created.user.uid);
});
