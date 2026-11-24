import { before, after, beforeEach, test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { createRequire } from 'node:module';

let require = createRequire(new URL('../package.json', import.meta.url));
try { require.resolve('@firebase/rules-unit-testing'); }
catch { require = createRequire(new URL('../.tooling/package.json', import.meta.url)); }
const { initializeTestEnvironment, assertSucceeds, assertFails } = require('@firebase/rules-unit-testing');
const { doc, collection, getDoc, getDocs, setDoc, updateDoc, deleteDoc, writeBatch, runTransaction } = require('firebase/firestore');
assert.match(process.env.FIRESTORE_EMULATOR_HOST || '', /^(127\.0\.0\.1|localhost):\d+$/, 'These tests require a local emulator, never production');
const rules = fs.readFileSync(new URL('../firestore.rules', import.meta.url), 'utf8');
const ownerUid = rules.match(/request.auth.uid == '([^']+)'/)[1];
const code = 'ABCD1234';
let env;
const db = (uid, email = `${uid}@example.test`) => env.authenticatedContext(uid, { email }).firestore();
const registry = uid => ({ uid, email: `${uid}@example.test`, shopName: 'Test shop', createdAt: '2026-11-24T12:00:00Z', active: true });
before(async () => { env = await initializeTestEnvironment({ projectId: 'demo-alhram-rules', firestore: { rules } }); });
after(async () => { await env?.cleanup(); });
beforeEach(async () => {
  await env.clearFirestore();
  await env.withSecurityRulesDisabled(async context => {
    const store = context.firestore();
    await Promise.all([
      setDoc(doc(store, 'users_registry/a'), registry('a')),
      setDoc(doc(store, 'users_registry/b'), registry('b')),
      setDoc(doc(store, 'users_registry/disabled'), { ...registry('disabled'), active: false }),
      setDoc(doc(store, 'activation_codes', code), { code, createdBy: ownerUid, createdAt: '2026-11-24', used: false }),
      setDoc(doc(store, 'users/a/repairs/r1'), { id: 'r1', paid: 0, customer: 'Test' }),
      setDoc(doc(store, 'users/b/repairs/r2'), { id: 'r2', paid: 0, customer: 'Other shop' })
    ]);
  });
});

test('unauthenticated requests cannot read or modify application data or codes', async () => {
  const store = env.unauthenticatedContext().firestore();
  await assertFails(getDoc(doc(store, 'users/a/repairs/r1')));
  await assertFails(setDoc(doc(store, 'users/a/repairs/r1'), { paid: 10 }));
  await assertFails(getDoc(doc(store, 'activation_codes', code)));
});

test('active users can query and maintain their own supported collections', async () => {
  const store = db('a');
  await assertSucceeds(getDocs(collection(store, 'users/a/repairs')));
  await assertSucceeds(setDoc(doc(store, 'users/a/cash_transactions/c1'), { id: 'c1', amount: 10, type: 'in' }));
  await assertSucceeds(updateDoc(doc(store, 'users/a/repairs/r1'), { paid: 10 }));
  await assertSucceeds(deleteDoc(doc(store, 'users/a/cash_transactions/c1')));
});

test('a user cannot access another shop, even with a forged owner email claim', async () => {
  const store = db('a', 'mohammed.soft7@gmail.com');
  await assertFails(getDoc(doc(store, 'users/b/repairs/r2')));
  await assertFails(setDoc(doc(store, 'users/b/repairs/r2'), { id: 'r2', paid: 0 }));
  await assertFails(getDocs(collection(store, 'users_registry')));
  await assertFails(getDocs(collection(store, 'activation_codes')));
});

test('disabled and unactivated accounts cannot access shop data or activate themselves', async () => {
  for (const uid of ['disabled', 'unactivated']) {
    const store = db(uid);
    await assertSucceeds(getDoc(doc(store, 'users_registry', uid)));
    await assertFails(getDocs(collection(store, `users/${uid}/repairs`)));
    await assertFails(setDoc(doc(store, `users/${uid}/repairs/r1`), { id: 'r1', paid: 0 }));
    await assertFails(setDoc(doc(store, 'users_registry', uid), registry(uid)));
  }
});

test('users cannot alter protected registry fields, but can update their shop name', async () => {
  const store = db('a');
  await assertSucceeds(updateDoc(doc(store, 'users_registry/a'), { shopName: 'Updated shop' }));
  await assertFails(updateDoc(doc(store, 'users_registry/a'), { active: false }));
  await assertFails(updateDoc(doc(store, 'users_registry/a'), { email: 'other@example.test' }));
  await assertFails(deleteDoc(doc(store, 'users_registry/a')));
});

test('signed-in users can get an unused code by value but cannot enumerate or manufacture codes', async () => {
  const store = db('new');
  await assertSucceeds(getDoc(doc(store, 'activation_codes', code)));
  await assertFails(getDocs(collection(store, 'activation_codes')));
  await assertFails(setDoc(doc(store, 'activation_codes/FAKE1234'), { code: 'FAKE1234', used: false }));
});

function activationBatch(uid, targetCode = code) {
  const store = db(uid), batch = writeBatch(store);
  batch.update(doc(store, 'activation_codes', targetCode), { used: true, usedBy: `${uid}@example.test`, usedByUid: uid, usedAt: '2026-11-24' });
  batch.set(doc(store, 'users_registry', uid), { ...registry(uid), activationCode: targetCode });
  batch.set(doc(store, `users/${uid}/settings/main`), { shopName: 'New shop' });
  return batch;
}

test('activation requires consuming the code and creating the registry atomically', async () => {
  const store = db('new');
  await assertFails(updateDoc(doc(store, 'activation_codes', code), { used: true, usedBy: 'new@example.test', usedByUid: 'new', usedAt: '2026-11-24' }));
  await assertFails(setDoc(doc(store, 'users_registry/new'), { ...registry('new'), activationCode: code }));
  await assertSucceeds(activationBatch('new').commit());
  await assertSucceeds(getDoc(doc(store, 'users/new/settings/main')));
});

test('a consumed code cannot activate a second account or be reset by its user', async () => {
  await assertSucceeds(activationBatch('new').commit());
  await assertFails(activationBatch('second').commit());
  await assertFails(updateDoc(doc(db('new'), 'activation_codes', code), { used: false }));
  await assertFails(getDoc(doc(db('second'), 'activation_codes', code)));
});

test('parallel activation transactions produce exactly one winner', async () => {
  const activate = uid => {
    const store = db(uid);
    return runTransaction(store, async transaction => {
      const ref = doc(store, 'activation_codes', code), snap = await transaction.get(ref);
      if (snap.data().used) throw new Error('already used');
      transaction.update(ref, { used: true, usedBy: `${uid}@example.test`, usedByUid: uid, usedAt: '2026-11-24' });
      transaction.set(doc(store, 'users_registry', uid), { ...registry(uid), activationCode: code });
    });
  };
  const results = await Promise.allSettled([activate('first'), activate('second')]);
  assert.equal(results.filter(result => result.status === 'fulfilled').length, 1);
});

test('only the verified owner UID can manage users and codes across shops', async () => {
  const store = db(ownerUid);
  await assertSucceeds(getDocs(collection(store, 'users_registry')));
  await assertSucceeds(getDocs(collection(store, 'activation_codes')));
  await assertSucceeds(getDoc(doc(store, 'users/b/repairs/r2')));
  await assertSucceeds(updateDoc(doc(store, 'users_registry/b'), { active: false }));
  await assertSucceeds(setDoc(doc(store, 'activation_codes/NEWC1234'), { code: 'NEWC1234', used: false }));
  await assertSucceeds(deleteDoc(doc(store, 'activation_codes/NEWC1234')));
});

test('negative amounts, excess paid amounts, invalid IDs and zero payments are rejected', async () => {
  const store = db('a');
  await assertFails(setDoc(doc(store, 'users/a/cash_transactions/c1'), { id: 'c1', amount: -1 }));
  await assertFails(setDoc(doc(store, 'users/a/debts/d1'), { id: 'd1', amount: 100, paid: 101 }));
  await assertFails(setDoc(doc(store, 'users/a/debt_payments/p1'), { id: 'p1', amount: 0 }));
  await assertFails(setDoc(doc(store, 'users/a/repairs/r1'), { id: 'different-id', paid: 0 }));
  await assertFails(setDoc(doc(store, 'users/a/cash_transactions/c1'), { id: 'c1', amount: Infinity }));
  await assertSucceeds(setDoc(doc(store, 'users/a/debts/d1'), { id: 'd1', amount: 100, paid: 10 }));
});

test('unknown collections, deeper paths and legacy root collections are denied', async () => {
  const store = db('a');
  for (const path of ['repairs/root', 'users/a/unknown/id', 'users/a/repairs/r1/private/data']) {
    await assertFails(setDoc(doc(store, path), { id: path.split('/').at(-1) }));
    await assertFails(getDoc(doc(store, path)));
  }
  await assertFails(setDoc(doc(store, 'users/a/settings/other'), { shopName: 'Not main' }));
});

test('large atomic batches share the account lookup without exhausting rule access limits', async () => {
  const store = db('a'), batch = writeBatch(store);
  for (let i = 0; i < 100; i++) batch.set(doc(store, `users/a/cash_transactions/c${i}`), { id: `c${i}`, amount: i, type: 'in' });
  await assertSucceeds(batch.commit());
});
