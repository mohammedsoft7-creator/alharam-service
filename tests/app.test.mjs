import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import * as core from '../app-core.mjs';
import * as accountSecurity from '../account-security.mjs';

const source = fs.readFileSync(new URL('../app.js', import.meta.url), 'utf8');
const html = fs.readFileSync(new URL('../index.html', import.meta.url), 'utf8');

function harness({ failCommit = false } = {}) {
  const records = new Map();
  const batches = [];
  const fields = new Map();
  const messages = [];
  const listeners = [];
  const element = id => {
    if (!fields.has(id)) fields.set(id, { id, value: '', textContent: '', innerHTML: '', dataset: {},
      style: {}, classList: { add() {}, remove() {} }, appendChild(child) { messages.push(child); },
      setAttribute() {}, removeAttribute() {}, remove() {}, focus() {}, replaceChildren() {}, click() {} });
    return fields.get(id);
  };
  const ref = (...parts) => ({ path: parts.slice(1).join('/') });
  const read = ref => ({ exists: () => records.has(ref.path), data: () => core.clone(records.get(ref.path)) });
  const context = {
    ...core, ...accountSecurity, console: { log() {}, warn() {}, error() {} },
    initializeApp: () => ({}), getFirestore: () => ({}), getAuth: () => ({ currentUser: { uid: 'u1', email: 'test@example.test' } }),
    doc: ref, collection: ref, getDoc: async ref => read(ref), getDocs: async () => ({ docs: [] }),
    writeBatch: () => {
      const ops = [];
      return {
        set(ref, value) { ops.push(['set', ref.path, core.clone(value)]); },
        delete(ref) { ops.push(['delete', ref.path]); },
        async commit() {
          if (failCommit) throw new Error('permission-denied');
          batches.push(ops);
          for (const [kind, path, value] of ops) kind === 'set' ? records.set(path, value) : records.delete(path);
        }
      };
    },
    async runTransaction(db, callback) {
      const staged = []; let wrote = false;
      const result = await callback({
        async get(ref) { assert.equal(wrote, false, 'all transaction reads precede writes'); return read(ref); },
        set(ref, value) { wrote = true; staged.push([ref.path, core.clone(value)]); }
      });
      if (failCommit) throw new Error('transaction-failed');
      staged.forEach(([path, value]) => records.set(path, value));
      batches.push(staged);
      return result;
    },
    onSnapshot: (ref, options, callback, error) => { listeners.push({ ref, options, callback, error }); return () => {}; }, signOut: async () => {}, onAuthStateChanged() {},
    signInWithEmailAndPassword() {}, createUserWithEmailAndPassword() {}, sendPasswordResetEmail() {},
    EmailAuthProvider: { credential: () => ({}) }, reauthenticateWithCredential: async () => {},
    updatePassword: async () => {}, validatePassword: async () => ({ isValid: true }),
    document: { getElementById: element, querySelector: selector => selector.includes(':checked') ? { value: 'كاش' } : null,
      querySelectorAll: () => [], createElement: () => element('created-' + fields.size), addEventListener() {} },
    navigator: {}, localStorage: { getItem() {}, setItem() {} },
    addEventListener() {}, setTimeout() {}, clearTimeout() {}, setInterval() {}, clearInterval() {}, queueMicrotask,
    URL, Blob, crypto: globalThis.crypto, prompt: () => null, __exposed: {}
  };
  context.window = context;
  vm.createContext(context);
  vm.runInContext(source.replace(/^import [\s\S]*?;\s*/gm, '').replace('Object.assign(window,{', 'Object.assign(globalThis.__exposed,{') + `
    globalThis.api = { DB, cache, loadedCollections, writer, commitDebtPayment, saveRepair, saveCustomer,
      renderCustomers, showCustomerHistory, htmlJSArg, startListeners, activeActions, toast, saveSettings,
      importBackup, exportBackup, closeModal, saveExpense, expenseCashId, deleteRepair, linkedCash, activateAccount, openAuthorizedSession,
      auth, adminAccounts, getAuthMode: () => authMode };
  `, context);
  core.COLLECTIONS.concat('settings').forEach(key => context.api.loadedCollections.add(key));
  return { ...context.api, records, batches, element, messages, listeners, context };
}

test('application modules parse and the page loads local entry points', () => {
  new vm.SourceTextModule(source);
  new vm.SourceTextModule(fs.readFileSync(new URL('../app-core.mjs', import.meta.url), 'utf8'));
  assert.match(html, /src="app.js"/);
  assert.match(html, /href="styles.css"/);
  assert.equal(/2687/.test(source), false);
  assert.equal(/await migrateRootDataToUser\(user.uid\)/.test(source), false);
});

test('editing a retrieved nested record produces a persisted write without mutating cache first', async () => {
  const app = harness();
  app.cache.employees = [{ id: 'e1', name: 'قديم', withdrawals: [{ amount: 5 }] }];
  const rows = app.DB.get('employees'); rows[0].name = 'جديد'; rows[0].withdrawals[0].amount = 10;
  assert.equal(app.cache.employees[0].name, 'قديم');
  assert.equal(app.cache.employees[0].withdrawals[0].amount, 5);
  await app.DB.set('employees', rows);
  assert.equal(app.records.get('users/u1/employees/e1').name, 'جديد');
});

test('related writes commit in one batch and queue captures independent values', async () => {
  const app = harness();
  const row = { id: 'r1', paid: 5 };
  const first = app.DB.set('repairs', [row]); row.paid = 99;
  const second = app.DB.set('cash_transactions', [{ id: 'c1', amount: 5 }]);
  await Promise.all([first, second]);
  assert.equal(app.batches.length, 1);
  assert.equal(app.batches[0].length, 2);
  assert.equal(app.records.get('users/u1/repairs/r1').paid, 5);
});

test('failed batch restores optimistic data and never announces success', async () => {
  const app = harness({ failCommit: true });
  app.cache.employees = [{ id: 'e1', name: 'قبل' }];
  const promise = app.DB.set('employees', [{ id: 'e1', name: 'بعد' }]);
  app.toast('success marker');
  await assert.rejects(promise, /permission-denied/);
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(app.cache.employees[0].name, 'قبل');
  assert.equal(app.records.size, 0);
  assert.equal(app.messages.some(message => message.textContent.includes('success marker')), false);
});

test('writes are blocked before collections finish loading', () => {
  const app = harness(); app.loadedCollections.delete('repairs');
  assert.throws(() => app.DB.set('repairs', []), /تحميل/);
});

test('invalid and excessive payments never write', async () => {
  for (const amount of [-10, 0, NaN, Infinity, 150, 0.001]) {
    const app = harness(); app.records.set('users/u1/debts/d1', { id: 'd1', name: 'عميل', amount: 100, paid: 0 });
    await assert.rejects(app.commitDebtPayment({ debtId: 'd1', amount, method: 'كاش', date: '2026-11-24' }));
    assert.equal(app.batches.length, 0);
    assert.equal(app.records.get('users/u1/debts/d1').paid, 0);
  }
});

test('payment uses server balance and atomically updates payment, debt, repair and cash', async () => {
  const app = harness();
  app.cache.debts = [{ id: 'd1', amount: 100, paid: 0 }]; // Deliberately stale browser state.
  app.records.set('users/u1/debts/d1', { id: 'd1', name: 'عميل', amount: 100, paid: 80, repairId: 'r1' });
  app.records.set('users/u1/repairs/r1', { id: 'r1', paid: 80 });
  await assert.rejects(app.commitDebtPayment({ debtId: 'd1', amount: 30, method: 'كاش', date: '2026-11-24' }), /يتجاوز/);
  await app.commitDebtPayment({ debtId: 'd1', amount: 20, method: 'بنك', date: '2026-11-24' });
  assert.equal(app.batches.length, 1);
  assert.equal(app.batches[0].length, 4);
  assert.equal(app.records.get('users/u1/debts/d1').paid, 100);
  assert.equal(app.records.get('users/u1/debts/d1').status, 'مسدد');
  assert.equal(app.records.get('users/u1/repairs/r1').paid, 100);
  const cash = [...app.records.entries()].find(([path]) => path.includes('/cash_transactions/'))[1];
  assert.equal(cash.type, 'bank_in');
});

test('editing a payment applies only its difference to both debt and repair', async () => {
  const app = harness();
  app.records.set('users/u1/debts/d1', { name: 'عميل', amount: 100, paid: 50, repairId: 'r1' });
  app.records.set('users/u1/repairs/r1', { paid: 50 });
  app.records.set('users/u1/debt_payments/p1', { id: 'p1', debtId: 'd1', amount: 20, cashId: 'c1' });
  app.records.set('users/u1/cash_transactions/c1', { amount: 20 });
  await app.commitDebtPayment({ debtId: 'd1', paymentId: 'p1', amount: 10, method: 'كاش', date: '2026-11-24' });
  assert.equal(app.records.get('users/u1/debts/d1').paid, 40);
  assert.equal(app.records.get('users/u1/repairs/r1').paid, 40);
  assert.equal(app.records.get('users/u1/cash_transactions/c1').amount, 10);
});

test('transaction failure leaves every financial document unchanged', async () => {
  const app = harness({ failCommit: true });
  app.records.set('users/u1/debts/d1', { name: 'عميل', amount: 100, paid: 0 });
  await assert.rejects(app.commitDebtPayment({ debtId: 'd1', amount: 10, method: 'كاش', date: '2026-11-24' }), /transaction-failed/);
  assert.equal(app.records.size, 1); assert.equal(app.records.get('users/u1/debts/d1').paid, 0);
});

test('new full backups retain all collections; old backups never erase missing sections', () => {
  const collections = Object.fromEntries(core.COLLECTIONS.map(key => [key, []]));
  collections.debt_payments.push({ id: 'p1', amount: 10 });
  const restored = core.validateBackup({ schemaVersion: 2, collections, settings: { shopName: 'محل' } });
  assert.equal(Object.keys(restored.collections).length, 10);
  assert.equal(restored.collections.debt_payments[0].amount, 10);
  const old = core.validateBackup({ repairs: [], cash: [], customers: [] });
  assert.deepEqual(Object.keys(old.collections).sort(), ['cash_transactions', 'customers', 'repairs']);
  assert.equal(old.collections.employees, undefined);
});

test('backup validation rejects malformed, duplicate, unsafe and unsupported data', () => {
  for (const data of [{}, null, [], { schemaVersion: 99 }, { repairs: 'bad' },
    { repairs: [{ id: 'x' }, { id: 'x' }] }, { repairs: [{ id: "x');alert(1)//" }] },
    { debts: [{ id: 'd1', amount: -5 }] }, { schemaVersion: 2, collections: { repairs: [] } }]) {
    assert.throws(() => core.validateBackup(data));
  }
});

test('oversized operations fail before any database commit', async () => {
  let commits = 0;
  const queue = core.createWriteQueue(() => ({ set() {}, delete() {}, async commit() { commits++; } }));
  const writes = Array.from({ length: 451 }, (_, index) => queue.set(index, { id: String(index) }));
  const results = await Promise.allSettled(writes);
  assert.equal(commits, 0); assert.equal(results.every(result => result.status === 'rejected'), true);
});

test('customer rename preserves legacy history and commits with the customer', async () => {
  const app = harness();
  app.cache.customers = [{ id: 'c1', name: 'قديم' }];
  app.cache.repairs = [{ id: 'r1', customer: 'قديم' }];
  app.cache.debts = [{ id: 'd1', name: 'قديم', amount: 100, paid: 0 }];
  app.cache.cash_transactions = [{ id: 'cash1', person: 'قديم' }];
  app.element('cust-edit-id').value = 'c1'; app.element('cust-name').value = 'جديد';
  app.element('cust-phone').value = '123'; app.element('cust-type').value = 'زبون';
  await app.saveCustomer();
  assert.equal(app.batches.length, 1);
  assert.equal(app.records.get('users/u1/repairs/r1').customer, 'جديد');
  assert.equal(app.records.get('users/u1/debts/d1').name, 'جديد');
  assert.equal(app.records.get('users/u1/cash_transactions/cash1').person, 'جديد');
});

test('customer text is escaped and dynamic actions do not interpolate executable names', () => {
  const app = harness();
  app.cache.customers = [{ id: 'c1', name: '<img src=x onerror=alert(1)>', phone: '<svg/onload=alert(2)>', type: 'زبون' }];
  app.cache.repairs = [{ customer: app.cache.customers[0].name, paid: 5 }];
  app.renderCustomers();
  const markup = app.element('customers-tbody').innerHTML;
  assert.equal(markup.includes('<img'), false); assert.equal(markup.includes('<svg'), false);
  assert.match(markup, /&lt;img/); assert.match(markup, /data-customer-history=/);
  assert.equal(markup.includes('onclick="showCustomerHistory'), false);
  assert.equal(core.escapeHTML('<>&"\''), '&lt;&gt;&amp;&quot;&#39;');
  const js = app.htmlJSArg("x');alert(1);//\"");
  const decoded = js.replace(/&quot;/g, '"').replace(/&#39;/g, "'").replace(/&amp;/g, '&');
  assert.equal(JSON.parse(decoded), "x');alert(1);//\"");
});

test('custom delete credentials are salted, verified and reject malformed parameters', async () => {
  const first = await core.createDeleteCredential('test-password');
  const second = await core.createDeleteCredential('test-password');
  assert.notEqual(first.salt, second.salt); assert.notEqual(first.hash, second.hash);
  assert.equal(JSON.stringify(first).includes('test-password'), false);
  assert.equal(await core.verifyDeleteCredential('test-password', first), true);
  assert.equal(await core.verifyDeleteCredential('wrong-password', first), false);
  assert.equal(await core.verifyDeleteCredential('test-password', { ...first, iterations: 999999999 }), false);
});

test('collections become ready only after a server snapshot', () => {
  const app = harness(); app.startListeners();
  const listener = app.listeners.find(item => item.ref.path.endsWith('/customers'));
  listener.callback({ docs: [], metadata: { fromCache: true } });
  assert.equal(app.loadedCollections.has('customers'), false);
  listener.callback({ docs: [], metadata: { fromCache: false } });
  assert.equal(app.loadedCollections.has('customers'), true);
  assert.equal(listener.options.includeMetadataChanges, true);
});

test('failed new repair preserves input and rolls back all related optimistic collections', async () => {
  const app = harness({ failCommit: true });
  app.element('rep-customer').value = 'عميل'; app.element('rep-paid').value = '25';
  app.element('rep-model').value = 'Phone';
  await app.saveRepair();
  assert.equal(app.records.size, 0);
  assert.equal(app.cache.repairs.length, 0);
  assert.equal(app.cache.cash_transactions.length, 0);
  assert.equal(app.cache.customers.length, 0);
  assert.equal(app.element('rep-customer').value, 'عميل');
  assert.equal(app.activeActions.size, 0);
});

test('double submit creates only one repair and its related cash and customer', async () => {
  const app = harness(); app.element('rep-customer').value = 'عميل'; app.element('rep-paid').value = '25';
  await Promise.all([app.saveRepair(), app.saveRepair()]);
  assert.equal(app.batches.length, 1);
  assert.equal([...app.records.keys()].filter(path => path.includes('/repairs/')).length, 1);
  assert.equal([...app.records.keys()].filter(path => path.includes('/cash_transactions/')).length, 1);
});

test('expense editing changes its linked cash only, despite equal dates and amounts', async () => {
  const app = harness();
  app.cache.expenses = [{ id: 'e1', cashId: 'cash1', category: 'قطع', desc: 'شاشة', amount: 20, date: '2026-11-24', payMethod: 'كاش' }];
  app.cache.cash_transactions = [{ id: 'unrelated', amount: 20, date: '2026-11-24', desc: 'مصروف آخر' }, { id: 'cash1', amount: 20 }];
  for (const [id, value] of Object.entries({ 'expense-desc': 'شاشة', 'expense-amount': '30', 'expense-cat': 'قطع', 'expense-edit-id': 'e1', 'expense-date': '2026-11-24' })) app.element(id).value = value;
  await app.saveExpense();
  assert.equal(app.batches.length, 1);
  assert.equal(app.records.get('users/u1/cash_transactions/cash1').amount, 30);
  assert.equal(app.cache.cash_transactions.find(row => row.id === 'unrelated').amount, 20);
  assert.equal(app.records.has('users/u1/cash_transactions/unrelated'), false);
});

test('ambiguous old expense linkage is rejected before any write', () => {
  const app = harness();
  const expense = { category: 'قطع', desc: 'شاشة', amount: 20, date: '2026-11-24', payMethod: 'كاش' };
  const cash = ['c1', 'c2'].map(id => ({ id, type: 'out', desc: 'مصروف: قطع — شاشة', amount: 20, date: '2026-11-24' }));
  assert.throws(() => app.expenseCashId(expense, cash), /بدقة/);
  assert.equal(app.batches.length, 0);
});

test('backup import does not write until the user confirms the reviewed plan', async () => {
  const app = harness();
  const collections = Object.fromEntries(core.COLLECTIONS.map(key => [key, []]));
  collections.employees = [{ id: 'e1', name: 'موظف' }];
  const file = { size: 100, text: async () => JSON.stringify({ schemaVersion: 2, collections, settings: { shopName: 'محل' } }) };
  await app.importBackup({ target: { files: [file], value: 'file' } });
  assert.equal(app.records.size, 0);
  assert.match(app.element('confirm-message').textContent, /استيراد 1 سجل/);
  await app.element('confirm-ok').onclick();
  assert.equal(app.batches.length, 1);
  assert.equal(app.records.get('users/u1/employees/e1').name, 'موظف');
});

test('backup import refuses a stale review if records changed before confirmation', async () => {
  const app = harness();
  await app.importBackup({ target: { files: [{ size: 10, text: async () => JSON.stringify({ customers: [] }) }], value: '' } });
  app.cache.customers.push({ id: 'c1', name: 'أضيف بعد المراجعة' });
  await app.element('confirm-ok').onclick();
  assert.equal(app.batches.length, 0);
  assert.equal(app.cache.customers.length, 1);
});

test('every static inline event handler can reach its function in the browser module', () => {
  const app = harness();
  const explicit = new Set([...source.matchAll(/window\.(\w+)\s*=/g)].map(match => match[1]));
  const available = new Set([...Object.keys(app.context.__exposed), ...explicit, 'if', 'setTimeout', 'parseFloat', 'Number']);
  const missing = new Set();
  for (const match of html.matchAll(/\bon(?:click|change|input|keydown)="([^"]*)"/g)) {
    for (const call of match[1].matchAll(/(?<![.\w])([A-Za-z_]\w*)\(/g)) {
      if (!available.has(call[1])) missing.add(call[1]);
    }
  }
  assert.deepEqual([...missing], []);
});

test('every local HTML asset exists and installation manifest is valid JSON', () => {
  for (const match of html.matchAll(/(?:src|href)="([^"#]+)"/g)) {
    if (!/^(?:https?:|data:)/.test(match[1])) assert.equal(fs.existsSync(new URL('../' + match[1], import.meta.url)), true, match[1]);
  }
  const manifest = JSON.parse(fs.readFileSync(new URL('../manifest.json', import.meta.url), 'utf8'));
  assert.equal(manifest.dir, 'rtl');
  assert.equal(fs.existsSync(new URL('../' + manifest.icons[0].src, import.meta.url)), true);
});

test('saving settings migrates a legacy delete password without retaining plaintext', async () => {
  const app = harness();
  app.cache.settings = { deletePassword: 'old-secret', modules: { repair: true } };
  app.element('set-shop-name').value = 'محل';
  await app.saveSettings();
  const settings = app.records.get('users/u1/settings/main');
  assert.equal(settings.deletePassword, undefined);
  assert.equal(JSON.stringify(settings).includes('old-secret'), false);
  assert.equal(settings.modules.repair, true);
  assert.equal(await core.verifyDeleteCredential('old-secret', settings.deleteCredential), true);
});

test('malformed existing debt balances are rejected rather than persisted as NaN', () => {
  for (const debt of [{ amount: undefined, paid: 0 }, { amount: 100, paid: 'invalid' }, { amount: 100, paid: -5 }]) {
    assert.throws(() => core.paymentBalance(debt, 10), /غير صالح/);
  }
});

test('signup activates the registry, consumes its code and saves settings in one transaction', async () => {
  const app = harness();
  app.records.set('activation_codes/ABCD1234', { code: 'ABCD1234', used: false, createdBy: 'owner' });
  await app.activateAccount({ uid: 'new', email: 'new@example.test' }, 'abcd1234', 'New shop');
  assert.equal(app.batches.length, 1);
  assert.equal(app.batches[0].length, 3);
  assert.equal(app.records.get('activation_codes/ABCD1234').usedByUid, 'new');
  assert.equal(app.records.get('users_registry/new').activationCode, 'ABCD1234');
  assert.equal(app.records.get('users/new/settings/main').shopName, 'New shop');
});

test('used activation codes and existing accounts never receive new activation writes', async () => {
  const used = harness();
  used.records.set('activation_codes/ABCD1234', { code: 'ABCD1234', used: true });
  await assert.rejects(used.activateAccount({ uid: 'new', email: 'new@example.test' }, 'ABCD1234', ''), /مستخدم/);
  assert.equal(used.batches.length, 0);
  const existing = harness(); existing.records.set('users_registry/u1', { active: false });
  await assert.rejects(existing.activateAccount({ uid: 'u1', email: 'test@example.test' }, 'ABCD1234', ''), /مسجل مسبقاً/);
  assert.equal(existing.batches.length, 0);
});

test('login never creates or reactivates an ordinary user registry', async () => {
  const app = harness();
  await app.openAuthorizedSession({ uid: 'u1', email: 'test@example.test' });
  assert.equal(app.records.has('users_registry/u1'), false);
  assert.equal(app.batches.length, 0);
  assert.match(app.element('l-err').textContent, /غير مفعّل/);
});

test('signup password confirmation is checked before creating an account', async () => {
  const app = harness(); let created = 0;
  app.context.createUserWithEmailAndPassword = async () => { created++; };
  for (const [id, value] of Object.entries({ 'l-email': 'new@example.test', 'l-pass': 'New-password-123', 'l-pass-confirm': 'different', 'l-actcode': 'ABCD1234' })) app.element(id).value = value;
  await app.context.doSignup();
  assert.equal(created, 0); assert.match(app.element('l-err').textContent, /غير متطابقتين/);
});

test('Enter routes to signup or reset according to the visible authentication view', async () => {
  const app = harness(); const calls = [];
  app.context.doSignup = () => calls.push('signup');
  app.context.doLogin = () => calls.push('login');
  app.context.requestLoginPasswordReset = () => calls.push('reset');
  app.context.toggleSignup(true); app.context.submitAuthForm();
  assert.equal(app.getAuthMode(), 'signup');
  app.context.openPasswordRecovery(); app.context.submitAuthForm();
  assert.equal(app.element('l-password-section').style.display, 'none');
  app.context.toggleSignup(false); app.context.submitAuthForm();
  assert.deepEqual(calls, ['signup', 'reset', 'login']);
});

test('successful password change reauthenticates first, clears secrets, signs out and preserves account data', async () => {
  const app = harness(); const calls = [];
  app.records.set('users/u1/repairs/r1', { id: 'r1', paid: 20 });
  app.context.reauthenticateWithCredential = async user => { calls.push('reauth:' + user.uid); };
  app.context.updatePassword = async user => { calls.push('update:' + user.uid); };
  app.context.signOut = async () => { calls.push('signout'); };
  app.element('account-current-password').value = 'Old-password-123';
  app.element('account-new-password').value = 'New-password-456';
  app.element('account-confirm-password').value = 'New-password-456';
  await app.context.saveAccountPassword();
  assert.deepEqual(calls, ['reauth:u1', 'update:u1', 'signout']);
  assert.equal(app.records.get('users/u1/repairs/r1').paid, 20);
  assert.equal(app.element('account-current-password').value, '');
  assert.equal(app.element('account-new-password').value, '');
  assert.equal(app.element('account-confirm-password').value, '');
  assert.match(app.element('l-notice').textContent, /تم تغيير/);
  assert.equal(app.element('l-email').value, 'test@example.test');
});

test('wrong current password leaves the password and sign-in state untouched', async () => {
  const app = harness(); let updated = 0, signedOut = 0;
  app.context.reauthenticateWithCredential = async () => { throw { code: 'auth/invalid-credential' }; };
  app.context.updatePassword = async () => { updated++; };
  app.context.signOut = async () => { signedOut++; };
  app.element('account-current-password').value = 'Wrong-old-password';
  app.element('account-new-password').value = 'New-password-456';
  app.element('account-confirm-password').value = 'New-password-456';
  await app.context.saveAccountPassword();
  assert.equal(updated, 0); assert.equal(signedOut, 0);
  assert.match(app.element('account-password-message').textContent, /الحالية غير صحيحة/);
  assert.equal(app.element('account-current-password').value, '');
});

test('recovery does not reveal account existence and sends each address only once during cooldown', async () => {
  const app = harness(); const sent = [];
  app.context.sendPasswordResetEmail = async (auth, email, options) => { sent.push({ email, url: options.url }); throw { code: 'auth/user-not-found' }; };
  app.element('l-email').value = 'Unknown@Example.test';
  await app.context.requestLoginPasswordReset(); await app.context.requestLoginPasswordReset();
  assert.deepEqual(sent, [{ email: 'unknown@example.test', url: 'https://alhram.online/' }]);
  assert.match(app.element('l-notice').textContent, /إذا كان البريد مسجلاً/);
  assert.equal(app.element('l-err').textContent, '');
});

test('administrator reset is restricted to the verified owner and registered target', async () => {
  const app = harness(); let sent = 0;
  app.context.sendPasswordResetEmail = async () => { sent++; };
  app.adminAccounts.set('target', { uid: 'target', email: 'target@example.test' });
  await app.context.adminResetPassword('target'); assert.equal(sent, 0);
  const owner = source.match(/const OWNER_UID = '([^']+)'/)[1]; app.auth.currentUser.uid = owner;
  app.records.set('users_registry/target', { uid: 'target', email: 'target@example.test', active: true });
  await app.context.adminResetPassword('target');
  assert.equal(app.element('confirm-ok').textContent, 'إرسال الرابط');
  await app.element('confirm-ok').onclick();
  assert.equal(sent, 1); assert.equal(app.batches.length, 0);
});
