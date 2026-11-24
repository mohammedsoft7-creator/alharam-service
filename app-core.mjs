// Pure helpers shared by the application and offline regression tests.
export const COLLECTIONS = ['repairs', 'cash_transactions', 'debts', 'customers',
  'faults', 'expenses', 'exp_categories', 'employees', 'withdrawals', 'debt_payments'];

export const clone = value => JSON.parse(JSON.stringify(value));
export const roundMoney = value => Math.round((Number(value) + Number.EPSILON) * 100) / 100;
export const positiveMoney = value => Number.isFinite(Number(value)) && Number(value) > 0;
export const escapeHTML = value => String(value ?? '').replace(/[&<>"']/g,
  char => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[char]));

const hex = bytes => [...bytes].map(byte => byte.toString(16).padStart(2, '0')).join('');
async function derivePassword(password, salt, iterations) {
  const key = await crypto.subtle.importKey('raw', new TextEncoder().encode(password), 'PBKDF2', false, ['deriveBits']);
  const bytes = Uint8Array.from(salt.match(/../g), pair => parseInt(pair, 16));
  return hex(new Uint8Array(await crypto.subtle.deriveBits({ name: 'PBKDF2', hash: 'SHA-256', salt: bytes, iterations }, key, 256)));
}
export async function createDeleteCredential(password) {
  const salt = hex(crypto.getRandomValues(new Uint8Array(16)));
  const iterations = 210000;
  return { algorithm: 'PBKDF2-SHA256', salt, iterations, hash: await derivePassword(password, salt, iterations) };
}
export async function verifyDeleteCredential(password, credential) {
  if (credential?.algorithm !== 'PBKDF2-SHA256' || !/^[a-f0-9]{32}$/.test(credential.salt) ||
      !/^[a-f0-9]{64}$/.test(credential.hash) || !Number.isInteger(credential.iterations) ||
      credential.iterations < 100000 || credential.iterations > 1000000) return false;
  const actual = await derivePassword(password, credential.salt, credential.iterations);
  return [...actual].reduce((difference, char, index) => difference | (char.charCodeAt(0) ^ credential.hash.charCodeAt(index)), 0) === 0;
}

export function paymentBalance(debt, amount, oldAmount = 0) {
  if (!positiveMoney(amount)) throw new Error('أدخل مبلغاً موجباً وصحيحاً');
  if (!positiveMoney(debt.amount) || !Number.isFinite(Number(debt.paid || 0)) || Number(debt.paid || 0) < 0 ||
      !Number.isFinite(Number(oldAmount)) || oldAmount < 0 || Number(oldAmount) > Number(debt.paid || 0)) {
    throw new Error('رصيد الدين غير صالح؛ راجع السجل قبل تسجيل الدفعة');
  }
  const paid = roundMoney(Number(debt.paid || 0) - Number(oldAmount) + Number(amount));
  if (paid < 0 || paid > roundMoney(debt.amount)) throw new Error('المبلغ يتجاوز رصيد الدين المتبقي');
  return { paid, status: paid === roundMoney(debt.amount) ? 'مسدد' : 'مستحق' };
}

export function validateBackup(input) {
  if (!input || typeof input !== 'object' || Array.isArray(input)) throw new Error('ملف النسخة غير صالح');
  if (input.schemaVersion != null && ![1, 2].includes(input.schemaVersion)) throw new Error('إصدار النسخة غير مدعوم');
  const source = input.schemaVersion === 2 ? input.collections : { ...input, cash_transactions: input.cash };
  if (!source || typeof source !== 'object') throw new Error('بيانات النسخة غير مكتملة');
  const collections = {};
  for (const key of COLLECTIONS) {
    if (source[key] === undefined) {
      if (input.schemaVersion === 2) throw new Error('النسخة الكاملة تفتقد: ' + key);
      continue; // Legacy backups must not erase collections they never contained.
    }
    if (!Array.isArray(source[key])) throw new Error('قائمة غير صالحة: ' + key);
    const ids = new Set();
    for (const row of source[key]) {
      if (!row || typeof row !== 'object' || Array.isArray(row) ||
          typeof row.id !== 'string' || !/^[A-Za-z0-9_.-]{1,128}$/.test(row.id) || ids.has(row.id)) {
        throw new Error('معرّف مكرر أو غير صالح في: ' + key);
      }
      ids.add(row.id);
      for (const field of ['amount', 'paid', 'partsCost', 'usdAmount', 'debtAmount', 'total']) {
        if (row[field] != null && (row[field] === '' || !Number.isFinite(Number(row[field])) || Number(row[field]) < 0)) {
          throw new Error('مبلغ غير صالح في: ' + key);
        }
      }
    }
    collections[key] = clone(source[key]);
  }
  if (!Object.keys(collections).length) throw new Error('الملف لا يحتوي سجلات التطبيق');
  if (input.settings != null && (typeof input.settings !== 'object' || Array.isArray(input.settings))) {
    throw new Error('إعدادات غير صالحة');
  }
  return { collections, settings: input.settings == null ? undefined : clone(input.settings) };
}

// Writes issued by one synchronous user action share one atomic Firestore batch.
// The limit is checked before commit so a large import never succeeds partially.
export function createWriteQueue(createBatch, onState = () => {}) {
  let queued = [], scheduled = false, failed = false;
  const pending = new Set();
  function enqueue(kind, ref, value, options) {
    if (!pending.size) failed = false;
    const promise = new Promise((resolve, reject) => queued.push({ kind, ref, value: value === undefined ? undefined : clone(value), options, resolve, reject }));
    pending.add(promise);
    onState('saving');
    const settle = () => {
      pending.delete(promise);
      if (!pending.size) onState(failed ? 'error' : 'saved');
    };
    promise.then(settle, settle); // Mark rejection handled even for legacy event handlers.
    if (!scheduled) { scheduled = true; queueMicrotask(flush); }
    return promise;
  }
  async function flush() {
    const entries = queued; queued = []; scheduled = false;
    try {
      if (entries.length > 450) throw new Error('العملية كبيرة جداً للحفظ الآمن دفعة واحدة (الحد 450 كتابة)');
      const batch = createBatch();
      for (const entry of entries) {
        if (entry.kind === 'delete') batch.delete(entry.ref);
        else if (entry.options) batch.set(entry.ref, entry.value, entry.options);
        else batch.set(entry.ref, entry.value);
      }
      await batch.commit();
      entries.forEach(entry => entry.resolve());
    } catch (error) {
      failed = true;
      onState('error', error);
      entries.forEach(entry => entry.reject(error));
    }
  }
  return {
    set: (ref, value, options) => enqueue('set', ref, value, options),
    delete: ref => enqueue('delete', ref),
    get pending() { return pending.size; },
    async wait() {
      while (pending.size) {
        const results = await Promise.allSettled([...pending]);
        const failure = results.find(result => result.status === 'rejected');
        if (failure) throw failure.reason;
      }
    }
  };
}
