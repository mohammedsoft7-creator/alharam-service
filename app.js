import { initializeApp }    from 'https://www.gstatic.com/firebasejs/10.12.0/firebase-app.js';
import { getFirestore, collection, doc, writeBatch, runTransaction, onSnapshot, getDocs, getDoc }
  from 'https://www.gstatic.com/firebasejs/10.12.0/firebase-firestore.js';
import { getAuth, signInWithEmailAndPassword, createUserWithEmailAndPassword, signOut, onAuthStateChanged, sendPasswordResetEmail, EmailAuthProvider, reauthenticateWithCredential, updatePassword, validatePassword }
  from 'https://www.gstatic.com/firebasejs/10.12.0/firebase-auth.js';

import { COLLECTIONS, clone, roundMoney, positiveMoney, escapeHTML, paymentBalance, validateBackup, createWriteQueue, createDeleteCredential, verifyDeleteCredential } from './app-core.mjs';

import { accountErrorMessage, accountValidation, normalizedEmail, checkNewPassword, changeOwnPassword, createPasswordResetSender } from './account-security.mjs';

// ── Config ──────────────────────────────────────────────
const app  = initializeApp({
  apiKey:            "AIzaSyBBfeZjLKQ16JdIz9B5V2kHpKQSUwb6b4Q",
  authDomain:        "alhram-teknik-servies.firebaseapp.com",
  projectId:         "alhram-teknik-servies",
  storageBucket:     "alhram-teknik-servies.firebasestorage.app",
  messagingSenderId: "47174101606",
  appId:             "1:47174101606:web:aabecde5b74db3576edba7"
});
const fdb  = getFirestore(app);
const auth = getAuth(app);
auth.languageCode = 'ar';
const requestAccountReset = createPasswordResetSender(email => sendPasswordResetEmail(auth, email, { url: 'https://alhram.online/' }));
const adminAccounts = new Map();

// ── Cache (in-memory mirror of Firestore) ───────────────
const cache = { repairs:[], cash_transactions:[], debts:[], customers:[], settings:{}, faults:[], expenses:[], exp_categories:[], employees:[], withdrawals:[], debt_payments:[] };

// ── DB API (sync reads, async writes) ───────────────────
const K_REPAIRS='repairs', K_CASH='cash_transactions', K_DEBTS='debts', K_CUSTOMERS='customers', K_SETTINGS='settings', K_FAULTS='faults', K_EXPENSES='expenses', K_EXP_CATS='exp_categories', K_EMPLOYEES='employees', K_WITHDRAWALS='withdrawals', K_DEBT_PAYMENTS='debt_payments';

const loadedCollections = new Set();
const optimisticWrites = new Map();
const activeActions = new Set();
let sessionEpoch = 0;
function updateSyncStatus(state, error) {
  const el = document.getElementById('sync-status');
  if (!el) return;
  el.dataset.state = state;
  el.textContent = ({ saving: 'جارٍ الحفظ…', saved: 'تم الحفظ', error: 'تعذر الحفظ — تحقق من الاتصال', loading: 'جارٍ تحميل البيانات…', offline: 'غير متصل بالإنترنت' })[state] || state;
  el.title = error?.message || '';
  if (state === 'error' && error) toast(error.message || 'تعذر الحفظ؛ لم يتم تأكيد العملية', 'error');
}
const writer = createWriteQueue(() => writeBatch(fdb), updateSyncStatus);
const setDoc = (ref, value, options) => writer.set(ref, value, options);
const deleteDoc = ref => writer.delete(ref);
function assertLoaded(key) {
  if (!auth.currentUser) throw new Error('يرجى تسجيل الدخول');
  if (!loadedCollections.has(key)) throw new Error('انتظر اكتمال تحميل البيانات قبل التعديل');
}
function rememberWrite(key, before, next, promise) {
  if (!optimisticWrites.has(key)) optimisticWrites.set(key, { before: clone(before), epoch: sessionEpoch });
  const journal = optimisticWrites.get(key);
  journal.expected = JSON.stringify(next);
  promise.then(() => {
    if (optimisticWrites.get(key) === journal) optimisticWrites.delete(key);
  }, () => {
    if (optimisticWrites.get(key) !== journal) return;
    if (journal.epoch === sessionEpoch && JSON.stringify(cache[key]) === journal.expected) cache[key] = journal.before;
    optimisticWrites.delete(key);
    refreshCurrentPage();
  });
  return promise;
}
const DB = {
  get: key => clone(cache[key] || []),
  getObj: (key, defaults = {}) => key === K_SETTINGS ? clone({ ...defaults, ...cache.settings }) : clone(defaults),
  set(key, rows) {
    assertLoaded(key);
    const userId = auth.currentUser.uid;
    const before = clone(cache[key] || []);
    const next = clone(rows);
    const oldMap = new Map(before.map(row => [row.id, JSON.stringify(row)]));
    const ids = new Set(next.map(row => row.id));
    const writes = [];
    for (const row of before) if (!ids.has(row.id)) writes.push(deleteDoc(doc(fdb, 'users', userId, key, row.id)));
    for (const row of next) if (oldMap.get(row.id) !== JSON.stringify(row)) writes.push(setDoc(doc(fdb, 'users', userId, key, row.id), row));
    cache[key] = next;
    return rememberWrite(key, before, next, Promise.all(writes));
  },
  setObj(key, value) {
    assertLoaded(K_SETTINGS);
    const before = clone(cache.settings);
    cache.settings = clone(value);
    return rememberWrite(K_SETTINGS, before, cache.settings,
      setDoc(doc(fdb, 'users', auth.currentUser.uid, 'settings', 'main'), value));
  }
};
async function saveAction(key, selector, task) {
  if (activeActions.has(key)) return;
  activeActions.add(key);
  const buttons = [...document.querySelectorAll(selector)].map(button => ({ button, disabled: button.disabled }));
  buttons.forEach(({ button }) => { button.disabled = true; button.setAttribute('aria-busy', 'true'); });
  try {
    return await task();
  } catch (error) {
    console.error('save', key, error);
    updateSyncStatus('error', error);

  } finally {
    activeActions.delete(key);
    buttons.forEach(({ button, disabled }) => { button.disabled = disabled; button.removeAttribute('aria-busy'); });
  }
}
function htmlJSArg(value) { return escapeHTML(JSON.stringify(String(value ?? ''))); }
function upsertCache(key, row) {
  const index = cache[key].findIndex(value => value.id === row.id);
  if (index === -1) cache[key].push(clone(row));
  else cache[key][index] = clone(row);
}

// ── Realtime Listeners ───────────────────────────────────
let started=false; const unsubs=[];
function startListeners(){
  if(started) return;
  const uid = auth.currentUser?.uid; if(!uid) return;
  started=true; loadedCollections.clear(); updateSyncStatus('loading');
  try {
    unsubs.push(onSnapshot(doc(fdb, 'users_registry', uid), { includeMetadataChanges: true }, snapshot => {
      if (!snapshot.metadata.fromCache && uid !== OWNER_UID && (!snapshot.exists() || snapshot.data().active !== true)) {
        stopListeners();
        Object.keys(cache).forEach(key => { cache[key] = Array.isArray(cache[key]) ? [] : {}; });
        document.getElementById('profit-section-content').style.display = 'none';
        toast('تم إيقاف تفعيل الحساب؛ تواصل مع المالك', 'error');
        void signOut(auth); showLogin();
      }
    }, failure => updateSyncStatus('error', failure)));
    [K_REPAIRS,K_CASH,K_DEBTS,K_CUSTOMERS,K_FAULTS,K_EXPENSES,K_EXP_CATS,K_EMPLOYEES,K_WITHDRAWALS,K_DEBT_PAYMENTS].forEach(col=>{
      unsubs.push(onSnapshot(collection(fdb,'users',uid,col), { includeMetadataChanges: true }, snap=>{
        cache[col]=snap.docs.map(d=>({...d.data(),id:d.id}));
        if (!snap.metadata.fromCache) loadedCollections.add(col);
        if (loadedCollections.size === COLLECTIONS.length + 1 && !writer.pending) updateSyncStatus('saved');
        refreshCurrentPage();
        if(col===K_FAULTS) loadFaultsDropdown();
        if(col===K_DEBTS || col===K_DEBT_PAYMENTS) updateNotifBadge();
      }, e=>{ console.error('snap',col,e.code); updateSyncStatus('error', e); }));
    });

    unsubs.push(onSnapshot(doc(fdb,'users',uid,'settings','main'), { includeMetadataChanges: true }, snap=>{
      cache.settings = snap.exists() ? snap.data() : {};
      if (!snap.metadata.fromCache) loadedCollections.add(K_SETTINGS);
      applySettings();
      if (loadedCollections.size === COLLECTIONS.length + 1 && !writer.pending) updateSyncStatus('saved');
    }, error => updateSyncStatus('error', error)));
  } catch(e) { console.error('startListeners error:', e); updateSyncStatus('error', e); }
}
function stopListeners(){ unsubs.forEach(u=>u()); unsubs.length=0; started=false; loadedCollections.clear(); optimisticWrites.clear(); sessionEpoch++; }

function applySettings(){
  const n=cache.settings.shopName;
  if(n) document.getElementById('shop-name-sidebar').textContent=n;
}

function refreshCurrentPage(){
  const a=document.querySelector('.page.active'); if(!a) return;
  const id=a.id.replace('page-','');
  if(id==='dashboard') renderDashboard();
  else if(id==='devices')   renderDevices();
  else if(id==='cash')      renderCash();
  else if(id==='expenses')  renderExpenses();
  else if(id==='employees') renderEmployees();
  else if(id==='debts')     renderDebts();
  else if(id==='customers') renderCustomers();
  else if(id==='reports')   renderReports();
}

// ── Auth ─────────────────────────────────────────────────
function showLoading(){ document.getElementById('s-loading').style.display='flex'; document.getElementById('s-login').style.display='none'; }
function showLogin(){ document.getElementById('s-loading').style.display='none'; document.getElementById('s-login').style.display='flex'; }
function showApp(){ document.getElementById('s-loading').style.display='none'; document.getElementById('s-login').style.display='none'; startPriceTicker(); }

/* ══ LIVE PRICE TICKER ══ */
let _tickerTimer = null;
let _lastPrices = null;

async function startPriceTicker() {
  if (_tickerTimer) return;
  _tickerTimer = setInterval(fetchPrices, 90000);
  // أظهر بيانات static أولاً ريثما تحمّل
  renderStaticTicker();
  await fetchPrices();

}

function renderStaticTicker() {
  const track = document.getElementById('ticker-track');
  if (!track) return;
  track.innerHTML = [
    `<span class="ticker-item"><span class="t-icon">₿</span><span class="t-name">BTC</span><span class="t-price" id="tk-btc">جاري التحميل...</span></span>`,
    `<span class="ticker-item"><span class="t-icon">Ξ</span><span class="t-name">ETH</span><span class="t-price" id="tk-eth">...</span></span>`,
    `<span class="ticker-item"><span class="t-icon">🥇</span><span class="t-name">GOLD</span><span class="t-price" id="tk-gold">...</span></span>`,
    `<span class="ticker-item"><span class="t-icon">💱</span><span class="t-name">EUR/USD</span><span class="t-price" id="tk-eur">...</span></span>`,
  ].join('') + [
    `<span class="ticker-item"><span class="t-icon">₿</span><span class="t-name">BTC</span><span class="t-price">•</span></span>`,
    `<span class="ticker-item"><span class="t-icon">Ξ</span><span class="t-name">ETH</span><span class="t-price">•</span></span>`,
    `<span class="ticker-item"><span class="t-icon">🥇</span><span class="t-name">GOLD</span><span class="t-price">•</span></span>`,
    `<span class="ticker-item"><span class="t-icon">💱</span><span class="t-name">EUR/USD</span><span class="t-price">•</span></span>`,
  ].join('');
}

async function fetchPrices() {
  const prices = {};

  // ── 1. Bitcoin + Ethereum (CoinGecko - مجاني بدون مفتاح) ──
  try {
    const r = await fetch(
      'https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd&include_24hr_change=true',
      { cache: 'no-store', signal: AbortSignal.timeout(8000) }
    );
    if (r.ok) {
      const d = await r.json();
      if (d.bitcoin)  prices.btc = { price: d.bitcoin.usd,  change: d.bitcoin.usd_24h_change };
      if (d.ethereum) prices.eth = { price: d.ethereum.usd, change: d.ethereum.usd_24h_change };
    }
  } catch(_) {}

  // ── 2. EUR/USD (Frankfurter - مجاني بدون مفتاح) ──
  try {
    const r = await fetch(
      'https://api.frankfurter.app/latest?from=USD&to=EUR',
      { cache: 'no-store', signal: AbortSignal.timeout(8000) }
    );
    if (r.ok) {
      const d = await r.json();
      if (d.rates?.EUR) prices.eur = { price: d.rates.EUR };
    }
  } catch(_) {}

  // ── 3. ذهب (metals.live - مجاني) ──
  try {
    const r = await fetch(
      'https://api.metals.live/v1/spot/gold',
      { cache: 'no-store', signal: AbortSignal.timeout(8000) }
    );
    if (r.ok) {
      const d = await r.json();
      const spot = Array.isArray(d) ? d[0]?.price : d?.price;
      if (spot) prices.gold = { price: spot };
    }
  } catch(_) {}

  // ── 4. fallback للذهب عبر XAU/USD ──
  if (!prices.gold) {
    try {
      const r = await fetch(
        'https://open.er-api.com/v6/latest/XAU',
        { cache: 'no-store', signal: AbortSignal.timeout(8000) }
      );
      if (r.ok) {
        const d = await r.json();
        if (d.rates?.USD) prices.gold = { price: d.rates.USD };
      }
    } catch(_) {}
  }

  // احفظ آخر قيم ناجحة
  if (Object.keys(prices).length > 0) {
    _lastPrices = { ..._lastPrices, ...prices };
  }

  renderTicker(_lastPrices || prices);
}

function renderTicker(prices) {
  const track = document.getElementById('ticker-track');
  if (!track) return;
  if (!prices || Object.keys(prices).length === 0) {
    track.innerHTML = '<span class="ticker-loading">📡 لا يوجد اتصال بالإنترنت</span>';
    return;
  }

  const fmt = (n, dec=0) => (+n).toLocaleString('en-US', {minimumFractionDigits:dec, maximumFractionDigits:dec});
  const chg = (c) => {
    if (c == null) return '';
    return c > 0
      ? `<span class="t-up">▲${Math.abs(c).toFixed(1)}%</span>`
      : c < 0
      ? `<span class="t-down">▼${Math.abs(c).toFixed(1)}%</span>`
      : '';
  };

  const items = [];
  if (prices.btc)  items.push(`<span class="ticker-item">₿ <span class="t-name">BTC</span> <span class="t-price">$${fmt(prices.btc.price)}</span> ${chg(prices.btc.change)}</span>`);
  if (prices.eth)  items.push(`<span class="ticker-item">Ξ <span class="t-name">ETH</span> <span class="t-price">$${fmt(prices.eth.price)}</span> ${chg(prices.eth.change)}</span>`);
  if (prices.gold) items.push(`<span class="ticker-item">🥇 <span class="t-name">GOLD</span> <span class="t-price">$${fmt(prices.gold.price)}</span></span>`);
  if (prices.eur)  items.push(`<span class="ticker-item">💱 <span class="t-name">EUR/USD</span> <span class="t-price">${(+prices.eur.price).toFixed(4)}</span></span>`);

  if (items.length === 0) {
    track.innerHTML = '<span class="ticker-loading">جاري المحاولة...</span>';
    return;
  }

  // مضاعفة لحركة لا نهائية
  const html = items.join('') + items.join('');
  track.innerHTML = html;
  track.style.animation = 'none';
  void track.offsetWidth;
  track.style.animation = '';
}



// ==================== DELETE PASSWORD SYSTEM ====================
let _pendingDeleteFn = null;
let _deleteConfirmed = false;
let _pendingDeleteMsg = '';

function requireDeletePass(msg, onConfirm) {
  const savedPass = cache.settings?.deleteCredential || cache.settings?.deletePassword;
  if (!savedPass) {
    // لو ما في كلمة سر محددة → نفذ مباشرة بعد confirm عادي
    document.getElementById('confirm-message').textContent = msg;
    document.getElementById('confirm-ok').onclick = () => { closeModal('modal-confirm'); onConfirm(); };
    openModal('modal-confirm');
    return;
  }
  // في كلمة سر → اعرض modal
  _pendingDeleteFn  = onConfirm;
  _pendingDeleteMsg = msg;
  document.getElementById('del-pass-msg').textContent = msg;
  document.getElementById('del-pass-input').value = '';
  document.getElementById('del-pass-err').textContent = '';
  openModal('modal-delete-pass');
}

window.confirmDeleteWithPass = async function() {
  return saveAction('delete-confirmation', '#modal-delete-pass button', async () => {
    const entered = document.getElementById('del-pass-input').value;
    const settings = DB.getObj(K_SETTINGS);
    const valid = settings.deleteCredential ? await verifyDeleteCredential(entered, settings.deleteCredential) : entered === (settings.deletePassword || '');
    if (!valid) {
      document.getElementById('del-pass-err').textContent = 'كلمة السر غير صحيحة';
      document.getElementById('del-pass-input').focus(); return;
    }
    if (!settings.deleteCredential && settings.deletePassword) {
      settings.deleteCredential = await createDeleteCredential(entered); delete settings.deletePassword;
      await DB.setObj(K_SETTINGS, settings);
    }
    const action = _pendingDeleteFn; _pendingDeleteFn = null;
    _deleteConfirmed = true; closeModal('modal-delete-pass'); _deleteConfirmed = false;
    document.getElementById('del-pass-input').value = '';
    if (action) await action();
  });
};

window.toggleDeletePassVis = function() {
  const inp = document.getElementById('set-delete-pass');
  const btn = document.getElementById('del-pass-eye');
  if (inp.type === 'password') { inp.type = 'text'; btn.textContent = '🙈'; }
  else                         { inp.type = 'password'; btn.textContent = '👁️'; }
};

// ── Wrappers للحذف بكلمة السر ─────────────────────────
window.askDeleteRepair = function(id) {
  const r = DB.get(K_REPAIRS).find(x=>x.id===id);
  requireDeletePass(`حذف جهاز "${r?.customer||''} — ${r?.deviceType||''}"؟`, () => deleteRepair(id));
};
window.askDeleteCash = function(id) {
  const c = DB.get(K_CASH).find(x=>x.id===id);
  requireDeletePass(`حذف معاملة "${c?.desc||''}" — ${formatNum(c?.amount||0)} ل؟`, () => deleteCash(id));
};
window.askDeleteDebt = function(id) {
  const d = DB.get(K_DEBTS).find(x=>x.id===id);
  requireDeletePass(`حذف دين "${d?.name||''}"؟`, () => deleteDebt(id));
};
window.askDeleteCustomer = function(id) {
  const c = DB.get(K_CUSTOMERS).find(x=>x.id===id);
  requireDeletePass(`حذف العميل "${c?.name||''}"؟`, () => deleteCustomer(id));
};
window.askDeleteEmployee = function(id) {
  const e = DB.get(K_EMPLOYEES).find(x=>x.id===id);
  requireDeletePass(`حذف الموظف "${e?.name||''}" وكل سحوباته؟`, () => deleteEmployee(id));
};
window.askDeleteExpense = function(id) {
  const e = DB.get(K_EXPENSES).find(x=>x.id===id);
  requireDeletePass(`حذف مصروف "${e?.desc||''}" — ${formatNum(e?.amount||0)} ل؟`, () => deleteExpense(id));
};

// -- اختيار الوحدة --
// ==================== COLLAPSIBLE NAV ====================

let authMode = 'login';
let loginInProgress = false;
function setAccountMessage(id, message, error = false) {
  const field = document.getElementById(id);
  if (!field) return;
  field.textContent = message;
  field.dataset.state = error ? 'error' : 'success';
  field.setAttribute('role', error ? 'alert' : 'status');
}
function clearAccountPasswordFields() {
  ['account-current-password', 'account-new-password', 'account-confirm-password'].forEach(id => {
    const field = document.getElementById(id); if (field) { field.value = ''; field.type = 'password'; }
  });
  document.querySelectorAll('#modal-account-password [data-password-toggle]').forEach(button => {
    button.textContent = 'إظهار'; button.setAttribute('aria-pressed', 'false');
  });
}
function clearLoginPasswords() {
  ['l-pass', 'l-pass-confirm'].forEach(id => { const field = document.getElementById(id); if (field) { field.value = ''; field.type = 'password'; } });
  document.querySelectorAll('#s-login [data-password-toggle]').forEach(button => { button.textContent = 'إظهار'; button.setAttribute('aria-pressed', 'false'); });
}
function resetAuthLoginSurface() {
  authMode = 'login';
  document.getElementById('l-login-form').style.display = 'block';
  document.getElementById('l-signup-form').style.display = 'none';
  document.getElementById('l-reset-form').style.display = 'none';
  document.getElementById('l-password-section').style.display = 'block';
  document.getElementById('l-pass').setAttribute('autocomplete', 'current-password');
}
function setAuthMode(mode) {
  if (signupInProgress || loginInProgress || activeActions.has('auth-reset')) return;
  if (!['login', 'signup', 'reset'].includes(mode)) return;
  if (mode !== authMode) clearLoginPasswords();
  authMode = mode;
  document.getElementById('l-login-form').style.display = mode === 'login' ? 'block' : 'none';
  document.getElementById('l-signup-form').style.display = mode === 'signup' ? 'block' : 'none';
  document.getElementById('l-reset-form').style.display = mode === 'reset' ? 'block' : 'none';
  document.getElementById('l-password-section').style.display = mode === 'reset' ? 'none' : 'block';
  document.getElementById('l-pass').setAttribute('autocomplete', mode === 'signup' ? 'new-password' : 'current-password');
  setAccountMessage('l-err', ''); setAccountMessage('l-notice', '');
}
window.toggleSignup = show => setAuthMode(show ? 'signup' : 'login');
window.openPasswordRecovery = () => { setAuthMode('reset'); document.getElementById('l-email').focus(); };
window.togglePasswordVisibility = function(id, button) {
  const field = document.getElementById(id); if (!field || !['password', 'text'].includes(field.type)) return;
  const visible = field.type === 'password'; field.type = visible ? 'text' : 'password';
  button.textContent = visible ? 'إخفاء' : 'إظهار'; button.setAttribute('aria-pressed', String(visible));
};
window.submitAuthForm = function() {
  if (authMode === 'signup') return doSignup();
  if (authMode === 'reset') return requestLoginPasswordReset();
  return doLogin();
};
window.requestLoginPasswordReset = async function() {
  return saveAction('auth-reset', '#l-reset-form button', async () => {
    setAccountMessage('l-err', ''); setAccountMessage('l-notice', '');
    try {
      await requestAccountReset(document.getElementById('l-email').value);
      setAccountMessage('l-notice', 'إذا كان البريد مسجلاً، سيصلك رابط لتغيير كلمة المرور. تفقد البريد غير المرغوب فيه أيضًا، ثم ارجع لتسجيل الدخول.');
    } catch (error) { setAccountMessage('l-err', accountErrorMessage(error), true); }
  });
};
window.openAccountPassword = function() {
  if (!auth.currentUser?.email) { toast('سجّل الدخول أولاً', 'error'); return; }
  if (writer.pending || activeActions.size) { toast('انتظر اكتمال الحفظ أولاً', 'info'); return; }
  clearAccountPasswordFields(); setAccountMessage('account-password-message', '');
  document.getElementById('account-password-email').textContent = auth.currentUser.email;
  openModal('modal-account-password'); document.getElementById('account-current-password').focus();
};
window.saveAccountPassword = async function() {
  return saveAction('change-account-password', '#modal-account-password button', async () => {
    setAccountMessage('account-password-message', '');
    if (writer.pending || activeActions.size > 1) { setAccountMessage('account-password-message', 'انتظر اكتمال العمليات الأخرى أولاً.', true); return; }
    let changed;
    try {
      changed = await changeOwnPassword({ auth,
        currentPassword: document.getElementById('account-current-password').value,
        newPassword: document.getElementById('account-new-password').value,
        confirmation: document.getElementById('account-confirm-password').value,
        validatePassword, credential: EmailAuthProvider.credential,
        reauthenticate: reauthenticateWithCredential, updatePassword });
    } catch (error) {
      document.getElementById('account-current-password').value = '';
      setAccountMessage('account-password-message', accountErrorMessage(error, true), true); return;
    }
    clearAccountPasswordFields(); closeModal('modal-account-password');
    stopListeners(); authGateSequence++;
    Object.keys(cache).forEach(key => { cache[key] = Array.isArray(cache[key]) ? [] : {}; });
    document.getElementById('profit-section-content').style.display = 'none';
    clearInterval(_tickerTimer); _tickerTimer = null; clearLoginPasswords();
    try { await signOut(auth); }
    catch { setAccountMessage('l-err', 'تغيّرت كلمة المرور، لكن تعذر إغلاق الجلسة؛ أعد تحميل الصفحة وسجّل الدخول بالكلمة الجديدة.', true); }
    showLogin(); document.getElementById('l-email').value = changed.email;
    // No mode switch is needed while the save action is locked; select the login surface directly.
    resetAuthLoginSurface();
    setAccountMessage('l-notice', 'تم تغيير كلمة المرور. سجّل الدخول بالكلمة الجديدة؛ بيانات حسابك محفوظة.');
  });
};
window.sendOwnPasswordReset = async function() {
  return saveAction('own-password-reset', '#account-security-card button', async () => {
    try {
      if (!auth.currentUser?.email) throw accountValidation('سجّل الدخول أولاً.');
      await requestAccountReset(auth.currentUser.email);
      setAccountMessage('account-security-message', 'طُلب إرسال رابط التغيير إلى بريد حسابك. تفقد بريدك والبريد غير المرغوب فيه.');
    } catch (error) { setAccountMessage('account-security-message', accountErrorMessage(error), true); }
  });
};

let signupInProgress = false;
let authGateSequence = 0;
async function activateAccount(user, activationCode, shopName) {
  const code = activationCode.trim().toUpperCase();
  if (!/^[A-Z0-9]{8}$/.test(code)) throw new Error('كود التفعيل غير صالح');
  const registryRef = doc(fdb, 'users_registry', user.uid);
  const codeRef = doc(fdb, 'activation_codes', code);
  await runTransaction(fdb, async transaction => {
    const registrySnap = await transaction.get(registryRef);
    if (registrySnap.exists()) throw new Error('الحساب مسجل مسبقاً؛ استخدم تسجيل الدخول أو تواصل مع المالك');
    const codeSnap = await transaction.get(codeRef);
    if (!codeSnap.exists() || codeSnap.data().used !== false) throw new Error('كود التفعيل غير صحيح أو مستخدم');
    const now = new Date().toISOString();
    transaction.set(codeRef, { ...codeSnap.data(), used: true, usedBy: user.email, usedByUid: user.uid, usedAt: now });
    transaction.set(registryRef, { uid: user.uid, email: user.email, shopName, createdAt: now, active: true, activationCode: code });
    transaction.set(doc(fdb, 'users', user.uid, 'settings', 'main'), { shopName });
  });
}
window.doSignup = async function() {
  if (signupInProgress) return;
  const email = document.getElementById('l-email').value.trim();
  const password = document.getElementById('l-pass').value;
  const confirmation = document.getElementById('l-pass-confirm').value;
  const shopName = document.getElementById('l-shopname').value.trim();
  const code = document.getElementById('l-actcode').value.trim().toUpperCase();
  const button = document.getElementById('l-signup-btn'), error = document.getElementById('l-err');
  error.textContent = '';
  if (password !== confirmation) { error.textContent = 'كلمتا المرور غير متطابقتين.'; return; }
  if (!email || password.length < 6 || !/^[A-Z0-9]{8}$/.test(code)) {
    error.textContent = 'أدخل البريد وكلمة مرور من 6 أحرف على الأقل وكود تفعيل من 8 خانات'; return;
  }
  signupInProgress = true; button.disabled = true; button.textContent = 'جارٍ التحقق والتفعيل…';
  try {
    normalizedEmail(email);
    await checkNewPassword(auth, password, confirmation, validatePassword);
    let credential;
    try { credential = await createUserWithEmailAndPassword(auth, email, password); }
    catch (failure) {
      // An interrupted signup can be completed using the same account and a valid unused code.
      if (failure.code !== 'auth/email-already-in-use') throw failure;
      credential = await signInWithEmailAndPassword(auth, email, password);
    }
    await activateAccount(credential.user, code, shopName);
    clearLoginPasswords();
    await openAuthorizedSession(credential.user);
  } catch (failure) {
    await signOut(auth).catch(() => {});
    showLogin();
    const messages = { 'auth/invalid-credential': 'البريد أو كلمة المرور غير صحيحة', 'auth/invalid-email': 'البريد غير صالح', 'permission-denied': 'تعذر التفعيل: الكود غير صالح أو مستخدم', 'auth/network-request-failed': 'تحقق من اتصال الإنترنت' };
    error.textContent = messages[failure.code] || (failure.code ? accountErrorMessage(failure) : failure.message) || 'تعذر إكمال التفعيل؛ أعد المحاولة بنفس البريد والكود';
  } finally {
    signupInProgress = false; button.disabled = false; button.textContent = 'إنشاء الحساب وتفعيله';
  }
};

// ── إدارة كودات التفعيل ──────────────────────────────────
function generateCodeStr() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  return Array.from(crypto.getRandomValues(new Uint8Array(8)), byte => chars[byte % chars.length]).join('');
}

window.generateActivationCodes = async function() {
  const uid = auth.currentUser?.uid;
  if (!uid) { toast('سجّل دخول أولاً', 'error'); return; }
  if (auth.currentUser?.uid !== OWNER_UID) { toast('❌ غير مصرّح', 'error'); return; }

  const count = parseInt(document.getElementById('codes-count').value) || 1;
  const btn = document.querySelector('[onclick="generateActivationCodes()"]');
  btn.disabled = true; btn.textContent = '⏳ جاري التوليد...';

  try {
    for (let i = 0; i < count; i++) {
      const code = generateCodeStr();
      await setDoc(doc(fdb, 'activation_codes', code), {
        code,
        createdBy: uid,
        createdAt: new Date().toISOString(),
        used: false
      });
    }
    toast(`✅ تم توليد ${count} كود جديد`);
    loadActivationCodes();
  } catch(e) {
    toast('❌ خطأ: ' + e.message, 'error');
  }
  btn.disabled = false; btn.textContent = '➕ توليد كودات جديدة';
};

// ==================== ADMIN DASHBOARD (المالك فقط) ====================

window.loadRegisteredUsers = async function() {
  if (auth.currentUser?.uid !== OWNER_UID) return;
  const el = document.getElementById('admin-users-list');
  if (!el) return;
  el.innerHTML = '<div style="text-align:center;color:var(--text-muted);padding:12px;font-size:13px">⏳ جاري التحميل...</div>';
  try {
    const [usersSnap, codesSnap] = await Promise.all([
      getDocs(collection(fdb, 'users_registry')),
      getDocs(collection(fdb, 'activation_codes'))
    ]);
    if (auth.currentUser?.uid !== OWNER_UID) return;
    const users = usersSnap.docs.map(d => ({ ...d.data(), uid: d.id })).sort((a,b) => (a.createdAt||'').localeCompare(b.createdAt||''));
    adminAccounts.clear(); users.forEach(user => adminAccounts.set(user.uid, user));
    // بناء map: email → code
    const emailToCode = {};
    codesSnap.docs.forEach(d => {
      const c = d.data();
      if (c.used && c.usedBy) emailToCode[c.usedBy] = c.code;
    });
    if (!users.length) {
      el.innerHTML = '<div style="text-align:center;color:var(--text-muted);padding:12px">لا توجد حسابات مسجّلة بعد</div>';
      return;
    }
    el.innerHTML = users.map(u => {
      const isMe = u.uid === OWNER_UID;
      const date = u.createdAt ? u.createdAt.substring(0,10) : '-';
      const usedCode = emailToCode[u.email];
      return `
        <div style="display:flex;justify-content:space-between;align-items:center;padding:12px 14px;background:var(--bg-card2);border-radius:10px;margin-bottom:8px;border:1px solid var(--border)${isMe?';border-color:rgba(201,150,12,0.4)':''}">
          <div style="flex:1">
            <div style="font-weight:700;font-size:13px;display:flex;align-items:center;gap:8px;flex-wrap:wrap">
              ${isMe?'👑 ':''}<span style="direction:ltr;unicode-bidi:embed">${escapeHTML(u.email)}</span>
              ${usedCode ? `<span style="background:rgba(201,150,12,0.15);border:1px solid rgba(201,150,12,0.3);color:#c9960c;padding:2px 8px;border-radius:6px;font-size:10px;letter-spacing:2px;font-family:monospace">${escapeHTML(usedCode)}</span>` : ''}
            </div>
            <div style="font-size:11px;color:var(--text-muted);margin-top:4px">
              ${u.shopName ? `🏪 ${escapeHTML(u.shopName)} &nbsp;|&nbsp;` : ''}📅 ${escapeHTML(date)}
            </div>
          </div>
          ${isMe ? `<span class="badge badge-warning">المالك</span>` : `
          <div style="display:flex;gap:6px;flex-shrink:0;flex-wrap:wrap">
            <button onclick="adminResetPassword(${htmlJSArg(u.uid)})"
              style="background:rgba(99,102,241,0.1);border:1px solid rgba(99,102,241,0.3);color:#818cf8;padding:5px 10px;border-radius:7px;cursor:pointer;font-size:11px;font-weight:700;font-family:inherit">
              📧 رابط تغيير كلمة المرور
            </button>
            <button onclick="adminDeleteUser(${htmlJSArg(u.uid)},${htmlJSArg(u.email)})"
              style="background:rgba(239,68,68,0.1);border:1px solid rgba(239,68,68,0.2);color:var(--danger);padding:5px 10px;border-radius:7px;cursor:pointer;font-size:11px;font-weight:700;font-family:inherit">
              🗑️ حذف
            </button>
          </div>`}
        </div>`;
    }).join('');
  } catch(e) {
    el.innerHTML = `<div style="color:var(--danger);padding:10px;font-size:12px">❌ ${e.message}</div>`;
  }
};

window.adminResetPassword = async function(userId) {
  if (auth.currentUser?.uid !== OWNER_UID) { toast('غير مصرّح', 'error'); return; }
  const account = adminAccounts.get(userId);
  if (!account?.email) { toast('حدّث قائمة الحسابات ثم اختر الحساب المطلوب.', 'error'); return; }
  document.getElementById('confirm-message').textContent = `إرسال رابط تغيير كلمة المرور إلى ${account.email}؟ يختار صاحب البريد كلمته الجديدة بنفسه. هذا الإجراء لا يغيّر حالة تفعيل الحساب.`;
  document.getElementById('confirm-ok').onclick = () => saveAction('admin-password-reset', '#modal-confirm button', async () => {
    try {
      if (auth.currentUser?.uid !== OWNER_UID) throw accountValidation('غير مصرّح.');
      const current = await getDoc(doc(fdb, 'users_registry', userId));
      if (!current.exists() || current.data().email !== account.email) throw accountValidation('تغيّر سجل الحساب؛ حدّث القائمة وأعد المحاولة.');
      if (auth.currentUser?.uid !== OWNER_UID) throw accountValidation('تغيّر الحساب؛ أعد تسجيل الدخول.');
      await requestAccountReset(account.email);
      closeModal('modal-confirm'); toast('طُلب إرسال رابط التغيير إلى بريد صاحب الحساب. لا يتم تغيير كلمة المرور إلا بعد استخدام الرابط.');
    } catch (error) { toast(accountErrorMessage(error), 'error'); }
  });
  openModal('modal-confirm');
  document.getElementById('confirm-ok').textContent = 'إرسال الرابط';
  document.getElementById('confirm-ok').style.background = '';
};

window.adminDeleteUser = async function(uid2, email) {
  if (auth.currentUser?.uid !== OWNER_UID) return;
  if (uid2 === auth.currentUser?.uid) { toast('لا يمكن حذف حسابك', 'error'); return; }
  document.getElementById('confirm-message').textContent = `⚠️ حذف جميع بيانات الحساب:\n${email}\n\nهذا الإجراء لا يمكن التراجع عنه. سيتم حذف كل بياناته من النظام.`;
  document.getElementById('confirm-ok').textContent = '🗑️ حذف البيانات';
  document.getElementById('confirm-ok').style.background = '#ef4444';
  document.getElementById('confirm-ok').onclick = async () => {
    closeModal('modal-confirm');
    const toastId = toast('⏳ جاري حذف البيانات...', 'info');
    try {
      // احذف كل collections تحت users/{uid}
      const cols = [K_REPAIRS, K_CASH, K_DEBTS, K_CUSTOMERS, K_FAULTS, K_EXPENSES,
                    K_EXP_CATS, K_EMPLOYEES, K_WITHDRAWALS, K_DEBT_PAYMENTS,
];
      for (const col of cols) {
        const colSnap = await getDocs(collection(fdb,'users',uid2,col));
        for (const d of colSnap.docs) await deleteDoc(doc(fdb,'users',uid2,col,d.id)).catch(()=>{});
      }
      // احذف settings
      await deleteDoc(doc(fdb,'users',uid2,'settings','main')).catch(()=>{});
      // احذف من registry
      await deleteDoc(doc(fdb,'users_registry',uid2)).catch(()=>{});
      toast(`✅ تم حذف بيانات ${email} بنجاح`, 'success');
      loadRegisteredUsers();
    } catch(e) {
      toast('❌ ' + e.message, 'error');
    }
    document.getElementById('confirm-ok').textContent = 'تأكيد';
    document.getElementById('confirm-ok').style.background = '';
  };
  openModal('modal-confirm');
};

window.loadActivationCodes = async function() {
  if (auth.currentUser?.uid !== OWNER_UID) return;
  const el = document.getElementById('codes-list');
  if (!el) return;
  el.innerHTML = '<div style="text-align:center;color:var(--text-muted);font-size:12px;padding:8px">⏳ جاري التحميل...</div>';
  try {
    const uid = auth.currentUser?.uid;
    const snap = await getDocs(collection(fdb, 'activation_codes'));
    const codes = snap.docs.map(d => ({ id: d.id, ...d.data() }))
                          .filter(c => c.createdBy === uid)
                          .sort((a,b) => (b.createdAt||'').localeCompare(a.createdAt||''));
    if (codes.length === 0) {
      el.innerHTML = '<div style="text-align:center;color:var(--text-muted);font-size:12px;padding:8px">لا توجد كودات — أنشئ كوداً جديداً</div>';
      return;
    }
    el.innerHTML = codes.map(c => `
      <div style="display:flex;justify-content:space-between;align-items:center;padding:10px 14px;background:var(--bg-card2);border-radius:8px;margin-bottom:6px;border-right:3px solid ${c.used?'var(--danger)':'#c9960c'}">
        <div>
          <div style="font-size:16px;font-weight:900;letter-spacing:3px;color:${c.used?'var(--text-muted)':'#f5c842'};${c.used?'text-decoration:line-through':''}">${escapeHTML(c.code)}</div>
          ${c.used
            ? `<div style="font-size:10px;color:var(--danger);margin-top:3px">✗ مستخدم بواسطة: <strong style="direction:ltr;unicode-bidi:embed">${escapeHTML(c.usedBy||'-')}</strong></div>
               <div style="font-size:10px;color:var(--text-muted)">${c.usedAt ? c.usedAt.substring(0,10) : ''}</div>`
            : '<div style="font-size:10px;color:#c9960c;margin-top:3px">✓ متاح للاستخدام</div>'}
        </div>
        <div style="display:flex;gap:6px;align-items:center">
          ${!c.used ? `<button onclick="copyCode('${escapeHTML(c.code)}')" style="background:rgba(201,150,12,0.15);border:1px solid rgba(201,150,12,0.3);color:#c9960c;padding:5px 10px;border-radius:6px;cursor:pointer;font-size:11px;font-family:inherit">📋 نسخ</button>` : ''}
          <button onclick="deleteCode('${escapeHTML(c.code)}')" style="background:rgba(239,68,68,0.1);border:1px solid rgba(239,68,68,0.2);color:var(--danger);padding:5px 8px;border-radius:6px;cursor:pointer;font-family:inherit;font-size:11px">🗑️</button>
        </div>
      </div>
    `).join('');
  } catch(e) {
    el.innerHTML = `<div style="color:var(--danger);font-size:12px;padding:8px">❌ ${e.message}</div>`;
  }
};

window.copyCode = function(code) {
  navigator.clipboard.writeText(code).then(() => toast(`✅ تم نسخ الكود: ${code}`));
};

window.deleteCode = async function(code) {
  if (auth.currentUser?.uid !== OWNER_UID) { toast('❌ غير مصرّح', 'error'); return; }
  await deleteDoc(doc(fdb, 'activation_codes', code)).catch(()=>{});
  toast('تم حذف الكود', 'info');
  loadActivationCodes();
};

window.doLogin = async function() {
  if (loginInProgress || signupInProgress || activeActions.has('auth-reset')) return;
  const password = document.getElementById('l-pass').value;
  const button = document.getElementById('l-btn');
  setAccountMessage('l-err', ''); setAccountMessage('l-notice', '');
  try {
    const email = normalizedEmail(document.getElementById('l-email').value);
    if (!password) throw accountValidation('أدخل كلمة المرور.');
    loginInProgress = true; button.disabled = true; button.textContent = 'جارٍ الدخول…';
    await signInWithEmailAndPassword(auth, email, password);
    clearLoginPasswords();
  } catch (error) { setAccountMessage('l-err', accountErrorMessage(error), true); }
  finally { loginInProgress = false; button.disabled = false; button.textContent = 'دخول'; }
};
window.doLogout=async function(){
  if (writer.pending || activeActions.size) { toast('انتظر اكتمال الحفظ قبل تسجيل الخروج', 'info'); return; }
  stopListeners();
  clearInterval(_tickerTimer); _tickerTimer = null;
  Object.keys(cache).forEach(k => { cache[k] = Array.isArray(cache[k]) ? [] : {}; });
  await signOut(auth);
  document.getElementById('l-btn').disabled = false;
  document.getElementById('l-btn').textContent = '🔐 دخول';
  showLogin();
};

// ── Migration: نقل البيانات القديمة (root) للمستخدم الحالي ──────────
async function migrateRootDataToUser(uid) {
  if (auth.currentUser?.uid !== OWNER_UID || uid !== auth.currentUser.uid) throw new Error('ترحيل البيانات متاح للمالك فقط');
  const cols = [K_REPAIRS,K_CASH,K_DEBTS,K_CUSTOMERS,K_FAULTS,K_EXPENSES,K_EXP_CATS,K_EMPLOYEES,K_WITHDRAWALS,K_DEBT_PAYMENTS];
  let migrated = 0;
  for (const col of cols) {
    try {
      // تحقق إذا في بيانات قديمة بالـ root
      const rootSnap = await getDocs(collection(fdb, col));
      if (rootSnap.empty) continue;
      // تحقق إذا المستخدم ما عنده بيانات بعد
      const userSnap = await getDocs(collection(fdb,'users',uid,col));
      if (!userSnap.empty) continue; // عنده بيانات، ما ننقل
      // انقل كل document
      for (const d of rootSnap.docs) {
        await setDoc(doc(fdb,'users',uid,col,d.id), d.data());
        migrated++;
      }
    } catch(e) { console.warn('migrate', col, e.message); }
  }
  // settings
  try {
    const rootSettings = await getDoc(doc(fdb,'settings','main'));
    if (rootSettings.exists()) {
      const userSettings = await getDoc(doc(fdb,'users',uid,'settings','main'));
      if (!userSettings.exists()) {
        await setDoc(doc(fdb,'users',uid,'settings','main'), rootSettings.data());
        migrated++;
      }
    }
  } catch(e) {}
  if (migrated > 0) console.log(`✅ Migration: ${migrated} documents moved to users/${uid}`);
  return migrated;
}

let editRepairId = null;

// ==================== UTILS ====================
function uid() { return Date.now().toString(36) + Math.random().toString(36).substr(2); }
const uid_gen = uid;

function today() {
  const d = new Date();
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, '0');
  const day = String(d.getDate()).padStart(2, '0');
  return `${y}-${m}-${day}`;
}

function currentMonth() {
  const d = new Date();
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, '0');
  return `${y}-${m}`;
}

function nowTimestamp() {
  return new Date().toISOString(); // 2026-03-12T14:35:22.000Z
}

function formatDate(d) {
  if (!d) return '-';
  const dt = new Date(d);
  return dt.toLocaleDateString('en-GB', { year: 'numeric', month: '2-digit', day: '2-digit' });
}

function formatDateTime(d) {
  if (!d) return '-';
  const dt = new Date(d);
  const date = dt.toLocaleDateString('en-GB', { year:'numeric', month:'2-digit', day:'2-digit' });
  const time = dt.toLocaleTimeString('en-GB', { hour:'2-digit', minute:'2-digit', hour12: false });
  return `${date} ${time}`;
}

function formatNum(n) {
  return Number(n || 0).toLocaleString('en');
}

function toast(msg, type = 'success') {
  const display = () => {
    const element = document.createElement('div');
    element.className = `toast ${type}`;
    element.setAttribute('role', type === 'error' ? 'alert' : 'status');
    element.textContent = `${({ success: '✅', error: '❌', info: 'ℹ️' })[type] || 'ℹ️'} ${msg}`;
    document.getElementById('toast-container').appendChild(element);
    setTimeout(() => element.remove(), type === 'error' ? 7000 : 3500);
  };
  if (type !== 'error' && writer.pending) writer.wait().then(display, () => {});
  else display();
}

function showPage(id) {
  const pageEl = document.getElementById('page-' + id);
  if (!pageEl) return; // صفحة غير موجودة — تجاهل
  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
  pageEl.classList.add('active');
  // إعادة التمرير للأعلى عند تغيير الصفحة (مهم على الجوال)
  window.scrollTo({ top: 0, behavior: 'instant' });
  document.documentElement.scrollTop = 0;
  document.body.scrollTop = 0;

  const map = {
    'dashboard':'الصفحة الرئيسية','new-repair':'تسجيل صيانة جديدة',
    'devices':'الأجهزة','cash':'الصندوق','expenses':'المصاريف','debts':'الديون',
    'employees':'الموظفون','customers':'العملاء','reports':'التقارير','settings':'الإعدادات'
  };
  document.getElementById('page-title').textContent = map[id] || '';

  document.querySelectorAll('.nav-item').forEach(item => {
    if (item.getAttribute('onclick') === `showPage('${id}')`) {
      item.classList.add('active');
    }
  });

  if (id === 'dashboard') renderDashboard();
  if (id === 'devices')   renderDevices();
  if (id === 'cash')      renderCash();
  if (id === 'expenses')  renderExpenses();
  if (id === 'employees') renderEmployees();
  if (id === 'debts')     renderDebts();
  if (id === 'customers') renderCustomers();
  if (id === 'reports')   renderReports();
  if (id === 'new-repair') initRepairForm();
  if (id === 'settings')   loadSettings();
}

function openModal(id) {
  document.getElementById(id).classList.add('open');
  // إعادة نص زر التأكيد لافتراضيته كل مرة
  if (id === 'modal-confirm') {
    const btn = document.getElementById('confirm-ok');
    if (btn) btn.textContent = '✔️ تأكيد';
  }
}

function closeModal(id) {
  document.getElementById(id).classList.remove('open');
  if (id === 'modal-account-password') clearAccountPasswordFields();
  if (id === 'modal-delete-pass' && !_deleteConfirmed) _pendingDeleteFn = null;
}

// ==================== CUSTOMERS DATA ====================
function getCustomersDatalist() {
  const custs = DB.get(K_CUSTOMERS);
  const dl = document.getElementById('customers-list');
  if (dl) dl.replaceChildren(...custs.map(customer => { const option = document.createElement('option'); option.value = customer.name; return option; }));
}

// ==================== INIT MONTH FILTERS ====================
function shiftMonth(inputId, delta, renderFnName) {
  const el = document.getElementById(inputId);
  if (!el) return;
  const cur = el.value || currentMonth();
  const [y, m] = cur.split('-').map(Number);
  const d = new Date(y, m - 1 + delta, 1);
  const ny = d.getFullYear();
  const nm = String(d.getMonth() + 1).padStart(2, '0');
  el.value = `${ny}-${nm}`;
  if (typeof renderFnName === 'string') window[renderFnName]();
  else if (typeof renderFnName === 'function') renderFnName();
}
function initMonthFilters() {
  const cm = currentMonth(); // الشهر الحالي دائماً
  const ids = ['dash-month-filter', 'cash-month-filter', 'emp-month-filter', 'report-month-custom', 'exp-month-filter'];
  ids.forEach(id => {
    const el = document.getElementById(id);
    if (el) el.value = cm;
  });
}

// ==================== DASHBOARD ====================
function renderDashboard() {
  const repairs = DB.get(K_REPAIRS);
  const cash = DB.get(K_CASH);
  const debts = DB.get(K_DEBTS);
  const todayStr = today();

  // الشهر المختار
  const dashMonthEl = document.getElementById('dash-month-filter');
  const monthStr = dashMonthEl?.value || currentMonth();

  // فلترة بالشهر المختار
  const mCash    = cash.filter(c => (c.date||'').startsWith(monthStr));
  const mRepairs = repairs.filter(r => (r.date||'').startsWith(monthStr));

  // Balance — الشهر المختار
  const totalIn  = mCash.filter(c => c.type === 'in').reduce((s, c) => s + +c.amount, 0);
  const totalOut = mCash.filter(c => c.type === 'out').reduce((s, c) => s + +c.amount, 0);
  const balance  = totalIn - totalOut;

  // صندوق اليوم
  const todayCashIn  = cash.filter(c => c.type === 'in'  && c.date === todayStr).reduce((s,c) => s + +c.amount, 0);
  const todayCashOut = cash.filter(c => c.type === 'out' && c.date === todayStr).reduce((s,c) => s + +c.amount, 0);
  const todayCash    = todayCashIn - todayCashOut;

  // صندوق الشهر المختار
  const monthCashIn  = mCash.filter(c => c.type === 'in').reduce((s,c) => s + +c.amount, 0);
  const monthCashOut = mCash.filter(c => c.type === 'out').reduce((s,c) => s + +c.amount, 0);
  const monthCash = mCash.reduce((sum, entry) => sum + (entry.type === 'bank_in' ? Number(entry.amount || 0) : entry.type === 'bank_out' ? -Number(entry.amount || 0) : 0), 0);

  // أجهزة تم إصلاحها في الشهر المختار
  const inRepair = mRepairs.length;

  // Debts — كل الديون المستحقة (مش مرتبطة بالشهر)
  const totalDebt = debts.reduce((sum, debt) => sum + Math.max(0, Number(debt.amount || 0) - Number(debt.paid || 0)), 0);

  // اسم الشهر
  const [my, mm] = monthStr.split('-');
  const monthNames = ['','يناير','فبراير','مارس','أبريل','مايو','يونيو','يوليو','أغسطس','سبتمبر','أكتوبر','نوفمبر','ديسمبر'];
  const mLabel = `${monthNames[+mm]} ${my}`;

  document.getElementById('dash-balance').textContent    = formatNum(balance);
  document.getElementById('dash-today-cash').textContent = formatNum(todayCash);
  document.getElementById('dash-month-cash').textContent = formatNum(monthCash);
  document.getElementById('dash-in-repair').textContent  = inRepair;
  document.getElementById('dash-debts').textContent      = formatNum(totalDebt);

  // حقول جديدة
  const dashMonthIn  = document.getElementById('dash-month-in');
  const dashMonthOut = document.getElementById('dash-month-out');
  if (dashMonthIn)  dashMonthIn.textContent  = formatNum(monthCashIn);
  if (dashMonthOut) dashMonthOut.textContent = formatNum(monthCashOut);

  // حدّث labels
  const dashBalLabel = document.getElementById('dash-balance-label');
  if (dashBalLabel) dashBalLabel.textContent = `صافي حركة الصندوق — ${mLabel}`;
  const dashMonthCashLabel = document.getElementById('dash-month-cash-label');
  if (dashMonthCashLabel) dashMonthCashLabel.textContent = `صافي حركة البنك — ${mLabel}`;
  const dashInRepairLabel = document.getElementById('dash-in-repair-label');
  if (dashInRepairLabel) dashInRepairLabel.textContent = `أجهزة ${mLabel}`;


  // Recent
  const recent = [...repairs].sort((a,b) => b.id.localeCompare(a.id)).slice(0, 8);
  const recentEl = document.getElementById('recent-ops');
  if (recent.length === 0) {
    recentEl.innerHTML = '<div class="empty-state"><div class="icon">📭</div><p>لا توجد عمليات بعد</p></div>';
  } else {
    recentEl.innerHTML = recent.map(r => `
      <div class="activity-item">
        <div class="activity-dot" style="background:var(--success)"></div>
        <div class="activity-text">
          <strong>${escapeHTML(r.customer)}</strong> — ${escapeHTML(r.deviceType)} ${escapeHTML(r.model || '')}
          <div style="font-size:11px;color:var(--text-muted)">${escapeHTML(r.problem?.substring(0,40)||'')}</div>
        </div>
        <div class="activity-time">${formatDate(r.date)}</div>
      </div>
    `).join('');
  }

  // Quick summary
  const qs = document.getElementById('quick-summary');
  qs.innerHTML = `
    <div style="display:flex;flex-direction:column;gap:10px">
      <div style="display:flex;justify-content:space-between;padding:8px 0;border-bottom:1px solid var(--border)">
        <span style="color:var(--text-muted)">عمليات ${mLabel}</span>
        <strong>${mRepairs.length}</strong>
      </div>
      <div style="display:flex;justify-content:space-between;padding:8px 0;border-bottom:1px solid var(--border)">
        <span style="color:var(--text-muted)">مقبوض ${mLabel}</span>
        <span class="badge badge-success">${formatNum(monthCashIn)} ليرة</span>
      </div>
      <div style="display:flex;justify-content:space-between;padding:8px 0;border-bottom:1px solid var(--border)">
        <span style="color:var(--text-muted)">مدفوع ${mLabel}</span>
        <span class="badge badge-danger">${formatNum(monthCashOut)} ليرة</span>
      </div>
      <div style="display:flex;justify-content:space-between;padding:8px 0;border-bottom:1px solid var(--border)">
        <span style="color:var(--text-muted)">الديون المستحقة</span>
        <span class="badge badge-danger">${formatNum(totalDebt)} ليرة</span>
      </div>
      <div style="display:flex;justify-content:space-between;padding:8px 0">
        <span style="color:var(--text-muted)">صافي ${mLabel}</span>
        <strong style="color:${balance>=0?'var(--accent)':'var(--danger)'}">${formatNum(balance)} ليرة</strong>
      </div>
    </div>
  `;
  renderSerialLog();
  updateNotifBadge();
}

// ==================== NOTIFICATION BADGE ====================
function updateNotifBadge() {
  const pendingDebts = DB.get(K_DEBTS).filter(d => (+d.amount - (+d.paid||0)) > 0);
  const badge = document.getElementById('notif-badge');
  if (!badge) return;
  if (pendingDebts.length > 0) {
    badge.style.display = 'flex';
    badge.textContent = pendingDebts.length > 99 ? '99+' : pendingDebts.length;
    badge.classList.add('has-debts');
    // tooltip
    const totalDebtAmt = pendingDebts.reduce((s,d) => s + (+d.amount - (+d.paid||0)), 0);
    badge.parentElement.title = `${pendingDebts.length} ديون مستحقة — إجمالي: ${formatNum(totalDebtAmt)} ليرة`;
  } else {
    badge.style.display = 'none';
    badge.classList.remove('has-debts');
    badge.parentElement.title = 'لا توجد ديون مستحقة';
  }
}

// ==================== SERIAL LOG ====================
function renderSerialLog() {
  const el = document.getElementById('serial-log-list');
  if (!el) return;
  const settings = DB.getObj(K_SETTINGS, {});
  const log = (settings.serialLog || []).slice().reverse(); // أحدث أولاً

  if (log.length === 0) {
    el.innerHTML = `<div style="text-align:center;color:var(--text-muted);padding:16px;font-size:13px">لا توجد أرقام مسجلة بعد — اضغط ➕ لإضافة</div>`;
    return;
  }

  el.innerHTML = log.map((entry, i) => `
    <div style="display:flex;justify-content:space-between;align-items:center;padding:10px 12px;background:var(--bg-card2);border-radius:8px;margin-bottom:8px;border-right:3px solid var(--accent)">
      <div>
        <div style="font-size:13px;color:var(--text-muted);margin-bottom:3px">📅 ${formatDate(entry.date)}</div>
        <div style="font-size:15px;font-weight:700;letter-spacing:1px">🔢 ${escapeHTML(entry.from)}</div>
        ${entry.note ? `<div style="font-size:12px;color:var(--text-muted);margin-top:2px">📝 ${escapeHTML(entry.note)}</div>` : ''}
      </div>
      <button class="btn btn-sm btn-danger" onclick="deleteSerial(${log.length - 1 - i})">🗑️</button>
    </div>
  `).join('');
}

function openAddSerial() {
  document.getElementById('serial-date').value = today();
  document.getElementById('serial-from').value = '';
  document.getElementById('serial-note').value = '';

  // اقتراح: الرقم التالي من آخر سجل
  const settings = DB.getObj(K_SETTINGS, {});
  const log = settings.serialLog || [];
  if (log.length > 0) {
    const last = log[log.length - 1];
    document.getElementById('serial-from').placeholder = `آخر رقم: ${last.from}`;
  }
  openModal('modal-serial');
}

function saveSerial() {
  const from = document.getElementById('serial-from').value.trim();
  if (!from) { toast('يرجى إدخال الرقم', 'error'); return; }
  const settings = DB.getObj(K_SETTINGS, {});
  const log = settings.serialLog || [];
  log.push({
    date: document.getElementById('serial-date').value || today(),
    from,
    note: document.getElementById('serial-note').value.trim(),
  });
  settings.serialLog = log;
  DB.setObj(K_SETTINGS, settings);
  closeModal('modal-serial');
  renderSerialLog();
  toast('✅ تم الحفظ');
}

function deleteSerial(idx) {
  const settings = DB.getObj(K_SETTINGS, {});
  const log = settings.serialLog || [];
  log.splice(idx, 1);
  settings.serialLog = log;
  DB.setObj(K_SETTINGS, settings);
  renderSerialLog();
  toast('تم الحذف', 'info');
}
function initRepairForm() {
  document.getElementById('rep-date').value = today();
  document.getElementById('rep-customer').value = '';
  document.getElementById('rep-phone').value = '';
  document.getElementById('rep-model').value = '';
  document.getElementById('rep-problem').value = '';
  document.getElementById('rep-parts-cost').value  = '';
  document.getElementById('rep-paid').value         = '';
  document.querySelectorAll('input[name="pay-status"]')[0].checked = true;
  document.getElementById('debt-field').style.display = 'none';
  document.getElementById('debt-amount-field').style.display = 'none';
  document.getElementById('rep-debt-who').value = '';
  document.getElementById('rep-debt-amount').value = '';
  // reset fault chips
  document.getElementById('fault-chips').innerHTML = '';
  document.getElementById('rep-fault-select').value = '';
  getCustomersDatalist();
  loadFaultsDropdown();
}

function toggleDebtField(el) {
  const show = el.value === 'دين' || el.value === 'جزئي';
  document.getElementById('debt-field').style.display = show ? '' : 'none';
  document.getElementById('debt-amount-field').style.display = show ? '' : 'none';
}

// ==================== FAULTS ====================
function loadFaultsDropdown() {
  const faults = DB.get(K_FAULTS).sort((a,b)=>(a.name||'').localeCompare(b.name||''));
  const sel = document.getElementById('rep-fault-select');
  if (!sel) return;
  sel.innerHTML = '<option value="">➕ اختر عطل من القائمة...</option>' +
    faults.map(f => `<option value="${escapeHTML(f.name)}">${escapeHTML(f.name)}</option>`).join('');
  // نزيل الـ listener القديم ونضيف جديد
  sel.onchange = null;
  sel.addEventListener('change', function() {
    const val = this.value;
    if (!val) return;
    this.value = ''; // reset فوراً
    const existing = [...document.querySelectorAll('#fault-chips .fault-chip-tag')].map(c=>c.dataset.v);
    if (existing.includes(val)) return;
    const chip = document.createElement('span');
    chip.className = 'fault-chip-tag';
    chip.dataset.v = val;
    chip.style.cssText = 'display:inline-flex;align-items:center;gap:5px;padding:5px 11px;background:var(--accent);color:#fff;border-radius:20px;font-size:13px;cursor:default';
    chip.innerHTML = `${escapeHTML(val)} <span onclick="this.parentElement.remove()" style="cursor:pointer;font-size:16px;line-height:1;opacity:.8">×</span>`;
    document.getElementById('fault-chips').appendChild(chip);
  });
}

function selectFaultChip(sel) {
  // kept for compatibility — logic moved to loadFaultsDropdown listener
}

function openFaultsMgr() {
  renderFaultsMgrList();
  openModal('modal-faults-mgr');
}

function renderFaultsMgrList() {
  const faults = DB.get(K_FAULTS).sort((a,b)=>(a.name||'').localeCompare(b.name||''));
  const el = document.getElementById('faults-manager-list');
  if (!el) return;
  if (faults.length === 0) {
    el.innerHTML = `<div style="text-align:center;color:var(--text-muted);padding:20px">لا توجد أعطال مضافة بعد</div>`;
    return;
  }
  el.innerHTML = faults.map(f => `
    <div style="display:flex;justify-content:space-between;align-items:center;padding:10px 12px;background:var(--bg-card2);border-radius:8px;margin-bottom:8px">
      <span style="font-size:14px">⚡ ${escapeHTML(f.name)}</span>
      <button class="btn btn-sm btn-danger" onclick="deleteFault(${htmlJSArg(f.id)})">🗑️</button>
    </div>
  `).join('');
}

function saveFault() {
  const name = document.getElementById('new-fault-name').value.trim();
  if (!name) { toast('اكتب اسم العطل', 'error'); return; }
  const existing = DB.get(K_FAULTS).find(f => f.name === name);
  if (existing) { toast('هذا العطل موجود مسبقاً', 'error'); return; }
  const faults = DB.get(K_FAULTS);
  faults.push({ id: uid(), name });
  DB.set(K_FAULTS, faults);
  document.getElementById('new-fault-name').value = '';
  renderFaultsMgrList();
  loadFaultsDropdown();
  toast('✅ تم إضافة العطل');
}

function deleteFault(id) {
  DB.set(K_FAULTS, DB.get(K_FAULTS).filter(f => f.id !== id));
  renderFaultsMgrList();
  loadFaultsDropdown();
  toast('تم الحذف', 'info');
}

async function saveRepair() {
  return saveAction('saveRepair', '#page-new-repair button', async () => {
  [...COLLECTIONS, K_SETTINGS].forEach(assertLoaded);
  const customer = document.getElementById('rep-customer').value.trim();
  if (!customer) { toast('يرجى إدخال اسم الزبون', 'error'); return; }

  // جمع الأعطال من الـ chips + الملاحظة الإضافية
  const chipFaults = [...document.querySelectorAll('#fault-chips .fault-chip-tag')]
    .map(c => c.dataset.v).filter(Boolean);
  const extraNote = document.getElementById('rep-problem').value.trim();
  const problem = [...chipFaults, extraNote].filter(Boolean).join(' / ') || '-';

  const payStatus  = document.querySelector('input[name="pay-status"]:checked').value;
  const partsCost  = parseFloat(document.getElementById('rep-parts-cost').value) || 0;
  const paid       = parseFloat(document.getElementById('rep-paid').value) || 0;
  const debtAmount = (payStatus === 'دين' || payStatus === 'جزئي')
    ? (parseFloat(document.getElementById('rep-debt-amount').value) || 0) : 0;
  if (![partsCost, paid, debtAmount].every(value => Number.isFinite(value) && value >= 0)) { toast('المبالغ يجب أن تكون موجبة أو صفراً', 'error'); return; }
  const total = paid; // Preserve existing report semantics; do not migrate financial data implicitly.

  const repair = {
    id: uid(),
    date:       document.getElementById('rep-date').value || today(),
    customer,
    phone:      document.getElementById('rep-phone').value.trim(),
    deviceType: '',
    model:      document.getElementById('rep-model').value.trim(),
    workType:   document.getElementById('rep-work-type').value,
    problem,
    parts:      '',
    partsCost,
    fee: 0,
    total,
    paid,
    payStatus,
    debtWho:    document.getElementById('rep-debt-who').value.trim(),
    debtAmount,

    createdAt:  nowTimestamp(),
  };

  // Save repair
  const repairs = DB.get(K_REPAIRS);
  repairs.push(repair);
  DB.set(K_REPAIRS, repairs);

  // Add to cash or bank
  if (paid > 0) {
    const txType = payStatus === 'بنك' ? 'bank_in' : 'in';
    const cashId = uid();
    const desc = `صيانة: ${customer} | ${repair.deviceType} ${repair.model||''} | ${repair.problem||''}`.replace(/\s+/g,' ').trim();
    const cashDoc = { id: cashId, type: txType, desc, amount: paid, person: customer, date: repair.date, createdAt: nowTimestamp() };
    DB.set(K_CASH, [...DB.get(K_CASH), cashDoc]);
    // احفظ cashId في الصيانة لربطها لاحقاً
    repair.cashId = cashId;
    DB.set(K_REPAIRS, repairs.map(row => row.id === repair.id ? repair : row));
  }

  // Add to debts if needed
  if ((payStatus === 'دين' || payStatus === 'جزئي') && debtAmount > 0) {
    const debts = DB.get(K_DEBTS);
    debts.push({
      id: uid(),
      name: repair.debtWho || customer,
      amount: debtAmount,
      paid: 0,
      date: repair.date,
      note: `صيانة: ${repair.deviceType} ${repair.model}`,
      status: 'مستحق',
      repairId: repair.id,
    });
    DB.set(K_DEBTS, debts);
  }

  // Auto-add customer
  autoAddCustomer(customer, repair.phone);

  await writer.wait();
  toast('✅ تم حفظ العملية بنجاح');
  initRepairForm();
  renderDashboard();

  });
}

function autoAddCustomer(name, phone, type='زبون') {
  if (!name) return;
  const customers = DB.get(K_CUSTOMERS);
  const existing = customers.find(c => c.name.trim().toLowerCase() === name.trim().toLowerCase());
  if (!existing) {
    const newCust = { id: uid(), name: name.trim(), phone: phone || '', type };
    DB.set(K_CUSTOMERS, [...customers, newCust]);
  } else if (phone && !existing.phone) {
    existing.phone = phone;
    DB.set(K_CUSTOMERS, customers);
  }
}

function addCashTransaction(type, desc, amount, date, person='') {
  const cash = DB.get(K_CASH);
  cash.push({ id: uid(), type, desc, amount, person, date: date || today(), createdAt: nowTimestamp() });
  DB.set(K_CASH, cash);
}

// ==================== DEVICES ====================
function renderDevices() {
  const repairs = DB.get(K_REPAIRS);
  const search = document.getElementById('dev-search').value.toLowerCase();

  const fromF = '';
  const toF = '';

  let list = repairs.filter(r => {
    if (search && !(
      r.customer?.toLowerCase().includes(search) ||
      r.deviceType?.toLowerCase().includes(search) ||
      r.model?.toLowerCase().includes(search) ||
      r.problem?.toLowerCase().includes(search) ||
      r.phone?.includes(search)
    )) return false;

    if (fromF && r.date < fromF) return false;
    if (toF && r.date > toF) return false;
    return true;
  }).sort((a,b) => {
    const ta = a.createdAt || (a.date + '_' + (a.id||''));
    const tb = b.createdAt || (b.date + '_' + (b.id||''));
    return tb > ta ? 1 : tb < ta ? -1 : 0;
  });

  const tbody = document.getElementById('devices-tbody');
  if (list.length === 0) {
    tbody.innerHTML = `<tr><td colspan="9"><div class="empty-state"><div class="icon">📭</div><p>لا توجد بيانات</p></div></td></tr>`;
    return;
  }

  tbody.innerHTML = list.map((r, i) => `
    <tr>
      <td>${i+1}</td>
      <td>
        <div style="font-size:13px">${r.createdAt ? formatDateTime(r.createdAt) : formatDate(r.date)}</div>
      </td>
      <td><strong>${escapeHTML(r.customer)}</strong></td>
      <td>${escapeHTML(r.phone||'-')}</td>
      <td>${escapeHTML(r.deviceType||'-')} ${escapeHTML(r.model||'')}</td>
      <td style="max-width:150px"><span title="${escapeHTML(r.problem)}">${(r.problem||'').substring(0,30)}${r.problem?.length>30?'...':''}</span></td>
      <td>${formatNum(r.paid)} ل</td>
      <td>${getPayBadge(r.payStatus)}</td>
      <td>
        <div style="display:flex;gap:4px">
          <button class="btn btn-sm btn-secondary" onclick="editRepair(${htmlJSArg(r.id)})" title="تعديل">✏️ تعديل</button>
          <button class="btn btn-sm btn-success" onclick="showInvoice(${htmlJSArg(r.id)})" title="فاتورة">🖨️</button>
          <button class="btn btn-sm btn-danger" onclick="askDeleteRepair(${htmlJSArg(r.id)})" title="حذف">🗑️</button>
        </div>
      </td>
    </tr>
  `).join('');
}

function getPayBadge(s) {
  const m = { 'كاش':'badge-success', 'بنك':'badge-info', 'دين':'badge-danger', 'جزئي':'badge-warning' };
  return `<span class="badge ${m[s]||'badge-info'}">${escapeHTML(s||'-')}</span>`;
}

function getStatusBadge(s) {
  const m = { 'قيد الصيانة':'badge-warning', 'تم الإصلاح':'badge-info', 'تم التسليم':'badge-success' };
  return `<span class="badge ${m[s]||'badge-info'}">${escapeHTML(s||'-')}</span>`;
}

function editRepair(id) {
  editRepairId = id;
  const r = DB.get(K_REPAIRS).find(x => x.id === id);
  if (!r) return;

  // اجلب أعطال للـ dropdown
  const faults = DB.get(K_FAULTS).map(f => f.name || f);

  document.getElementById('edit-repair-form').innerHTML = `
    <div class="form-group">
      <label>التاريخ</label>
      <input type="date" id="er-date" value="${escapeHTML(r.date)}">
    </div>
    <div class="form-group">
      <label>الزبون</label>
      <input type="text" id="er-customer" value="${escapeHTML(r.customer)}" list="customers-list">
    </div>
    <div class="form-group">
      <label>الهاتف</label>
      <input type="tel" id="er-phone" value="${escapeHTML(r.phone||'')}">
    </div>
    <div class="form-group">
      <label>نوع الجهاز</label>
      <input type="text" id="er-type" value="${escapeHTML(r.deviceType||'')}">
    </div>
    <div class="form-group">
      <label>الموديل</label>
      <input type="text" id="er-model" value="${escapeHTML(r.model||'')}">
    </div>
    <div class="form-group">
      <label>نوع العمل</label>
      <select id="er-work-type">
        ${['هاردوير','سوفتوير','هاردوير وسوفتوير'].map(v=>`<option ${r.workType===v?'selected':''}>${v}</option>`).join('')}
      </select>
    </div>
    <div class="form-group full">
      <label>العطل / الملاحظة</label>
      <textarea id="er-problem">${escapeHTML(r.problem||'')}</textarea>
    </div>
    <div class="form-group">
      <label>القطع</label>
      <input type="text" id="er-parts" value="${escapeHTML(r.parts||'')}">
    </div>
    <div class="form-group">
      <label>سعر القطع (ليرة)</label>
      <input type="number" id="er-parts-cost" value="${r.partsCost||0}" min="0" step="0.5">
    </div>
    <div class="form-group">
      <label>المبلغ المقبوض (ليرة)</label>
      <input type="number" id="er-paid" value="${r.paid||0}" min="0" step="0.5">
    </div>
    <div class="form-group full">
      <label>حالة الدفع</label>
      <div class="radio-group">
        <label class="radio-option"><input type="radio" name="er-pay-status" value="كاش" ${r.payStatus==='كاش'?'checked':''} onchange="erToggleDebt(this)"> 💵 كاش</label>
        <label class="radio-option"><input type="radio" name="er-pay-status" value="بنك" ${r.payStatus==='بنك'?'checked':''} onchange="erToggleDebt(this)"> 🏦 بنك</label>
        <label class="radio-option"><input type="radio" name="er-pay-status" value="دين" ${r.payStatus==='دين'?'checked':''} onchange="erToggleDebt(this)"> 📋 دين</label>
        <label class="radio-option"><input type="radio" name="er-pay-status" value="جزئي" ${r.payStatus==='جزئي'?'checked':''} onchange="erToggleDebt(this)"> 💳 جزئي</label>
      </div>
    </div>
    <div class="form-group" id="er-debt-field" style="display:${['دين','جزئي'].includes(r.payStatus)?'block':'none'}">
      <label>الدين على</label>
      <input type="text" id="er-debt-who" value="${escapeHTML(r.debtWho||'')}" list="customers-list" placeholder="اسم المدين">
    </div>
    <div class="form-group" id="er-debt-amount-field" style="display:${['دين','جزئي'].includes(r.payStatus)?'block':'none'}">
      <label>مبلغ الدين (ليرة)</label>
      <input type="number" id="er-debt-amount" value="${r.debtAmount||0}" min="0" step="0.5">
    </div>
  `;
  openModal('modal-edit-repair');
}

function erToggleDebt(el) {
  const show = el.value === 'دين' || el.value === 'جزئي';
  document.getElementById('er-debt-field').style.display       = show ? 'block' : 'none';
  document.getElementById('er-debt-amount-field').style.display = show ? 'block' : 'none';
}

async function saveEditRepair() {
  return saveAction('saveEditRepair', '#modal-edit-repair button', async () => {
  if (!editRepairId) return;
  const repairs = DB.get(K_REPAIRS);
  const idx = repairs.findIndex(x => x.id === editRepairId);
  if (idx === -1) return;

  const old        = repairs[idx];
  const partsCost  = parseFloat(document.getElementById('er-parts-cost').value) || 0;
  const paid       = parseFloat(document.getElementById('er-paid').value) || 0;
  const payStatus  = document.querySelector('input[name="er-pay-status"]:checked').value;
  const debtWho    = document.getElementById('er-debt-who')?.value.trim() || '';
  const debtAmount = (['دين','جزئي'].includes(payStatus))
    ? parseFloat(document.getElementById('er-debt-amount').value) || 0 : 0;

  if (![partsCost, paid, debtAmount].every(value => Number.isFinite(value) && value >= 0)) throw new Error('المبالغ يجب أن تكون موجبة أو صفراً');
  repairs[idx] = {
    ...old,
    date:       document.getElementById('er-date').value,
    customer:   document.getElementById('er-customer').value.trim(),
    phone:      document.getElementById('er-phone').value.trim(),
    deviceType: document.getElementById('er-type').value.trim(),
    model:      document.getElementById('er-model').value.trim(),
    workType:   document.getElementById('er-work-type').value,
    problem:    document.getElementById('er-problem').value.trim(),
    parts:      document.getElementById('er-parts').value.trim(),
    partsCost, paid, payStatus, debtWho, debtAmount,
    total: paid,
  };
  DB.set(K_REPAIRS, repairs);

  // ── مزامنة الصندوق ──
  const oldPaid   = +old.paid || 0;
  const oldStatus = old.payStatus;
  if (oldPaid !== paid || oldStatus !== payStatus) {
    // احذف المعاملة القديمة بدقة بالـ cashId
    if (old.cashId) {
      DB.set(K_CASH, DB.get(K_CASH).filter(c => c.id !== old.cashId));
    } else {
      // fallback: match بالبيانات
      DB.set(K_CASH, DB.get(K_CASH).filter(c =>
        !(c.desc?.includes(old.customer) && c.date === old.date &&
          +c.amount === oldPaid && ['in','bank_in'].includes(c.type))
      ));
    }
    // أضف الجديدة لو في مبلغ
    if (paid > 0) {
      const txType = payStatus === 'بنك' ? 'bank_in' : 'in';
      const newCashId = uid();
      const desc = `صيانة: ${repairs[idx].customer} | ${repairs[idx].deviceType||''} ${repairs[idx].model||''} | ${repairs[idx].problem||''}`.replace(/\s+/g,' ').trim();
      const cashDoc = { id: newCashId, type: txType, desc, amount: paid, person: repairs[idx].customer, date: repairs[idx].date, createdAt: nowTimestamp() };
      DB.set(K_CASH, [...DB.get(K_CASH), cashDoc]);
      repairs[idx].cashId = newCashId;
    } else {
      repairs[idx].cashId = null;
    }
    DB.set(K_REPAIRS, repairs);
  }

  // ── مزامنة الديون ──
  const debts = DB.get(K_DEBTS);
  const debtIdx = debts.findIndex(d => d.repairId === editRepairId);

  if (['دين','جزئي'].includes(payStatus) && debtAmount > 0) {
    if (debtIdx !== -1) {
      // حدّث الدين الموجود
      debts[debtIdx] = { ...debts[debtIdx], name: debtWho || repairs[idx].customer, amount: debtAmount, date: repairs[idx].date };
      DB.set(K_DEBTS, debts);
    } else {
      // أضف دين جديد
      debts.push({ id: uid(), name: debtWho || repairs[idx].customer, amount: debtAmount, paid: 0, date: repairs[idx].date, note: `${repairs[idx].model||''} — ${repairs[idx].problem||''}`.trim(), status: 'مستحق', repairId: editRepairId });
      DB.set(K_DEBTS, debts);
    }
  } else if (debtIdx !== -1) {
    // حذف الدين لو تغيرت طريقة الدفع
    DB.set(K_DEBTS, debts.filter((_,i) => i !== debtIdx));
  }

  // ── مزامنة العملاء ──
  const customers = DB.get(K_CUSTOMERS);
  const custIdx = customers.findIndex(c => c.name === repairs[idx].customer);
  if (custIdx === -1 && repairs[idx].customer) {
    customers.push({ id: uid(), name: repairs[idx].customer, phone: repairs[idx].phone, note: '' });
    DB.set(K_CUSTOMERS, customers);
  } else if (custIdx !== -1 && repairs[idx].phone) {
    customers[custIdx].phone = repairs[idx].phone;
    DB.set(K_CUSTOMERS, customers);
  }

  await writer.wait();
  closeModal('modal-edit-repair');
  renderDevices();
  renderDebts();
  toast('✅ تم تحديث العملية');

  });
}

function deleteRepair(id) {
  document.getElementById('confirm-message').textContent = 'حذف الصيانة وديونها ودفعاتها ومعاملات الصندوق المرتبطة؟';
  document.getElementById('confirm-ok').onclick = () => saveAction('delete-repair', '#modal-confirm button', async () => {
    [K_REPAIRS, K_DEBTS, K_DEBT_PAYMENTS, K_CASH].forEach(assertLoaded);
    const repairs = DB.get(K_REPAIRS), repair = repairs.find(row => row.id === id);
    if (!repair) throw new Error('الصيانة غير موجودة');
    const debts = DB.get(K_DEBTS), payments = DB.get(K_DEBT_PAYMENTS), cash = DB.get(K_CASH);
    const debtIds = new Set(debts.filter(row => row.repairId === id).map(row => row.id));
    const cashIds = new Set(payments.filter(row => debtIds.has(row.debtId)).map(row => row.cashId).filter(Boolean));
    if (repair.cashId) cashIds.add(repair.cashId);
    else if (Number(repair.paid) > 0) {
      const candidates = cash.filter(row => row.desc?.startsWith(`صيانة: ${repair.customer} |`) && row.date === repair.date &&
        Number(row.amount) === Number(repair.paid) && ['in', 'bank_in'].includes(row.type));
      if (candidates.length !== 1) throw new Error('تعذر تحديد معاملة الصيانة القديمة بدقة؛ راجع الصندوق أولاً');
      cashIds.add(candidates[0].id);
    }
    DB.set(K_REPAIRS, repairs.filter(row => row.id !== id));
    DB.set(K_DEBTS, debts.filter(row => !debtIds.has(row.id)));
    DB.set(K_DEBT_PAYMENTS, payments.filter(row => !debtIds.has(row.debtId)));
    DB.set(K_CASH, cash.filter(row => !cashIds.has(row.id)));
    await writer.wait(); closeModal('modal-confirm'); renderDevices(); renderDashboard(); updateNotifBadge(); toast('تم حذف الصيانة وسجلاتها المرتبطة');
  });
  openModal('modal-confirm');
}

// ==================== INVOICE ====================
function showInvoice(id) {
  const repairs = DB.get(K_REPAIRS);
  const r = repairs.find(x => x.id === id);
  if (!r) return;
  const settings = DB.getObj(K_SETTINGS, {});

  const remaining = (r.total || 0) - (r.paid || 0);

  document.getElementById('invoice-content').innerHTML = `
    <div style="text-align:center;padding:20px 0;border-bottom:1px solid var(--border);margin-bottom:20px">
      <div style="font-size:22px;font-weight:900;margin-bottom:4px">${escapeHTML(settings.shopName || 'محل الصيانة')}</div>
      <div style="font-size:12px;color:var(--text-muted)">${escapeHTML(settings.shopPhone||'')} | ${escapeHTML(settings.shopAddress||'')}</div>
    </div>
    <table style="width:100%;border-collapse:collapse;font-size:13px">
      <tr style="border-bottom:1px solid var(--border)">
        <td style="padding:8px;color:var(--text-muted);width:40%">التاريخ</td>
        <td style="padding:8px"><strong>${formatDate(r.date)}</strong></td>
      </tr>
      <tr style="border-bottom:1px solid var(--border)">
        <td style="padding:8px;color:var(--text-muted)">اسم الزبون</td>
        <td style="padding:8px"><strong>${escapeHTML(r.customer)}</strong></td>
      </tr>
      <tr style="border-bottom:1px solid var(--border)">
        <td style="padding:8px;color:var(--text-muted)">رقم الهاتف</td>
        <td style="padding:8px">${escapeHTML(r.phone||'-')}</td>
      </tr>
      <tr style="border-bottom:1px solid var(--border)">
        <td style="padding:8px;color:var(--text-muted)">الجهاز</td>
        <td style="padding:8px">${escapeHTML(r.deviceType||'-')} ${escapeHTML(r.model||'')}</td>
      </tr>
      <tr style="border-bottom:1px solid var(--border)">
        <td style="padding:8px;color:var(--text-muted)">نوع العمل</td>
        <td style="padding:8px">${escapeHTML(r.workType||'-')}</td>
      </tr>
      <tr style="border-bottom:1px solid var(--border)">
        <td style="padding:8px;color:var(--text-muted)">وصف العطل</td>
        <td style="padding:8px">${escapeHTML(r.problem||'-')}</td>
      </tr>
      <tr style="border-bottom:1px solid var(--border)">
        <td style="padding:8px;color:var(--text-muted)">القطع المستخدمة</td>
        <td style="padding:8px">${r.parts||'-'}</td>
      </tr>
      <tr style="border-bottom:1px solid var(--border)">
        <td style="padding:8px;color:var(--text-muted)">سعر القطع</td>
        <td style="padding:8px">${formatNum(r.partsCost)} ليرة</td>
      </tr>
      <tr style="border-bottom:1px solid var(--border)">
        <td style="padding:8px;color:var(--text-muted)">أجرة الصيانة</td>
        <td style="padding:8px">${formatNum(r.fee)} ليرة</td>
      </tr>
      <tr style="border-bottom:1px solid var(--border)">
        <td style="padding:8px;color:var(--text-muted)">الإجمالي</td>
        <td style="padding:8px"><strong style="font-size:16px">${formatNum(r.total)} ليرة</strong></td>
      </tr>
      <tr style="border-bottom:1px solid var(--border)">
        <td style="padding:8px;color:var(--text-muted)">المدفوع</td>
        <td style="padding:8px;color:var(--success)"><strong>${formatNum(r.paid)} ليرة</strong></td>
      </tr>
      <tr>
        <td style="padding:8px;color:var(--text-muted)">المتبقي</td>
        <td style="padding:8px;color:${remaining>0?'var(--danger)':'var(--success)'}">
          <strong>${formatNum(remaining)} ليرة</strong>
        </td>
      </tr>
    </table>
    ${settings.invoiceNote ? `<div style="margin-top:16px;padding:12px;background:var(--bg-card2);border-radius:8px;font-size:12px;color:var(--text-muted)">${escapeHTML(settings.invoiceNote)}</div>` : ''}
  `;

  openModal('modal-invoice');
}

function printInvoice() {
  const settings = DB.getObj(K_SETTINGS, {});
  const content = document.getElementById('invoice-content').innerHTML;
  const w = window.open('', '_blank');
  w.document.write(`
    <html dir="rtl"><head>
    <meta charset="UTF-8">
    <link href="https://fonts.googleapis.com/css2?family=Cairo:wght@400;700;900&display=swap" rel="stylesheet">
    <style>
      body { font-family: 'Cairo', sans-serif; padding: 40px; direction: rtl; max-width: 600px; margin: auto; }
      table { width: 100%; border-collapse: collapse; }
      td { padding: 10px; border-bottom: 1px solid #eee; font-size: 14px; }
      .header { text-align: center; margin-bottom: 30px; border-bottom: 2px solid #333; padding-bottom: 20px; }
      h1 { font-size: 28px; margin: 0; }
      .footer { text-align: center; margin-top: 40px; font-size: 12px; color: #888; border-top: 1px solid #eee; padding-top: 20px; }
      @media print { button { display: none !important; } }
    </style></head><body>
    <div class="header">
      <h1>${escapeHTML(settings.shopName || 'محل الصيانة')}</h1>
      <p>${escapeHTML(settings.shopPhone || '')} | ${escapeHTML(settings.shopAddress || '')}</p>
    </div>
    <h2 style="text-align:center;margin:20px 0;font-size:18px">فاتورة صيانة</h2>
    ${content}
    <div class="footer">${escapeHTML(settings.invoiceNote || 'شكراً لتعاملكم معنا')}</div>
    <div style="margin-top:20px;text-align:center">
      <button onclick="window.print()" style="padding:10px 30px;font-size:16px;font-family:Cairo,sans-serif;cursor:pointer">🖨️ طباعة</button>
    </div>
    </body></html>
  `);
  w.document.close();
}

// ==================== CASH ====================
function renderCash() {
  // الشهر المختار (افتراضي: الشهر الحالي)
  const monthEl = document.getElementById('cash-month-filter');
  const selectedMonth = monthEl?.value || '';

  const all = DB.get(K_CASH)
    .filter(c => !selectedMonth || (c.date || '').startsWith(selectedMonth))
    .sort((a,b) => {
      const ta = a.createdAt || a.date || '';
      const tb = b.createdAt || b.date || '';
      return tb.localeCompare(ta);
    });
  const filter = document.getElementById('cash-filter')?.value || 'all';

  // كاش
  const cashIn  = all.filter(c => c.type === 'in').reduce((s,c) => s + +c.amount, 0);
  const cashOut = all.filter(c => c.type === 'out').reduce((s,c) => s + +c.amount, 0);

  const cashOutExp   = all.filter(c => c.type === 'out' && c.desc?.startsWith('مصروف:')).reduce((s,c) => s + +c.amount, 0);
  const cashOutEmp   = all.filter(c => c.type === 'out' && c.desc?.startsWith('سحب موظف:')).reduce((s,c) => s + +c.amount, 0);
  const cashOutOther = cashOut - cashOutExp - cashOutEmp;

  document.getElementById('cash-in-total').textContent      = formatNum(cashIn);
  document.getElementById('cash-out-total').textContent     = formatNum(cashOut);
  document.getElementById('cash-balance').textContent       = formatNum(cashIn - cashOut);
  document.getElementById('cash-out-expenses').textContent  = formatNum(cashOutExp);
  document.getElementById('cash-out-employees').textContent = formatNum(cashOutEmp);
  document.getElementById('cash-out-other').textContent     = formatNum(cashOutOther);

  // بنك
  const bankIn  = all.filter(c => c.type === 'bank_in').reduce((s,c) => s + +c.amount, 0);
  const bankOut = all.filter(c => c.type === 'bank_out').reduce((s,c) => s + +c.amount, 0);
  const bankOutExp   = all.filter(c => c.type === 'bank_out' && c.desc?.startsWith('مصروف:')).reduce((s,c) => s + +c.amount, 0);
  const bankOutEmp   = all.filter(c => c.type === 'bank_out' && c.desc?.startsWith('سحب موظف:')).reduce((s,c) => s + +c.amount, 0);
  const bankOutOther = bankOut - bankOutExp - bankOutEmp;

  document.getElementById('bank-in-total').textContent      = formatNum(bankIn);
  document.getElementById('bank-out-total').textContent     = formatNum(bankOut);
  document.getElementById('bank-balance').textContent       = formatNum(bankIn - bankOut);
  document.getElementById('bank-out-expenses').textContent  = formatNum(bankOutExp);
  document.getElementById('bank-out-employees').textContent = formatNum(bankOutEmp);
  document.getElementById('bank-out-other').textContent     = formatNum(bankOutOther);

  // دولار
  const usdIn  = all.filter(c => c.type === 'usd_in').reduce((s,c) => s + +c.usdAmount, 0);
  const usdOut = all.filter(c => c.type === 'usd_out').reduce((s,c) => s + +c.usdAmount, 0);
  document.getElementById('usd-in-total').textContent  = usdIn.toFixed(2);
  document.getElementById('usd-out-total').textContent = usdOut.toFixed(2);
  const usdBal = usdIn - usdOut;
  document.getElementById('usd-balance').textContent   = usdBal.toFixed(2);

  // فلترة الجدول
  const shown = filter === 'cash' ? all.filter(c => c.type==='in'||c.type==='out')
              : filter === 'bank' ? all.filter(c => c.type==='bank_in'||c.type==='bank_out')
              : filter === 'usd'  ? all.filter(c => c.type==='usd_in'||c.type==='usd_out')
              : all;

  const tbody = document.getElementById('cash-tbody');
  if (shown.length === 0) {
    tbody.innerHTML = `<tr><td colspan="7"><div class="empty-state"><div class="icon">📭</div><p>لا توجد معاملات</p></div></td></tr>`;
    return;
  }

  tbody.innerHTML = shown.map(c => {
    const isUsd  = c.type === 'usd_in' || c.type === 'usd_out';
    const isIn   = c.type === 'in' || c.type === 'bank_in' || c.type === 'usd_in';
    const isBank = c.type === 'bank_in' || c.type === 'bank_out';
    const mediaBadge = isUsd  ? '<span class="badge" style="background:#b45309">💲 دولار</span>'
                     : isBank ? '<span class="badge badge-info">🏦 بنك</span>'
                     :          '<span class="badge badge-success">💵 كاش</span>';
    const typeBadge = isIn
      ? '<span class="badge badge-success">⬆️ مقبوض</span>'
      : '<span class="badge badge-danger">⬇️ مدفوع</span>';

    let amountCell;
    if (isUsd) {
      const usdAmt = +c.usdAmount;
      const liraAmt = +c.amount; // تكلفة الليرة عند الشراء
      amountCell = `<strong style="color:${isIn?'var(--success)':'var(--danger)'}">${isIn?'+':'−'}${usdAmt.toFixed(2)}$</strong>
        ${isIn && liraAmt ? `<br><small style="color:var(--text-muted)">كلفة: ${formatNum(liraAmt)} ل</small>` : ''}`;
    } else {
      amountCell = `<strong style="color:${isIn?'var(--success)':'var(--danger)'}">${isIn?'+':'−'}${formatNum(c.amount)} ل</strong>`;
    }

    return `
    <tr>
      <td style="white-space:nowrap;font-size:12px">${c.createdAt ? formatDateTime(c.createdAt) : formatDate(c.date)}</td>
      <td>${mediaBadge}</td>
      <td>${typeBadge}</td>
      <td>${escapeHTML(c.desc)}</td>
      <td>${c.person ? `<span class="badge badge-info">👤 ${escapeHTML(c.person)}</span>` : '-'}</td>
      <td>${amountCell}</td>
      <td style="display:flex;gap:4px">
        <button onclick="editCash(${htmlJSArg(c.id)})" style="background:rgba(0,212,170,0.1);border:1px solid rgba(0,212,170,0.2);border-radius:6px;padding:3px 8px;cursor:pointer;color:var(--accent);font-size:11px;font-family:inherit">✏️</button>
        <button onclick="askDeleteCash(${htmlJSArg(c.id)})" style="background:rgba(239,68,68,0.1);border:1px solid rgba(239,68,68,0.2);border-radius:6px;padding:3px 8px;cursor:pointer;color:var(--danger);font-size:11px;font-family:inherit">🗑️</button>
      </td>
    </tr>`;
  }).join('');
}

function toggleCashOutDetail() {
  const el   = document.getElementById('cash-out-detail');
  const lbl  = document.querySelector('[onclick="toggleCashOutDetail()"] .stat-label');
  const open = el.style.display !== 'none';
  el.style.display = open ? 'none' : 'block';
  if (lbl) lbl.textContent = open ? 'مدفوعات كاش ▼' : 'مدفوعات كاش ▲';
}

function toggleBankOutDetail() {
  const el   = document.getElementById('bank-out-detail');
  const lbl  = document.querySelector('[onclick="toggleBankOutDetail()"] .stat-label');
  const open = el.style.display !== 'none';
  el.style.display = open ? 'none' : 'block';
  if (lbl) lbl.textContent = open ? 'إجمالي مدفوعات البنك ▼' : 'إجمالي مدفوعات البنك ▲';
}

function linkedCash(id) {
  return cache.repairs.some(row => row.cashId === id) || cache.debt_payments.some(row => row.cashId === id) ||
    cache.withdrawals.some(row => row.cashId === id) || cache.expenses.some(row => row.cashId === id);
}
window.editCash = function(id) {
  if (linkedCash(id)) { toast('عدّل هذه المعاملة من سجلها الأصلي (الصيانة أو الدين أو المصروف أو السحب)', 'error'); return; }
  const c = DB.get(K_CASH).find(x => x.id === id);
  if (!c) return;
  document.getElementById('ec-id').value     = id;
  document.getElementById('ec-desc').value   = c.desc   || '';
  document.getElementById('ec-person').value = c.person || '';
  document.getElementById('ec-date').value   = c.date   || today();

  const isUsd  = (c.type === 'usd_in' || c.type === 'usd_out');
  const isCash = (c.type === 'in' || c.type === 'out' || isUsd);

  // ضبط نوع المعاملة في القائمة
  const typeEl = document.getElementById('ec-type');
  if (typeEl) typeEl.value = c.type || 'in';

  // ضبط المبلغ حسب العملة
  document.getElementById('ec-amount').value = isUsd ? (c.usdAmount || '') : (c.amount || '');

  // إظهار/إخفاء خيار العملة
  const currGrp = document.getElementById('ec-currency-group');
  if (currGrp) currGrp.style.display = isCash ? '' : 'none';

  // ضبط راديو العملة
  const cur = isUsd ? 'USD' : 'TRY';
  document.querySelectorAll('input[name="ec-currency"]').forEach(r => { r.checked = r.value === cur; });

  // ضبط الـ label
  const lbl = document.getElementById('ec-amount-label');
  if (lbl) lbl.textContent = isUsd ? 'المبلغ (دولار) *' : 'المبلغ (ليرة تركية) *';

  getCustomersDatalist();
  openModal('modal-edit-cash');
};

window.saveEditCash = function() {
  const id     = document.getElementById('ec-id').value;
  if (linkedCash(id)) { toast('المعاملة مرتبطة بسجل آخر؛ عدّلها من القسم الأصلي', 'error'); return; }
  const desc   = document.getElementById('ec-desc').value.trim();
  const amount = parseFloat(document.getElementById('ec-amount').value);
  if (!desc || !positiveMoney(amount)) { toast('يرجى إدخال الوصف والمبلغ', 'error'); return; }

  let type = document.getElementById('ec-type').value;
  // إذا كانت كاش عادي وتم اختيار USD → حوّل النوع
  const isCash = (type === 'in' || type === 'out');
  if (isCash) {
    const cur = document.querySelector('input[name="ec-currency"]:checked')?.value || 'TRY';
    if (cur === 'USD') type = type === 'in' ? 'usd_in' : 'usd_out';
  }

  const isUsd = (type === 'usd_in' || type === 'usd_out');
  const cash  = DB.get(K_CASH);
  const idx   = cash.findIndex(c => c.id === id);
  if (idx === -1) return;

  cash[idx] = {
    ...cash[idx], type, desc,
    person: document.getElementById('ec-person').value.trim(),
    date:   document.getElementById('ec-date').value || today(),
    amount:    isUsd ? 0     : amount,
    usdAmount: isUsd ? amount : undefined,
  };
  DB.set(K_CASH, cash);
  closeModal('modal-edit-cash');
  renderCash();
  renderDashboard();
  toast('✅ تم تحديث المعاملة');
};

function onCashCurrencyChange() {
  const cur = document.querySelector('input[name="cash-currency"]:checked')?.value || 'TRY';
  const lbl = document.getElementById('cash-amount-label');
  if (lbl) lbl.textContent = cur === 'USD' ? 'المبلغ (دولار)' : 'المبلغ (ليرة تركية)';
}

function onEditCashTypeChange() {
  const type = document.getElementById('ec-type')?.value;
  const grp  = document.getElementById('ec-currency-group');
  const lbl  = document.getElementById('ec-amount-label');
  const isCash = (type === 'in' || type === 'out');
  const isUsd  = (type === 'usd_in' || type === 'usd_out');
  if (grp) grp.style.display = isCash ? '' : 'none';
  if (lbl) {
    if (isUsd) lbl.textContent = 'المبلغ (دولار) *';
    else if (isCash) {
      const cur = document.querySelector('input[name="ec-currency"]:checked')?.value || 'TRY';
      lbl.textContent = cur === 'USD' ? 'المبلغ (دولار) *' : 'المبلغ (ليرة تركية) *';
    } else lbl.textContent = 'المبلغ (ليرة تركية) *';
  }
  // إذا اختار usd_in/usd_out نضبط radio على USD
  if (isUsd) {
    const usdRadio = document.querySelector('input[name="ec-currency"][value="USD"]');
    if (usdRadio) usdRadio.checked = true;
  }
}

function onEditCashCurrencyChange() {
  const cur  = document.querySelector('input[name="ec-currency"]:checked')?.value || 'TRY';
  const type = document.getElementById('ec-type')?.value;
  const lbl  = document.getElementById('ec-amount-label');
  if (!lbl) return;
  if (cur === 'USD') {
    lbl.textContent = 'المبلغ (دولار) *';
    // غيّر النوع تلقائياً
    if (type === 'in')  document.getElementById('ec-type').value = 'usd_in';
    if (type === 'out') document.getElementById('ec-type').value = 'usd_out';
  } else {
    lbl.textContent = 'المبلغ (ليرة تركية) *';
    if (type === 'usd_in')  document.getElementById('ec-type').value = 'in';
    if (type === 'usd_out') document.getElementById('ec-type').value = 'out';
  }
}

function openAddCash(type) {
  document.getElementById('cash-type').value = type;
  const isCash = (type === 'in' || type === 'out');
  const isBankOut = (type === 'bank_out');
  const titles = { in:'➕ مقبوض كاش', out:'➖ مدفوع كاش', bank_in:'🏦 مقبوض بنك', bank_out:'🏦 مدفوع بنك' };
  document.getElementById('cash-modal-title').textContent = titles[type] || '➕ إضافة معاملة';
  document.getElementById('cash-desc').value   = '';
  document.getElementById('cash-person').value = '';
  document.getElementById('cash-amount').value = '';
  document.getElementById('cash-date').value   = today();
  // خيار العملة فقط للكاش
  const currGrp = document.getElementById('cash-currency-group');
  if (currGrp) currGrp.style.display = isCash ? '' : 'none';
  // reset العملة لليرة
  const tryRadio = document.querySelector('input[name="cash-currency"][value="TRY"]');
  if (tryRadio) { tryRadio.checked = true; onCashCurrencyChange(); }
  // حقل الشخص للمدفوعات فقط
  document.getElementById('cash-person-group').style.display = (type === 'out' || isBankOut) ? '' : 'none';
  getCustomersDatalist();
  openModal('modal-cash');
}

async function saveCash() {
  return saveAction('saveCash', '#modal-cash button', async () => {
  const desc   = document.getElementById('cash-desc').value.trim();
  const amount = parseFloat(document.getElementById('cash-amount').value);
  let   type   = document.getElementById('cash-type').value;
  if (!desc || !positiveMoney(amount)) { toast('يرجى إدخال الوصف والمبلغ', 'error'); return; }

  const isCash = (type === 'in' || type === 'out');
  const currency = isCash
    ? (document.querySelector('input[name="cash-currency"]:checked')?.value || 'TRY')
    : 'TRY';

  // إذا اختار دولار على كاش عادي → غيّر النوع
  if (isCash && currency === 'USD') {
    type = type === 'in' ? 'usd_in' : 'usd_out';
  }

  const person = (type === 'out' || type === 'bank_out' || type === 'usd_out')
    ? (document.getElementById('cash-person').value.trim() || '') : '';

  const entry = { id: uid(), type, desc, person,
    date: document.getElementById('cash-date').value || today(),
    createdAt: nowTimestamp() };

  if (type === 'usd_in' || type === 'usd_out') {
    entry.amount    = 0;
    entry.usdAmount = amount;
  } else {
    entry.amount = amount;
  }

  const cash = DB.get(K_CASH);
  cash.push(entry);
  DB.set(K_CASH, cash);
  await writer.wait();
  closeModal('modal-cash');
  renderCash();
  toast('✅ تم إضافة المعاملة');

  });
}

function deleteCash(id) {
  if (linkedCash(id)) { toast('احذف هذه المعاملة من سجلها الأصلي لضمان تطابق الحسابات', 'error'); return; }
  document.getElementById('confirm-message').textContent = 'هل أنت متأكد من حذف هذه المعاملة؟';
  document.getElementById('confirm-ok').onclick = () => {
    DB.set(K_CASH, DB.get(K_CASH).filter(c => c.id !== id));
    closeModal('modal-confirm');
    renderCash();
    toast('تم الحذف', 'info');
  };
  openModal('modal-confirm');
}

function openTransfer() {
  document.getElementById('transfer-amount').value = '';
  document.getElementById('transfer-date').value = today();
  document.getElementById('transfer-note').value = '';
  document.querySelectorAll('input[name="transfer-dir"]')[0].checked = true;
  updateTransferLabels();
  openModal('modal-transfer');
}

function updateTransferLabels() {
  const dir = document.querySelector('input[name="transfer-dir"]:checked').value;
  const lbl1 = document.getElementById('lbl-cash-to-bank');
  const lbl2 = document.getElementById('lbl-bank-to-cash');
  if (dir === 'cash_to_bank') {
    lbl1.style.borderColor = 'var(--accent)';
    lbl2.style.borderColor = 'var(--border)';
  } else {
    lbl1.style.borderColor = 'var(--border)';
    lbl2.style.borderColor = 'var(--accent)';
  }
}

async function saveTransfer() {
  return saveAction('saveTransfer', '#modal-transfer button', async () => {
  const amount = parseFloat(document.getElementById('transfer-amount').value);
  if (!amount || amount <= 0) { toast('يرجى إدخال مبلغ صحيح', 'error'); return; }
  const dir  = document.querySelector('input[name="transfer-dir"]:checked').value;
  const date = document.getElementById('transfer-date').value || today();
  const note = document.getElementById('transfer-note').value.trim();
  const label = note ? `تحويل: ${note}` : 'تحويل بين الصندوق والبنك';

  const cash = DB.get(K_CASH);
  if (dir === 'cash_to_bank') {
    // خصم من الكاش، إضافة للبنك
    cash.push({ id: uid(), type: 'out',     desc: `${label} (سحب من الصندوق)`, amount, person: '', date });
    cash.push({ id: uid(), type: 'bank_in', desc: `${label} (إيداع في البنك)`,  amount, person: '', date });
  } else {
    // خصم من البنك، إضافة للكاش
    cash.push({ id: uid(), type: 'bank_out', desc: `${label} (سحب من البنك)`,     amount, person: '', date });
    cash.push({ id: uid(), type: 'in',       desc: `${label} (إيداع في الصندوق)`, amount, person: '', date });
  }
  DB.set(K_CASH, cash);
  await writer.wait();
  closeModal('modal-transfer');
  renderCash();
  toast('✅ تم التحويل بنجاح');

  });
}

function openDollarModal(opType) {
  // لو ما اتمرر opType افتح نافذة اختيار
  if (!opType) {
    // نستعمل confirm modal مبسط — نفتح مباشرة نافذة الشراء كـ default
    opType = 'buy';
  }
  document.getElementById('dollar-op-type').value = opType;
  document.getElementById('dollar-usd-amount').value = '';
  document.getElementById('dollar-pay-amount').value = '';
  document.getElementById('dollar-pay-to').value = '';
  document.getElementById('dollar-rate').value = '';
  document.getElementById('dollar-note').value = '';
  document.getElementById('dollar-date').value = today();
  document.getElementById('dollar-lira-preview').textContent = 'المبلغ بالليرة: —';

  if (opType === 'buy') {
    document.getElementById('dollar-modal-title').textContent = '💲 شراء دولار';
    document.getElementById('dollar-buy-section').style.display = '';
    document.getElementById('dollar-pay-section').style.display = 'none';
    document.getElementById('dollar-rate-group').style.display = '';
    document.getElementById('dollar-save-btn').textContent = '🛒 تسجيل الشراء';
    // تحديث المعاينة عند الكتابة
    document.getElementById('dollar-usd-amount').oninput = updateDollarPreview;
    document.getElementById('dollar-rate').oninput        = updateDollarPreview;
  } else {
    document.getElementById('dollar-modal-title').textContent = '💸 دفع بالدولار';
    document.getElementById('dollar-buy-section').style.display = 'none';
    document.getElementById('dollar-pay-section').style.display = '';
    document.getElementById('dollar-save-btn').textContent = '💸 تسجيل الدفع';
  }
  getCustomersDatalist();
  openModal('modal-dollar');
}

function updateDollarPreview() {
  const usd  = parseFloat(document.getElementById('dollar-usd-amount').value) || 0;
  const rate = parseFloat(document.getElementById('dollar-rate').value) || 0;
  const lira = usd * rate;
  document.getElementById('dollar-lira-preview').innerHTML =
    lira > 0
      ? `ستُخصم من الصندوق: <strong style="color:var(--danger)">${formatNum(lira)} ليرة</strong>`
      : 'المبلغ بالليرة: —';
}

function saveDollarOp() {
  const opType = document.getElementById('dollar-op-type').value;
  const date   = document.getElementById('dollar-date').value || today();
  const note   = document.getElementById('dollar-note').value.trim();
  const cash   = DB.get(K_CASH);

  if (opType === 'buy') {
    const usdAmt = parseFloat(document.getElementById('dollar-usd-amount').value);
    const rate   = parseFloat(document.getElementById('dollar-rate').value);
    if (!positiveMoney(usdAmt)) { toast('أدخل مبلغ الدولار', 'error'); return; }
    if (!positiveMoney(rate)) { toast('أدخل سعر الصرف',    'error'); return; }
    const liraAmt = roundMoney(usdAmt * rate);
    if (!positiveMoney(liraAmt)) { toast('قيمة التحويل غير صالحة', 'error'); return; }
    const desc    = note ? `شراء دولار — ${note}` : `شراء دولار (سعر صرف ${formatNum(rate)} ل/$)`;

    // خصم من الصندوق بالليرة
    cash.push({ id: uid(), type: 'out', desc: `${desc} — خصم ليرة`, amount: liraAmt, person: '', date });
    // إضافة دولار
    cash.push({ id: uid(), type: 'usd_in', desc, amount: liraAmt, usdAmount: usdAmt, rate, person: '', date });

  } else {
    const usdAmt = parseFloat(document.getElementById('dollar-pay-amount').value);
    const to     = document.getElementById('dollar-pay-to').value.trim();
    if (!positiveMoney(usdAmt)) { toast('أدخل مبلغ الدولار المدفوع', 'error'); return; }
    const desc = note
      ? `دفع دولار — ${note}`
      : `دفع دولار${to ? ' لـ ' + to : ''}`;

    cash.push({ id: uid(), type: 'usd_out', desc, amount: 0, usdAmount: usdAmt, person: to, date });
  }

  DB.set(K_CASH, cash);
  closeModal('modal-dollar');
  renderCash();
  toast('✅ تم التسجيل بنجاح');
}

// ==================== EMPLOYEES ====================
function renderEmployees() {
  const employees = DB.get(K_EMPLOYEES).sort((a,b) => (a.name||'').localeCompare(b.name||''));
  const allW      = DB.get(K_WITHDRAWALS);

  const empMonthEl = document.getElementById('emp-month-filter');
  const monthStr   = empMonthEl?.value || currentMonth();

  const filteredW     = allW.filter(w => w.date?.startsWith(monthStr));
  const monthTotalTRY = filteredW.filter(w => (w.currency||'ليرة') !== 'دولار').reduce((s,w) => s + +w.amount, 0);
  const monthTotalUSD = filteredW.filter(w => w.currency === 'دولار').reduce((s,w) => s + +w.amount, 0);

  const [y, m] = monthStr.split('-');
  const monthNames = ['','يناير','فبراير','مارس','أبريل','مايو','يونيو','يوليو','أغسطس','سبتمبر','أكتوبر','نوفمبر','ديسمبر'];
  const monthLabel = `${monthNames[+m]} ${y}`;

  document.getElementById('emp-count').textContent = employees.length;
  document.getElementById('emp-month-withdrawn').textContent     = formatNum(monthTotalTRY);
  const usdEl = document.getElementById('emp-month-withdrawn-usd');
  if (usdEl) usdEl.textContent = monthTotalUSD.toFixed(2);

  // حدّث label الشهر
  const tryCard = document.querySelector('#emp-month-withdrawn')?.closest('.stat-card')?.querySelector('.stat-label');
  if (tryCard) tryCard.textContent = `سحوبات ${monthLabel} ليرة`;
  const usdCard = document.querySelector('#emp-month-withdrawn-usd')?.closest('.stat-card')?.querySelector('.stat-label');
  if (usdCard) usdCard.textContent = `سحوبات ${monthLabel} دولار`;

  const el = document.getElementById('employees-list');
  if (employees.length === 0) {
    el.innerHTML = `<div class="empty-state"><div class="icon">👷</div><p>لا يوجد موظفون — أضف موظفاً أولاً</p></div>`;
    return;
  }

  el.innerHTML = employees.map(emp => {
    const withdrawals = allW.filter(w => w.empId === emp.id && w.date?.startsWith(monthStr))
      .sort((a,b) => (b.createdAt||b.date||'').localeCompare(a.createdAt||a.date||''));

    // رصيد الشهر منفصل لكل عملة
    const empTRY = withdrawals.filter(w => (w.currency||'ليرة') !== 'دولار').reduce((s,w) => s + +w.amount, 0);
    const empUSD = withdrawals.filter(w => w.currency === 'دولار').reduce((s,w) => s + +w.amount, 0);

    // إجمالي كل الأوقات
    const allEmpW   = allW.filter(w => w.empId === emp.id);
    const totalTRY  = allEmpW.filter(w => (w.currency||'ليرة') !== 'دولار').reduce((s,w) => s + +w.amount, 0);
    const totalUSD  = allEmpW.filter(w => w.currency === 'دولار').reduce((s,w) => s + +w.amount, 0);

    const isOpen = emp._open;

    return `
    <div class="card" style="margin-bottom:16px">
      <!-- رأس البطاقة -->
      <div style="display:flex;justify-content:space-between;align-items:center;cursor:pointer" onclick="toggleEmployeeCard(${htmlJSArg(emp.id)})">
        <div style="display:flex;align-items:center;gap:12px">
          <div style="width:42px;height:42px;border-radius:50%;background:var(--accent);display:flex;align-items:center;justify-content:center;font-size:18px;flex-shrink:0">👷</div>
          <div>
            <div style="font-size:16px;font-weight:700">${escapeHTML(emp.name)}</div>
            ${emp.phone ? `<div style="font-size:12px;color:var(--text-muted)">${escapeHTML(emp.phone)}</div>` : ''}
          </div>
        </div>
        <div style="display:flex;align-items:center;gap:10px">
          <!-- رصيدا الشهر -->
          <div style="display:flex;gap:8px">
            <div style="text-align:center;background:rgba(239,68,68,0.1);border:1px solid rgba(239,68,68,0.2);border-radius:8px;padding:4px 10px;min-width:70px">
              <div style="font-size:9px;color:var(--text-muted)">🇸🇾 ${monthLabel}</div>
              <div style="font-size:13px;font-weight:800;color:var(--danger)">${formatNum(empTRY)}</div>
            </div>
            <div style="text-align:center;background:rgba(34,197,94,0.1);border:1px solid rgba(34,197,94,0.2);border-radius:8px;padding:4px 10px;min-width:60px">
              <div style="font-size:9px;color:var(--text-muted)">💵 ${monthLabel}</div>
              <div style="font-size:13px;font-weight:800;color:#22c55e">${empUSD > 0 ? empUSD.toFixed(2) : '0'} $</div>
            </div>
          </div>
          <button class="btn btn-sm btn-primary" onclick="event.stopPropagation();openEmpWithdraw(${htmlJSArg(emp.id)})">💸 سحب</button>
          <button class="btn btn-sm btn-secondary" onclick="event.stopPropagation();editEmployee(${htmlJSArg(emp.id)})">✏️</button>
          <button class="btn btn-sm btn-danger" onclick="event.stopPropagation();askDeleteEmployee(${htmlJSArg(emp.id)})">🗑️</button>
          <span style="color:var(--text-muted);font-size:18px">${isOpen ? '▲' : '▼'}</span>
        </div>
      </div>

      ${isOpen ? `
      <div style="margin-top:16px;border-top:1px solid var(--border);padding-top:14px">

        <!-- إجماليات الموظف الكلية -->
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:14px">
          <div style="background:var(--bg-card2);border:1px solid var(--border);border-radius:10px;padding:10px;text-align:center">
            <div style="font-size:10px;color:var(--text-muted);margin-bottom:3px">📊 إجمالي كل الأوقات ليرة</div>
            <div style="font-size:16px;font-weight:900;color:var(--danger)">${formatNum(totalTRY)} <span style="font-size:11px">ل</span></div>
          </div>
          <div style="background:var(--bg-card2);border:1px solid var(--border);border-radius:10px;padding:10px;text-align:center">
            <div style="font-size:10px;color:var(--text-muted);margin-bottom:3px">📊 إجمالي كل الأوقات دولار</div>
            <div style="font-size:16px;font-weight:900;color:#22c55e">${totalUSD > 0 ? totalUSD.toFixed(2) : '0'} <span style="font-size:11px">$</span></div>
          </div>
        </div>

        <!-- قائمة السحوبات -->
        ${withdrawals.length === 0
          ? `<div style="text-align:center;color:var(--text-muted);padding:12px">لا توجد سحوبات في ${monthLabel}</div>`
          : `<div style="display:flex;flex-direction:column;gap:6px">
              ${withdrawals.map(w => {
                const isUsd = w.currency === 'دولار';
                const amtTxt = isUsd ? `${(+w.amount).toFixed(2)} $` : `${formatNum(w.amount)} ل`;
                const color  = isUsd ? '#22c55e' : 'var(--danger)';
                const methLabel = isUsd ? '💵 دولار' : w.method === 'بنك' ? '🏦 بنك' : '💵 كاش';
                return `
                <div style="display:flex;justify-content:space-between;align-items:center;padding:8px 12px;background:var(--bg-card2);border-radius:8px;border-right:3px solid ${color}">
                  <div>
                    <div style="font-size:13px;font-weight:600">${escapeHTML(w.note || 'سحب')}</div>
                    <div style="font-size:11px;color:var(--text-muted)">${w.createdAt ? formatDateTime(w.createdAt) : formatDate(w.date)} — ${methLabel}</div>
                  </div>
                  <div style="display:flex;align-items:center;gap:8px">
                    <strong style="color:${color}">− ${amtTxt}</strong>
                    <button class="btn btn-sm btn-secondary" onclick="editEmpWithdrawal(${htmlJSArg(w.id)})">✏️</button>
                    <button class="btn btn-sm btn-danger" onclick="deleteEmpWithdrawal(${htmlJSArg(w.id)})">🗑️</button>
                  </div>
                </div>`;
              }).join('')}
            </div>`
        }
      </div>` : ''}
    </div>`;
  }).join('');
}

function toggleEmployeeCard(id) {
  const employees = DB.get(K_EMPLOYEES);
  const idx = employees.findIndex(e => e.id === id);
  if (idx === -1) return;
  employees[idx]._open = !employees[idx]._open;
  // تحديث محلي بدون حفظ Firestore
  cache.employees = employees;
  renderEmployees();
}

function openAddEmployee() {
  document.getElementById('emp-edit-id').value = '';
  document.getElementById('emp-modal-title').textContent = '➕ إضافة موظف';
  document.getElementById('emp-name').value  = '';
  document.getElementById('emp-phone').value = '';
  document.getElementById('emp-note').value  = '';
  openModal('modal-employee');
}

function editEmployee(id) {
  const e = DB.get(K_EMPLOYEES).find(x => x.id === id);
  if (!e) return;
  document.getElementById('emp-edit-id').value = id;
  document.getElementById('emp-modal-title').textContent = '✏️ تعديل موظف';
  document.getElementById('emp-name').value  = e.name  || '';
  document.getElementById('emp-phone').value = e.phone || '';
  document.getElementById('emp-note').value  = e.note  || '';
  openModal('modal-employee');
}

async function saveEmployee() {
  return saveAction('saveEmployee', '#modal-employee button', async () => {
  const name = document.getElementById('emp-name').value.trim();
  if (!name) { toast('يرجى إدخال اسم الموظف', 'error'); return; }
  const editId = document.getElementById('emp-edit-id').value;
  const employees = DB.get(K_EMPLOYEES);
  if (editId) {
    const idx = employees.findIndex(e => e.id === editId);
    if (idx >= 0) { employees[idx].name = name; employees[idx].phone = document.getElementById('emp-phone').value.trim(); employees[idx].note = document.getElementById('emp-note').value.trim(); }
  } else {
    employees.push({ id: uid(), name, phone: document.getElementById('emp-phone').value.trim(), note: document.getElementById('emp-note').value.trim(), withdrawals: [] });
  }
  DB.set(K_EMPLOYEES, employees);
  await writer.wait();
  closeModal('modal-employee');
  renderEmployees();
  toast('✅ تم الحفظ');

  });
}

function deleteEmployee(id) {
  const emp = DB.get(K_EMPLOYEES).find(e => e.id === id);
  if (!emp) return;

  document.getElementById('confirm-message').innerHTML = `
    هل أنت متأكد من حذف <strong>${escapeHTML(emp.name)}</strong>؟<br>
    <span style="font-size:12px;color:var(--text-muted)">سيتم حذف كل سحوباته ومعاملاته من الصندوق.</span>
  `;
  document.getElementById('confirm-ok').onclick = () => {
    closeModal('modal-confirm');

    // احفظ نسخة كاملة للتراجع
    const empBackup = { ...emp };
    const withdrawalsBackup = DB.get(K_WITHDRAWALS).filter(w => w.empId === id);
    const cashBackup = DB.get(K_CASH).filter(c =>
      withdrawalsBackup.map(w => w.cashId).includes(c.id)
    );

    // حذف مؤقت من الواجهة (cache فقط)
    cache[K_EMPLOYEES] = cache[K_EMPLOYEES].filter(e => e.id !== id);
    cache[K_WITHDRAWALS] = cache[K_WITHDRAWALS].filter(w => w.empId !== id);
    cache[K_CASH] = cache[K_CASH].filter(c => !cashBackup.map(x => x.id).includes(c.id));
    renderEmployees();

    // timer للتراجع — 10 ثواني
    let undone = false;
    const undoToast = document.createElement('div');
    undoToast.style.cssText = `position:fixed;bottom:80px;left:50%;transform:translateX(-50%);background:#1e293b;border:1px solid var(--border);border-radius:12px;padding:12px 20px;display:flex;align-items:center;gap:14px;z-index:9999;box-shadow:0 8px 24px rgba(0,0,0,0.4);font-size:13px;min-width:280px`;
    undoToast.innerHTML = `
      <span>🗑️ تم حذف <strong>${escapeHTML(emp.name)}</strong></span>
      <div style="flex:1;height:3px;background:#334155;border-radius:2px;overflow:hidden">
        <div id="undo-bar" style="height:100%;background:var(--warning);width:100%;transition:width 10s linear"></div>
      </div>
      <button onclick="this.closest('div[style]').dataset.undone='1'" style="background:var(--warning);border:none;color:#000;font-weight:700;padding:5px 12px;border-radius:7px;cursor:pointer;font-family:inherit;font-size:12px">↩️ تراجع</button>
    `;
    document.body.appendChild(undoToast);
    requestAnimationFrame(() => {
      const bar = document.getElementById('undo-bar');
      if (bar) bar.style.width = '0%';
    });

    const timer = setTimeout(() => {
      if (undoToast.dataset.undone === '1') {
        // تراجع — أعد الموظف والبيانات
        cache[K_EMPLOYEES] = [...cache[K_EMPLOYEES], empBackup];
        setDoc(doc(fdb,'users',auth.currentUser?.uid,K_EMPLOYEES,empBackup.id), empBackup).catch(()=>{});
        withdrawalsBackup.forEach(w => {
          cache[K_WITHDRAWALS] = [...cache[K_WITHDRAWALS], w];
          setDoc(doc(fdb,'users',auth.currentUser?.uid,K_WITHDRAWALS,w.id), w).catch(()=>{});
        });
        cashBackup.forEach(c => {
          cache[K_CASH] = [...cache[K_CASH], c];
          setDoc(doc(fdb,'users',auth.currentUser?.uid,K_CASH,c.id), c).catch(()=>{});
        });
        renderEmployees();
        toast('✅ تم استرجاع بيانات ' + empBackup.name);
      } else {
        // تأكيد الحذف النهائي من Firestore
        deleteDoc(doc(fdb,'users',auth.currentUser?.uid,K_EMPLOYEES,id)).catch(()=>{});
        withdrawalsBackup.forEach(w => deleteDoc(doc(fdb,'users',auth.currentUser?.uid,K_WITHDRAWALS,w.id)).catch(()=>{}));
        cashBackup.forEach(c => deleteDoc(doc(fdb,'users',auth.currentUser?.uid,K_CASH,c.id)).catch(()=>{}));
        toast('تم الحذف نهائياً', 'info');
      }
      undoToast.remove();
    }, 10000);

    // لو ضغط تراجع قبل الـ timer
    const observer = new MutationObserver(() => {
      if (undoToast.dataset.undone === '1') {
        clearTimeout(timer);
        observer.disconnect();
        // استرجاع فوري
        cache[K_EMPLOYEES] = [...cache[K_EMPLOYEES], empBackup];
        setDoc(doc(fdb,'users',auth.currentUser?.uid,K_EMPLOYEES,empBackup.id), empBackup).catch(()=>{});
        withdrawalsBackup.forEach(w => {
          cache[K_WITHDRAWALS] = [...cache[K_WITHDRAWALS], w];
          setDoc(doc(fdb,'users',auth.currentUser?.uid,K_WITHDRAWALS,w.id), w).catch(()=>{});
        });
        cashBackup.forEach(c => {
          cache[K_CASH] = [...cache[K_CASH], c];
          setDoc(doc(fdb,'users',auth.currentUser?.uid,K_CASH,c.id), c).catch(()=>{});
        });
        renderEmployees();
        toast('✅ تم استرجاع بيانات ' + empBackup.name);
        undoToast.remove();
      }
    });
    observer.observe(undoToast, { attributes: true, attributeFilter: ['data-undone'] });
  };
  openModal('modal-confirm');
}

function openEmpWithdraw(id) {
  const e = DB.get(K_EMPLOYEES).find(x => x.id === id);
  if (!e) return;
  document.getElementById('emp-withdraw-id').value = id;
  document.getElementById('emp-withdraw-title').textContent = `💸 سحب — ${e.name}`;
  document.getElementById('emp-withdraw-amount').value = '';
  document.getElementById('emp-withdraw-date').value   = today();
  document.getElementById('emp-withdraw-note').value   = '';
  document.querySelectorAll('input[name="emp-pay-method"]')[0].checked = true;
  // reset currency to ليرة
  const liraRadio = document.querySelector('input[name="emp-currency"][value="ليرة"]');
  if (liraRadio) { liraRadio.checked = true; toggleWithdrawCurrency(); }
  openModal('modal-emp-withdraw');
}

window.toggleWithdrawCurrency = function() {
  const cur = document.querySelector('input[name="emp-currency"]:checked')?.value || 'ليرة';
  const lbl = document.getElementById('emp-withdraw-amount-label');
  if (lbl) lbl.textContent = cur === 'دولار' ? 'المبلغ (دولار) *' : 'المبلغ (ليرة تركية) *';
};

window.toggleEditWithdrawCurrency = function() {
  const cur = document.querySelector('input[name="ew-currency"]:checked')?.value || 'ليرة';
  const lbl = document.getElementById('ew-amount-label');
  if (lbl) lbl.textContent = cur === 'دولار' ? 'المبلغ (دولار) *' : 'المبلغ (ليرة تركية) *';
};

function saveEmpWithdraw() {
  const btn = document.querySelector('#modal-emp-withdraw .btn-danger');
  if (btn?.disabled) return;
  if (btn) btn.disabled = true;

  const id     = document.getElementById('emp-withdraw-id').value;
  const amount = parseFloat(document.getElementById('emp-withdraw-amount').value);
  if (!positiveMoney(amount)) { toast('يرجى إدخال المبلغ', 'error'); if(btn) btn.disabled=false; return; }

  const employees = DB.get(K_EMPLOYEES);
  const idx = employees.findIndex(e => e.id === id);
  if (idx === -1) { if(btn) btn.disabled=false; return; }

  const method = document.querySelector('input[name="emp-pay-method"]:checked').value;
  const date   = document.getElementById('emp-withdraw-date').value || today();
  const note   = document.getElementById('emp-withdraw-note').value.trim();
  const currency = document.querySelector('input[name="emp-currency"]:checked')?.value || 'ليرة';

  if (!employees[idx].withdrawals) employees[idx].withdrawals = [];
  const cashId = uid();
  const ts = nowTimestamp();
  const w = { id: uid(), empId: id, empName: employees[idx].name, amount, method, date, note, cashId, currency, createdAt: ts };
  const withdrawals = DB.get(K_WITHDRAWALS);
  withdrawals.push(w);
  DB.set(K_WITHDRAWALS, withdrawals);

  const currLabel = currency === 'دولار' ? '💵' : '🇸🇾';
  const cashType = currency === 'دولار' ? 'usd_out'
                 : method === 'بنك'     ? 'bank_out' : 'out';
  const cashEntry = currency === 'دولار'
    ? { id: cashId, type: 'usd_out', desc: `سحب موظف: ${employees[idx].name}${note ? ' — ' + note : ''} ${currLabel}`, amount: 0, usdAmount: amount, person: employees[idx].name, date: date || today(), createdAt: ts }
    : { id: cashId, type: cashType,  desc: `سحب موظف: ${employees[idx].name}${note ? ' — ' + note : ''} ${currLabel}`, amount, person: employees[idx].name, date: date || today(), createdAt: ts };
  const cash = DB.get(K_CASH);
  cash.push(cashEntry);
  DB.set(K_CASH, cash);

  closeModal('modal-emp-withdraw');
  renderEmployees();
  toast('✅ تم تسجيل السحب');
  setTimeout(() => { if(btn) btn.disabled=false; }, 1000);
}

window.editEmpWithdrawal = function(wid) {
  const w = DB.get(K_WITHDRAWALS).find(x => x.id === wid);
  if (!w) return;
  document.getElementById('ew-id').value     = wid;
  document.getElementById('ew-amount').value = w.amount || '';
  document.getElementById('ew-date').value   = w.date   || today();
  document.getElementById('ew-note').value   = w.note   || '';
  document.querySelectorAll('input[name="ew-pay-method"]').forEach(r => {
    r.checked = r.value === (w.method || 'كاش');
  });
  // load currency
  const ewCur = w.currency || 'ليرة';
  document.querySelectorAll('input[name="ew-currency"]').forEach(r => { r.checked = r.value === ewCur; });
  toggleEditWithdrawCurrency();
  openModal('modal-edit-withdraw');
};

window.saveEditWithdrawal = function() {
  const wid    = document.getElementById('ew-id').value;
  const amount = parseFloat(document.getElementById('ew-amount').value);
  if (!positiveMoney(amount)) { toast('أدخل المبلغ', 'error'); return; }
  const method   = document.querySelector('input[name="ew-pay-method"]:checked').value;
  const currency = document.querySelector('input[name="ew-currency"]:checked')?.value || 'ليرة';
  const date     = document.getElementById('ew-date').value || today();
  const note     = document.getElementById('ew-note').value.trim();

  const withdrawals = DB.get(K_WITHDRAWALS);
  const idx = withdrawals.findIndex(x => x.id === wid);
  if (idx === -1) return;
  const w = withdrawals[idx];

  withdrawals[idx] = { ...w, amount, method, currency, date, note };
  DB.set(K_WITHDRAWALS, withdrawals);

  // حدّث معاملة الصندوق المرتبطة
  if (w.cashId) {
    const cash = DB.get(K_CASH);
    const ci   = cash.findIndex(c => c.id === w.cashId);
    if (ci !== -1) {
      const currLabel = currency === 'دولار' ? '💵' : '🇸🇾';
      if (currency === 'دولار') {
        cash[ci] = { ...cash[ci], type: 'usd_out', amount: 0, usdAmount: amount, date,
          desc: `سحب موظف: ${w.empName||''}${note ? ' — ' + note : ''} ${currLabel}`, person: w.empName||'' };
      } else {
        cash[ci] = { ...cash[ci],
          type: method === 'بنك' ? 'bank_out' : 'out',
          amount, usdAmount: undefined, date,
          desc: `سحب موظف: ${w.empName||''}${note ? ' — ' + note : ''} ${currLabel}`, person: w.empName||'' };
      }
      DB.set(K_CASH, cash);
    }
  }

  closeModal('modal-edit-withdraw');
  renderEmployees();
  toast('✅ تم تحديث السحب');
};

function deleteEmpWithdrawal(wid) {
  document.getElementById('confirm-message').textContent = 'حذف هذا السحب؟ سيتم حذفه من الصندوق أيضاً.';
  document.getElementById('confirm-ok').onclick = () => {
    const w = DB.get(K_WITHDRAWALS).find(x => x.id === wid);
    // احذف من السحوبات
    DB.set(K_WITHDRAWALS, DB.get(K_WITHDRAWALS).filter(x => x.id !== wid));
    // احذف معاملة الصندوق المرتبطة
    if (w?.cashId) {
      DB.set(K_CASH, DB.get(K_CASH).filter(c => c.id !== w.cashId));
    }
    closeModal('modal-confirm');
    renderEmployees();
    toast('تم الحذف', 'info');
  };
  openModal('modal-confirm');
}

// ==================== EXPENSES ====================
function getExpCats() {
  return DB.get(K_EXP_CATS).sort((a,b) => (a.name||'').localeCompare(b.name||''));
}

function getCatColor(name) {
  // لون ثابت لكل تصنيف بناءً على اسمه
  const colors = ['var(--accent2)','var(--info)','var(--warning)','var(--success)','var(--accent)','#7c3aed','#0f766e','#b45309'];
  let hash = 0;
  for (let i = 0; i < name.length; i++) hash = name.charCodeAt(i) + ((hash << 5) - hash);
  return colors[Math.abs(hash) % colors.length];
}

function loadExpCatsDropdown(selectedVal) {
  const cats = getExpCats();
  const sel  = document.getElementById('expense-cat');
  if (!sel) return;
  sel.innerHTML = '<option value="">-- اختر تصنيف --</option>' +
    cats.map(c => `<option value="${escapeHTML(c.name)}" ${c.name===selectedVal?'selected':''}>${escapeHTML(c.name)}</option>`).join('');
}

function renderExpenseCatFilter() {
  const cats = getExpCats();
  const sel  = document.getElementById('exp-filter-cat');
  if (!sel) return;
  const cur  = sel.value;
  sel.innerHTML = '<option value="">كل التصنيفات</option>' +
    cats.map(c => `<option value="${escapeHTML(c.name)}" ${c.name===cur?'selected':''}>${escapeHTML(c.name)}</option>`).join('');
}

function renderExpenses() {
  const all = DB.get(K_EXPENSES).sort((a,b) => (b.date||'').localeCompare(a.date||''));

  renderExpenseCatFilter();

  const catF    = document.getElementById('exp-filter-cat')?.value || '';
  const monthEl = document.getElementById('exp-month-filter');
  const monthF  = monthEl?.value || '';
  const displayMonth = monthF || currentMonth();

  // إجماليات للشهر المعروض
  const monthTotal = all.filter(e => e.date?.startsWith(displayMonth)).reduce((s,e) => s + +e.amount, 0);
  const allTotal   = monthF
    ? all.filter(e => e.date?.startsWith(monthF)).reduce((s,e) => s + +e.amount, 0)
    : all.reduce((s,e) => s + +e.amount, 0);

  // حدّث label البطاقات
  const [y, m] = displayMonth.split('-');
  const monthNames = ['','يناير','فبراير','مارس','أبريل','مايو','يونيو','يوليو','أغسطس','سبتمبر','أكتوبر','نوفمبر','ديسمبر'];
  const mLabel = `${monthNames[+m]} ${y}`;
  const ml1 = document.querySelector('#exp-month-total')?.closest('.stat-card')?.querySelector('.stat-label');
  if (ml1) ml1.textContent = monthF ? `مصاريف ${mLabel}` : `مصاريف الشهر الحالي`;
  const ml2 = document.querySelector('#exp-all-total')?.closest('.stat-card')?.querySelector('.stat-label');
  if (ml2) ml2.textContent = monthF ? `إجمالي ${mLabel}` : 'إجمالي كل المصاريف';

  document.getElementById('exp-month-total').textContent = formatNum(monthTotal);
  document.getElementById('exp-all-total').textContent   = formatNum(allTotal);
  document.getElementById('exp-count').textContent       = monthF
    ? all.filter(e => e.date?.startsWith(monthF)).length
    : all.length;

  // تطبيق الفلتر على الجدول
  const shown = all.filter(e => {
    if (catF  && e.category !== catF)          return false;
    if (monthF && !e.date?.startsWith(monthF)) return false;
    return true;
  });

  // جدول
  const tbody = document.getElementById('exp-tbody');
  if (shown.length === 0) {
    tbody.innerHTML = `<tr><td colspan="7"><div class="empty-state"><div class="icon">📭</div><p>لا توجد مصاريف</p></div></td></tr>`;
  } else {
    tbody.innerHTML = shown.map(e => {
      const color = getCatColor(e.category || '');
      const payBadge = e.payMethod === 'بنك'
        ? '<span class="badge badge-info">🏦 بنك</span>'
        : '<span class="badge badge-success">💵 كاش</span>';
      return `<tr>
        <td>${formatDate(e.date)}</td>
        <td><span class="badge" style="background:${color}">🏷️ ${escapeHTML(e.category||'-')}</span></td>
        <td>${escapeHTML(e.desc)}</td>
        <td>${e.to ? `<span class="badge badge-info">👤 ${escapeHTML(e.to)}</span>` : '-'}</td>
        <td>${payBadge}</td>
        <td style="color:var(--danger)"><strong>− ${formatNum(e.amount)} ل</strong></td>
        <td style="display:flex;gap:4px">
          <button class="btn btn-sm btn-secondary" onclick="editExpense(${htmlJSArg(e.id)})">✏️</button>
          <button class="btn btn-sm btn-danger" onclick="askDeleteExpense(${htmlJSArg(e.id)})">🗑️</button>
        </td>
      </tr>`;
    }).join('');
  }

  // ملخص حسب التصنيف — مفلتر بنفس الشهر والتصنيف المختار
  const byCat = {};
  shown.forEach(e => { byCat[e.category] = (byCat[e.category]||0) + +e.amount; });
  const shownTotal = shown.reduce((s,e) => s + +e.amount, 0);
  const catEl = document.getElementById('exp-by-cat');
  if (Object.keys(byCat).length === 0) {
    catEl.innerHTML = `<div style="color:var(--text-muted);text-align:center;padding:12px">لا توجد بيانات</div>`;
  } else {
    catEl.innerHTML = Object.entries(byCat).sort((a,b)=>b[1]-a[1]).map(([cat, amt]) => {
      const color = getCatColor(cat);
      const pct = shownTotal > 0 ? Math.round(amt/shownTotal*100) : 0;
      return `
        <div style="margin-bottom:12px">
          <div style="display:flex;justify-content:space-between;margin-bottom:4px">
            <span>🏷️ ${escapeHTML(cat)}</span>
            <strong>${formatNum(amt)} ل (${pct}%)</strong>
          </div>
          <div style="height:6px;background:var(--bg-card2);border-radius:4px">
            <div style="height:6px;width:${pct}%;background:${color};border-radius:4px;transition:width .3s"></div>
          </div>
        </div>`;
    }).join('');
  }
}

function openAddExpense() {
  document.getElementById('expense-edit-id').value = '';
  document.getElementById('expense-modal-title').textContent = '➕ إضافة مصروف';
  document.getElementById('expense-desc').value   = '';
  document.getElementById('expense-to').value     = '';
  document.getElementById('expense-amount').value = '';
  document.getElementById('expense-date').value   = today();
  document.getElementById('expense-note').value   = '';
  document.querySelectorAll('input[name="exp-pay-method"]')[0].checked = true;
  loadExpCatsDropdown('');
  openModal('modal-expense');
}

function editExpense(id) {
  const e = DB.get(K_EXPENSES).find(x => x.id === id);
  if (!e) return;
  document.getElementById('expense-edit-id').value = id;
  document.getElementById('expense-modal-title').textContent = '✏️ تعديل مصروف';
  document.getElementById('expense-desc').value   = e.desc   || '';
  document.getElementById('expense-to').value     = e.to     || '';
  document.getElementById('expense-amount').value = e.amount || '';
  document.getElementById('expense-date').value   = e.date   || today();
  document.getElementById('expense-note').value   = e.note   || '';
  document.querySelectorAll('input[name="exp-pay-method"]').forEach(r => {
    r.checked = r.value === (e.payMethod || 'كاش');
  });
  loadExpCatsDropdown(e.category || '');
  openModal('modal-expense');
}

function expenseCashId(expense, cash) {
  if (expense.cashId) {
    if (!cash.some(row => row.id === expense.cashId)) throw new Error('معاملة المصروف المرتبطة مفقودة؛ راجع السجل');
    return expense.cashId;
  }
  const matches = cash.filter(row => row.type === (expense.payMethod === 'بنك' ? 'bank_out' : 'out') &&
    row.desc === `مصروف: ${expense.category} — ${expense.desc}` && Number(row.amount) === Number(expense.amount) && row.date === expense.date);
  if (matches.length !== 1) throw new Error('تعذر تحديد معاملة المصروف القديم بدقة؛ راجع الصندوق قبل التعديل');
  return matches[0].id;
}
async function saveExpense() {
  return saveAction('expense', '#modal-expense button', async () => {
    [K_EXPENSES, K_CASH].forEach(assertLoaded);
    const desc = document.getElementById('expense-desc').value.trim();
    const amount = Number(document.getElementById('expense-amount').value);
    const category = document.getElementById('expense-cat').value;
    if (!desc || !category || !positiveMoney(amount)) throw new Error('أدخل التصنيف والوصف ومبلغاً موجباً');
    const editId = document.getElementById('expense-edit-id').value;
    const expenses = DB.get(K_EXPENSES), cash = DB.get(K_CASH);
    const old = editId ? expenses.find(row => row.id === editId) : null;
    if (editId && !old) throw new Error('المصروف غير موجود');
    const cashId = old ? expenseCashId(old, cash) : uid();
    const entry = { ...old, id: editId || uid(), cashId, category, desc, amount,
      to: document.getElementById('expense-to').value.trim(),
      payMethod: document.querySelector('input[name="exp-pay-method"]:checked').value,
      date: document.getElementById('expense-date').value || today(), note: document.getElementById('expense-note').value.trim() };
    const cashEntry = { id: cashId, expenseId: entry.id, type: entry.payMethod === 'بنك' ? 'bank_out' : 'out',
      desc: `مصروف: ${category} — ${desc}`, amount, person: entry.to, date: entry.date };
    DB.set(K_EXPENSES, old ? expenses.map(row => row.id === entry.id ? entry : row) : [...expenses, entry]);
    DB.set(K_CASH, old ? cash.map(row => row.id === cashId ? { ...row, ...cashEntry } : row) : [...cash, cashEntry]);
    await writer.wait(); closeModal('modal-expense'); renderExpenses(); toast('تم حفظ المصروف والصندوق');
  });
}
function deleteExpense(id) {
  document.getElementById('confirm-message').textContent = 'حذف المصروف ومعاملة الصندوق المرتبطة؟';
  document.getElementById('confirm-ok').onclick = () => saveAction('delete-expense', '#modal-confirm button', async () => {
    [K_EXPENSES, K_CASH].forEach(assertLoaded);
    const expenses = DB.get(K_EXPENSES), cash = DB.get(K_CASH);
    const expense = expenses.find(row => row.id === id);
    if (!expense) throw new Error('المصروف غير موجود');
    const cashId = expenseCashId(expense, cash);
    DB.set(K_EXPENSES, expenses.filter(row => row.id !== id));
    DB.set(K_CASH, cash.filter(row => row.id !== cashId));
    await writer.wait(); closeModal('modal-confirm'); renderExpenses(); toast('تم حذف المصروف ومعاملته');
  });
  openModal('modal-confirm');
}

// ── إدارة تصنيفات المصاريف ──
function openExpenseCatsMgr() {
  renderExpCatsList();
  document.getElementById('new-exp-cat-name').value = '';
  openModal('modal-exp-cats');
}

function renderExpCatsList() {
  const cats = getExpCats();
  const el   = document.getElementById('exp-cats-list');
  if (!el) return;
  if (cats.length === 0) {
    el.innerHTML = `<div style="text-align:center;color:var(--text-muted);padding:16px">لا توجد تصنيفات — أضف تصنيفاتك</div>`;
    return;
  }
  el.innerHTML = cats.map(c => `
    <div style="display:flex;justify-content:space-between;align-items:center;padding:10px 12px;background:var(--bg-card2);border-radius:8px;margin-bottom:8px">
      <span style="font-size:14px"><span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:${getCatColor(c.name)};margin-left:8px"></span>${escapeHTML(c.name)}</span>
      <button class="btn btn-sm btn-danger" onclick="deleteExpenseCat(${htmlJSArg(c.id)})">🗑️</button>
    </div>
  `).join('');
}

function saveExpenseCat() {
  const name = document.getElementById('new-exp-cat-name').value.trim();
  if (!name) { toast('اكتب اسم التصنيف', 'error'); return; }
  if (getExpCats().find(c => c.name === name)) { toast('هذا التصنيف موجود مسبقاً', 'error'); return; }
  const cats = DB.get(K_EXP_CATS);
  cats.push({ id: uid(), name });
  DB.set(K_EXP_CATS, cats);
  document.getElementById('new-exp-cat-name').value = '';
  renderExpCatsList();
  toast('✅ تم إضافة التصنيف');
}

function deleteExpenseCat(id) {
  DB.set(K_EXP_CATS, DB.get(K_EXP_CATS).filter(c => c.id !== id));
  renderExpCatsList();
  toast('تم الحذف', 'info');
}

function renderDebts() {
  const debts  = DB.get(K_DEBTS).sort((a,b) => b.date.localeCompare(a.date));
  const allPay = DB.get(K_DEBT_PAYMENTS);
  const getPaid = id => allPay.filter(p => p.debtId === id).reduce((s,p) => s + +p.amount, 0);

  // تجميع الديون حسب الاسم
  const groups = {};
  debts.forEach(d => {
    const key = (d.name || '').trim().toLowerCase();
    if (!groups[key]) groups[key] = { name: d.name, debts: [] };
    groups[key].debts.push(d);
  });

  const groupList = Object.values(groups);

  // إجماليات كاملة (المستحق فقط)
  const totalDebt  = debts.reduce((s,d) => s + Math.max(0, +d.amount - getPaid(d.id)), 0);
  const activeCount = Object.values(groups).filter(g =>
    g.debts.some(d => getPaid(d.id) < +d.amount)
  ).length;

  document.getElementById('debt-total').textContent = formatNum(totalDebt);
  document.getElementById('debt-count').textContent = activeCount;

  const tbody = document.getElementById('debts-tbody');
  if (groupList.length === 0) {
    tbody.innerHTML = `<tr><td colspan="8"><div class="empty-state"><div class="icon">✅</div><p>لا توجد ديون</p></div></td></tr>`;
    return;
  }

  tbody.innerHTML = groupList.map(g => {
    const totalAmt  = g.debts.reduce((s,d) => s + +d.amount, 0);
    const totalPaid = g.debts.reduce((s,d) => s + getPaid(d.id), 0);
    const totalRem  = totalAmt - totalPaid;
    const allPaidOff = totalRem <= 0;
    const gid = 'grp_' + (g.name||'').replace(/\s+/g,'_');

    // صف الاسم الرئيسي (قابل للطي)
    const headerRow = `
      <tr style="cursor:pointer;background:var(--bg-card2)" onclick="toggleDebtGroup(${htmlJSArg(gid)})">
        <td colspan="3">
          <div style="display:flex;align-items:center;gap:10px">
            <span id="${gid}_icon" style="font-size:16px">▶</span>
            <strong style="font-size:15px">${escapeHTML(g.name)}</strong>
            <span class="badge ${allPaidOff?'badge-success':'badge-danger'}" style="font-size:11px">
              ${g.debts.length} ${g.debts.length===1?'دين':'ديون'}
            </span>
          </div>
        </td>
        <td><span style="color:var(--success);font-weight:700">${formatNum(totalPaid)} ل</span></td>
        <td><span style="color:${totalRem>0?'var(--danger)':'var(--success)'};font-weight:900;font-size:15px">${formatNum(totalRem)} ل</span></td>
        <td colspan="3">
          ${!allPaidOff ? `<span style="font-size:12px;color:var(--text-muted)">إجمالي الديون: ${formatNum(totalAmt)} ل</span>` : '<span class="badge badge-success">✅ مسدد بالكامل</span>'}
        </td>
      </tr>
    `;

    // صفوف التفاصيل (مخفية بالبداية)
    const detailRows = g.debts.map(d => {
      const paid      = getPaid(d.id);
      const remaining = +d.amount - paid;
      const settled   = remaining <= 0;
      return `
        <tr id="${gid}_details" style="display:none;background:rgba(0,0,0,0.15)">
          <td style="padding-right:30px">
            <span style="color:var(--text-muted);font-size:12px">${formatDate(d.date)}</span>
          </td>
          <td colspan="2">
            <span style="font-size:13px">${escapeHTML(d.note||'—')}</span>
          </td>
          <td style="color:var(--success)">${formatNum(paid)} ل</td>
          <td style="color:${remaining>0?'var(--danger)':'var(--success)'}"><strong>${formatNum(remaining)} ل</strong></td>
          <td>${settled?'<span class="badge badge-success">✅ مسدد</span>':'<span class="badge badge-danger">⏳ مستحق</span>'}</td>
          <td></td>
          <td>
            <div style="display:flex;gap:4px;flex-wrap:wrap">
              ${!settled ? `<button class="btn btn-sm btn-success" onclick="openPayDebt(${htmlJSArg(d.id)})">💳 تسديد</button>` : ''}
              <button class="btn btn-sm btn-info" onclick="showDebtHistory(${htmlJSArg(d.id)})">📋</button>
              <button class="btn btn-sm btn-secondary" onclick="editDebt(${htmlJSArg(d.id)})">✏️</button>
              <button class="btn btn-sm btn-danger" onclick="askDeleteDebt(${htmlJSArg(d.id)})">🗑️</button>
            </div>
          </td>
        </tr>
      `;
    }).join('');

    return headerRow + detailRows;
  }).join('');
}

function toggleDebtGroup(gid) {
  const rows = document.querySelectorAll(`tr[id="${gid}_details"]`);
  const icon = document.getElementById(`${gid}_icon`);
  const isOpen = icon?.textContent === '▼';
  rows.forEach(r => r.style.display = isOpen ? 'none' : 'table-row');
  if (icon) icon.textContent = isOpen ? '▶' : '▼';
}

function openAddDebt() {
  document.getElementById('debt-edit-id').value = '';
  document.getElementById('debt-modal-title').textContent = 'إضافة دين جديد';
  document.getElementById('debt-name').value = '';
  document.getElementById('debt-amount').value = '';
  document.getElementById('debt-date').value = today();
  document.getElementById('debt-note').value = '';
  document.getElementById('debt-cust-type').value = 'زبون';
  getCustomersDatalist();
  openModal('modal-debt');
}

function editDebt(id) {
  const d = DB.get(K_DEBTS).find(x => x.id === id);
  if (!d) return;
  document.getElementById('debt-edit-id').value = id;
  document.getElementById('debt-modal-title').textContent = 'تعديل دين';
  document.getElementById('debt-name').value = d.name;
  document.getElementById('debt-amount').value = d.amount;
  document.getElementById('debt-date').value = d.date;
  document.getElementById('debt-note').value = d.note || '';
  // جيب نوع الزبون من الـ customers
  const cust = DB.get(K_CUSTOMERS).find(c => c.name.trim().toLowerCase() === (d.name||'').trim().toLowerCase());
  document.getElementById('debt-cust-type').value = cust?.type || 'زبون';
  openModal('modal-debt');
}

async function saveDebt() {
  return saveAction('saveDebt', '#modal-debt button', async () => {
  const name = document.getElementById('debt-name').value.trim();
  const amount = parseFloat(document.getElementById('debt-amount').value);
  if (!name || !positiveMoney(amount)) { toast('يرجى إدخال الاسم والمبلغ', 'error'); return; }

  const editId = document.getElementById('debt-edit-id').value;
  const debts = DB.get(K_DEBTS);

  if (editId) {
    const idx = debts.findIndex(d => d.id === editId);
    if (idx !== -1) {
      if (amount < Number(debts[idx].paid || 0)) { toast('قيمة الدين أقل من المبلغ المسدد', 'error'); return; }
      debts[idx] = { ...debts[idx], name, amount, date: document.getElementById('debt-date').value, note: document.getElementById('debt-note').value };
      DB.set(K_DEBTS, debts);
      toast('✅ تم تحديث الدين');
    }
  } else {
    debts.push({ id: uid(), name, amount, paid: 0, date: document.getElementById('debt-date').value, note: document.getElementById('debt-note').value, status: 'مستحق' });
    DB.set(K_DEBTS, debts);
    const custType = document.getElementById('debt-cust-type').value || 'زبون';
    autoAddCustomer(name, '', custType);
    toast('✅ تم إضافة الدين');
  }
  await writer.wait();
  closeModal('modal-debt');
  renderDebts();

  });
}

function openPayDebt(id) {
  const d = DB.get(K_DEBTS).find(x => x.id === id);
  if (!d) return;
  const remaining = +d.amount - (+d.paid || 0);
  document.getElementById('pay-debt-id').value = id;
  document.getElementById('pay-debt-amount').value = '';
  document.getElementById('pay-debt-remaining').textContent = formatNum(remaining);
  openModal('modal-pay-debt');
}

function fillFullDebt() {
  const id = document.getElementById('pay-debt-id').value;
  const d  = DB.get(K_DEBTS).find(x => x.id === id);
  if (!d) return;
  const remaining = +d.amount - (+d.paid || 0);
  document.getElementById('pay-debt-amount').value = remaining;
}

async function commitDebtPayment({ debtId, amount, method, date, paymentId = null }) {
  if (!positiveMoney(amount) || roundMoney(amount) !== amount) throw new Error('أدخل مبلغاً موجباً بمنزلتين عشريتين كحد أقصى');
  if (!['كاش', 'بنك'].includes(method)) throw new Error('طريقة دفع غير صالحة');
  await writer.wait();
  const userId = auth.currentUser?.uid;
  if (!userId) throw new Error('يرجى تسجيل الدخول');
  const epoch = sessionEpoch;
  const payId = paymentId || uid();
  const newCashId = uid();
  updateSyncStatus('saving');
  const result = await runTransaction(fdb, async transaction => {
    const debtRef = doc(fdb, 'users', userId, K_DEBTS, debtId);
    const payRef = doc(fdb, 'users', userId, K_DEBT_PAYMENTS, payId);
    const debtSnap = await transaction.get(debtRef);
    if (!debtSnap.exists()) throw new Error('الدين غير موجود');
    const debt = { ...debtSnap.data(), id: debtId };
    const oldPaySnap = paymentId ? await transaction.get(payRef) : null;
    if (paymentId && (!oldPaySnap.exists() || oldPaySnap.data().debtId !== debtId)) throw new Error('الدفعة غير موجودة أو لا تخص هذا الدين');
    const oldPay = oldPaySnap?.data();
    const oldAmount = Number(oldPay?.amount || 0);
    const balance = paymentBalance(debt, amount, oldAmount);
    const cashId = oldPay?.cashId || newCashId;
    const cashRef = doc(fdb, 'users', userId, K_CASH, cashId);
    const cashSnap = oldPay?.cashId ? await transaction.get(cashRef) : null;
    if (oldPay?.cashId && !cashSnap.exists()) throw new Error('معاملة الصندوق المرتبطة مفقودة؛ راجع السجل أولاً');
    const repairRef = debt.repairId ? doc(fdb, 'users', userId, K_REPAIRS, debt.repairId) : null;
    const repairSnap = repairRef ? await transaction.get(repairRef) : null;
    if (repairRef && !repairSnap.exists()) throw new Error('سجل الصيانة المرتبط مفقود');
    const payment = { ...oldPay, id: payId, debtId, debtName: debt.name, amount, date, method, cashId };
    const updatedDebt = { ...debt, ...balance };
    const cash = { ...(cashSnap?.data() || {}), id: cashId, type: method === 'بنك' ? 'bank_in' : 'in', desc: `تسديد دين: ${debt.name}`, amount, person: debt.name, date, paymentId: payId };
    const repair = repairSnap ? { ...repairSnap.data(), id: debt.repairId, paid: roundMoney(Number(repairSnap.data().paid || 0) + amount - oldAmount) } : null;
    if (repair && repair.paid < 0) throw new Error('رصيد الصيانة غير متوافق مع الدفعة');
    transaction.set(payRef, payment);
    transaction.set(debtRef, updatedDebt);
    transaction.set(cashRef, cash);
    if (repair) transaction.set(repairRef, repair);
    return { payment, debt: updatedDebt, cash, repair };
  });
  if (epoch === sessionEpoch) {
    upsertCache(K_DEBT_PAYMENTS, result.payment); upsertCache(K_DEBTS, result.debt);
    upsertCache(K_CASH, result.cash); if (result.repair) upsertCache(K_REPAIRS, result.repair);
    updateSyncStatus(writer.pending ? 'saving' : 'saved');
  }
}

async function confirmPayDebt() {
  return saveAction('debt-payment', '#modal-pay-debt button', async () => {
    await commitDebtPayment({
      debtId: document.getElementById('pay-debt-id').value,
      amount: Number(document.getElementById('pay-debt-amount').value),
      method: document.querySelector('input[name="pay-debt-method"]:checked')?.value,
      date: today()
    });
    closeModal('modal-pay-debt'); renderDebts(); renderDashboard(); updateNotifBadge();
    toast('تم تسجيل التسديد');
  });
}

function deleteDebt(id) {
  document.getElementById('confirm-message').textContent = 'هل أنت متأكد من حذف هذا الدين؟ سيتم حذف كل دفعاته أيضاً.';
  document.getElementById('confirm-ok').onclick = () => {
    // احذف الدفعات المرتبطة + معاملات الصندوق الخاصة بها
    const payments = DB.get(K_DEBT_PAYMENTS).filter(p => p.debtId === id);
    if (payments.length > 0) {
      const cashIds = payments.map(p => p.cashId).filter(Boolean);
      DB.set(K_DEBT_PAYMENTS, DB.get(K_DEBT_PAYMENTS).filter(p => p.debtId !== id));
      if (cashIds.length > 0)
        DB.set(K_CASH, DB.get(K_CASH).filter(c => !cashIds.includes(c.id)));
    }
    // احذف الدين نفسه
    DB.set(K_DEBTS, DB.get(K_DEBTS).filter(d => d.id !== id));
    closeModal('modal-confirm');
    renderDebts();
    toast('تم الحذف', 'info');
  };
  openModal('modal-confirm');
}

// ==================== CUSTOMERS ====================
function renderCustomers() {
  const search = document.getElementById('cust-search').value.toLowerCase();
  const typeF  = document.getElementById('cust-type-filter').value;
  const repairs   = DB.get(K_REPAIRS);
  const allCash   = DB.get(K_CASH);
  const allDebts  = DB.get(K_DEBTS);

  let list = DB.get(K_CUSTOMERS).filter(c => {
    if (search && !c.name.toLowerCase().includes(search) && !(c.phone||'').includes(search)) return false;
    if (typeF && c.type !== typeF) return false;
    return true;
  });

  const tbody = document.getElementById('customers-tbody');
  if (list.length === 0) {
    tbody.innerHTML = `<tr><td colspan="6"><div class="empty-state"><div class="icon">👥</div><p>لا توجد عملاء</p></div></td></tr>`;
    return;
  }

  tbody.innerHTML = list.map((c, i) => {
    const ops = repairs.filter(r => r.customer === c.name);
    const totalPaid = ops.reduce((s,r) => s + +r.paid, 0);

    // مدفوعات الصندوق لهذا العميل
    const cashPays = allCash.filter(x =>
      (x.person||'').trim().toLowerCase() === c.name.trim().toLowerCase() &&
      (x.type === 'out' || x.type === 'usd_out' || x.type === 'bank_out')
    );
    const cashTRY = cashPays.filter(x => x.type !== 'usd_out').reduce((s,x) => s + +x.amount, 0);
    const cashUSD = cashPays.filter(x => x.type === 'usd_out').reduce((s,x) => s + +x.usdAmount, 0);

    // ديون العميل
    const debts = allDebts.filter(d => d.name === c.name);
    const debtRem = debts.reduce((s,d) => s + (+d.amount - +d.paid), 0);

    // أي نشاط = صيانات أو دفعات أو ديون
    const hasActivity = ops.length > 0 || cashPays.length > 0 || debts.length > 0;

    // بناء ملخص المبالغ
    const summaryParts = [];
    if (totalPaid > 0) summaryParts.push(`${formatNum(totalPaid)} ل صيانات`);
    if (cashTRY   > 0) summaryParts.push(`${formatNum(cashTRY)} ل دفع`);
    if (cashUSD   > 0) summaryParts.push(`${cashUSD.toFixed(2)} $ دفع`);
    if (debtRem   > 0) summaryParts.push(`<span style="color:var(--danger)">${formatNum(debtRem)} ل دين</span>`);

    return `
      <tr>
        <td>${i+1}</td>
        <td>
          <strong>${escapeHTML(c.name)}</strong>
          ${summaryParts.length > 0 ? `<br><span style="font-size:11px;color:var(--text-muted)">${summaryParts.join(' | ')}</span>` : ''}
        </td>
        <td>${escapeHTML(c.phone||'-')}</td>
        <td><span class="badge ${c.type==='محل'?'badge-info':'badge-teal'}">${escapeHTML(c.type||'زبون')}</span></td>
        <td>
          ${ops.length > 0 ? `<span class="badge badge-teal">${ops.length} صيانة</span>` : ''}
          ${cashPays.length > 0 ? `<span class="badge badge-warning" style="margin-top:2px">${cashPays.length} دفعة</span>` : ''}
          ${!hasActivity ? '-' : ''}
        </td>
        <td>
          <div style="display:flex;gap:4px">
            ${hasActivity ? `<button class="btn btn-sm btn-primary" data-customer-history="${escapeHTML(c.name)}">📋 السجل</button>` : ''}
            <button class="btn btn-sm btn-secondary" onclick="editCustomer(${htmlJSArg(c.id)})">✏️</button>
            <button class="btn btn-sm btn-danger" onclick="askDeleteCustomer(${htmlJSArg(c.id)})">🗑️</button>
          </div>
        </td>
      </tr>
    `;
  }).join('');

  getCustomersDatalist();
}

function showCustomerHistory(name) {
  const repairs = DB.get(K_REPAIRS).filter(r => r.customer === name).sort((a,b) => b.date.localeCompare(a.date));
  const debts   = DB.get(K_DEBTS).filter(d => d.name === name).sort((a,b) => b.date.localeCompare(a.date));
  // مدفوعات الصندوق المباشرة لهذا العميل (دفع ليرة أو دولار من الصندوق)
  const cashPays = DB.get(K_CASH).filter(c =>
    (c.person || '').trim().toLowerCase() === name.trim().toLowerCase() &&
    (c.type === 'out' || c.type === 'usd_out' || c.type === 'bank_out')
  ).sort((a,b) => (b.date||'').localeCompare(a.date||''));

  const totalPaid   = repairs.reduce((s,r) => s + +r.paid, 0);
  const totalAmount = repairs.reduce((s,r) => s + +r.total, 0);
  const totalDebt   = debts.reduce((s,d) => s + (+d.amount - +d.paid), 0);
  const totalCashOut = cashPays.reduce((s,c) => s + (c.type==='usd_out' ? 0 : +c.amount||0), 0);
  const totalUsdOut  = cashPays.reduce((s,c) => s + (c.type==='usd_out' ? +c.usdAmount||0 : 0), 0);

  document.getElementById('cust-history-title').innerHTML = `📋 سجل العميل: <span style="color:var(--accent)">${escapeHTML(name)}</span>`;

  if (repairs.length === 0 && debts.length === 0 && cashPays.length === 0) {
    document.getElementById('cust-history-content').innerHTML = `<div class="empty-state"><div class="icon">📭</div><p>لا توجد عمليات لهذا العميل</p></div>`;
    openModal('modal-customer-history');
    return;
  }

  document.getElementById('cust-history-content').innerHTML = `
    <!-- Summary -->
    <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin-bottom:16px;padding:0 20px">
      <div style="background:var(--bg-card2);border:1px solid var(--border);border-radius:10px;padding:12px;text-align:center">
        <div style="font-size:11px;color:var(--text-muted);margin-bottom:4px">عدد الصيانات</div>
        <div style="font-size:20px;font-weight:900;color:var(--info)">${repairs.length}</div>
      </div>
      <div style="background:var(--bg-card2);border:1px solid var(--border);border-radius:10px;padding:12px;text-align:center">
        <div style="font-size:11px;color:var(--text-muted);margin-bottom:4px">إجمالي المقبوض</div>
        <div style="font-size:20px;font-weight:900;color:var(--success)">${formatNum(totalPaid)}</div>
        <div style="font-size:10px;color:var(--text-muted)">ليرة</div>
      </div>
      <div style="background:var(--bg-card2);border:1px solid var(--border);border-radius:10px;padding:12px;text-align:center">
        <div style="font-size:11px;color:var(--text-muted);margin-bottom:4px">ديون متبقية</div>
        <div style="font-size:20px;font-weight:900;color:${totalDebt>0?'var(--danger)':'var(--success)'}">${formatNum(totalDebt)}</div>
        <div style="font-size:10px;color:var(--text-muted)">ليرة</div>
      </div>
    </div>
    ${(totalCashOut > 0 || totalUsdOut > 0) ? `
    <div style="padding:0 20px;margin-bottom:14px">
      <div style="background:rgba(239,68,68,0.1);border:1px solid rgba(239,68,68,0.3);border-radius:10px;padding:12px;display:flex;align-items:center;gap:12px">
        <span style="font-size:20px">💸</span>
        <div>
          <div style="font-size:12px;color:var(--text-muted)">مدفوعات من الصندوق لهذا العميل</div>
          <div style="font-weight:800;color:var(--danger)">
            ${totalCashOut > 0 ? formatNum(totalCashOut) + ' ليرة' : ''}
            ${totalCashOut > 0 && totalUsdOut > 0 ? ' + ' : ''}
            ${totalUsdOut > 0 ? totalUsdOut.toFixed(2) + ' $' : ''}
          </div>
        </div>
      </div>
    </div>` : ''}

    <!-- زر نسخ الجرد -->
    <div style="padding:0 20px;margin-bottom:14px">
      <button data-copy-customer="${escapeHTML(name)}"
        style="width:100%;padding:10px;background:linear-gradient(135deg,#c9960c,#f5c842);border:none;border-radius:10px;color:#0a0e1a;font-size:13px;font-weight:800;cursor:pointer;font-family:inherit;display:flex;align-items:center;justify-content:center;gap:8px">
        📋 نسخ الجرد لإرساله للعميل
      </button>
    </div>

    <!-- تبويبات -->
    <div style="display:flex;gap:0;border-bottom:2px solid var(--border);margin:0 20px 16px">
      <button id="tab-repairs-btn" onclick="switchCustTab('repairs')"
        style="padding:10px 20px;background:none;border:none;border-bottom:3px solid var(--accent);color:var(--accent);font-weight:700;font-size:13px;cursor:pointer;font-family:inherit;margin-bottom:-2px">
        🔧 الصيانات (${repairs.length})
      </button>
      <button id="tab-debts-btn" onclick="switchCustTab('debts')"
        style="padding:10px 20px;background:none;border:none;border-bottom:3px solid transparent;color:var(--text-muted);font-weight:600;font-size:13px;cursor:pointer;font-family:inherit;margin-bottom:-2px">
        📋 الديون (${debts.length})
      </button>
      ${cashPays.length > 0 ? `
      <button id="tab-cashpays-btn" onclick="switchCustTab('cashpays')"
        style="padding:10px 20px;background:none;border:none;border-bottom:3px solid transparent;color:var(--text-muted);font-weight:600;font-size:13px;cursor:pointer;font-family:inherit;margin-bottom:-2px">
        💸 مدفوعات (${cashPays.length})
      </button>` : ''}
    </div>

    <!-- محتوى الصيانات -->
    <div id="tab-repairs-content" style="padding:0 20px 20px;display:flex;flex-direction:column;gap:12px">
      ${repairs.length === 0
        ? `<div style="text-align:center;color:var(--text-muted);padding:20px">لا توجد صيانات</div>`
        : repairs.map((r, i) => {
            const debtAmt = +r.debtAmount || (+r.total - +r.paid);
            return `
        <div style="background:var(--bg-card2);border:1px solid var(--border);border-radius:10px;overflow:hidden">
          <div style="display:flex;justify-content:space-between;align-items:center;padding:10px 14px;background:rgba(0,0,0,0.2);border-bottom:1px solid var(--border)">
            <div style="display:flex;align-items:center;gap:8px">
              <span style="font-size:12px;color:var(--text-muted)">#${repairs.length - i}</span>
              <strong style="font-size:14px">${escapeHTML(r.deviceType||'-')} ${escapeHTML(r.model||'')}</strong>
              <span class="badge ${r.workType==='سوفتوير'?'badge-success':r.workType==='هاردوير وسوفتوير'?'badge-warning':'badge-info'}" style="font-size:11px">${escapeHTML(r.workType||'-')}</span>
            </div>
            <span style="font-size:12px;color:var(--text-muted)">${formatDate(r.date)}</span>
          </div>
          <div style="display:grid;grid-template-columns:1fr 1fr;gap:0;font-size:13px">
            <div style="padding:10px 14px;border-left:1px solid var(--border);border-bottom:1px solid var(--border)">
              <div style="color:var(--text-muted);font-size:11px;margin-bottom:3px">العطل</div>
              <div>${escapeHTML(r.problem||'-')}</div>
            </div>
            <div style="padding:10px 14px;border-bottom:1px solid var(--border)">
              <div style="color:var(--text-muted);font-size:11px;margin-bottom:3px">نوع العمل</div>
              <div>${escapeHTML(r.workType||'-')}</div>
            </div>
            <div style="padding:10px 14px;border-left:1px solid var(--border)">
              <div style="color:var(--text-muted);font-size:11px;margin-bottom:3px">مبلغ الدين</div>
              <div style="font-weight:700;color:${debtAmt>0?'var(--danger)':'var(--success)'}">${formatNum(debtAmt)} ليرة</div>
            </div>
            <div style="padding:10px 14px">
              <div style="color:var(--text-muted);font-size:11px;margin-bottom:3px">المدفوع</div>
              <div style="font-weight:700;color:${+r.paid>= +r.total?'var(--success)':'var(--warning)'}">${formatNum(r.paid)} ل — ${getPayBadge(r.payStatus)}</div>
            </div>
          </div>
        </div>
      `}).join('')}
    </div>

    <!-- محتوى الديون -->
    <div id="tab-debts-content" style="padding:0 20px 20px;display:none;flex-direction:column;gap:12px">
      ${debts.length === 0
        ? `<div style="text-align:center;color:var(--text-muted);padding:20px">لا توجد ديون مسجلة</div>`
        : debts.map(d => {
            const rem = +d.amount - +d.paid;
            const payments = DB.get(K_DEBT_PAYMENTS).filter(p => p.debtId === d.id);
            return `
            <div style="background:var(--bg-card2);border:1px solid var(--border);border-radius:10px;overflow:hidden">
              <div style="display:flex;justify-content:space-between;align-items:center;padding:10px 14px;background:rgba(0,0,0,0.2);border-bottom:1px solid var(--border)">
                <div style="display:flex;align-items:center;gap:8px">
                  <strong>${escapeHTML(d.name)}</strong>
                  ${rem <= 0 ? '<span class="badge badge-success">✅ مسدد</span>' : '<span class="badge badge-danger">⏳ مستحق</span>'}
                </div>
                <span style="font-size:12px;color:var(--text-muted)">${formatDate(d.date)}</span>
              </div>
              <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:0;font-size:13px;border-bottom:1px solid var(--border)">
                <div style="padding:10px 14px;border-left:1px solid var(--border);text-align:center">
                  <div style="color:var(--text-muted);font-size:11px;margin-bottom:3px">الدين</div>
                  <div style="font-weight:700;color:var(--accent2)">${formatNum(d.amount)} ل</div>
                </div>
                <div style="padding:10px 14px;border-left:1px solid var(--border);text-align:center">
                  <div style="color:var(--text-muted);font-size:11px;margin-bottom:3px">المدفوع</div>
                  <div style="font-weight:700;color:var(--success)">${formatNum(d.paid)} ل</div>
                </div>
                <div style="padding:10px 14px;text-align:center">
                  <div style="color:var(--text-muted);font-size:11px;margin-bottom:3px">المتبقي</div>
                  <div style="font-weight:700;color:${rem>0?'var(--danger)':'var(--success)'}">${formatNum(rem)} ل</div>
                </div>
              </div>
              ${payments.length > 0 ? `
              <div style="padding:10px 14px">
                <div style="font-size:11px;color:var(--text-muted);margin-bottom:8px">الدفعات (${payments.length})</div>
                <div style="display:flex;flex-direction:column;gap:5px">
                  ${payments.map((p,i) => `
                    <div style="display:flex;justify-content:space-between;padding:6px 10px;background:rgba(0,0,0,0.15);border-radius:6px;font-size:12px">
                      <span style="color:var(--text-muted)">دفعة ${i+1} — ${formatDate(p.date)}</span>
                      <strong style="color:var(--success)">+ ${formatNum(p.amount)} ل</strong>
                    </div>
                  `).join('')}
                </div>
              </div>` : ''}
            </div>`;
          }).join('')}
    </div>

    <!-- محتوى مدفوعات الصندوق -->
    <div id="tab-cashpays-content" style="padding:0 20px 20px;display:none;flex-direction:column;gap:10px">
      ${cashPays.length === 0
        ? `<div style="text-align:center;color:var(--text-muted);padding:20px">لا توجد مدفوعات</div>`
        : cashPays.map((c,i) => {
            const isUsd = c.type === 'usd_out';
            const amtTxt = isUsd ? `${(+c.usdAmount||0).toFixed(2)} $` : `${formatNum(c.amount)} ل`;
            const typeLabel = isUsd ? '💵 دولار' : c.type==='bank_out' ? '🏦 بنك' : '💵 كاش';
            return `
            <div style="background:var(--bg-card2);border:1px solid rgba(239,68,68,0.3);border-radius:10px;padding:12px 14px;display:flex;justify-content:space-between;align-items:center">
              <div>
                <div style="font-weight:700;font-size:13px;color:var(--danger)">${amtTxt}</div>
                <div style="font-size:11px;color:var(--text-muted);margin-top:3px">${escapeHTML(c.desc||'دفع من الصندوق')}</div>
              </div>
              <div style="text-align:left">
                <div style="font-size:11px;color:var(--text-muted)">${formatDate(c.date||'')}</div>
                <div style="font-size:10px;color:var(--text-muted);margin-top:2px">${typeLabel}</div>
              </div>
            </div>`;
          }).join('')}
    </div>
  `;

  openModal('modal-customer-history');
}

function copyCustomerReport(name) {
  const repairs = DB.get(K_REPAIRS).filter(r => r.customer === name).sort((a,b) => b.date.localeCompare(a.date));
  const debts   = DB.get(K_DEBTS).filter(d => d.name === name);
  const totalDebt = debts.reduce((s,d) => s + (+d.amount - +d.paid), 0);

  const lines = [];
  lines.push('🏪 الهرم للاتصالات والصيانة');
  lines.push('━━━━━━━━━━━━━━━━━━━━━━');
  lines.push(`👤 العميل: ${name}`);
  lines.push(`📅 التاريخ: ${formatDate(today())}`);
  lines.push('━━━━━━━━━━━━━━━━━━━━━━');

  if (repairs.length > 0) {
    lines.push('🔧 الصيانات:');
    repairs.forEach((r, i) => {
      const debtAmt = +r.debtAmount || (+r.total - +r.paid);
      lines.push('');
      lines.push(`  ${i+1}. ${r.deviceType||''} ${r.model||''}`);
      lines.push(`     العطل: ${r.problem||'-'}`);
      lines.push(`     نوع العمل: ${r.workType||'-'}`);
      lines.push(`     مبلغ الدين: ${formatNum(debtAmt)} ليرة`);
      lines.push(`     المدفوع: ${formatNum(r.paid)} ليرة`);
      lines.push(`     التاريخ: ${formatDate(r.date)}`);
    });
    lines.push('');
  }

  lines.push('━━━━━━━━━━━━━━━━━━━━━━');
  lines.push(`💰 إجمالي الديون المتبقية: ${formatNum(totalDebt)} ليرة`);
  lines.push('━━━━━━━━━━━━━━━━━━━━━━');

  const text = lines.join('\n');
  navigator.clipboard.writeText(text).then(() => {
    toast('✅ تم نسخ الجرد — يمكنك إرساله للعميل');
  }).catch(() => {
    // fallback
    const ta = document.createElement('textarea');
    ta.value = text;
    document.body.appendChild(ta);
    ta.select();
    document.execCommand('copy');
    document.body.removeChild(ta);
    toast('✅ تم نسخ الجرد');
  });
}

window.openEditPayment = function(payId, debtId) {
  const p = DB.get(K_DEBT_PAYMENTS).find(x => x.id === payId);
  if (!p) return;
  document.getElementById('ep-id').value     = payId;
  document.getElementById('ep-debt-id').value= debtId;
  document.getElementById('ep-amount').value = p.amount || '';
  document.getElementById('ep-date').value   = p.date   || today();
  document.querySelectorAll('input[name="ep-method"]').forEach(r => {
    r.checked = r.value === (p.method || 'كاش');
  });
  openModal('modal-edit-payment');
};

window.saveEditPayment = async function() {
  return saveAction('edit-payment', '#modal-edit-payment button', async () => {
    const debtId = document.getElementById('ep-debt-id').value;
    await commitDebtPayment({ debtId, paymentId: document.getElementById('ep-id').value,
      amount: Number(document.getElementById('ep-amount').value),
      method: document.querySelector('input[name="ep-method"]:checked')?.value,
      date: document.getElementById('ep-date').value || today() });
    closeModal('modal-edit-payment'); showDebtHistory(debtId); renderDebts(); updateNotifBadge();
    toast('تم تحديث الدفعة');
  });
};

function showDebtHistory(id) {
  const d = DB.get(K_DEBTS).find(x => x.id === id);
  if (!d) return;

  const remaining = +d.amount - +d.paid;
  const payments  = DB.get(K_DEBT_PAYMENTS).filter(p => p.debtId === id).sort((a,b) => a.date.localeCompare(b.date));

  document.getElementById('debt-history-title').innerHTML =
    `📋 سجل الدفعات: <span style="color:var(--accent)">${escapeHTML(d.name)}</span>`;

  document.getElementById('debt-history-content').innerHTML = `
    <!-- ملخص -->
    <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin-bottom:20px">
      <div style="background:var(--bg-card2);border-radius:10px;padding:12px;text-align:center">
        <div style="font-size:11px;color:var(--text-muted);margin-bottom:4px">إجمالي الدين</div>
        <div style="font-size:20px;font-weight:900;color:var(--accent2)">${formatNum(d.amount)}</div>
        <div style="font-size:10px;color:var(--text-muted)">ليرة</div>
      </div>
      <div style="background:var(--bg-card2);border-radius:10px;padding:12px;text-align:center">
        <div style="font-size:11px;color:var(--text-muted);margin-bottom:4px">إجمالي المدفوع</div>
        <div style="font-size:20px;font-weight:900;color:var(--success)">${formatNum(d.paid)}</div>
        <div style="font-size:10px;color:var(--text-muted)">ليرة</div>
      </div>
      <div style="background:var(--bg-card2);border-radius:10px;padding:12px;text-align:center">
        <div style="font-size:11px;color:var(--text-muted);margin-bottom:4px">المتبقي</div>
        <div style="font-size:20px;font-weight:900;color:${remaining>0?'var(--danger)':'var(--success)'}">${formatNum(remaining)}</div>
        <div style="font-size:10px;color:var(--text-muted)">ليرة</div>
      </div>
    </div>

    <!-- شريط التقدم -->
    <div style="margin-bottom:20px">
      <div style="display:flex;justify-content:space-between;font-size:12px;color:var(--text-muted);margin-bottom:6px">
        <span>نسبة السداد</span>
        <span>${d.amount > 0 ? Math.round(+d.paid / +d.amount * 100) : 0}%</span>
      </div>
      <div style="height:8px;background:var(--bg-card2);border-radius:4px">
        <div style="height:8px;width:${d.amount > 0 ? Math.min(100, Math.round(+d.paid/+d.amount*100)) : 0}%;background:${remaining<=0?'var(--success)':'var(--accent)'};border-radius:4px;transition:width .3s"></div>
      </div>
    </div>

    <!-- سجل الدفعات -->
    <div style="font-size:13px;font-weight:700;color:var(--text-muted);margin-bottom:10px">سجل الدفعات (${payments.length})</div>
    ${payments.length === 0
      ? `<div style="text-align:center;color:var(--text-muted);padding:20px">لا توجد دفعات مسجلة بعد</div>`
      : `<div style="display:flex;flex-direction:column;gap:8px">
          ${payments.map((p, i) => `
            <div style="display:flex;justify-content:space-between;align-items:center;padding:10px 14px;background:var(--bg-card2);border-radius:8px;border-right:3px solid var(--success)">
              <div>
                <div style="font-size:12px;color:var(--text-muted)">دفعة ${i+1} — ${formatDate(p.date)}</div>
                <span class="badge ${p.method==='بنك'?'badge-info':'badge-success'}" style="font-size:10px;margin-top:3px">${p.method==='بنك'?'🏦 بنك':'💵 كاش'}</span>
              </div>
              <div style="display:flex;align-items:center;gap:8px">
                <strong style="color:var(--success);font-size:15px">+ ${formatNum(p.amount)} ل</strong>
                <button onclick="openEditPayment(${htmlJSArg(p.id)},${htmlJSArg(d.id)})" style="background:rgba(0,212,170,0.1);border:1px solid rgba(0,212,170,0.2);border-radius:6px;padding:3px 8px;cursor:pointer;color:var(--accent);font-size:11px;font-family:inherit">✏️</button>
              </div>
            </div>
          `).join('')}
        </div>`
    }
  `;

  openModal('modal-debt-history');
}

function switchCustTab(tab) {
  const tabs = ['repairs','debts','cashpays'];
  tabs.forEach(t => {
    const content = document.getElementById(`tab-${t}-content`);
    const btn     = document.getElementById(`tab-${t}-btn`);
    if (!content) return;
    if (t === tab) {
      content.style.display = 'flex';
      if (btn) { btn.style.borderBottomColor = 'var(--accent)'; btn.style.color = 'var(--accent)'; btn.style.fontWeight = '700'; }
    } else {
      content.style.display = 'none';
      if (btn) { btn.style.borderBottomColor = 'transparent'; btn.style.color = 'var(--text-muted)'; btn.style.fontWeight = '600'; }
    }
  });
}

function openAddCustomer() {
  document.getElementById('cust-edit-id').value = '';
  document.getElementById('cust-modal-title').textContent = 'إضافة عميل جديد';
  document.getElementById('cust-name').value = '';
  document.getElementById('cust-phone').value = '';
  document.getElementById('cust-type').value = 'زبون';
  openModal('modal-customer');
}

function editCustomer(id) {
  const c = DB.get(K_CUSTOMERS).find(x => x.id === id);
  if (!c) return;
  document.getElementById('cust-edit-id').value = id;
  document.getElementById('cust-modal-title').textContent = 'تعديل بيانات عميل';
  document.getElementById('cust-name').value = c.name;
  document.getElementById('cust-phone').value = c.phone || '';
  document.getElementById('cust-type').value = c.type || 'زبون';
  openModal('modal-customer');
}

async function saveCustomer() {
  return saveAction('saveCustomer', '#modal-customer button', async () => {
    [K_CUSTOMERS, K_REPAIRS, K_DEBTS, K_CASH, K_DEBT_PAYMENTS].forEach(assertLoaded);
    const name = document.getElementById('cust-name').value.trim();
    if (!name) throw new Error('أدخل اسم العميل');
    const editId = document.getElementById('cust-edit-id').value;
    const customers = DB.get(K_CUSTOMERS);
    if (customers.some(customer => customer.id !== editId && customer.name.trim().toLowerCase() === name.toLowerCase())) throw new Error('هذا الاسم موجود مسبقاً');
    const old = editId ? customers.find(customer => customer.id === editId) : null;
    if (editId && !old) throw new Error('العميل غير موجود');
    const updated = { ...old, id: editId || uid(), name, phone: document.getElementById('cust-phone').value.trim(), type: document.getElementById('cust-type').value };
    if (old && old.name !== name) {
      for (const [key, field] of [[K_REPAIRS, 'customer'], [K_DEBTS, 'name'], [K_CASH, 'person'], [K_DEBT_PAYMENTS, 'debtName']]) {
        const rows = DB.get(key).map(row => row[field] === old.name ? { ...row, [field]: name } : row);
        DB.set(key, rows);
      }
    }
    DB.set(K_CUSTOMERS, old ? customers.map(customer => customer.id === editId ? updated : customer) : [...customers, updated]);
    await writer.wait();
    closeModal('modal-customer'); renderCustomers(); toast('تم حفظ بيانات العميل وسجله');
  });
}

function deleteCustomer(id) {
  document.getElementById('confirm-message').textContent = 'هل أنت متأكد من حذف هذا العميل؟';
  document.getElementById('confirm-ok').onclick = () => {
    DB.set(K_CUSTOMERS, DB.get(K_CUSTOMERS).filter(c => c.id !== id));
    closeModal('modal-confirm');
    renderCustomers();
    toast('تم الحذف', 'info');
  };
  openModal('modal-confirm');
}

// ==================== REPORTS ====================
function showDrill(type) {
  const period      = document.getElementById('report-period')?.value || 'month';
  const customMonth = document.getElementById('report-month-custom')?.value || '';
  const todayStr    = today();
  const monthStr    = customMonth || currentMonth();

  function inPeriod(date) {
    if (!date) return false;
    if (customMonth) return date.startsWith(customMonth);
    if (period === 'all') return true;
    if (period === 'today') return date === todayStr;
    if (period === 'month') return date.startsWith(monthStr);
    if (period === 'year')  return date.startsWith(new Date().getFullYear().toString());
    if (period === 'week') {
      const d = new Date(date);
      const diff = (new Date() - d) / (1000*60*60*24);
      return diff <= 7;
    }
    return false;
  }

  const repairs  = DB.get(K_REPAIRS).filter(r => inPeriod(r.date));
  const cash     = DB.get(K_CASH).filter(c => inPeriod(c.date));
  const expenses = DB.get(K_EXPENSES).filter(e => inPeriod(e.date));

  let title = '', rows = '';

  if (type === 'today-profit' || type === 'month-profit') {
    title = type === 'today-profit' ? '📈 تفاصيل أرباح اليوم' : '📅 تفاصيل أرباح الشهر';
    const data = type === 'today-profit'
      ? DB.get(K_REPAIRS).filter(r => r.date === todayStr)
      : DB.get(K_REPAIRS).filter(r => r.date?.startsWith(monthStr));
    rows = data.length === 0
      ? '<div style="text-align:center;color:var(--text-muted);padding:20px">لا توجد بيانات</div>'
      : data.sort((a,b)=>b.date.localeCompare(a.date)).map(r => `
        <div style="display:flex;justify-content:space-between;align-items:center;padding:10px 12px;background:var(--bg-card2);border-radius:8px;margin-bottom:6px;border-right:3px solid var(--accent)">
          <div>
            <div style="font-weight:600">${escapeHTML(r.customer)} — ${escapeHTML(r.model||'-')}</div>
            <div style="font-size:12px;color:var(--text-muted)">${formatDate(r.date)} | ${escapeHTML(r.problem||'-')}</div>
          </div>
          <div style="text-align:left">
            <div style="color:var(--success);font-weight:700">مقبوض: ${formatNum(r.paid)} ل</div>
            ${+r.partsCost > 0 ? `<div style="font-size:12px;color:var(--danger)">قطع: ${formatNum(r.partsCost)} ل</div>` : ''}
            <div style="font-size:13px;font-weight:700;color:var(--accent)">ربح: ${formatNum(+r.paid - +r.partsCost)} ل</div>
          </div>
        </div>`).join('');

  } else if (type === 'cash-in') {
    title = '💵 تفاصيل المقبوضات كاش';
    const data = cash.filter(c => c.type === 'in');
    rows = _drillCashRows(data, 'success');

  } else if (type === 'bank-in') {
    title = '🏦 تفاصيل مقبوضات البنك';
    const data = cash.filter(c => c.type === 'bank_in');
    rows = _drillCashRows(data, 'info');

  } else if (type === 'cash-out') {
    title = '💸 تفاصيل المدفوعات';
    const data = cash.filter(c => c.type === 'out' || c.type === 'bank_out');
    rows = _drillCashRows(data, 'danger');

  } else if (type === 'expenses') {
    title = '💸 تفاصيل المصاريف';
    rows = expenses.length === 0
      ? '<div style="text-align:center;color:var(--text-muted);padding:20px">لا توجد مصاريف</div>'
      : expenses.sort((a,b)=>b.date.localeCompare(a.date)).map(e => `
        <div style="display:flex;justify-content:space-between;align-items:center;padding:10px 12px;background:var(--bg-card2);border-radius:8px;margin-bottom:6px;border-right:3px solid var(--danger)">
          <div>
            <div style="font-weight:600">${escapeHTML(e.desc)}</div>
            <div style="font-size:12px;color:var(--text-muted)">${formatDate(e.date)} | 🏷️ ${escapeHTML(e.category||'-')}${e.to?' | 👤 '+escapeHTML(e.to):''}</div>
          </div>
          <strong style="color:var(--danger)">− ${formatNum(e.amount)} ل</strong>
        </div>`).join('');

  } else if (type === 'fixed') {
    title = '🔧 تفاصيل الأجهزة المصلّحة';
    rows = repairs.length === 0
      ? '<div style="text-align:center;color:var(--text-muted);padding:20px">لا توجد بيانات</div>'
      : repairs.sort((a,b)=>b.date.localeCompare(a.date)).map(r => `
        <div style="display:flex;justify-content:space-between;align-items:center;padding:10px 12px;background:var(--bg-card2);border-radius:8px;margin-bottom:6px">
          <div>
            <div style="font-weight:600">${escapeHTML(r.customer)} — ${escapeHTML(r.model||'-')}</div>
            <div style="font-size:12px;color:var(--text-muted)">${formatDate(r.date)} | ${escapeHTML(r.problem||'-')}</div>
          </div>
          <span class="badge badge-success">${escapeHTML(r.workType||'-')}</span>
        </div>`).join('');

  } else if (type === 'parts-cost') {
    title = '🔩 تفاصيل تكلفة القطع';
    const data = repairs.filter(r => +r.partsCost > 0);
    rows = data.length === 0
      ? '<div style="text-align:center;color:var(--text-muted);padding:20px">لا توجد قطع مسجلة</div>'
      : data.sort((a,b)=>b.date.localeCompare(a.date)).map(r => `
        <div style="display:flex;justify-content:space-between;align-items:center;padding:10px 12px;background:var(--bg-card2);border-radius:8px;margin-bottom:6px;border-right:3px solid #e55">
          <div>
            <div style="font-weight:600">${escapeHTML(r.customer)} — ${escapeHTML(r.model||'-')}</div>
            <div style="font-size:12px;color:var(--text-muted)">${formatDate(r.date)}</div>
          </div>
          <strong style="color:var(--danger)">− ${formatNum(r.partsCost)} ل</strong>
        </div>`).join('');
  }

  document.getElementById('drill-title').textContent = title;
  document.getElementById('drill-content').innerHTML  = rows;
  openModal('modal-report-drill');
}

function _drillCashRows(data, color) {
  if (data.length === 0) return '<div style="text-align:center;color:var(--text-muted);padding:20px">لا توجد بيانات</div>';
  return data.sort((a,b)=>b.date.localeCompare(a.date)).map(c => `
    <div style="display:flex;justify-content:space-between;align-items:center;padding:10px 12px;background:var(--bg-card2);border-radius:8px;margin-bottom:6px;border-right:3px solid var(--${color})">
      <div>
        <div style="font-weight:600">${escapeHTML(c.desc)}</div>
        <div style="font-size:12px;color:var(--text-muted)">${formatDate(c.date)}${c.person?' | 👤 '+escapeHTML(c.person):''}</div>
      </div>
      <strong style="color:var(--${color})">${formatNum(c.amount)} ل</strong>
    </div>`).join('');
}

function onReportMonthChange() {
  // عند اختيار شهر مخصص، امسح فلتر الفترة العام
  const customMonth = document.getElementById('report-month-custom')?.value;
  if (customMonth) {
    document.getElementById('report-period').value = 'month';
  }
  renderReports();
}

function renderReports() {
  const period = document.getElementById('report-period').value;
  const customMonth = document.getElementById('report-month-custom')?.value || '';
  const repairs = DB.get(K_REPAIRS);
  const cash = DB.get(K_CASH);
  const todayStr = today();
  const monthStr = currentMonth();

  function inPeriod(date) {
    if (!date) return false;
    // لو في شهر مخصص محدد، يأخذ الأولوية
    if (customMonth) return date.startsWith(customMonth);
    if (period === 'all') return true;
    if (period === 'today') return date === todayStr;
    if (period === 'week') {
      const d = new Date(date);
      const now = new Date();
      const diff = (now - d) / (1000 * 60 * 60 * 24);
      return diff <= 7;
    }
    if (period === 'month') return date.startsWith(monthStr);
    if (period === 'year') return date.startsWith(new Date().getFullYear().toString());
    return false;
  }

  const filteredCash    = cash.filter(c => inPeriod(c.date));
  const filteredRepairs = repairs.filter(r => inPeriod(r.date));
  const filteredExpenses = DB.get(K_EXPENSES).filter(e => inPeriod(e.date));

  const totalIn    = filteredCash.filter(c => c.type === 'in').reduce((s,c) => s + +c.amount, 0);
  const bankIn     = filteredCash.filter(c => c.type === 'bank_in').reduce((s,c) => s + +c.amount, 0);
  const totalExp   = filteredExpenses.reduce((s,e) => s + +e.amount, 0);

  // تكلفة القطع: كل الأجهزة بالشهر (مش بس المدفوعة)
  const partsCash = filteredRepairs.reduce((s,r) => s + (+r.partsCost||0), 0);
  const partsDebt = filteredRepairs.filter(r => +r.paid === 0).reduce((s,r) => s + +r.partsCost, 0);

  document.getElementById('rep-total-in').textContent = formatNum(totalIn);
  document.getElementById('rep-bank-in').textContent  = formatNum(bankIn);
  document.getElementById('rep-expenses').textContent = formatNum(totalExp);
  document.getElementById('rep-fixed').textContent    = filteredRepairs.length;
  document.getElementById('rep-parts-cash').textContent = formatNum(partsCash);
  document.getElementById('rep-parts-debt').textContent = formatNum(partsDebt);

  // ==================== حساب الأرباح الصحيح ====================
  // الإيرادات = كل المقبوضات نقداً (type=in) في الفترة من الصندوق
  // المصاريف = مجموع المصاريف + تكلفة القطع للأجهزة المُصلَحة
  // الربح الصافي = الإيرادات - المصاريف - تكلفة القطع

  const hwOnly   = filteredRepairs.filter(r => r.workType === 'هاردوير');
  const swOnly   = filteredRepairs.filter(r => r.workType === 'سوفتوير');
  const hwswBoth = filteredRepairs.filter(r => r.workType === 'هاردوير وسوفتوير');

  // إيرادات كل نوع = المبلغ المقبوض (paid) فقط — الآجل لا يُحتسب كإيراد حتى يُدفع
  const hwCashIn   = hwOnly.reduce((s,r)   => s + (+r.paid||0), 0);
  const swCashIn   = swOnly.reduce((s,r)   => s + (+r.paid||0), 0);
  const bothCashIn = hwswBoth.reduce((s,r) => s + (+r.paid||0), 0);

  // تكلفة القطع لكل نوع
  const hwParts   = hwOnly.reduce((s,r)   => s + (+r.partsCost||0), 0);
  const swParts   = swOnly.reduce((s,r)   => s + (+r.partsCost||0), 0);
  const bothParts = hwswBoth.reduce((s,r) => s + (+r.partsCost||0), 0);

  // ربح صافي لكل نوع (المشترك يُقسَّم مناصفة)
  const hwIncome   = (hwCashIn - hwParts) + (bothCashIn - bothParts) / 2;
  const swIncome   = (swCashIn - swParts) + (bothCashIn - bothParts) / 2;

  const hwLabel = hwswBoth.length > 0 ? `${hwOnly.length} + ${hwswBoth.length} مشترك` : `${hwOnly.length}`;
  const swLabel = hwswBoth.length > 0 ? `${swOnly.length} + ${hwswBoth.length} مشترك` : `${swOnly.length}`;

  document.getElementById('hw-count').textContent = hwLabel;
  document.getElementById('hw-income').textContent = formatNum(hwIncome) + ' ليرة';
  document.getElementById('sw-count').textContent = swLabel;
  document.getElementById('sw-income').textContent = formatNum(swIncome) + ' ليرة';

  // قسم الأرباح المحمي
  const totalHwProfit = hwIncome;
  const totalSwProfit = swIncome;
  window._profitHW = totalHwProfit;
  window._profitSW = totalSwProfit;
  document.getElementById('profit-hw').textContent = formatNum(totalHwProfit);
  document.getElementById('profit-sw').textContent = formatNum(totalSwProfit);
  document.getElementById('profit-hw-anour').textContent   = formatNum(Math.max(0, totalHwProfit) * 0.45);
  document.getElementById('profit-hw-aobayda').textContent = formatNum(Math.max(0, totalHwProfit) * 0.55);

  // Build device lists (hidden by default, toggled on demand)
  const buildList = (list, color) => {
    if (list.length === 0) return `<div class="empty-state" style="padding:20px"><p>لا توجد أجهزة</p></div>`;
    return `
      <div style="border-top:1px solid var(--border);padding-top:12px">
        ${list.sort((a,b) => b.date.localeCompare(a.date)).map((r,i) => `
          <div style="display:grid;grid-template-columns:1fr auto;gap:8px;padding:9px 0;border-bottom:1px solid rgba(30,45,74,0.4);font-size:13px;align-items:start">
            <div>
              <div style="font-weight:700">${escapeHTML(r.customer)} — <span style="color:${color}">${escapeHTML(r.deviceType||'')} ${escapeHTML(r.model||'')}</span></div>
              <div style="color:var(--text-muted);font-size:11.5px;margin-top:2px">${escapeHTML(r.problem||'')}</div>
              ${r.parts ? `<div style="color:var(--text-muted);font-size:11px">🔩 ${r.parts}</div>` : ''}
            </div>
            <div style="text-align:left;white-space:nowrap">
              <div style="font-weight:700;color:${color}">${formatNum(+r.paid > 0 ? +r.paid - +r.partsCost : 0)} ل</div>
              <div style="font-size:11px;color:var(--text-muted)">${formatDate(r.date)}</div>
              <div style="margin-top:2px">${getPayBadge(r.payStatus)}</div>
            </div>
          </div>
        `).join('')}
      </div>
    `;
  };

  document.getElementById('hw-list').innerHTML = buildList([...hwOnly, ...hwswBoth], 'var(--accent2)');
  document.getElementById('sw-list').innerHTML = buildList([...swOnly, ...hwswBoth], 'var(--success)');

  // Reset list visibility on period change
  document.getElementById('hw-list').style.display = 'none';
  document.getElementById('sw-list').style.display = 'none';

  // Top problems
  const problems = Object.create(null);
  filteredRepairs.forEach(r => {
    const p = (r.problem||'').substring(0,30);
    problems[p] = (problems[p] || 0) + 1;
  });
  const topP = Object.entries(problems).sort((a,b) => b[1]-a[1]).slice(0,5);
  document.getElementById('top-problems').innerHTML = topP.length === 0 ? '<div class="empty-state"><p>لا توجد بيانات</p></div>' :
    topP.map(([p, n]) => `
      <div style="display:flex;justify-content:space-between;padding:8px 0;border-bottom:1px solid var(--border)">
        <span style="font-size:13px">${escapeHTML(p)}</span>
        <span class="badge badge-warning">${n}</span>
      </div>
    `).join('');

  // Top devices
  const devs = Object.create(null);
  filteredRepairs.forEach(r => { const t = r.deviceType || 'أخرى'; devs[t] = (devs[t] || 0) + 1; });
  const topD = Object.entries(devs).sort((a,b) => b[1]-a[1]).slice(0,5);
  document.getElementById('top-devices').innerHTML = topD.length === 0 ? '<div class="empty-state"><p>لا توجد بيانات</p></div>' :
    topD.map(([d, n]) => `
      <div style="display:flex;justify-content:space-between;padding:8px 0;border-bottom:1px solid var(--border)">
        <span style="font-size:13px">${escapeHTML(d)}</span>
        <span class="badge badge-info">${n}</span>
      </div>
    `).join('');

  // Init inventory month to current month
  const invMonth = document.getElementById('inv-month');
  if (!invMonth.value) invMonth.value = monthStr;
  renderInventory();
}

// ==================== WORK TYPE LIST TOGGLE ====================
function toggleWorkList(type) {
  const el = document.getElementById(type + '-list');
  const btn = el.closest('.card').querySelector('button');
  if (el.style.display === 'none') {
    el.style.display = 'block';
    btn.textContent = '🔼 إخفاء';
  } else {
    el.style.display = 'none';
    btn.textContent = '📋 السجل';
  }
}

// ==================== PROFIT SECTION ====================
async function openProfitSection() {
  const section = document.getElementById('profit-section-content');
  if (section.style.display === 'block') { section.style.display = 'none'; return; }
  return saveAction('profit', '#profit-section-content button', async () => {
    const user = auth.currentUser;
    if (!user?.email) throw new Error('يرجى تسجيل الدخول');
    const password = prompt('لتأكيد هويتك، أدخل كلمة مرور حسابك:');
    if (password === null) return;
    await reauthenticateWithCredential(user, EmailAuthProvider.credential(user.email, password));
    section.style.display = 'block';
  });
}

// ==================== MONTHLY INVENTORY ====================
function renderInventory() {
  const selectedMonth = document.getElementById('inv-month').value;
  if (!selectedMonth) return;

  const cash = DB.get(K_CASH);
  // Filter out-type transactions (كاش + بنك) for this month that have a person
  const monthExpenses = cash.filter(c =>
    (c.type === 'out' || c.type === 'bank_out') &&
    c.date && c.date.startsWith(selectedMonth) &&
    c.person && c.person.trim() !== ''
  );

  const el = document.getElementById('inventory-content');

  if (monthExpenses.length === 0) {
    el.innerHTML = `<div class="empty-state"><div class="icon">📭</div><p>لا توجد مدفوعات بأسماء أشخاص لهذا الشهر</p><p style="font-size:12px;margin-top:8px">تأكد من إدخال اسم الشخص عند تسجيل المدفوعات</p></div>`;
    return;
  }

  // Group by person
  const byPerson = Object.create(null);
  monthExpenses.forEach(c => {
    const name = c.person.trim();
    if (!byPerson[name]) byPerson[name] = { total: 0, transactions: [] };
    byPerson[name].total += +c.amount;
    byPerson[name].transactions.push(c);
  });

  const grandTotal = Object.values(byPerson).reduce((s, p) => s + p.total, 0);

  // Format month display
  const [y, m] = selectedMonth.split('-');
  const monthNames = ['','يناير','فبراير','مارس','أبريل','مايو','يونيو','يوليو','أغسطس','سبتمبر','أكتوبر','نوفمبر','ديسمبر'];
  const monthLabel = `${monthNames[+m]} ${y}`;

  el.innerHTML = `
    <div style="margin-bottom:16px;padding:12px 16px;background:rgba(239,68,68,0.08);border:1px solid rgba(239,68,68,0.2);border-radius:10px;display:flex;justify-content:space-between;align-items:center">
      <span style="font-size:14px;font-weight:700;color:var(--text-muted)">📅 جرد شهر: <strong style="color:var(--text-main)">${monthLabel}</strong></span>
      <span style="font-size:15px;font-weight:900;color:var(--danger)">الإجمالي: ${formatNum(grandTotal)} ليرة</span>
    </div>

    ${Object.entries(byPerson).sort((a,b) => b[1].total - a[1].total).map(([name, data]) => `
      <div style="margin-bottom:16px;background:var(--bg-card2);border:1px solid var(--border);border-radius:10px;overflow:hidden">
        <div style="display:flex;justify-content:space-between;align-items:center;padding:12px 16px;border-bottom:1px solid var(--border);background:rgba(0,0,0,0.2)">
          <span style="font-size:14px;font-weight:700">👤 ${escapeHTML(name)}</span>
          <span style="font-size:15px;font-weight:900;color:var(--danger)">${formatNum(data.total)} ليرة</span>
        </div>
        <table style="width:100%;border-collapse:collapse;font-size:12.5px">
          <thead>
            <tr style="background:rgba(0,0,0,0.15)">
              <th style="padding:8px 14px;text-align:right;color:var(--text-muted);font-weight:600">التاريخ</th>
              <th style="padding:8px 14px;text-align:right;color:var(--text-muted);font-weight:600">الوصف</th>
              <th style="padding:8px 14px;text-align:right;color:var(--text-muted);font-weight:600">الطريقة</th>
              <th style="padding:8px 14px;text-align:right;color:var(--text-muted);font-weight:600">المبلغ</th>
            </tr>
          </thead>
          <tbody>
            ${data.transactions.map(t => `
              <tr style="border-bottom:1px solid rgba(30,45,74,0.4)">
                <td style="padding:8px 14px;color:var(--text-dim)">${formatDate(t.date)}</td>
                <td style="padding:8px 14px">${escapeHTML(t.desc)}</td>
                <td style="padding:8px 14px"><span class="badge ${t.type==='bank_out'?'badge-info':'badge-success'}">${t.type==='bank_out'?'🏦 بنك':'💵 كاش'}</span></td>
                <td style="padding:8px 14px;color:var(--danger);font-weight:700">${formatNum(t.amount)} ل</td>
              </tr>
            `).join('')}
          </tbody>
          <tfoot>
            <tr style="background:rgba(239,68,68,0.05)">
              <td colspan="3" style="padding:10px 14px;font-weight:700;color:var(--text-muted)">مجموع ${escapeHTML(name)}</td>
              <td style="padding:10px 14px;font-weight:900;color:var(--danger);font-size:14px">${formatNum(data.total)} ليرة</td>
            </tr>
          </tfoot>
        </table>
      </div>
    `).join('')}

    <div style="text-align:left;margin-top:8px">
      <button class="btn btn-secondary btn-sm" onclick="printInventory(${htmlJSArg(selectedMonth)})">🖨️ طباعة الجرد</button>
    </div>
  `;
}

function printInventory(selectedMonth) {
  const cash = DB.get(K_CASH);
  const settings = DB.getObj(K_SETTINGS, {});
  const monthExpenses = cash.filter(c =>
    c.type === 'out' && c.date && c.date.startsWith(selectedMonth) && c.person && c.person.trim()
  );

  const byPerson = Object.create(null);
  monthExpenses.forEach(c => {
    const name = c.person.trim();
    if (!byPerson[name]) byPerson[name] = { total: 0, transactions: [] };
    byPerson[name].total += +c.amount;
    byPerson[name].transactions.push(c);
  });

  const grandTotal = Object.values(byPerson).reduce((s, p) => s + p.total, 0);
  const [y, m] = selectedMonth.split('-');
  const monthNames = ['','يناير','فبراير','مارس','أبريل','مايو','يونيو','يوليو','أغسطس','سبتمبر','أكتوبر','نوفمبر','ديسمبر'];
  const monthLabel = `${monthNames[+m]} ${y}`;

  const w = window.open('', '_blank');
  w.document.write(`
    <html dir="rtl"><head>
    <meta charset="UTF-8">
    <link href="https://fonts.googleapis.com/css2?family=Cairo:wght@400;600;700;900&display=swap" rel="stylesheet">
    <style>
      body { font-family:'Cairo',sans-serif; padding:30px; direction:rtl; max-width:700px; margin:auto; color:#222; }
      h1 { font-size:22px; margin:0; }
      .header { text-align:center; margin-bottom:24px; padding-bottom:16px; border-bottom:2px solid #333; }
      .person-block { margin-bottom:20px; border:1px solid #ddd; border-radius:8px; overflow:hidden; }
      .person-header { background:#f5f5f5; padding:10px 16px; display:flex; justify-content:space-between; font-weight:700; font-size:14px; }
      table { width:100%; border-collapse:collapse; font-size:13px; }
      th { background:#f9f9f9; padding:8px 14px; text-align:right; border-bottom:1px solid #ddd; color:#555; }
      td { padding:8px 14px; border-bottom:1px solid #eee; }
      .total-row td { font-weight:700; background:#fff8f8; }
      .grand-total { margin-top:16px; padding:12px 16px; background:#fee2e2; border-radius:8px; display:flex; justify-content:space-between; font-size:16px; font-weight:900; }
      .footer { text-align:center; margin-top:32px; font-size:12px; color:#888; border-top:1px solid #eee; padding-top:16px; }
      @media print { button { display:none!important; } }
    </style></head><body>
    <div class="header">
      <h1>${escapeHTML(settings.shopName || 'محل الصيانة')}</h1>
      <p style="color:#666;font-size:13px;margin-top:4px">${escapeHTML(settings.shopPhone||'')} ${settings.shopAddress ? '| '+settings.shopAddress : ''}</p>
      <h2 style="font-size:17px;margin-top:12px">📋 كشف جرد المدفوعات — ${monthLabel}</h2>
    </div>

    ${Object.entries(byPerson).sort((a,b)=>b[1].total-a[1].total).map(([name, data]) => `
      <div class="person-block">
        <div class="person-header">
          <span>👤 ${escapeHTML(name)}</span>
          <span style="color:#dc2626">${Number(data.total).toLocaleString('en')} ليرة</span>
        </div>
        <table>
          <thead><tr><th>التاريخ</th><th>الوصف</th><th>المبلغ</th></tr></thead>
          <tbody>
            ${data.transactions.map(t=>`
              <tr><td>${new Date(t.date).toLocaleDateString('en-GB')}</td><td>${escapeHTML(t.desc)}</td><td style="color:#dc2626;font-weight:600">${Number(t.amount).toLocaleString('en')} ل</td></tr>
            `).join('')}
          </tbody>
          <tfoot><tr class="total-row"><td colspan="2">مجموع ${escapeHTML(name)}</td><td style="color:#dc2626">${Number(data.total).toLocaleString('en')} ليرة</td></tr></tfoot>
        </table>
      </div>
    `).join('')}

    <div class="grand-total">
      <span>الإجمالي الكلي للشهر</span>
      <span style="color:#dc2626">${Number(grandTotal).toLocaleString('en')} ليرة</span>
    </div>

    <div class="footer">تم إنشاء الكشف بتاريخ: ${new Date().toLocaleDateString('en-GB')}</div>
    <div style="text-align:center;margin-top:20px">
      <button onclick="window.print()" style="padding:10px 30px;font-size:15px;font-family:Cairo,sans-serif;cursor:pointer">🖨️ طباعة</button>
    </div>
    </body></html>
  `);
  w.document.close();
}

// ==================== SETTINGS ====================
function applyTheme(theme) {
  document.body.setAttribute('data-theme', theme);
  localStorage.setItem('app-theme', theme);
  // update active btn
  document.querySelectorAll('.theme-btn').forEach(b => b.classList.remove('active'));
  const btn = document.getElementById('theme-' + theme);
  if (btn) btn.classList.add('active');
}

function loadTheme() {
  const saved = localStorage.getItem('app-theme') || 'dark-blue';
  applyTheme(saved);
}

const OWNER_EMAIL = 'mohammed.soft7@gmail.com';
const OWNER_UID = 'fz6uNoWQRWXqOD0LZuIAsozmqFB3';

function loadSettings() {
  document.getElementById('account-security-email').textContent = auth.currentUser?.email || '';
  setAccountMessage('account-security-message', '');
  const s = DB.getObj(K_SETTINGS, {});
  document.getElementById('set-shop-name').value    = s.shopName    || '';
  document.getElementById('set-shop-phone').value   = s.shopPhone   || '';
  document.getElementById('set-shop-address').value = s.shopAddress || '';
  document.getElementById('set-invoice-note').value = s.invoiceNote || '';
  const delPassEl = document.getElementById('set-delete-pass');
  if (delPassEl) { delPassEl.value = ''; delPassEl.placeholder = s.deleteCredential || s.deletePassword ? 'محفوظة — اتركه فارغاً للإبقاء عليها' : 'أدخل كلمة سر للحذف'; }
  // highlight active theme
  const saved = localStorage.getItem('app-theme') || 'dark-blue';
  document.querySelectorAll('.theme-btn').forEach(b => b.classList.remove('active'));
  const btn = document.getElementById('theme-' + saved);
  if (btn) btn.classList.add('active');

  // قسم المالك فقط
  const ownerSection = document.getElementById('owner-admin-section');
  if (ownerSection) {
    const isOwner = auth.currentUser?.uid === OWNER_UID;
    ownerSection.style.display = isOwner ? 'block' : 'none';
    if (isOwner) { loadActivationCodes(); loadRegisteredUsers(); }
  }
  // ضبط checkboxes الوحدات
  const mods = cache.settings?.modules || { repair: true, sales: false };
  const repairCb = document.getElementById('mod-set-repair');
  const salesCb  = document.getElementById('mod-set-sales');
  if (repairCb) repairCb.checked = !!mods.repair;
  if (salesCb)  salesCb.checked  = !!mods.sales;
}

window.saveModuleSettings = async function() {
  const uid = auth.currentUser?.uid; if (!uid) return;
  const mods = {
    repair: document.getElementById('mod-set-repair')?.checked || false,
    sales:  document.getElementById('mod-set-sales')?.checked  || false,
  };
  if (!mods.repair && !mods.sales) { toast('يجب تفعيل وحدة واحدة على الأقل', 'error'); return; }
  cache.settings.modules = mods;
  const s = { ...cache.settings, modules: mods };
  await setDoc(doc(fdb,'users',uid,'settings','main'), s);
  cache.settings = s;
  toast('✅ تم حفظ الوحدات', 'success');
};

async function saveSettings() {
  return saveAction('saveSettings', '#page-settings button', async () => {
  const settings = {
    ...cache.settings, // حافظ على modules والإعدادات الأخرى
    shopName:       document.getElementById('set-shop-name').value,
    shopPhone:      document.getElementById('set-shop-phone').value,
    shopAddress:    document.getElementById('set-shop-address').value,
    invoiceNote:    document.getElementById('set-invoice-note').value,

  };
  const newPassword = document.getElementById('set-delete-pass')?.value || settings.deletePassword;
  if (newPassword) settings.deleteCredential = await createDeleteCredential(newPassword);
  delete settings.deletePassword;
  await DB.setObj(K_SETTINGS, settings);
  document.getElementById('set-delete-pass').value = '';
  await writer.wait();
  document.getElementById('shop-name-sidebar').textContent = settings.shopName || 'محل الصيانة';
  toast('✅ تم حفظ الإعدادات');

  });
}

function backupSettings() {
  const settings = DB.getObj(K_SETTINGS);
  delete settings.deletePassword; delete settings.deleteCredential; return settings;
}
function exportBackup() {
  try {
    if (writer.pending || activeActions.size) throw new Error('انتظر اكتمال الحفظ قبل التصدير');
    [...COLLECTIONS, K_SETTINGS].forEach(assertLoaded);
    const data = { schemaVersion: 2, exportDate: new Date().toISOString(),
      collections: Object.fromEntries(COLLECTIONS.map(key => [key, DB.get(key)])), settings: backupSettings() };
    const url = URL.createObjectURL(new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' }));
    const anchor = document.createElement('a'); anchor.href = url; anchor.download = `backup_${today()}.json`;
    anchor.click(); setTimeout(() => URL.revokeObjectURL(url), 1000);
    toast('تم تصدير نسخة شاملة لجميع السجلات');
  } catch (error) { toast(error.message, 'error'); }
}

async function importBackup(event) {
  const input = event.target;
  const file = input.files?.[0]; input.value = '';
  if (!file) return;
  return saveAction('import-backup', '#page-settings button', async () => {
    if (file.size > 5 * 1024 * 1024) throw new Error('حجم النسخة يتجاوز الحد المسموح (5 ميغابايت)');
    [...COLLECTIONS, K_SETTINGS].forEach(assertLoaded);
    const restored = validateBackup(JSON.parse(await file.text()));
    if (restored.settings) {
      delete restored.settings.deletePassword; delete restored.settings.deleteCredential;
      if (cache.settings.deleteCredential) restored.settings.deleteCredential = clone(cache.settings.deleteCredential);
      else if (cache.settings.deletePassword) restored.settings.deletePassword = cache.settings.deletePassword;
    }
    const count = Object.values(restored.collections).reduce((sum, rows) => sum + rows.length, 0);
    await writer.wait();
    const operations = [];
    const userId = auth.currentUser.uid;
    const epoch = sessionEpoch;
    for (const [key, rows] of Object.entries(restored.collections)) {
      const ids = new Set(rows.map(row => row.id));
      const old = new Map(DB.get(key).map(row => [row.id, JSON.stringify(row)]));
      for (const id of old.keys()) if (!ids.has(id)) operations.push({ kind: 'delete', ref: doc(fdb, 'users', userId, key, id) });
      for (const row of rows) if (old.get(row.id) !== JSON.stringify(row)) operations.push({ kind: 'set', ref: doc(fdb, 'users', userId, key, row.id), value: row });
    }
    if (restored.settings) operations.push({ kind: 'set', ref: doc(fdb, 'users', userId, K_SETTINGS, 'main'), value: restored.settings });
    if (operations.length > 450) throw new Error('الاستيراد يتطلب أكثر من 450 كتابة. لم تُغيّر أي بيانات؛ يلزم استيراد إداري للنسخ الكبيرة');
    const reviewedState = JSON.stringify({ collections: Object.fromEntries(Object.keys(restored.collections).map(key => [key, cache[key]])), settings: cache.settings });
    document.getElementById('confirm-message').textContent = `استيراد ${count} سجل من ${Object.keys(restored.collections).length} أقسام؟ سيتم استبدال الأقسام الموجودة في الملف. صدّر نسخة حالية أولاً. الأقسام الغائبة من النسخ القديمة ستبقى محفوظة.`;
    document.getElementById('confirm-ok').onclick = () => saveAction('restore-backup', '#modal-confirm button', async () => {
      if (epoch !== sessionEpoch || auth.currentUser?.uid !== userId) throw new Error('تغيّرت الجلسة؛ أعد اختيار الملف');
      if (writer.pending || activeActions.size > 1) throw new Error('انتظر اكتمال العمليات الأخرى ثم أعد اختيار الملف');
      const currentState = JSON.stringify({ collections: Object.fromEntries(Object.keys(restored.collections).map(key => [key, cache[key]])), settings: cache.settings });
      if (currentState !== reviewedState) throw new Error('تغيّرت البيانات بعد معاينة النسخة؛ أعد اختيار الملف قبل الاستبدال');
      updateSyncStatus('saving');
      await Promise.all(operations.map(operation => operation.kind === 'delete' ? deleteDoc(operation.ref) : setDoc(operation.ref, operation.value)));
      for (const [key, rows] of Object.entries(restored.collections)) cache[key] = clone(rows);
      if (restored.settings) cache.settings = clone(restored.settings);
      applySettings(); closeModal('modal-confirm'); renderDashboard(); updateNotifBadge();
      toast('تم استيراد النسخة بالكامل');
    });
    openModal('modal-confirm');
  });
}

// ==================== SETTINGS DELETE MANAGEMENT ====================
function loadDelRepairs() {
  const sel = document.getElementById('del-repair-select');
  if (sel.options.length > 1) return;
  DB.get(K_REPAIRS).sort((a,b)=>b.date.localeCompare(a.date)).forEach(r => {
    const o = document.createElement('option');
    o.value = r.id;
    o.textContent = `${r.customer} — ${r.deviceType||''} ${r.model||''} (${formatDate(r.date)})`;
    sel.appendChild(o);
  });
}
function loadDelCash() {
  const sel = document.getElementById('del-cash-select');
  if (sel.options.length > 1) return;
  DB.get(K_CASH).sort((a,b)=>(b.createdAt||b.date).localeCompare(a.createdAt||a.date)).slice(0,100).forEach(c => {
    const o = document.createElement('option');
    o.value = c.id;
    o.textContent = `${c.desc} — ${formatNum(c.amount)} ل (${formatDate(c.date)})`;
    sel.appendChild(o);
  });
}
function loadDelDebts() {
  const sel = document.getElementById('del-debt-select');
  if (sel.options.length > 1) return;
  DB.get(K_DEBTS).forEach(d => {
    const o = document.createElement('option');
    o.value = d.id;
    o.textContent = `${d.name} — ${formatNum(d.amount)} ل (${formatDate(d.date)})`;
    sel.appendChild(o);
  });
}
function loadDelCustomers() {
  const sel = document.getElementById('del-cust-select');
  if (sel.options.length > 1) return;
  DB.get(K_CUSTOMERS).sort((a,b)=>(a.name||'').localeCompare(b.name||'')).forEach(c => {
    const o = document.createElement('option');
    o.value = c.id;
    o.textContent = `${c.name} ${c.phone?'('+c.phone+')':''}`;
    sel.appendChild(o);
  });
}
function loadDelEmployees() {
  const sel = document.getElementById('del-emp-select');
  if (sel.options.length > 1) return;
  DB.get(K_EMPLOYEES).forEach(e => {
    const o = document.createElement('option');
    o.value = e.id;
    o.textContent = e.name;
    sel.appendChild(o);
  });
}
function loadDelExpenses() {
  const sel = document.getElementById('del-exp-select');
  if (sel.options.length > 1) return;
  DB.get(K_EXPENSES).sort((a,b)=>b.date.localeCompare(a.date)).forEach(e => {
    const o = document.createElement('option');
    o.value = e.id;
    o.textContent = `${e.desc} — ${formatNum(e.amount)} ل (${formatDate(e.date)})`;
    sel.appendChild(o);
  });
}

function delSelectedRepair() {
  const id = document.getElementById('del-repair-select').value;
  if (!id) { toast('اختر سجلاً أولاً', 'error'); return; }
  askDeleteRepair(id);
}

function delSelectedCash() {
  const id = document.getElementById('del-cash-select').value;
  if (!id) { toast('اختر سجلاً أولاً', 'error'); return; }
  askDeleteCash(id);
}

function delSelectedDebt() {
  const id = document.getElementById('del-debt-select').value;
  if (!id) { toast('اختر سجلاً أولاً', 'error'); return; }
  askDeleteDebt(id);
}

function delSelectedCustomer() {
  const id = document.getElementById('del-cust-select').value;
  if (!id) { toast('اختر سجلاً أولاً', 'error'); return; }
  askDeleteCustomer(id);
}

function delSelectedEmployee() {
  const id = document.getElementById('del-emp-select').value;
  if (!id) { toast('اختر موظفاً أولاً', 'error'); return; }
  deleteEmployee(id);
  document.getElementById('del-emp-select').innerHTML='<option value="">— اختر الموظف —</option>';
}
function delSelectedExpense() {
  const id = document.getElementById('del-exp-select').value;
  if (!id) { toast('اختر سجلاً أولاً', 'error'); return; }
  askDeleteExpense(id);
}

function clearAllData() {
  requireDeletePass('مسح جميع السجلات نهائياً مع الإبقاء على الموظفين والإعدادات؟ صدّر نسخة احتياطية أولاً.', () => saveAction('clear-data', '#page-settings button', async () => {
    const keys = [K_REPAIRS, K_CASH, K_DEBTS, K_CUSTOMERS, K_EXPENSES, K_EXP_CATS, K_FAULTS, K_WITHDRAWALS, K_DEBT_PAYMENTS];
    keys.forEach(assertLoaded);
    const count = keys.reduce((sum, key) => sum + cache[key].length, 0);
    if (count > 450) throw new Error('البيانات كبيرة؛ يلزم مسح إداري آمن. لم يتم حذف أي سجل');
    keys.forEach(key => DB.set(key, []));
    await writer.wait(); renderDashboard(); updateNotifBadge(); toast('تم مسح السجلات مع الإبقاء على الموظفين والإعدادات');
  }));
}

// ── Init ─────────────────────────────────────────────────
function initApp() {
  document.getElementById('current-date').textContent = new Date().toLocaleDateString('ar',{weekday:'long',year:'numeric',month:'long',day:'numeric'});
  initRepairForm();
  document.addEventListener('click', event => {
    const history = event.target.closest('[data-customer-history]');
    const copy = event.target.closest('[data-copy-customer]');
    if (history) showCustomerHistory(history.dataset.customerHistory);
    if (copy) copyCustomerReport(copy.dataset.copyCustomer);
  });
  document.querySelectorAll('label').forEach(label => {
    if (label.querySelector('input, select, textarea')) return;
    const fields = label.closest('.form-group')?.querySelectorAll('input[id], select[id], textarea[id]');
    if (fields?.length === 1 && !label.htmlFor) label.htmlFor = fields[0].id;
  });
  document.querySelectorAll('input[type="number"]').forEach(field => { if (field.id === 'codes-count') { field.step = '1'; field.inputMode = 'numeric'; return; } field.min = '0'; field.step = '0.01'; field.inputMode = 'decimal'; });
  document.querySelectorAll('button').forEach(button => { if (!button.hasAttribute('type')) button.type = 'button'; if (button.title && !button.hasAttribute('aria-label')) button.setAttribute('aria-label', button.title); });
  document.addEventListener('keydown', event => {
    if (event.key === 'Escape' && !activeActions.size) {
      const modal = [...document.querySelectorAll('.modal-overlay.open')].pop();
      if (modal) closeModal(modal.id);
    }
  });
  window.addEventListener('error', event => { if (event.error) updateSyncStatus('error', event.error); });
  window.addEventListener('unhandledrejection', event => { updateSyncStatus('error', event.reason); });
  window.addEventListener('offline', () => updateSyncStatus('offline'));
  window.addEventListener('online', () => updateSyncStatus(writer.pending ? 'saving' : loadedCollections.size === COLLECTIONS.length + 1 ? 'saved' : 'loading'));
  window.addEventListener('beforeunload', event => { if (writer.pending || activeActions.size) { event.preventDefault(); event.returnValue = ''; } });
  document.querySelectorAll('.modal-overlay').forEach(o=>o.addEventListener('click',e=>{if(e.target===o && !activeActions.size)o.classList.remove('open');}));
}

window.addEventListener('DOMContentLoaded', ()=>{
  const _cm = currentMonth();
  ['dash-month-filter','cash-month-filter','emp-month-filter','report-month-custom','exp-month-filter'].forEach(id => {
    const el = document.getElementById(id);
    if (el) { el.value = _cm; el.defaultValue = _cm; }
  });

  loadTheme();
  initApp();


  signOut(auth).catch(() => {}).finally(() => {
    onAuthStateChanged(auth, user => {
      if (signupInProgress) return;
      if (user) { void openAuthorizedSession(user); return; }
      authGateSequence++; stopListeners(); resetAuthLoginSurface(); clearLoginPasswords(); clearAccountPasswordFields(); adminAccounts.clear();
      Object.keys(cache).forEach(key => { cache[key] = Array.isArray(cache[key]) ? [] : {}; });
      document.getElementById('profit-section-content').style.display = 'none';
      document.getElementById('l-btn').disabled = false;
      document.getElementById('l-btn').textContent = 'دخول';
      showLogin();
    });
  });
});

async function openAuthorizedSession(user) {
  const sequence = ++authGateSequence;
  showLoading(); stopListeners();
  Object.keys(cache).forEach(key => { cache[key] = Array.isArray(cache[key]) ? [] : {}; });
  try {
    const registryRef = doc(fdb, 'users_registry', user.uid);
    let registry = await getDoc(registryRef);
    if (!registry.exists() && user.uid === OWNER_UID) {
      await setDoc(registryRef, { uid: user.uid, email: user.email || '', shopName: '', createdAt: new Date().toISOString(), active: true });
      registry = await getDoc(registryRef);
    }
    if (!registry.exists() || (registry.data().active !== true && user.uid !== OWNER_UID)) throw new Error('الحساب غير مفعّل؛ أكمل التسجيل بكود صالح أو تواصل مع المالك');
    if (sequence !== authGateSequence || auth.currentUser?.uid !== user.uid) return;
    const month = currentMonth();
    ['dash-month-filter','cash-month-filter','emp-month-filter','report-month-custom','exp-month-filter']
      .forEach(id => { const field = document.getElementById(id); if (field) field.value = month; });
    showApp(); startListeners();
  } catch (failure) {
    if (sequence !== authGateSequence) return;
    document.getElementById('l-err').textContent = failure.message || 'تعذر التحقق من تفعيل الحساب';
    await signOut(auth).catch(() => {}); showLogin();
  }
}

// ==================== MOBILE NAV ====================
window.setMobNav = function(id) {
  document.querySelectorAll('.mob-nav-item').forEach(b => b.classList.remove('active'));
  const btn = document.getElementById('mobnav-' + id);
  if (btn) btn.classList.add('active');
  else {
    // الصفحات في "المزيد" — نشط زر المزيد
    const moreBtn = document.getElementById('mobnav-more');
    if (moreBtn) moreBtn.classList.add('active');
  }
};

window.toggleMobMore = function() {
  const menu = document.getElementById('mob-more-menu');
  const overlay = document.getElementById('mob-more-overlay');
  if (!menu || !overlay) return;
  if (menu.classList.contains('open')) {
    closeMobMore();
  } else {
    overlay.style.display = 'block';
    menu.classList.add('open');
  }
};

window.closeMobMore = function() {
  const menu = document.getElementById('mob-more-menu');
  const overlay = document.getElementById('mob-more-overlay');
  if (!menu || !overlay) return;
  menu.classList.remove('open');
  overlay.style.display = 'none';
};

// ربط showPage بـ mobile nav تلقائياً (wrapper بعد تعريفها)
const _showPageMobWrapper = showPage;
window._origShowPage = _showPageMobWrapper;

// نعيد تعريف showPage كـ wrapper يتعامل مع الـ mobile nav
window.showPage = function(id) {
  _showPageMobWrapper(id);
  setMobNav(id);
};
// حدّث المرجع في الـ HTML onclick handlers
// لا حاجة — showPage على window كافي

// ==================== EXPOSE TO HTML ====================
Object.assign(window,{
  setMobNav, toggleMobMore, closeMobMore,
  showPage: window.showPage, openModal, closeModal,
  saveRepair, toggleDebtField,
  editRepair, saveEditRepair, erToggleDebt, deleteRepair, showInvoice, printInvoice,
  openAddCash, saveCash, deleteCash, renderCash, toggleCashOutDetail, toggleBankOutDetail, openTransfer, updateTransferLabels, saveTransfer,
  onCashCurrencyChange, onEditCashTypeChange, onEditCashCurrencyChange,
  editCash, saveEditCash,
  openDollarModal, updateDollarPreview, saveDollarOp,
  openAddDebt, editDebt, saveDebt, openPayDebt, confirmPayDebt, fillFullDebt, deleteDebt, toggleDebtGroup,
  openEditPayment, saveEditPayment,
  renderCustomers, renderDevices, renderReports, renderDashboard, renderEmployees, onReportMonthChange, showDrill, openProfitSection,
  showCustomerHistory, switchCustTab, copyCustomerReport, showDebtHistory, toggleWorkList, renderInventory, printInventory,
  openAddCustomer, editCustomer, saveCustomer, deleteCustomer,
  loadSettings, saveSettings, exportBackup, importBackup, clearAllData, applyTheme,
  toggleDeletePassVis, confirmDeleteWithPass,
  askDeleteRepair, askDeleteCash, askDeleteDebt, askDeleteCustomer, askDeleteEmployee, askDeleteExpense,
  generateActivationCodes, loadActivationCodes, copyCode, deleteCode,
  loadRegisteredUsers, adminResetPassword, adminDeleteUser,
  loadDelRepairs, loadDelCash, loadDelDebts, loadDelCustomers, loadDelEmployees, loadDelExpenses,
  delSelectedRepair, delSelectedCash, delSelectedDebt, delSelectedCustomer, delSelectedEmployee, delSelectedExpense,
  doLogin, doLogout, shiftMonth, setAuthMode, submitAuthForm, openPasswordRecovery, requestLoginPasswordReset,
  togglePasswordVisibility, openAccountPassword, saveAccountPassword, sendOwnPasswordReset,
  selectFaultChip, saveFault, deleteFault, openFaultsMgr,
  openAddEmployee, editEmployee, saveEmployee, deleteEmployee, toggleEmployeeCard,
  openEmpWithdraw, saveEmpWithdraw, deleteEmpWithdrawal,
  editEmpWithdrawal, saveEditWithdrawal,
  openExpenseCatsMgr, saveExpenseCat, deleteExpenseCat,
  openAddExpense, editExpense, saveExpense, deleteExpense, renderExpenses,
  openAddSerial, saveSerial, deleteSerial,
  toggleSignup,
});
