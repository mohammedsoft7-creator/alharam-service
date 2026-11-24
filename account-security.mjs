// Firebase owns account passwords. Never write them to application storage or logs.
export function accountErrorMessage(error, currentPassword = false) {
  const messages = {
    'auth/invalid-credential': currentPassword ? 'كلمة المرور الحالية غير صحيحة.' : 'البريد أو كلمة المرور غير صحيحة.',
    'auth/wrong-password': 'كلمة المرور الحالية غير صحيحة.',
    'auth/user-not-found': 'البريد أو كلمة المرور غير صحيحة.',
    'auth/invalid-email': 'أدخل بريدًا إلكترونيًا صحيحًا.',
    'auth/weak-password': 'كلمة المرور الجديدة لا تحقق متطلبات الأمان.',
    'auth/password-does-not-meet-requirements': 'كلمة المرور الجديدة لا تحقق متطلبات الأمان.',
    'auth/requires-recent-login': 'انتهت صلاحية التحقق؛ سجّل الدخول مجددًا ثم أعد المحاولة.',
    'auth/user-token-expired': 'انتهت الجلسة؛ سجّل الدخول مجددًا.',
    'auth/user-disabled': 'الحساب موقوف؛ تواصل مع المالك.',
    'auth/network-request-failed': 'تعذر الاتصال. تحقق من الإنترنت ثم أعد المحاولة.',
    'auth/too-many-requests': 'محاولات كثيرة؛ انتظر قليلًا قبل إعادة المحاولة.',
    'auth/operation-not-allowed': 'تسجيل الدخول بكلمة مرور غير مفعّل لهذا الحساب.',
    'auth/unauthorized-continue-uri': 'تعذر تجهيز رابط العودة إلى التطبيق؛ تواصل مع المالك.',
    'auth/invalid-continue-uri': 'تعذر تجهيز رابط الاستعادة؛ تواصل مع المالك.'
  };
  if (error?.code === 'account/validation') return error.message;
  return messages[error?.code] || 'تعذر إكمال العملية. أعد المحاولة أو استخدم استعادة كلمة المرور.';
}
export function accountValidation(message) {
  return Object.assign(new Error(message), { code: 'account/validation' });
}
export function normalizedEmail(value) {
  const email = String(value || '').trim().toLowerCase();
  if (email.length > 254 || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) throw accountValidation('أدخل بريدًا إلكترونيًا صحيحًا.');
  return email;
}
export async function checkNewPassword(auth, password, confirmation, validatePassword) {
  if (password !== confirmation) throw accountValidation('كلمتا المرور غير متطابقتين.');
  if (typeof password !== 'string' || password.length < 6 || password.length > 4096) {
    throw accountValidation('كلمة المرور يجب أن تكون من 6 إلى 4096 حرفًا. يفضّل استخدام 12 حرفًا أو أكثر.');
  }
  const status = await validatePassword(auth, password);
  if (!status.isValid) {
    const requirements = [];
    if (status.meetsMinPasswordLength === false) requirements.push('طول أكبر');
    if (status.meetsMaxPasswordLength === false) requirements.push('طول أقصر');
    if (status.containsLowercaseLetter === false) requirements.push('حرف صغير');
    if (status.containsUppercaseLetter === false) requirements.push('حرف كبير');
    if (status.containsNumericCharacter === false) requirements.push('رقم');
    if (status.containsNonAlphanumericCharacter === false) requirements.push('رمز');
    throw accountValidation('كلمة المرور لا تحقق سياسة الحساب' + (requirements.length ? ': ' + requirements.join('، ') : '') + '.');
  }
}
export async function changeOwnPassword({ auth, currentPassword, newPassword, confirmation,
  validatePassword, credential, reauthenticate, updatePassword }) {
  const user = auth.currentUser;
  if (!user?.uid || !user.email) throw accountValidation('سجّل الدخول قبل تغيير كلمة المرور.');
  if (!currentPassword) throw accountValidation('أدخل كلمة المرور الحالية، أو استخدم رابط الاستعادة.');
  if (newPassword === currentPassword) throw accountValidation('اختر كلمة مرور مختلفة عن الحالية.');
  await checkNewPassword(auth, newPassword, confirmation, validatePassword);
  const ensureSameUser = () => {
    if (auth.currentUser?.uid !== user.uid) throw accountValidation('تغيّر الحساب أثناء العملية؛ أعد تسجيل الدخول.');
  };
  ensureSameUser();
  await reauthenticate(user, credential(user.email, currentPassword));
  ensureSameUser();
  await updatePassword(user, newPassword);
  return { uid: user.uid, email: user.email };
}
export function createPasswordResetSender(send, now = () => Date.now()) {
  const requests = new Map();
  return function requestReset(value) {
    const email = normalizedEmail(value);
    const previous = requests.get(email);
    if (previous && (previous.pending || now() - previous.started < 60000)) return previous.promise;
    const entry = { started: now(), pending: true };
    entry.promise = Promise.resolve().then(() => send(email)).catch(error => {
      // The same visible outcome for unknown and known mailboxes prevents account enumeration.
      if (error.code !== 'auth/user-not-found') { requests.delete(email); throw error; }
    }).finally(() => { entry.pending = false; });
    requests.set(email, entry);
    return entry.promise;
  };
}
