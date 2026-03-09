
import { initializeApp } from "https://www.gstatic.com/firebasejs/10.12.0/firebase-app.js";
import { getAuth, createUserWithEmailAndPassword, signInWithEmailAndPassword, signOut, onAuthStateChanged, updateProfile } from "https://www.gstatic.com/firebasejs/10.12.0/firebase-auth.js";
import { getFirestore, doc, getDoc, setDoc, updateDoc, collection, query, where, onSnapshot, addDoc, serverTimestamp, getDocs, deleteDoc, deleteField } from "https://www.gstatic.com/firebasejs/10.12.0/firebase-firestore.js";

const firebaseConfig = {
  apiKey: "AIzaSyBGZvsC1-gEWpagGl7SM6F_bVbmPHjRALE",
  authDomain: "tennisgame-e747b.firebaseapp.com",
  projectId: "tennisgame-e747b",
  storageBucket: "tennisgame-e747b.firebasestorage.app",
  messagingSenderId: "532261430790",
  appId: "1:532261430790:web:37b9fce3daf1b743b18587"
};

const app = initializeApp(firebaseConfig);
const auth = getAuth(app);
const db = getFirestore(app);

// ── TOURNAMENTS DATA ──
const TOURNAMENTS = [
  {id:'aus',name:'Australian Open',city:'Melbourne',country:'Australia',surface:'Hard',emoji:'🦘',theme:'melbourne',cat:'Grand Slam',months:'Gen',tz:'Australia/Melbourne',drawUrl:'https://www.flashscore.com/tennis/australia/australian-open/',dates2026:{start:'2026-01-19',end:'2026-02-01'}},
  {id:'rg',name:'Roland Garros',city:'Parigi',country:'Francia',surface:'Clay',emoji:'🗼',theme:'paris',cat:'Grand Slam',months:'Mag-Giu',tz:'Europe/Paris',drawUrl:'https://www.flashscore.com/tennis/france/roland-garros/',dates2026:{start:'2026-05-24',end:'2026-06-07'}},
  {id:'wim',name:'Wimbledon',city:'Londra',country:'UK',surface:'Grass',emoji:'🏰',theme:'wimbledon',cat:'Grand Slam',months:'Lug',tz:'Europe/London',drawUrl:'https://www.flashscore.com/tennis/great-britain/wimbledon/',dates2026:{start:'2026-06-29',end:'2026-07-12'}},
  {id:'uso',name:'US Open',city:'New York',country:'USA',surface:'Hard',emoji:'🗽',theme:'newyork',cat:'Grand Slam',months:'Ago-Set',tz:'America/New_York',drawUrl:'https://www.flashscore.com/tennis/usa/us-open/',dates2026:{start:'2026-08-31',end:'2026-09-13'}},
  {id:'iw',name:'Indian Wells Masters',city:'Indian Wells',country:'USA',surface:'Hard',emoji:'🌵',theme:'indian-wells',cat:'Masters 1000',months:'Mar',tz:'America/Los_Angeles',drawUrl:'https://www.flashscore.com/tennis/usa/bnp-paribas-open/',dates2026:{start:'2026-03-04',end:'2026-03-15'}},
  {id:'mia',name:'Miami Open',city:'Miami',country:'USA',surface:'Hard',emoji:'🌴',theme:'miami',cat:'Masters 1000',months:'Mar-Apr',tz:'America/New_York',drawUrl:'https://www.flashscore.com/tennis/usa/miami-open/',dates2026:{start:'2026-03-18',end:'2026-03-29'}},
  {id:'mc',name:'Monte-Carlo Masters',city:'Montecarlo',country:'Monaco',surface:'Clay',emoji:'🎰',theme:'montecarlo',cat:'Masters 1000',months:'Apr',tz:'Europe/Monaco',drawUrl:'https://www.flashscore.com/tennis/monaco/monte-carlo-masters/',dates2026:{start:'2026-04-06',end:'2026-04-12'}},
  {id:'mad',name:'Mutua Madrid Open',city:'Madrid',country:'Spagna',surface:'Clay',emoji:'🐂',theme:'madrid',cat:'Masters 1000',months:'Apr-Mag',tz:'Europe/Madrid',drawUrl:'https://www.flashscore.com/tennis/spain/madrid-open/',dates2026:{start:'2026-04-22',end:'2026-05-03'}},
  {id:'rom',name:'Internazionali di Roma',city:'Roma',country:'Italia',surface:'Clay',emoji:'🏛️',theme:'rome',cat:'Masters 1000',months:'Mag',tz:'Europe/Rome',drawUrl:'https://www.flashscore.com/tennis/italy/internazionali-bnl-ditalia/',dates2026:{start:'2026-05-06',end:'2026-05-17'}},
  {id:'can',name:'Canadian Open',city:'Toronto/Montreal',country:'Canada',surface:'Hard',emoji:'🍁',theme:'canada',cat:'Masters 1000',months:'Ago',tz:'America/Toronto',drawUrl:'https://www.flashscore.com/tennis/canada/canadian-open/',dates2026:{start:'2026-08-03',end:'2026-08-09'}},
  {id:'cin',name:'Cincinnati Masters',city:'Cincinnati',country:'USA',surface:'Hard',emoji:'🏙️',theme:'cincinnati',cat:'Masters 1000',months:'Ago',tz:'America/New_York',drawUrl:'https://www.flashscore.com/tennis/usa/western-southern-open/',dates2026:{start:'2026-08-10',end:'2026-08-16'}},
  {id:'sha',name:'Shanghai Masters',city:'Shanghai',country:'Cina',surface:'Hard',emoji:'🏯',theme:'shanghai',cat:'Masters 1000',months:'Ott',tz:'Asia/Shanghai',drawUrl:'https://www.flashscore.com/tennis/china/shanghai-masters/',dates2026:{start:'2026-10-05',end:'2026-10-18'}},
  {id:'pma',name:'Paris Masters',city:'Parigi',country:'Francia',surface:'Hard',emoji:'🌃',theme:'paris-masters',cat:'Masters 1000',months:'Nov',tz:'Europe/Paris',drawUrl:'https://www.flashscore.com/tennis/france/open-parisien-de-tennis/',dates2026:{start:'2026-11-02',end:'2026-11-08'}},
];

// ── TOURNAMENT TIMEZONE HELPERS ──
// Returns "today" and "tomorrow" in the tournament's local timezone
function getTournamentDates() {
  const tid = gameConfig?.tournament;
  const t = tid ? TOURNAMENTS.find(x => x.id === tid) : null;
  const tz = t?.tz || Intl.DateTimeFormat().resolvedOptions().timeZone;
  const fmt = date => new Intl.DateTimeFormat('en-CA', { timeZone: tz, year:'numeric', month:'2-digit', day:'2-digit' }).format(date);
  const now = new Date();
  const todayStr = fmt(now);
  const tomorrowStr = fmt(new Date(now.getTime() + 86400000));
  return { today: todayStr, tomorrow: tomorrowStr, tz, tournamentName: t?.city || '' };
}

// Returns the tournament-local date string for a UTC commence_time
function getMatchTournamentDate(commenceTimeUtc) {
  const tid = gameConfig?.tournament;
  const t = tid ? TOURNAMENTS.find(x => x.id === tid) : null;
  const tz = t?.tz || Intl.DateTimeFormat().resolvedOptions().timeZone;
  return new Intl.DateTimeFormat('en-CA', { timeZone: tz, year:'numeric', month:'2-digit', day:'2-digit' }).format(new Date(commenceTimeUtc));
}

// ── AVATAR COLORS ──
const AVATAR_COLORS = [
  '#e05555','#e07a30','#d4a017','#5a9e40','#2a8a6e',
  '#3a7ec0','#6050c0','#a040b0','#c03070','#607080'
];
function getAvatarColor(uid) {
  if (!uid) return AVATAR_COLORS[0];
  let hash = 0;
  for (let i = 0; i < uid.length; i++) hash = uid.charCodeAt(i) + ((hash << 5) - hash);
  return AVATAR_COLORS[Math.abs(hash) % AVATAR_COLORS.length];
}

// ── STATE ──
let currentUser = null;
let userProfile = null;
let gameConfig = null;   // from Firestore /config/game
let allUsers = [];
let allBets = [];
let allAnteposts = [];
let allLeagues = [];
let myLeagues = [];
let resolvingBetId = null;
let unsubBets = null, unsubAnte = null, unsubConfig = null, unsubUsers = null;

// ── CONTEXT: 'general' or leagueId ──
let activeContext = 'general'; // 'general' | leagueId

function getActiveContextConfig() {
  if (activeContext === 'general') return gameConfig || {};
  const league = allLeagues.find(l => l.id === activeContext);
  // League inherits from gameConfig, overrides rules + anteQuestions
  return {
    ...(gameConfig || {}),
    rules: { ...(gameConfig?.rules||{}), ...(league?.rules||{}) },
    anteQuestions: league?.anteQuestions ?? gameConfig?.anteQuestions ?? [],
    _league: league
  };
}

function getContextBets() {
  if (activeContext === 'general') return allBets.filter(b => !b.leagueId);
  return allBets.filter(b => b.leagueId === activeContext);
}

function getContextAnteposts() {
  if (activeContext === 'general') return allAnteposts.filter(a => !a.leagueId);
  return allAnteposts.filter(a => a.leagueId === activeContext);
}

function getContextUsers() {
  if (activeContext === 'general') return allUsers;
  const league = allLeagues.find(l => l.id === activeContext);
  if (!league) return allUsers;
  return allUsers.filter(u => league.members?.includes(u.id || u.uid));
}

function switchContext(ctx) {
  activeContext = ctx;
  try { localStorage.setItem('tennis_context', ctx); } catch(e) {}
  renderContextSwitcher();
  renderAll();
}
window.switchContext = switchContext;

// ── EXPOSE TO WINDOW ──
window.switchAuth = switchAuth;
window.doLogin = doLogin;
window.doRegister = doRegister;
window.doLogout = doLogout;
window.goPage = goPage;
window.switchBetTab = switchBetTab;
window.updateCalc = updateCalc;
window.placeBet = placeBet;
window.openResolve = openResolve;
window.confirmResolve = confirmResolve;
window.closeModal = closeModal;
window.updateAnteCalc = updateAnteCalc;
window.addAntepost = addAntepost;
window.resolveAntepost = resolveAntepost;
window.loadOdds = loadOdds;
window.saveApiKey = saveApiKey;
window.prefillBet = prefillBet;
window.cancelBet = cancelBet;
window.placeBetDirect = placeBetDirect;
window.placeBetManual = placeBetManual;
window.openBetConfirm = openBetConfirm;
window.switchOddsTab = switchOddsTab;
window.selectMatchCard = selectMatchCard;
window.renderHistory = renderHistory;
window.exportCSV = exportCSV;
window.exportJSON = exportJSON;
window.saveTournament = saveTournament;
window.saveRules = saveRules;
// selectTournament removed — replaced by setTournamentActive
window.resetSeason = resetSeason;
window.createLeague = createLeague;
window.joinLeague = joinLeague;
window.openLeague = openLeague;
window.leaveLeague = leaveLeague;

// ── AUTH ──
function switchAuth(tab, btn) {
  document.querySelectorAll('.auth-tab').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  document.getElementById('login-form').style.display = tab === 'login' ? 'block' : 'none';
  document.getElementById('reg-form').style.display = tab === 'register' ? 'block' : 'none';
  document.getElementById('auth-err').style.display = 'none';
}

async function doLogin() {
  const email = document.getElementById('l-email').value.trim();
  const pass = document.getElementById('l-pass').value;
  const errEl = document.getElementById('auth-err');
  errEl.style.display = 'none';
  try {
    await signInWithEmailAndPassword(auth, email, pass);
  } catch(e) {
    errEl.textContent = translateAuthError(e.code);
    errEl.style.display = 'block';
  }
}

async function doRegister() {
  const name = document.getElementById('r-name').value.trim();
  const email = document.getElementById('r-email').value.trim();
  const pass = document.getElementById('r-pass').value;
  const errEl = document.getElementById('auth-err');
  errEl.style.display = 'none';
  if (!name) { errEl.textContent = 'Inserisci il tuo nome!'; errEl.style.display = 'block'; return; }
  try {
    const cred = await createUserWithEmailAndPassword(auth, email, pass);
    await updateProfile(cred.user, { displayName: name });
    await setDoc(doc(db, 'users', cred.user.uid), {
      name, email, uid: cred.user.uid,
      points: 0, isAdmin: false,
      createdAt: serverTimestamp()
    });
  } catch(e) {
    errEl.textContent = translateAuthError(e.code);
    errEl.style.display = 'block';
  }
}

async function doLogout() {
  if (!confirm('Vuoi uscire?')) return;
  if (unsubBets) unsubBets();
  if (unsubAnte) unsubAnte();
  if (unsubConfig) unsubConfig();
  if (unsubUsers) unsubUsers();
  await signOut(auth);
}

function translateAuthError(code) {
  const map = {
    'auth/invalid-email': 'Email non valida',
    'auth/user-not-found': 'Utente non trovato',
    'auth/wrong-password': 'Password errata',
    'auth/email-already-in-use': 'Email già in uso',
    'auth/weak-password': 'Password troppo corta (min. 6 caratteri)',
    'auth/invalid-credential': 'Email o password errati',
  };
  return map[code] || 'Errore: ' + code;
}

// ── AUTH STATE ──
onAuthStateChanged(auth, async (user) => {
  hideSplash();
  if (user) {
    currentUser = user;
    const uSnap = await getDoc(doc(db, 'users', user.uid));
    if (uSnap.exists()) {
      userProfile = uSnap.data();
    } else {
      userProfile = { name: user.displayName || user.email, uid: user.uid, isAdmin: false };
    }
    enterApp();
  } else {
    currentUser = null;
    userProfile = null;
    showAuth();
  }
});

function hideSplash() {
  setTimeout(() => {
    const s = document.getElementById('splash');
    s.classList.add('out');
    setTimeout(() => s.style.display = 'none', 400);
  }, 200);
}

function showAuth() {
  document.getElementById('auth-screen').classList.remove('hidden');
  document.getElementById('app').style.display = 'none';
}

async function enterApp() {
  document.getElementById('auth-screen').classList.add('hidden');
  document.getElementById('app').style.display = 'block';

  const name = userProfile?.name || currentUser.displayName || currentUser.email.split('@')[0];
  document.getElementById('h-name').textContent = name;
  document.getElementById('h-av').textContent = name.charAt(0).toUpperCase();
  const avColor = getAvatarColor(currentUser.uid);
  document.getElementById('h-av').style.background = avColor;

  const isAdmin = userProfile?.isAdmin || false;
  document.getElementById('admin-btn').style.display = isAdmin ? '' : 'none';
  const mobAdminBtn = document.getElementById('mob-admin-btn');
  if (mobAdminBtn) mobAdminBtn.style.display = isAdmin ? '' : 'none';

  // Subscribe to realtime data
  subscribeConfig();
  subscribeUsers();
  subscribeBets();
  subscribeAnteposts();
  subscribeLeagues();

  setupInstallBanner();
}

// ── FIRESTORE SUBSCRIPTIONS ──
// ── DEBOUNCED RENDER — evita 5 renderAll() simultanei all'avvio ──
let _renderTimer = null;
let _subsReady = { config: false, users: false, bets: false, antes: false, leagues: false };
function scheduleRender() {
  clearTimeout(_renderTimer);
  _renderTimer = setTimeout(() => {
    // Only render once all initial data is loaded
    if (Object.values(_subsReady).every(Boolean)) {
      renderAll();
    }
  }, 80); // batch all rapid-fire snapshots into one render
}

function subscribeConfig() {
  unsubConfig = onSnapshot(doc(db, 'config', 'game'), snap => {
    gameConfig = snap.exists() ? snap.data() : { tournament: null, tournamentDates: {}, rules: { basePts: 10, minQuota: 1.20 }, apiKey: 'c5f52e5c8246c14fa721e9879d1ace44' };
    applyTheme();
    _subsReady.config = true;
    scheduleRender();
  });
}

function subscribeUsers() {
  unsubUsers = onSnapshot(collection(db, 'users'), snap => {
    allUsers = snap.docs.map(d => ({ id: d.id, ...d.data() }));
    _subsReady.users = true;
    scheduleRender();
  });
}

function subscribeBets() {
  unsubBets = onSnapshot(collection(db, 'bets'), snap => {
    allBets = snap.docs.map(d => ({ id: d.id, ...d.data() }));
    _subsReady.bets = true;
    scheduleRender();
  });
}

function subscribeAnteposts() {
  unsubAnte = onSnapshot(collection(db, 'anteposts'), snap => {
    allAnteposts = snap.docs.map(d => ({ id: d.id, ...d.data() }));
    _subsReady.antes = true;
    scheduleRender();
  });
}

function subscribeLeagues() {
  onSnapshot(collection(db, 'leagues'), snap => {
    allLeagues = snap.docs.map(d => ({ id: d.id, ...d.data() }));
    myLeagues = allLeagues.filter(l => l.members && l.members.includes(currentUser.uid));
    try {
      const saved = localStorage.getItem('tennis_context');
      if (saved && (saved === 'general' || allLeagues.find(l => l.id === saved))) activeContext = saved;
    } catch(e) {}
    _subsReady.leagues = true;
    scheduleRender();
  });
}

// ── THEME ──
function applyTheme() {
  document.body.className = '';
  if (!gameConfig?.tournament) { document.body.classList.add('theme-default'); return; }
  const t = TOURNAMENTS.find(x => x.id === gameConfig.tournament);
  document.body.classList.add(t ? 'theme-' + t.theme : 'theme-default');
}

// ── CONTEXT SWITCHER ──
function renderContextSwitcher() {
  const bar = document.getElementById('context-bar');
  const tabs = document.getElementById('context-tabs');
  if (!bar || !tabs) return;
  // Show bar if user is in at least one league
  if (!myLeagues.length) { bar.style.display = 'none'; return; }
  bar.style.display = 'block';

  const contexts = [
    { id: 'general', label: '🌍 Generale', sub: 'Tutti i giocatori' },
    ...myLeagues.map(l => ({ id: l.id, label: `🏅 ${l.name}`, sub: `${l.members?.length||0} membri` }))
  ];

  tabs.innerHTML = contexts.map(c => `
    <button onclick="switchContext('${c.id}')" style="
      padding:5px 14px;border-radius:20px;border:1px solid ${c.id===activeContext?'var(--accent)':'var(--border)'};
      background:${c.id===activeContext?'var(--accent)':'transparent'};
      color:${c.id===activeContext?'#0a0f08':'var(--text)'};
      font-size:.78rem;font-weight:${c.id===activeContext?'700':'400'};
      cursor:pointer;font-family:inherit;transition:all .2s;white-space:nowrap">
      ${c.label}
    </button>`).join('');
}

// ── RENDER ALL ──
function renderAll() {
  if (!currentUser) return;
  renderLeaderboard();
  renderTournamentBanner();
  renderTournamentGrid();
  renderMyStats();
  updateGlobalStats();
  populateDropdowns();
  renderAntepostList();
  renderHistory();
  renderPendingBets();
  updateBetUI();
  updateAnteCalc();
  // Fill admin fields
  if (gameConfig) {
    // Auto-load odds only when bet page is active and not yet loaded
    if (gameConfig.apiKey && document.getElementById("page-bet")?.classList.contains("active") && !oddsData.length) loadOdds();
    const r = gameConfig.rules || {};
    const el = document.getElementById('s-base'); if(el) el.value = r.basePts || 10;
    const el2 = document.getElementById('s-minq'); if(el2) el2.value = r.minQuota || 1.20;
    const ak = document.getElementById('api-key'); if(ak && gameConfig.apiKey) ak.value = gameConfig.apiKey;
    const dl = document.getElementById('t-deadline'); if(dl && gameConfig.betDeadline) dl.value = gameConfig.betDeadline;
    // Show active tournament banner in admin
    const activeBanner = document.getElementById('t-active-banner');
    const activeName = document.getElementById('t-active-name');
    const activeDates = document.getElementById('t-active-dates');
    if (activeBanner && gameConfig.tournament) {
      const at = TOURNAMENTS.find(x => x.id === gameConfig.tournament);
      if (at) {
        activeName.textContent = `${at.emoji} ${at.name}`;
        const d = gameConfig.tournamentDates || {};
        activeDates.textContent = `${d.start||'?'} → ${d.end||'?'}`;
        activeBanner.style.display = 'block';
      }
    } else if (activeBanner) { activeBanner.style.display = 'none'; }
    const aak = document.getElementById('admin-api-key'); if(aak && gameConfig.apiKey) { aak.value = gameConfig.apiKey; document.getElementById('api-key-status') && (document.getElementById('api-key-status').innerHTML = '<span style="color:var(--accent)">✅ Key attiva</span>'); }
    document.getElementById('bpl').textContent = r.basePts || 10;
    if (gameConfig.tournamentDates) {
      const ts = document.getElementById('t-start'); if(ts) ts.value = gameConfig.tournamentDates.start || '';
      const te = document.getElementById('t-end'); if(te) te.value = gameConfig.tournamentDates.end || '';
    }
  }
  renderAdminUsers();
  renderAnteConfigList();
  renderAnteQuestions();
  renderAntepostQuestions();
  updateCloseTournamentCard();
  loadFixedAnteSettings();
  renderContextSwitcher();
  // Start nightly auto-resolve for admin (idempotent — only runs once)
  if (userProfile?.isAdmin && !window._nightlyScheduled) {
    window._nightlyScheduled = true;
    scheduleNightlyAutoResolve();
    // Show last run info in admin panel
    if (gameConfig?.autoResolveLastRun) {
      const statusEl = document.getElementById('auto-resolve-status');
      if (statusEl) statusEl.innerHTML = `Ultimo controllo: <strong>${new Date(gameConfig.autoResolveLastRun).toLocaleString('it')}</strong> · ${gameConfig.autoResolveResolved || 0} risolte`;
    }
  }
}

function canCloseTournament() {
  const end = gameConfig?.tournamentDates?.end;
  if (!end) return true;
  const closeDate = new Date(end);
  closeDate.setDate(closeDate.getDate() + 2);
  return new Date() >= closeDate;
}

function updateCloseTournamentCard() {
  // Legacy no-op — replaced by renderTournamentGrid
}

// ── NAVIGATION ──
function goPage(p, btn) {
  document.querySelectorAll('.page').forEach(el => el.classList.remove('active'));
  document.querySelectorAll('nav button, .mob-nav-btn').forEach(el => el.classList.remove('active'));
  document.getElementById('page-' + p).classList.add('active');
  // Sync active state on both navbars by page id
  const pages = ['home','bet','live','antepost','leagues','ranking','history','admin'];
  document.querySelectorAll('nav button').forEach((b, i) => { if (pages[i] === p) b.classList.add('active'); });
  document.querySelectorAll('.mob-nav-btn').forEach((b, i) => { if (pages[i] === p) b.classList.add('active'); });
  renderAll();
  if (p === 'bet' && gameConfig?.apiKey && !oddsData.length) loadOdds();
  if (p === 'antepost') renderAntepostQuestions();
  if (p === 'ranking') renderRankingPage();
  if (p === 'live') initLivePage();
}

function switchBetTab(t, btn) {
  document.querySelectorAll('#page-bet .tab').forEach(el => el.classList.remove('active'));
  if (btn) btn.classList.add('active');
  const ids = ['bt-odds','bt-manual','bt-resolve'];
  ids.forEach(id => { const el = document.getElementById(id); if(el) el.style.display = 'none'; });
  const map = { odds:'bt-odds', manual:'bt-manual', resolve:'bt-resolve', place:'bt-manual' };
  const show = document.getElementById(map[t] || 'bt-odds');
  if (show) show.style.display = 'block';
}

// ── TOURNAMENT BANNER ──
function renderTournamentBanner() {
  const el = document.getElementById('t-banner');
  if (!gameConfig?.tournament) { el.style.display = 'none'; return; }
  const t = TOURNAMENTS.find(x => x.id === gameConfig.tournament);
  if (!t) { el.style.display = 'none'; return; }
  el.style.display = 'flex';
  document.getElementById('tb-em').textContent = t.emoji;
  document.getElementById('tb-name').textContent = t.name;
  const d = gameConfig.tournamentDates || {};
  document.getElementById('tb-meta').textContent = `${t.city}, ${t.country}${d.start ? ' · ' + d.start : ''}${d.end ? ' → ' + d.end : ''}`;
  document.getElementById('tb-surf').textContent = '🎾 ' + t.surface;
  // Draw link
  const drawLink = document.getElementById('tb-draw-link');
  if (drawLink && t.drawUrl) {
    drawLink.href = t.drawUrl;
    drawLink.style.display = 'inline-flex';
  }
}

// ── TOURNAMENT MANAGE GRID (admin) — stato per ogni torneo ──
function renderTournamentGrid() {
  const grid = document.getElementById('t-manage-grid');
  if (!grid) return;
  const states = gameConfig?.tournamentStates || {};
  const activeTid = gameConfig?.tournament;
  const history = gameConfig?.tournamentHistory || [];

  const bycat = {};
  TOURNAMENTS.forEach(t => { if (!bycat[t.cat]) bycat[t.cat] = []; bycat[t.cat].push(t); });

  let html = '';
  Object.entries(bycat).forEach(([cat, list]) => {
    html += `<div style="font-family:'Bebas Neue',sans-serif;font-size:.78rem;letter-spacing:1.5px;color:var(--dim);margin:10px 0 6px;padding-left:2px">${cat}</div>`;
    list.forEach(t => {
      const isActive = activeTid === t.id;
      const state = isActive ? 'active' : (states[t.id] || 'future');
      const hasSnap = history.some(h => h.tournamentId === t.id);
      const d = t.dates2026 ? `${t.dates2026.start} → ${t.dates2026.end}` : '';
      const badgeClass = isActive ? 'b-active' : state === 'finished' ? 'b-finished' : 'b-future';
      const badgeLabel = isActive ? '🟢 ATTIVO' : state === 'finished' ? '🏁 FINITO' : '⚪ Futuro';
      const rowClass = isActive ? 'tm-active' : state === 'finished' ? 'tm-finished' : '';

      html += `<div class="tm-row ${rowClass}">
        <div class="tm-em">${t.emoji}</div>
        <div class="tm-info">
          <div class="tm-name">${t.name}</div>
          <div class="tm-meta">${t.city} · ${t.surface} · ${d}</div>
        </div>
        <span class="tm-badge ${badgeClass}">${badgeLabel}</span>
        <div class="tm-actions">
          ${!isActive ? `<button class="btn btn-p btn-sm" style="font-size:.72rem;padding:5px 10px" onclick="setTournamentActive('${t.id}')">▶ Attiva</button>` : ''}
          ${isActive ? `<button class="btn btn-d btn-sm" style="font-size:.72rem;padding:5px 10px;background:rgba(224,85,85,.12);border-color:rgba(224,85,85,.3);color:var(--red)" onclick="closeTournamentNew('${t.id}')">🏁 Chiudi</button>` : ''}
          ${state === 'finished' && !isActive ? `<button class="btn btn-g btn-sm" style="font-size:.72rem;padding:5px 10px" onclick="reopenTournament('${t.id}')">↩ Riapri</button>` : ''}
          ${hasSnap ? `<button class="btn btn-g btn-sm" style="font-size:.72rem;padding:5px 10px" title="Snapshot salvato" onclick="toast('📸 Snapshot ${t.name} già salvato nello storico')">📸</button>` : ''}
        </div>
      </div>`;
    });
  });
  grid.innerHTML = html;
}

window.setTournamentActive = async function(id) {
  const t = TOURNAMENTS.find(x => x.id === id);
  if (!t) return;
  const prevTid = gameConfig?.tournament;
  const prevT = TOURNAMENTS.find(x => x.id === prevTid);

  // Confirm switch
  if (prevTid && prevTid !== id) {
    if (!confirm(`Impostare ${t.emoji} ${t.name} come torneo attivo?\n\nIl torneo precedente (${prevT?.name || prevTid}) verrà messo in stato "Futuro" (puoi chiuderlo manualmente quando vuoi).`)) return;
    // Save snapshot of previous tournament if enough time has passed
    if (canCloseTournament()) {
      await saveTournamentSnapshot(prevTid);
    }
  }

  const updates = { tournament: id };
  if (t.dates2026) {
    updates.tournamentDates = { start: t.dates2026.start, end: t.dates2026.end };
    const ts = document.getElementById('t-start'); if(ts) ts.value = t.dates2026.start;
    const te = document.getElementById('t-end'); if(te) te.value = t.dates2026.end;
  }
  await updateConfig(updates);
  applyTheme();
  toast(`${t.emoji} ${t.name} ora attivo!`);
  renderTournamentGrid();
  renderTournamentBanner();
};

window.closeTournamentNew = async function(id) {
  const t = TOURNAMENTS.find(x => x.id === id);
  if (!t) return;
  if (!confirm(`Dichiarare ${t.emoji} ${t.name} come FINITO?\n\nVerrà salvato lo snapshot finale della classifica. L'operazione è reversibile.`)) return;

  // Save final snapshot
  await saveTournamentSnapshot(id, true);

  // Update state
  const states = { ...(gameConfig?.tournamentStates || {}), [id]: 'finished' };
  await updateConfig({ tournamentStates: states });
  toast(`🏁 ${t.name} chiuso! Snapshot salvato.`);
  renderTournamentGrid();
};

window.reopenTournament = async function(id) {
  const t = TOURNAMENTS.find(x => x.id === id);
  if (!t) return;
  if (!confirm(`Riaprire ${t.emoji} ${t.name}?\n\nVerrà rimesso in stato "Futuro". Per renderlo attivo usa il bottone Attiva.`)) return;
  const states = { ...(gameConfig?.tournamentStates || {}), [id]: 'future' };
  await updateConfig({ tournamentStates: states });
  toast(`↩ ${t.name} riaperto`);
  renderTournamentGrid();
};

let previewTournamentId = null;
window.cancelTournamentPreview = function() {
  previewTournamentId = null;
  const bar = document.getElementById('t-preview-bar');
  if (bar) bar.style.display = 'none';
};
window.confirmSetTournament = async function() { /* legacy — not used in new UI */ };

async function saveTournamentSnapshot(tournamentId, isClosure = false) {
  const t = TOURNAMENTS.find(x => x.id === tournamentId);
  if (!t) return;
  const sorted = [...allUsers].sort((a,b) => totalPts(b.id||b.uid) - totalPts(a.id||a.uid));
  const classifica = sorted.map((u,i) => ({
    pos: i+1, uid: u.id||u.uid, name: u.name,
    pts: totalPts(u.id||u.uid)
  }));
  const tid = tournamentId;
  const betsSnapshot = allBets.filter(b => !b.tournament || b.tournament === tid).map(b => ({...b}));
  const antesSnapshot = allAnteposts.filter(a => !a.tournament || a.tournament === tid).map(a => ({...a}));
  const history = [...(gameConfig?.tournamentHistory || [])];
  const existing = history.findIndex(h => h.tournamentId === tournamentId);
  const prevSnap = existing >= 0 ? history[existing] : {};
  const snap = {
    ...prevSnap,
    tournamentId, tournamentName: t.name, tournamentEmoji: t.emoji,
    dates: gameConfig?.tournamentDates || {},
    classifica, bets: betsSnapshot, anteposts: antesSnapshot,
    savedAt: new Date().toISOString(),
    ranking: sorted.map(u => u.id||u.uid)
  };
  if (isClosure) snap.closedAt = new Date().toISOString();
  if (existing >= 0) history.splice(existing, 1);
  history.push(snap);
  await updateConfig({ tournamentHistory: history });
  if (!isClosure) toast(`📸 Snapshot ${t.name} salvato!`);
}

async function saveTournament() {
  const start = document.getElementById('t-start').value;
  const end = document.getElementById('t-end').value;
  const deadline = document.getElementById('t-deadline').value || '23:00';
  await updateConfig({ tournamentDates: { start, end }, betDeadline: deadline });
  toast('Torneo salvato! 📅');
}

async function saveRules() {
  const basePts = parseInt(document.getElementById('s-base').value) || 10;
  const minQuota = parseFloat(document.getElementById('s-minq').value) || 1.20;
  await updateConfig({ rules: { basePts, minQuota } });
  toast('Regole salvate! ⚖️');
}

async function updateConfig(data) {
  await setDoc(doc(db, 'config', 'game'), data, { merge: true });
}

// ── POINTS ──
function totalPts(uid) {
  const tid = gameConfig?.tournament || '';
  const ctxBets = getContextBets();
  const ctxAntes = getContextAnteposts();
  const b = ctxBets.filter(x => x.userId === uid && x.status !== 'pending' && (!x.tournament || x.tournament === tid)).reduce((s, x) => s + (x.pointsResult || 0), 0);
  const a = ctxAntes.filter(x => x.userId === uid && x.status !== 'pending' && (!x.tournament || x.tournament === tid)).reduce((s, x) => s + (x.pointsResult || 0), 0);
  return b + a;
}

// ── LEADERBOARD ──
function renderLeaderboard() {
  const el = document.getElementById('leaderboard');
  if (!el) return;
  populateLbSelector(); // must run first — restores saved selection
  const sel = document.getElementById('lb-selector');
  const view = sel?.value || '__current__'; // read AFTER populate restores value
  const ctx = getActiveContextConfig();
  const isLeague = activeContext !== 'general';
  const ctxLabel = isLeague ? `🏅 ${ctx._league?.name || ''}` : '🌍 Generale';
  const ctxUsers = getContextUsers();
  const ctxBets = getContextBets();
  const ctxAntes = getContextAnteposts();

  // Helper: pts for a user in a specific tournament scope
  function ptsForScope(uid, tid) {
    const b = ctxBets.filter(x => x.userId === uid && x.status !== 'pending' && (!x.tournament || x.tournament === tid)).reduce((s, x) => s + (x.pointsResult || 0), 0);
    const a = ctxAntes.filter(x => x.userId === uid && x.status !== 'pending' && (!x.tournament || x.tournament === tid)).reduce((s, x) => s + (x.pointsResult || 0), 0);
    return b + a;
  }

  // Helper: season pts (all tournaments)
  function seasonPts(uid) {
    const b = ctxBets.filter(x => x.userId === uid && x.status !== 'pending').reduce((s, x) => s + (x.pointsResult || 0), 0);
    const a = ctxAntes.filter(x => x.userId === uid && x.status !== 'pending').reduce((s, x) => s + (x.pointsResult || 0), 0);
    return b + a;
  }

  if (view === '__season__') {
    // ── SEASON VIEW ──
    document.getElementById('home-sub').textContent = `${ctxLabel} · 📅 Stagione totale`;
    const history = gameConfig?.tournamentHistory || [];
    const allTids = [...new Set([
      ...(gameConfig?.tournament ? [gameConfig.tournament] : []),
      ...history.map(h => h.tournamentId)
    ])];

    const sorted = [...ctxUsers].sort((a, b) => seasonPts(b.id||b.uid) - seasonPts(a.id||a.uid));
    el.innerHTML = sorted.map((u, i) => {
      const uid = u.id || u.uid;
      const pts = seasonPts(uid);
      const userBets = ctxBets.filter(b => b.userId === uid && b.status !== 'pending');
      const wins = userBets.filter(b => b.status === 'win').length;
      const total = userBets.length;
      const pct = total > 0 ? Math.round(wins / total * 100) : 0;
      const rc = i===0?'r1':i===1?'r2':i===2?'r3':'';
      const rn = i===0?'🥇':i===1?'🥈':i===2?'🥉':i+1;
      const mine = uid === currentUser.uid ? 'mine' : '';
      const avC = getAvatarColor(uid);
      // Per-tournament breakdown
      const breakdown = allTids.map(tid => {
        const t = TOURNAMENTS.find(x => x.id === tid);
        const tp = ptsForScope(uid, tid);
        if (!tp) return '';
        return `<span style="font-size:.68rem;color:var(--dim);margin-right:8px">${t?.emoji||'🎾'} ${tp>0?'+':''}${tp}pt</span>`;
      }).join('');
      return `<div class="lb-row ${rc} ${mine}">
        <div class="rn" style="display:flex;align-items:center;gap:8px">
          <div style="width:32px;height:32px;border-radius:50%;background:${avC};display:flex;align-items:center;justify-content:center;font-weight:700;font-size:.9rem;color:#fff;flex-shrink:0">${u.name.charAt(0).toUpperCase()}</div>
          ${typeof rn==='string'?`<span style="font-size:1.2rem">${rn}</span>`:`<span style="font-size:.85rem;color:var(--dim)">${rn}</span>`}
        </div>
        <div>
          <div class="lb-name">${u.name}${mine?' <span style="font-size:.68rem;color:var(--accent)">← tu</span>':''}</div>
          <div class="lb-sub">${total} scommesse · ${wins} vinte · ${pct}% win rate</div>
          ${breakdown ? `<div style="margin-top:3px">${breakdown}</div>` : ''}
          <div class="bar"><div class="bar-f" style="width:${pct}%"></div></div>
        </div>
        <div>
          <div class="lb-pts ${pts<0?'neg':''}">${pts>0?'+':''}${pts}</div>
          <div class="lb-sub tr">pt totali</div>
        </div>
      </div>`;
    }).join('');

  } else {
    // ── SINGLE TOURNAMENT VIEW (current or closed) ──
    let tid, tName, tEmoji, tDates;
    if (view === '__current__') {
      tid = gameConfig?.tournament || '';
      const t = tid ? TOURNAMENTS.find(x => x.id === tid) : null;
      tName = t?.name || 'Torneo attivo';
      tEmoji = t?.emoji || '🎾';
      tDates = gameConfig?.tournamentDates || {};
    } else {
      tid = view;
      const snap = (gameConfig?.tournamentHistory||[]).find(h => h.tournamentId === tid);
      const t = TOURNAMENTS.find(x => x.id === tid);
      tName = snap?.tournamentName || t?.name || tid;
      tEmoji = snap?.tournamentEmoji || t?.emoji || '🎾';
      tDates = snap?.dates || {};
    }

    document.getElementById('home-sub').textContent = `${ctxLabel} · ${tEmoji} ${tName}`;

    // For closed tournaments use snapshot if available, else live data
    const snap = (gameConfig?.tournamentHistory||[]).find(h => h.tournamentId === tid);
    let usersToShow = ctxUsers;
    let betsToUse = ctxBets;
    let antesToUse = ctxAntes;

    if (view !== '__current__' && snap) {
      // Use snapshot data for closed tournament
      const snapUsers = allUsers.filter(u => snap.ranking?.includes(u.id||u.uid));
      usersToShow = snapUsers.length ? snapUsers : ctxUsers;
    }

    const sorted = [...usersToShow].sort((a, b) => ptsForScope(b.id||b.uid, tid) - ptsForScope(a.id||a.uid, tid));

    if (!sorted.length) {
      el.innerHTML = `<div class="empty"><div class="empty-i">👥</div><h3>Nessun giocatore</h3></div>`;
      return;
    }

    el.innerHTML = sorted.map((u, i) => {
      const uid = u.id || u.uid;
      const pts = ptsForScope(uid, tid);
      const userBets = betsToUse.filter(b => b.userId === uid && (!b.tournament || b.tournament === tid));
      const wins = userBets.filter(b => b.status === 'win').length;
      const losses = userBets.filter(b => b.status === 'loss').length;
      const total = userBets.length;
      const pct = total > 0 ? Math.round(wins / total * 100) : 0;
      const rc = i===0?'r1':i===1?'r2':i===2?'r3':'';
      const rn = i===0?'🥇':i===1?'🥈':i===2?'🥉':i+1;
      const mine = uid === currentUser.uid ? 'mine' : '';
      const avC = getAvatarColor(uid);
      return `<div class="lb-row ${rc} ${mine}">
        <div class="rn" style="display:flex;align-items:center;gap:8px">
          <div style="width:32px;height:32px;border-radius:50%;background:${avC};display:flex;align-items:center;justify-content:center;font-weight:700;font-size:.9rem;color:#fff;flex-shrink:0">${u.name.charAt(0).toUpperCase()}</div>
          ${typeof rn==='string'?`<span style="font-size:1.2rem">${rn}</span>`:`<span style="font-size:.85rem;color:var(--dim)">${rn}</span>`}
        </div>
        <div>
          <div class="lb-name">${u.name}${mine?' <span style="font-size:.68rem;color:var(--accent)">← tu</span>':''}</div>
          <div class="lb-sub">${total} scommesse · ${wins} vinte · ${losses} perse · ${pct}% win</div>
          <div class="bar"><div class="bar-f" style="width:${pct}%"></div></div>
        </div>
        <div>
          <div class="lb-pts ${pts<0?'neg':''}">${pts>0?'+':''}${pts}</div>
          <div class="lb-sub tr">pt</div>
        </div>
      </div>`;
    }).join('');
  }
}

function renderMyStats() {
  const panel = document.getElementById('my-panel');
  if (!currentUser) { panel.style.display = 'none'; return; }
  const uid = currentUser.uid;
  const tid = gameConfig?.tournament || '';
  const ctxBets = getContextBets().filter(b => !b.tournament || b.tournament === tid);
  const bets = ctxBets.filter(b => b.userId === uid);
  const wins = bets.filter(b => b.status === 'win').length;
  const losses = bets.filter(b => b.status === 'loss').length;
  const pts = totalPts(uid);
  panel.style.display = 'block';
  document.getElementById('my-sg').innerHTML = `
    <div class="sb"><div class="sv ${pts < 0 ? 'neg' : ''}">${pts > 0 ? '+' : ''}${pts}</div><div class="sl">Punti</div></div>
    <div class="sb"><div class="sv gold">${bets.length}</div><div class="sl">Scommesse</div></div>
    <div class="sb"><div class="sv">${wins}</div><div class="sl">Vinte</div></div>
    <div class="sb"><div class="sv ${losses > wins ? 'neg' : ''}">${losses}</div><div class="sl">Perse</div></div>
  `;
}

function updateGlobalStats() {
  const ctxUsers = getContextUsers();
  const ctxBets = getContextBets();
  const tid = gameConfig?.tournament || '';
  // Filter bets/anteposts by current tournament
  const tournBets = ctxBets.filter(b => !b.tournament || b.tournament === tid);
  document.getElementById('st-p').textContent = ctxUsers.length;
  document.getElementById('st-b').textContent = tournBets.length;
  const today = new Date().toISOString().split('T')[0];
  const t = gameConfig?.tournament ? TOURNAMENTS.find(x => x.id === gameConfig.tournament) : null;
  if (gameConfig?.tournamentDates?.start && t) {
    const start = gameConfig.tournamentDates.start;
    const end = gameConfig.tournamentDates.end;
    // Use min(today, end) so day count stops at tournament end
    const effectiveDate = (end && today > end) ? end : today;
    const dayNum = Math.floor((new Date(effectiveDate) - new Date(start)) / 86400000) + 1;
    const capped = Math.max(1, dayNum);
    document.getElementById('st-d').textContent = capped + '° gg';
  } else {
    document.getElementById('st-d').textContent = '—';
  }
  document.getElementById('st-pend').textContent = tournBets.filter(b => b.status === 'pending').length;
}

// ── BET UI ──
function updateBetUI() {
  const isAdmin = userProfile?.isAdmin;
  const { today, tomorrow } = getTournamentDates();

  // Live odds tab - who strip
  const whoName = document.getElementById('bet-who-name');
  const whoAdmin = document.getElementById('bet-who-admin');
  const betDate = document.getElementById('bet-date');
  if (whoName) whoName.textContent = userProfile?.name || '';
  if (whoAdmin) whoAdmin.style.display = isLeagueAdmin() ? 'block' : 'none';
  // Init bet-date to today if empty
  if (betDate && !betDate.value) betDate.value = today;

  // Update day banner label
  const dateVal = betDate?.value || today;
  const dayLabel = document.getElementById('bet-day-label');
  const btnToday = document.getElementById('btn-day-today');
  const btnTomorrow = document.getElementById('btn-day-tomorrow');
  if (dayLabel) {
    const d = new Date(dateVal + 'T12:00:00');
    const dayName = d.toLocaleDateString('it', {weekday:'long'}).toUpperCase();
    const dateFormatted = d.toLocaleDateString('it', {day:'2-digit', month:'long'});
    const isToday = dateVal === today;
    const isTomorrow = dateVal === tomorrow;
    const label = isToday ? `OGGI · ${dayName} ${dateFormatted}` : isTomorrow ? `DOMANI · ${dayName} ${dateFormatted}` : `${dayName} ${dateFormatted}`;
    dayLabel.textContent = label;
    dayLabel.style.color = isToday ? 'var(--accent)' : 'var(--gold)';
    const tzHint = document.getElementById('bet-tz-hint');
    if (tzHint) {
      const { tz, tournamentName } = getTournamentDates();
      tzHint.textContent = `ora locale: ${tournamentName} (${tz.replace('_',' ')})`;
    }
  }
  if (btnToday) {
    const sel = dateVal === today;
    btnToday.style.background = sel ? 'var(--accent)' : 'transparent';
    btnToday.style.color = sel ? '#0a0f08' : 'var(--text)';
    btnToday.style.borderColor = sel ? 'var(--accent)' : 'var(--border)';
    btnToday.style.fontWeight = sel ? '700' : '400';
  }
  if (btnTomorrow) {
    const sel = dateVal === tomorrow;
    btnTomorrow.style.background = sel ? 'var(--gold)' : 'transparent';
    btnTomorrow.style.color = sel ? '#0a0f08' : 'var(--text)';
    btnTomorrow.style.borderColor = sel ? 'var(--gold)' : 'var(--border)';
    btnTomorrow.style.fontWeight = sel ? '700' : '400';
  }

  // Check if user already bet for selected date
  const uid = isAdmin ? (document.getElementById('bet-psel')?.value || currentUser.uid) : currentUser.uid;
  const ctxBets = getContextBets();
  const existingBet = ctxBets.find(b => b.userId === uid && b.date === dateVal);
  const statusEl = document.getElementById('today-bet-status');
  if (statusEl) {
    if (existingBet) {
      const chosen = existingBet.choice === 'p1' ? existingBet.p1 : existingBet.p2;
      statusEl.innerHTML = `<span style="color:var(--gold)">⚡ Già scommesso su <strong>${chosen}</strong> (${existingBet.quotaChosen}x)</span>`;
    } else {
      const isFuture = dateVal > today;
      statusEl.innerHTML = `<span style="color:var(--accent)">${isFuture ? '📅 Prenotazione per domani disponibile!' : '✅ Puoi scommettere!'}</span>`;
    }
  }

  // ── Schedule info: first match + deadline ──
  updateScheduleInfo(dateVal, today, tomorrow);

  // Manual tab
  const adminRow = document.getElementById('bet-admin-row');
  const userRow = document.getElementById('bet-user-row');
  const whoEl = document.getElementById('bet-who2');
  if (adminRow) adminRow.style.display = isAdmin ? 'grid' : 'none';
  if (userRow) userRow.style.display = !isAdmin ? 'block' : 'none';
  if (whoEl) { whoEl.textContent = isAdmin ? '' : `Scommetti come: ${userProfile?.name || ''}`; whoEl.style.color = 'var(--accent)'; }
  const d2 = document.getElementById('bet-date2'); if (d2 && !d2.value) d2.value = today;
  const d3 = document.getElementById('bet-date3'); if (d3 && !d3.value) d3.value = today;
}

function updateScheduleInfo(dateVal, today, tomorrow) {
  const infoEl = document.getElementById('bet-schedule-info');
  const firstMatchEl = document.getElementById('bet-first-match');
  const deadlineEl = document.getElementById('bet-deadline-info');
  const pillEl = document.getElementById('bet-countdown-pill');
  if (!infoEl) return;

  // Find earliest match for selected day from oddsData
  const { tz } = getTournamentDates();
  const matchesForDay = oddsData.filter(m => getMatchTournamentDate(m.commence_time) === dateVal);
  matchesForDay.sort((a, b) => new Date(a.commence_time) - new Date(b.commence_time));
  const firstMatch = matchesForDay[0];

  // Deadline for today
  const deadline = gameConfig?.betDeadline || '23:00';
  const [dh, dm] = deadline.split(':').map(Number);
  const isToday = dateVal === today;
  const isTomorrow = dateVal === tomorrow;

  infoEl.style.display = 'flex';

  if (firstMatch) {
    // Show in tournament local time + user local time
    const tLocal = new Intl.DateTimeFormat('it', { timeZone: tz, hour:'2-digit', minute:'2-digit', timeZoneName:'short' }).format(new Date(firstMatch.commence_time));
    const uLocal = new Intl.DateTimeFormat('it', { hour:'2-digit', minute:'2-digit', timeZoneName:'short' }).format(new Date(firstMatch.commence_time));
    const p1 = firstMatch.home_team?.split(' ').pop();
    const p2 = firstMatch.away_team?.split(' ').pop();
    const sameTime = tLocal === uLocal;
    firstMatchEl.innerHTML = sameTime
      ? `${p1} vs ${p2} · <strong>${tLocal}</strong>`
      : `${p1} vs ${p2} · <strong>${tLocal}</strong> <span style="color:var(--dim)">(${uLocal} ora tua)</span>`;
  } else {
    firstMatchEl.textContent = 'Nessuna partita trovata';
  }

  // Deadline display
  const deadlineDate = new Date();
  if (isTomorrow) deadlineDate.setDate(deadlineDate.getDate() + 1);
  deadlineDate.setHours(dh, dm, 0, 0);
  const deadlineTz = new Intl.DateTimeFormat('it', { hour:'2-digit', minute:'2-digit', timeZoneName:'short' }).format(deadlineDate);
  const now = new Date();
  const msLeft = deadlineDate - now;

  if (isToday && msLeft > 0) {
    const hLeft = Math.floor(msLeft / 3600000);
    const mLeft = Math.floor((msLeft % 3600000) / 60000);
    const timeStr = hLeft > 0 ? `${hLeft}h ${mLeft}m` : `${mLeft}m`;
    deadlineEl.textContent = `${deadline} · ${deadlineTz}`;
    pillEl.textContent = `⏳ ${timeStr} rimasti`;
    pillEl.style.background = msLeft < 3600000 ? 'rgba(255,80,80,.2)' : 'rgba(126,200,80,.15)';
    pillEl.style.color = msLeft < 3600000 ? 'var(--red)' : 'var(--accent)';
  } else if (isToday && msLeft <= 0) {
    deadlineEl.textContent = `${deadline} — CHIUSE`;
    pillEl.textContent = '🔒 Deadline passata';
    pillEl.style.background = 'rgba(255,80,80,.15)';
    pillEl.style.color = 'var(--red)';
  } else {
    // Tomorrow
    deadlineEl.textContent = `${deadline} domani · ${deadlineTz}`;
    pillEl.textContent = '📅 Prenotazione aperta';
    pillEl.style.background = 'rgba(255,180,0,.15)';
    pillEl.style.color = 'var(--gold)';
  }
}

// ── DROPDOWNS ──
function populateDropdowns() {
  const ids = ['bet-psel', 'bet-psel2', 'ante-psel', 'hf'];
  ids.forEach(id => {
    const el = document.getElementById(id);
    if (!el) return;
    const cur = el.value;
    el.innerHTML = (id === 'hf' ? '<option value="">Tutti</option>' : '') +
      allUsers.map(u => `<option value="${u.id || u.uid}">${u.name}</option>`).join('');
    if (cur) el.value = cur;
    else if (id === 'bet-psel' || id === 'bet-psel2') {
      // default to current user
      const opt = el.querySelector(`option[value="${currentUser?.uid}"]`);
      if (opt) el.value = currentUser.uid;
    }
  });
  const af = document.getElementById('ante-admin-row');
  if (af) af.style.display = userProfile?.isAdmin ? 'block' : 'none';
}

// ── PLACE BET ──
function updateCalc() {
  const q1 = parseFloat(document.getElementById('bq1').value);
  const q2 = parseFloat(document.getElementById('bq2').value);
  const ch = document.getElementById('bch').value;
  const p1 = document.getElementById('bp1').value;
  const p2 = document.getElementById('bp2').value;
  const base = gameConfig?.rules?.basePts || 10;
  const minQ = gameConfig?.rules?.minQuota || 1.20;
  const prev = document.getElementById('calc-prev');
  const warn = document.getElementById('minq-w');
  document.getElementById('minq-v').textContent = minQ;
  if (!ch || (ch === 'p1' && isNaN(q1)) || (ch === 'p2' && isNaN(q2))) { prev.style.display = 'none'; warn.style.display = 'none'; return; }
  const q = ch === 'p1' ? q1 : q2;
  const name = ch === 'p1' ? p1 : p2;
  if (q < minQ) { prev.style.display = 'none'; warn.style.display = 'flex'; return; }
  warn.style.display = 'none';
  const win = Math.round(base * q * 10) / 10;
  document.getElementById('cn').textContent = name || '?';
  document.getElementById('cw').textContent = `+${win} pt`;
  document.getElementById('cl').textContent = `-${win} pt`;
  prev.style.display = 'block';
}

// ── DEADLINE CHECK ──
function isBettingOpen(date) {
  const deadline = gameConfig?.betDeadline || '23:00';
  const [dh, dm] = deadline.split(':').map(Number);
  const today = new Date().toISOString().split('T')[0];
  const tomorrow = new Date(Date.now() + 86400000).toISOString().split('T')[0];
  const now = new Date();
  // Only today and tomorrow allowed (max 1 day in advance)
  if (date < today) return false;
  if (date > tomorrow) return false;
  // If betting for today, check deadline
  if (date === today) {
    const deadlineTime = new Date();
    deadlineTime.setHours(dh, dm, 0, 0);
    if (now > deadlineTime) return false;
  }
  // Tomorrow betting: open anytime (until tomorrow's deadline)
  if (date === tomorrow) {
    // Can bet on tomorrow until tomorrow's deadline passes
    // i.e. always open from today's perspective
    return true;
  }
  return true;
}

window.selectBetDay = function(day) {
  const { today, tomorrow } = getTournamentDates();
  const date = day === 'today' ? today : tomorrow;
  const betDate = document.getElementById('bet-date');
  if (betDate) betDate.value = date;
  // Sync odds tab
  selectedOddsTab = day === 'tomorrow' ? 'tomorrow' : 'today';
  updateBetUI();
  renderOddsPanel();
};

// ── CANCEL BET ──
async function cancelBet(betId) {
  const bet = allBets.find(b => b.id === betId);
  if (!bet) return;
  const isAdmin = isLeagueAdmin();
  if (!isAdmin && !isBettingOpen(bet.date)) {
    return toastErr(`Scommessa bloccata! Deadline: ${gameConfig?.betDeadline || '23:00'}`);
  }
  if (!confirm('Vuoi cancellare questa scommessa?')) return;
  await deleteDoc(doc(db, 'bets', betId));
  toast('Scommessa cancellata! 🗑️');
}

// Core function used by both one-click and manual
async function saveBet(userId, date, p1, p2, q1, q2, choice) {
  const ctx = getActiveContextConfig();
  const base = ctx.rules?.basePts || 10;
  const minQ = ctx.rules?.minQuota || 1.20;
  if (!userId) return toastErr('Seleziona un giocatore!');
  if (!p1 || !p2) return toastErr('Inserisci i nomi!');
  if (isNaN(q1) || isNaN(q2)) return toastErr('Inserisci le quote!');
  if (!choice) return toastErr('Scegli su chi scommettere!');
  const q = choice === 'p1' ? q1 : q2;
  if (q < minQ) return toastErr(`Quota troppo bassa! Min: ${minQ}`);
  if (!isBettingOpen(date) && !isLeagueAdmin()) return toastErr(`Scommesse chiuse! Deadline: ${gameConfig?.betDeadline || '23:00'}`);
  const ctxBets = getContextBets();
  const dup = ctxBets.find(b => b.userId === userId && b.date === date);
  if (dup && !isLeagueAdmin()) return toastErr('Hai già scommesso per questo giorno! Cancella prima quella esistente.');
  const pts = Math.round(base * q * 10) / 10;
  const userName = allUsers.find(u => (u.id || u.uid) === userId)?.name || userProfile?.name || '';
  const betDoc = {
    userId, userName, date, p1, p2, q1, q2, choice,
    quotaChosen: q, ptsWin: pts, ptsLoss: -pts,
    status: 'pending', pointsResult: 0,
    tournament: gameConfig?.tournament || '',
    createdAt: serverTimestamp()
  };
  if (activeContext !== 'general') betDoc.leagueId = activeContext;
  await addDoc(collection(db, 'bets'), betDoc);
  return true;
}

// ONE-CLICK from odds panel
async function placeBetDirect(p1, p2, q1, q2, choice) {
  const isAdmin = isLeagueAdmin();
  const userId = isAdmin ? (document.getElementById('bet-psel')?.value || currentUser.uid) : currentUser.uid;
  const date = document.getElementById('bet-date')?.value || new Date().toISOString().split('T')[0];
  const ok = await saveBet(userId, date, p1, p2, q1, q2, choice);
  if (ok) {
    const chosen = choice === 'p1' ? p1 : p2;
    const q = choice === 'p1' ? q1 : q2;
    const pts = Math.round((gameConfig?.rules?.basePts || 10) * q * 10) / 10;
    toast(`✅ Scommessa piazzata! ${chosen.split(' ').pop()} · +${pts} se vince`);
    renderOddsPanel(); // refresh to show already-bet status
    updateBetUI();
  }
}

// MANUAL form submit
async function placeBetManual() {
  const isAdmin = userProfile?.isAdmin;
  const userId = isAdmin ? document.getElementById('bet-psel2').value : currentUser.uid;
  const date = isAdmin ? document.getElementById('bet-date2').value : document.getElementById('bet-date3').value;
  const p1 = document.getElementById('bp1').value.trim();
  const p2 = document.getElementById('bp2').value.trim();
  const q1 = parseFloat(document.getElementById('bq1').value);
  const q2 = parseFloat(document.getElementById('bq2').value);
  const choice = document.getElementById('bch').value;
  const ok = await saveBet(userId, date, p1, p2, q1, q2, choice);
  if (ok) {
    ['bp1','bp2','bq1','bq2'].forEach(id => { const e = document.getElementById(id); if(e) e.value=''; });
    document.getElementById('bch').value = '';
    document.getElementById('calc-prev').style.display = 'none';
    toast('Scommessa registrata! 🎯');
  }
}

// Keep old name for backward compat
async function placeBet() { return placeBetManual(); }

// ── RESOLVE ──
function isLeagueAdmin() {
  // User is admin if: global admin OR creator of any active league
  if (userProfile?.isAdmin) return true;
  return allLeagues.some(l => l.createdBy === currentUser.uid);
}

function renderPendingBets() {
  const el = document.getElementById('pending-list');
  if (!el) return;
  const isAdmin = isLeagueAdmin();
  // Admin/league-admin sees ALL bets of current tournament (pending + resolved), users see only their pending of current tournament
  const tid = gameConfig?.tournament || '';
  let bets = isAdmin
    ? allBets.filter(b => !b.tournament || b.tournament === tid)
    : allBets.filter(b => b.status === 'pending' && b.userId === currentUser.uid && (!b.tournament || b.tournament === tid));
  if (!bets.length) { el.innerHTML = `<div class="empty"><div class="empty-i">⏳</div><h3>Nessuna scommessa</h3></div>`; return; }
  // Sort: pending first, then by date desc
  bets.sort((a,b) => {
    if (a.status === 'pending' && b.status !== 'pending') return -1;
    if (a.status !== 'pending' && b.status === 'pending') return 1;
    return (b.date||'').localeCompare(a.date||'');
  });
  const statusIcon = { pending:'⏳', win:'✅', loss:'❌' };
  el.innerHTML = bets.map(b => {
    const chosen = b.choice === 'p1' ? b.p1 : b.p2;
    const canCancel = isAdmin || isBettingOpen(b.date);
    const resolved = b.status !== 'pending';
    return `
      <div class="mr" style="${resolved ? 'opacity:.7' : ''}">
        <div style="flex:1">
          <div class="fw6">${b.p1} vs ${b.p2}
            <span class="badge ${b.status==='pending'?'b-pend':b.status==='win'?'b-win':'b-loss'}" style="margin-left:6px;font-size:.65rem">${statusIcon[b.status]||'⏳'} ${b.status}</span>
          </div>
          <div class="dim" style="font-size:.76rem">${b.userName} · ${b.date} · su <strong>${chosen}</strong></div>
          <div style="font-size:.76rem;margin-top:3px">
            <span class="qp ok">${b.quotaChosen}</span>
            <span class="green" style="margin-left:7px">+${b.ptsWin}</span>
            <span class="red" style="margin-left:5px">${b.ptsLoss}</span>
            ${resolved ? `<span style="margin-left:7px;color:${b.status==='win'?'var(--accent)':'var(--red)'}">→ ${b.pointsResult>0?'+':''}${b.pointsResult||0}pt</span>` : ''}
          </div>
          ${canCancel ? `<button class="btn btn-d btn-sm" onclick="cancelBet('${b.id}')" style="margin-top:6px">🗑️ Cancella</button>` : ''}
        </div>
        <button class="btn btn-p btn-sm" onclick="openResolve('${b.id}')">${resolved ? '✏️ Modifica' : '🏁 Risolvi'}</button>
      </div>`;
  }).join('');
}

function openResolve(id) {
  resolvingBetId = id;
  const b = allBets.find(x => x.id === id);
  const chosen = b.choice === 'p1' ? b.p1 : b.p2;
  const resolved = b.status !== 'pending';
  document.getElementById('resolve-content').innerHTML = `
    <p><strong>${b.userName}</strong> ha scommesso su <strong>${chosen}</strong></p>
    <p class="dim mt2">${b.p1} vs ${b.p2} · ${b.date}</p>
    <p class="mt2">Quota: <span class="mono green">${b.quotaChosen}</span></p>
    <p class="green mt2">✅ Se vince: <strong>+${b.ptsWin} pt</strong></p>
    <p class="red mt2">❌ Se perde: <strong>${b.ptsLoss} pt</strong></p>
    ${resolved ? `<p style="margin-top:10px;padding:8px;background:rgba(255,255,255,.05);border-radius:8px">Risultato attuale: <strong>${b.status==='win'?'✅ Vinta':'❌ Persa'}</strong> (${b.pointsResult>0?'+':''}${b.pointsResult||0}pt) — puoi modificarlo</p>` : ''}`;
  document.getElementById('resolve-modal').classList.add('open');
}

async function confirmResolve(won) {
  const b = allBets.find(x => x.id === resolvingBetId);
  if (!b) return;
  await updateDoc(doc(db, 'bets', b.id), {
    status: won ? 'win' : 'loss',
    pointsResult: won ? b.ptsWin : b.ptsLoss
  });
  closeModal('resolve-modal');
  toast(won ? `✅ Vinta! +${b.ptsWin} pt` : `❌ Persa ${b.ptsLoss} pt`);
}

function closeModal(id) { document.getElementById(id).classList.remove('open'); }

// ── AUTO RESOLVE ──
// Tries to match pending bets against completed match results from The Odds API scores endpoint
window.autoResolveBets = async function(manual = false) {
  const key = gameConfig?.apiKey;
  if (!key) { if (manual) toastErr('API key non configurata'); return; }
  const tid = gameConfig?.tournament;
  const atpSport = tid && SPORT_MAP[tid] ? SPORT_MAP[tid] : null;
  const wtaSport = tid && WTA_MAP[tid] ? WTA_MAP[tid] : null;
  if (!atpSport) { if (manual) toastErr('Torneo non riconosciuto'); return; }

  const logEl = document.getElementById('auto-resolve-log');
  const statusEl = document.getElementById('auto-resolve-status');
  const logs = [];
  const addLog = msg => { logs.push(msg); if (logEl) { logEl.style.display = 'block'; logEl.innerHTML = logs.slice(-30).join('<br>'); logEl.scrollTop = logEl.scrollHeight; } };

  if (manual) addLog('⏳ Avvio risoluzione automatica...');

  // ── Fetch scores for ATP + WTA ──
  async function fetchScores(sport) {
    const url = `https://api.the-odds-api.com/v4/sports/${sport}/scores/?apiKey=${key}&daysFrom=3`;
    let data;
    try {
      const res = await fetch(url, { signal: AbortSignal.timeout(10000) });
      if (!res.ok) throw new Error('direct');
      data = await res.json();
    } catch {
      const res2 = await fetch(`https://api.allorigins.win/get?url=${encodeURIComponent(url)}`, { signal: AbortSignal.timeout(12000) });
      const w = await res2.json();
      data = JSON.parse(w.contents);
    }
    return Array.isArray(data) ? data.filter(m => m.completed) : [];
  }

  let scores = [];
  try {
    const [atp, wta] = await Promise.allSettled([
      fetchScores(atpSport),
      wtaSport ? fetchScores(wtaSport) : Promise.resolve([])
    ]);
    scores = [
      ...(atp.status === 'fulfilled' ? atp.value : []),
      ...(wta.status === 'fulfilled' ? wta.value : [])
    ];
    addLog(`📡 ${scores.length} partite completate trovate (ATP + WTA)`);
  } catch(e) {
    addLog(`❌ Errore fetch risultati: ${e.message}`);
    if (manual) toastErr('Errore: ' + e.message);
    return;
  }

  // ── Helper: fuzzy match player name against home/away ──
  function nameMatch(betName, scoreName) {
    if (!betName || !scoreName) return false;
    const b = betName.toLowerCase();
    const s = scoreName.toLowerCase();
    const bLast = b.split(' ').pop();
    const sLast = s.split(' ').pop();
    return bLast === sLast || s.includes(bLast) || b.includes(sLast);
  }

  // ── Helper: find match in scores for a bet ──
  function findMatch(bet) {
    // Extract player names from bet — handle "P1 vs P2 — market" format
    const rawP1 = bet.p1?.split('—')[0]?.trim() || bet.p1 || '';
    const rawP2 = bet.p2?.split('—')[0]?.trim() || bet.p2 || '';
    return scores.find(s => {
      const h = s.home_team || '';
      const a = s.away_team || '';
      const p1ok = nameMatch(rawP1, h) || nameMatch(rawP1, a);
      const p2ok = nameMatch(rawP2, h) || nameMatch(rawP2, a);
      return p1ok && p2ok;
    });
  }

  // ── Helper: detect bet market type ──
  function detectMarket(bet) {
    const p1 = bet.p1 || '';
    const p2 = bet.p2 || '';
    if (/over|under/i.test(p1) || /over|under/i.test(p2)) return 'ou';
    if (/1°\s*set|1st set|primo set/i.test(p1) || /1°\s*set|1st set/i.test(p2)) return 'set1';
    if (/[+\-]\d+(\.\d+)?/.test(p1) || /[+\-]\d+(\.\d+)?/.test(p2)) return 'handicap';
    return 'h2h';
  }

  // ── Helper: parse scores into {winner, totalGames, player1Games, player2Games, setScores} ──
  function parseMatchResult(match) {
    if (!match?.scores || match.scores.length < 2) return null;
    const s0 = match.scores[0]; // {name, score}
    const s1 = match.scores[1];

    // score can be "6-4,7-5,6-3" (set scores) or simple number (sets won)
    let p0sets = 0, p1sets = 0, p0games = 0, p1games = 0;
    const setScores = [];

    const raw0 = String(s0.score || '');
    const raw1 = String(s1.score || '');

    if (raw0.includes('-') || raw0.includes(',')) {
      // Detailed format: "6-4,7-5" — parse each set
      const sets = raw0.split(',');
      sets.forEach(setStr => {
        const parts = setStr.trim().split('-');
        if (parts.length >= 2) {
          const g0 = parseInt(parts[0]) || 0;
          const g1 = parseInt(parts[1]) || 0;
          p0games += g0; p1games += g1;
          if (g0 > g1) p0sets++; else p1sets++;
          setScores.push({ p0: g0, p1: g1 });
        }
      });
    } else {
      // Simple sets format: "2" vs "1"
      p0sets = parseInt(raw0) || 0;
      p1sets = parseInt(raw1) || 0;
    }

    const winner = p0sets > p1sets ? s0.name : s1.name;
    const totalGames = p0games + p1games;

    return {
      winner,
      p0: { name: s0.name, sets: p0sets, games: p0games },
      p1: { name: s1.name, sets: p1sets, games: p1games },
      totalGames,
      setScores
    };
  }

  // ── Resolve each pending bet ──
  const pendingBets = allBets.filter(b => b.status === 'pending' && (!b.tournament || b.tournament === tid));
  addLog(`⏳ ${pendingBets.length} scommesse pendenti da controllare`);

  let resolved = 0, skipped = 0;

  for (const bet of pendingBets) {
    const match = findMatch(bet);
    if (!match) { skipped++; continue; }

    const result = parseMatchResult(match);
    if (!result) { skipped++; continue; }

    const market = detectMarket(bet);
    let won = null;
    let detail = '';

    if (market === 'h2h') {
      // Vincente: chi ha scommesso ha vinto?
      const chosen = bet.choice === 'p1' ? bet.p1 : bet.p2;
      won = nameMatch(chosen, result.winner);
      detail = `vincitore: ${result.winner}`;

    } else if (market === 'ou') {
      // Over/Under: basato su totale games
      const chosen = bet.choice === 'p1' ? bet.p1 : bet.p2;
      const overMatch = chosen.match(/over\s*([\d.]+)/i);
      const underMatch = chosen.match(/under\s*([\d.]+)/i);
      if (overMatch) {
        const line = parseFloat(overMatch[1]);
        won = result.totalGames > line;
        detail = `totale games: ${result.totalGames} vs linea ${line}`;
      } else if (underMatch) {
        const line = parseFloat(underMatch[1]);
        won = result.totalGames < line;
        detail = `totale games: ${result.totalGames} vs linea ${line}`;
      } else { skipped++; continue; }

    } else if (market === 'handicap') {
      // Handicap: scarto games tra i due giocatori
      const chosen = bet.choice === 'p1' ? bet.p1 : bet.p2;
      const hMatch = chosen.match(/([+\-][\d.]+)/);
      if (!hMatch) { skipped++; continue; }
      const handicap = parseFloat(hMatch[1]);
      // Find which player the handicap applies to
      const p0name = result.p0.name;
      const chosenBase = chosen.replace(/[+\-][\d.]+/, '').replace(/[()]/g,'').trim();
      const isP0 = nameMatch(chosenBase, p0name);
      const p0adj = result.p0.games + (isP0 ? handicap : 0);
      const p1adj = result.p1.games + (isP0 ? 0 : handicap);
      won = isP0 ? p0adj > p1adj : p1adj > p0adj;
      detail = `games ${result.p0.name}: ${result.p0.games}, ${result.p1.name}: ${result.p1.games}, handicap: ${handicap}`;

    } else if (market === 'set1') {
      // Primo set: chi ha vinto il primo set?
      if (!result.setScores.length) { skipped++; continue; }
      const firstSet = result.setScores[0];
      const set1winner = firstSet.p0 > firstSet.p1 ? result.p0.name : result.p1.name;
      const chosen = bet.choice === 'p1' ? bet.p1 : bet.p2;
      // Extract player name from "P1 vs P2 — 1° set: Sinner"
      const setWinLabel = chosen.replace(/.*1°\s*set[:\s]*/i, '').trim();
      won = nameMatch(setWinLabel, set1winner);
      detail = `1° set: ${result.p0.name} ${firstSet.p0}-${firstSet.p1} ${result.p1.name}, vince: ${set1winner}`;
    }

    if (won === null) { skipped++; continue; }

    try {
      await updateDoc(doc(db, 'bets', bet.id), {
        status: won ? 'win' : 'loss',
        pointsResult: won ? bet.ptsWin : bet.ptsLoss,
        autoResolved: true,
        autoResolvedAt: new Date().toISOString()
      });
      resolved++;
      const chosen = bet.choice === 'p1' ? bet.p1 : bet.p2;
      addLog(`${won ? '✅' : '❌'} [${market.toUpperCase()}] ${chosen} — ${detail}`);
    } catch(e) {
      addLog(`⚠️ Errore salvataggio: ${e.message}`);
    }
  }

  await updateConfig({ autoResolveLastRun: new Date().toISOString(), autoResolveResolved: resolved });
  if (statusEl) statusEl.innerHTML = `Ultimo controllo: <strong>${new Date().toLocaleString('it')}</strong> · ✅ ${resolved} risolte, ⏭ ${skipped} saltate`;
  addLog(`✅ Completato: ${resolved} risolte, ${skipped} saltate`);
  if (manual) toast(`✅ Auto-resolve: ${resolved} scommesse risolte!`);
};

window.checkAutoResolveStatus = function() {
  const pendingBets = allBets.filter(b => b.status === 'pending' && (!b.tournament || b.tournament === gameConfig?.tournament));
  const logEl = document.getElementById('auto-resolve-log');
  if (logEl) {
    logEl.style.display = 'block';
    logEl.innerHTML = pendingBets.map(b => `⏳ ${b.date} · ${b.p1} vs ${b.p2} · scommessa: <strong>${b.choice==='p1'?b.p1:b.p2}</strong>`).join('<br>') || 'Nessuna scommessa pendente';
  }
  toast(`${pendingBets.length} scommesse pendenti`);
};

// Schedule nightly auto-resolve at midnight tournament time
function scheduleNightlyAutoResolve() {
  if (!userProfile?.isAdmin) return;
  const { tz } = getTournamentDates();
  // Calculate ms until next midnight in tournament timezone
  const now = new Date();
  const midnight = new Date(new Intl.DateTimeFormat('en-CA', { timeZone: tz, year:'numeric', month:'2-digit', day:'2-digit' }).format(now) + 'T00:00:00');
  midnight.setDate(midnight.getDate() + 1); // next midnight
  const msUntilMidnight = midnight - now;
  setTimeout(async () => {
    await autoResolveBets(false);
    scheduleNightlyAutoResolve(); // reschedule for next night
  }, msUntilMidnight);
  console.log(`🌙 Auto-resolve schedulato tra ${Math.round(msUntilMidnight/3600000)}h`);
}
window.scheduleNightlyAutoResolve = scheduleNightlyAutoResolve;

// ── MOBILE MENU ──
window.toggleMobileMenu = function() {
  const menu = document.getElementById('mobile-menu');
  const overlay = document.getElementById('mobile-menu-overlay');
  const isOpen = menu.style.transform === 'translateX(0%)';
  if (isOpen) {
    closeMobileMenu();
  } else {
    // Show iOS install tip once
    if (/iPhone|iPad/i.test(navigator.userAgent) && !navigator.standalone && !localStorage.getItem('iosHintShown')) {
      localStorage.setItem('iosHintShown','1');
      setTimeout(() => toast("💡 Suggerimento: aggiungi al Home Screen per un'esperienza migliore! (Condividi → Aggiungi a Home)"), 800);
    }
    menu.style.display = 'block';
    overlay.style.display = 'block';
    // Sync mobile user info
    const mobAv = document.getElementById('mob-av');
    const mobName = document.getElementById('mob-name');
    const mobUsername = document.getElementById('mob-username');
    if (mobAv) { mobAv.textContent = document.getElementById('h-av')?.textContent || '?'; mobAv.style.background = document.getElementById('h-av')?.style.background || ''; }
    if (mobName) mobName.textContent = document.getElementById('h-name')?.textContent || '';
    if (mobUsername) mobUsername.textContent = document.getElementById('h-name')?.textContent || '';
    // Show admin if needed
    const adminBtn = document.getElementById('mob-admin-btn');
    if (adminBtn) adminBtn.style.display = userProfile?.isAdmin ? '' : 'none';
    setTimeout(() => { menu.style.transform = 'translateX(0%)'; }, 10);
  }
};

window.closeMobileMenu = function() {
  const menu = document.getElementById('mobile-menu');
  const overlay = document.getElementById('mobile-menu-overlay');
  menu.style.transform = 'translateX(100%)';
  setTimeout(() => { menu.style.display = 'none'; overlay.style.display = 'none'; }, 280);
};

function syncNavActive(page) {
  // Map page id to button index for both navbars
  const pages = ['home','bet','live','antepost','leagues','ranking','history','admin'];
  document.querySelectorAll('nav button').forEach((btn, i) => {
    btn.classList.toggle('active', pages[i] === page);
  });
  document.querySelectorAll('.mob-nav-btn').forEach((btn, i) => {
    btn.classList.toggle('active', pages[i] === page);
  });
}

// ── ANTEPOST ──
function updateAnteCalc() {
  const type = document.getElementById('ante-type')?.value;
  if (!type) return;
  const qf = document.getElementById('ante-qf');
  const cf = document.getElementById('ante-cf');
  const ac = document.getElementById('ante-calc');
  const quota = parseFloat(document.getElementById('ante-q')?.value);
  const base = gameConfig?.rules?.basePts || 10;
  if (type === 'winner') {
    if(qf)qf.style.display='block'; if(cf)cf.style.display='none';
    if (!isNaN(quota)) {
      document.getElementById('acw').textContent = `+${Math.round(base * quota * 10)} pt`;
      document.getElementById('acl').textContent = '0 pt';
      if(ac)ac.style.display='block';
    }
  } else if (type === 'custom') {
    if(qf)qf.style.display='none'; if(cf)cf.style.display='block';
    const w = parseInt(document.getElementById('ante-pw')?.value) || 0;
    document.getElementById('acw').textContent = `+${w} pt`;
    document.getElementById('acl').textContent = '0 pt';
    if(ac)ac.style.display='block';
  } else {
    if(qf)qf.style.display='none'; if(cf)cf.style.display='none';
    const pts = type === 'semifinal' ? 20 : 35;
    document.getElementById('acw').textContent = `+${pts} pt`;
    document.getElementById('acl').textContent = '0 pt';
    if(ac)ac.style.display='block';
  }
}

async function addAntepost() {
  const isAdmin = userProfile?.isAdmin;
  const userId = isAdmin ? document.getElementById('ante-psel').value : currentUser.uid;
  const tennisPlayer = document.getElementById('ante-player').value.trim();
  const type = document.getElementById('ante-type').value;
  const quota = parseFloat(document.getElementById('ante-q').value);
  const base = gameConfig?.rules?.basePts || 10;
  if (!tennisPlayer) return toastErr('Inserisci il tennista!');
  let ptsWin, ptsLose;
  if (type === 'winner') {
    if (isNaN(quota)) return toastErr('Inserisci la quota!');
    ptsWin = Math.round(base * quota * 10); ptsLose = 0;
  } else if (type === 'semifinal') { ptsWin = 20; ptsLose = 0; }
  else if (type === 'final') { ptsWin = 35; ptsLose = 0; }
  else { ptsWin = parseInt(document.getElementById('ante-pw').value) || 0; ptsLose = 0; }
  const userName = allUsers.find(u => (u.id || u.uid) === userId)?.name || userProfile?.name || '';
  await addDoc(collection(db, 'anteposts'), {
    userId, userName, tennisPlayer, type,
    quota: isNaN(quota) ? null : quota,
    ptsWin, ptsLose, status: 'pending', pointsResult: 0,
    createdAt: serverTimestamp()
  });
  document.getElementById('ante-player').value = '';
  document.getElementById('ante-q').value = '';
  toast('Pronostico aggiunto! 📋');
}

async function resolveAntepost(id, won) {
  const a = allAnteposts.find(x => x.id === id);
  if (!a) return;
  await updateDoc(doc(db, 'anteposts', a.id), {
    status: won ? 'win' : 'loss',
    pointsResult: won ? a.ptsWin : a.ptsLose
  });
  toast(won ? `✅ +${a.ptsWin} pt` : '❌ Sbagliato');
}

function renderAntepostList() {
  const el = document.getElementById('ante-list');
  if (!el) return;
  const isAdmin = userProfile?.isAdmin;
  let list = allAnteposts;
  if (!isAdmin) list = list.filter(a => a.userId === currentUser.uid);
  if (!list.length) { el.innerHTML = `<div class="empty"><div class="empty-i">📋</div><h3>Nessun pronostico ancora</h3></div>`; return; }
  const tl = { winner:'🏆 Vincitore', semifinal:'🎾 Semifinalista', final:'🥈 Finalista', custom:'✏️ Custom' };
  el.innerHTML = list.map(a => {
    const canResolve = isAdmin && a.status === 'pending';
    return `
      <div class="mr">
        <div style="flex:1">
          <div class="fw6">${a.tennisPlayer}</div>
          <div class="dim" style="font-size:.76rem">${a.userName} · ${tl[a.type]}</div>
          <div style="font-size:.76rem;margin-top:3px"><span class="green">+${a.ptsWin} pt se corretto</span></div>
        </div>
        ${canResolve ? `
          <div class="flex gap2">
            <button class="btn btn-p btn-sm" onclick="resolveAntepost('${a.id}',true)">✅</button>
            <button class="btn btn-d btn-sm" onclick="resolveAntepost('${a.id}',false)">❌</button>
          </div>` :
          `<span class="badge ${a.status==='pending'?'b-pend':a.status==='win'?'b-win':'b-loss'}">${a.status==='pending'?'⏳':a.status==='win'?'✅ OK':'❌'}</span>`
        }
      </div>`;
  }).join('');
}

// ── LEAGUES ──
function generateCode() {
  return Math.random().toString(36).substring(2,8).toUpperCase();
}

async function createLeague() {
  const name = document.getElementById('lg-name').value.trim();
  if (!name) return toastErr('Inserisci il nome della lega!');
  const code = generateCode();
  await addDoc(collection(db, 'leagues'), {
    name, code,
    createdBy: currentUser.uid,
    creatorName: userProfile?.name || '',
    members: [currentUser.uid],
    memberNames: [userProfile?.name || ''],
    createdAt: serverTimestamp()
  });
  document.getElementById('lg-name').value = '';
  toast(`Lega creata! Codice: ${code} 🏅`);
}

async function joinLeague() {
  const code = document.getElementById('lg-code').value.trim().toUpperCase();
  if (!code) return toastErr('Inserisci un codice!');
  const league = allLeagues.find(l => l.code === code);
  if (!league) return toastErr('Codice non trovato!');
  if (league.members?.includes(currentUser.uid)) return toastErr('Sei già in questa lega!');
  await updateDoc(doc(db, 'leagues', league.id), {
    members: [...(league.members || []), currentUser.uid],
    memberNames: [...(league.memberNames || []), userProfile?.name || '']
  });
  document.getElementById('lg-code').value = '';
  toast(`Entrato nella lega: ${league.name}! 🎉`);
}

function renderLeagues() {
  const el = document.getElementById('leagues-list');
  if (!el) return;
  if (!myLeagues.length) {
    el.innerHTML = `<div class="empty"><div class="empty-i">🏅</div><h3>Nessuna lega ancora</h3><p>Crea una lega o unisciti con un codice</p></div>`;
    return;
  }
  el.innerHTML = myLeagues.map(l => `
    <div class="league-card" onclick="openLeague('${l.id}')">
      <div class="flex jb ic">
        <div class="league-name">${l.name}</div>
        <span class="badge b-blue">${l.members?.length || 0} membri</span>
      </div>
      <div class="mt2">
        <span class="league-code">🔗 ${l.code}</span>
        <span class="dim" style="font-size:.76rem;margin-left:8px">Creata da ${l.creatorName}</span>
      </div>
    </div>
  `).join('');
}

function openLeague(id) {
  const l = allLeagues.find(x => x.id === id);
  if (!l) return;
  const members = l.members || [];
  const memberUsers = allUsers.filter(u => members.includes(u.id || u.uid));
  // Use league context for points
  const prevCtx = activeContext;
  activeContext = id;
  const sorted = [...memberUsers].sort((a, b) => totalPts(b.id || b.uid) - totalPts(a.id || a.uid));
  activeContext = prevCtx;
  const isLeagueOwner = l.createdBy === currentUser?.uid || userProfile?.isAdmin;
  const leagueRules = l.rules || {};
  const leagueAnteQs = l.anteQuestions ?? null; // null = use global

  document.getElementById('lm-title').textContent = '🏅 ' + l.name;
  document.getElementById('lm-content').innerHTML = `
    <div class="mb2 flex jb ic gap2">
      <span class="league-code">🔗 Codice: ${l.code}</span>
      <button class="btn btn-p btn-sm" onclick="switchContext('${id}');closeModal('league-modal')">▶ Gioca in questa lega</button>
    </div>
    <div class="divider"></div>
    ${sorted.map((u, i) => {
      const uid = u.id || u.uid;
      activeContext = id;
      const pts = totalPts(uid);
      activeContext = prevCtx;
      const rn = i === 0 ? '🥇' : i === 1 ? '🥈' : i === 2 ? '🥉' : `${i+1}.`;
      return `<div class="mr"><div style="font-size:1.2rem;width:28px">${rn}</div><div style="flex:1"><span class="fw6">${u.name}</span></div><span class="mono green fw6">${pts > 0 ? '+' : ''}${pts} pt</span></div>`;
    }).join('')}
    <div class="divider"></div>
    ${isLeagueOwner ? `
    <div style="background:rgba(255,255,255,.03);border-radius:10px;padding:12px;margin-bottom:10px">
      <div style="font-size:.78rem;color:var(--accent);font-weight:600;margin-bottom:10px">⚙️ REGOLE LEGA</div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:10px">
        <div><label style="font-size:.72rem">Quota minima</label><input type="number" id="lm-minq" value="${leagueRules.minQuota||gameConfig?.rules?.minQuota||1.20}" step="0.05" style="font-size:.82rem"></div>
        <div><label style="font-size:.72rem">Punti base</label><input type="number" id="lm-basepts" value="${leagueRules.basePts||gameConfig?.rules?.basePts||10}" style="font-size:.82rem"></div>
      </div>
      <div style="font-size:.72rem;color:var(--dim);margin-bottom:8px">
        Antepost: <strong>${leagueAnteQs === null ? 'usa domande globali' : `${leagueAnteQs.length} domande personalizzate`}</strong>
        <button class="btn btn-g btn-sm" style="margin-left:8px" onclick="resetLeagueAnte('${id}')">↩ Ripristina globali</button>
        <button class="btn btn-g btn-sm" style="margin-left:4px" onclick="customizeLeagueAnte('${id}')">✏️ Personalizza</button>
      </div>
      <button class="btn btn-p btn-sm" onclick="saveLeagueRules('${id}')">💾 Salva regole</button>
    </div>` : ''}
    <button class="btn btn-d btn-sm" onclick="leaveLeague('${id}')">Lascia lega</button>
  `;
  document.getElementById('league-modal').classList.add('open');
}

window.saveLeagueRules = async function(id) {
  const minQ = parseFloat(document.getElementById('lm-minq')?.value) || 1.20;
  const basePts = parseInt(document.getElementById('lm-basepts')?.value) || 10;
  await updateDoc(doc(db, 'leagues', id), { rules: { minQuota: minQ, basePts } });
  toast('✅ Regole lega salvate!');
};

window.resetLeagueAnte = async function(id) {
  if (!confirm('Ripristinare le domande antepost globali per questa lega?')) return;
  await updateDoc(doc(db, 'leagues', id), { anteQuestions: deleteField() });
  toast('✅ Antepost ripristinati alle domande globali');
  closeModal('league-modal');
};

window.customizeLeagueAnte = function(id) {
  const l = allLeagues.find(x => x.id === id);
  if (!l) return;
  closeModal('league-modal');
  // Switch to league context and go to antepost admin
  switchContext(id);
  goPage('admin', null);
  toast('Modifica le domande antepost — si salveranno per questa lega');
};

async function leaveLeague(id) {
  if (!confirm('Vuoi lasciare questa lega?')) return;
  const l = allLeagues.find(x => x.id === id);
  if (!l) return;
  await updateDoc(doc(db, 'leagues', id), {
    members: l.members.filter(m => m !== currentUser.uid),
    memberNames: l.memberNames.filter((_, i) => l.members[i] !== currentUser.uid)
  });
  closeModal('league-modal');
  toast('Hai lasciato la lega');
}

// ── HISTORY ──
let selectedHistTournament = null;

function renderHistory() {
  const listEl = document.getElementById('hist-tournament-list');
  const detailEl = document.getElementById('hist-detail');
  if (!listEl) return;
  detailEl.style.display = 'none';
  listEl.style.display = 'block';

  const history = gameConfig?.tournamentHistory || [];
  // Also show current active tournament
  const currentId = gameConfig?.tournament;
  const currentT = currentId ? TOURNAMENTS.find(x => x.id === currentId) : null;

  if (!history.length && !currentT) {
    listEl.innerHTML = '<div class="empty"><div class="empty-i">📜</div><h3>Nessun torneo ancora</h3><p>Lo storico si costruisce torneo dopo torneo!</p></div>';
    return;
  }

  let html = '';
  // Show current tournament first
  if (currentT) {
    const sorted = [...allUsers].sort((a,b) => totalPts(b.id||b.uid) - totalPts(a.id||a.uid));
    html += `<div class="card" style="cursor:pointer;border:1px solid rgba(126,200,80,.3)" onclick="openHistDetail('__current__')">
      <div class="flex jb ic">
        <div>
          <div style="font-size:1.3rem">${currentT.emoji} <strong>${currentT.name}</strong> <span class="badge b-blue" style="font-size:.65rem">IN CORSO</span></div>
          <div class="dim" style="font-size:.78rem;margin-top:4px">${gameConfig?.tournamentDates?.start||'?'} → ${gameConfig?.tournamentDates?.end||'?'}</div>
        </div>
        <div style="text-align:right">
          <div style="font-size:.8rem;color:var(--dim)">${allUsers.length} giocatori</div>
          <div style="font-size:.8rem;color:var(--accent);margin-top:2px">Leader: ${sorted[0]?.name||'—'} (${totalPts(sorted[0]?.id||sorted[0]?.uid||'')}pt)</div>
        </div>
      </div>
    </div>`;
  }

  // Show past tournaments
  [...history].reverse().forEach(h => {
    const t = TOURNAMENTS.find(x => x.id === h.tournamentId);
    const top3 = (h.classifica||[]).slice(0,3);
    html += `<div class="card" style="cursor:pointer" onclick="openHistDetail('${h.tournamentId}')">
      <div class="flex jb ic">
        <div>
          <div style="font-size:1.2rem">${h.tournamentEmoji||t?.emoji||'🎾'} <strong>${h.tournamentName||t?.name||h.tournamentId}</strong></div>
          <div class="dim" style="font-size:.78rem;margin-top:4px">${h.dates?.start||'?'} → ${h.dates?.end||'?'}</div>
          <div style="font-size:.78rem;margin-top:6px">${top3.map((p,i) => `${['🥇','🥈','🥉'][i]} ${p.name} (${p.pts}pt)`).join(' · ')}</div>
        </div>
        <div style="color:var(--dim);font-size:1.2rem">›</div>
      </div>
    </div>`;
  });

  listEl.innerHTML = html;
}

window.openHistDetail = function(tournamentId) {
  selectedHistTournament = tournamentId;
  const listEl = document.getElementById('hist-tournament-list');
  const detailEl = document.getElementById('hist-detail');
  listEl.style.display = 'none';
  detailEl.style.display = 'block';

  let data, tName, tEmoji;
  if (tournamentId === '__current__') {
    const t = TOURNAMENTS.find(x => x.id === gameConfig?.tournament);
    tName = t?.name || 'Torneo corrente';
    tEmoji = t?.emoji || '🎾';
    const sorted = [...allUsers].sort((a,b) => totalPts(b.id||b.uid) - totalPts(a.id||a.uid));
    data = {
      classifica: sorted.map((u,i) => ({ pos:i+1, uid:u.id||u.uid, name:u.name, pts:totalPts(u.id||u.uid) })),
      bets: allBets,
      anteposts: allAnteposts
    };
  } else {
    const h = (gameConfig?.tournamentHistory||[]).find(x => x.tournamentId === tournamentId);
    if (!h) return;
    const t = TOURNAMENTS.find(x => x.id === tournamentId);
    tName = h.tournamentName || t?.name || tournamentId;
    tEmoji = h.tournamentEmoji || t?.emoji || '🎾';
    data = h;
  }

  document.getElementById('hist-detail-title').textContent = `${tEmoji} ${tName}`;
  detailEl._data = data;
  switchHistTab('classifica', detailEl.querySelector('.tab'));
};

window.closeHistDetail = function() {
  document.getElementById('hist-tournament-list').style.display = 'block';
  document.getElementById('hist-detail').style.display = 'none';
};

window.switchHistTab = function(tab, btn) {
  document.querySelectorAll('#hist-detail .tab').forEach(b => b.classList.remove('active'));
  if (btn) btn.classList.add('active');
  ['classifica','scommesse','antepost'].forEach(t => {
    const el = document.getElementById('hist-' + t);
    if (el) el.style.display = t === tab ? 'block' : 'none';
  });
  const data = document.getElementById('hist-detail')?._data;
  if (!data) return;
  if (tab === 'classifica') renderHistClassifica(data);
  if (tab === 'scommesse') renderHistScommesse(data);
  if (tab === 'antepost') renderHistAntepost(data);
};

function renderHistClassifica(data) {
  const el = document.getElementById('hist-classifica');
  if (!el) return;
  const cls = data.classifica || [];
  if (!cls.length) { el.innerHTML = '<div class="empty"><div class="empty-i">🏆</div><h3>Nessun dato</h3></div>'; return; }
  el.innerHTML = cls.map((p,i) => {
    const avC = getAvatarColor(p.uid);
    const mine = p.uid === currentUser.uid ? 'mine' : '';
    const rn = i===0?'🥇':i===1?'🥈':i===2?'🥉':`${i+1}°`;
    return `<div class="lb-row ${mine}">
      <div class="rn" style="display:flex;align-items:center;gap:8px">
        <div style="width:32px;height:32px;border-radius:50%;background:${avC};display:flex;align-items:center;justify-content:center;font-weight:700;color:#fff">${p.name.charAt(0).toUpperCase()}</div>
        <span>${rn}</span>
      </div>
      <div><div class="lb-name">${p.name}${mine?' <span style="font-size:.68rem;color:var(--accent)">← tu</span>':''}</div></div>
      <div><div class="lb-pts">${p.pts > 0 ? '+' : ''}${p.pts}</div><div class="lb-sub tr">pt</div></div>
    </div>`;
  }).join('');
}

function renderHistScommesse(data) {
  const el = document.getElementById('hist-scommesse');
  if (!el) return;
  const isAdmin = userProfile?.isAdmin;
  let bets = [...(data.bets||[])].sort((a,b) => (b.date||'').localeCompare(a.date||''));
  if (!isAdmin) bets = bets.filter(b => b.userId === currentUser.uid);
  if (!bets.length) { el.innerHTML = '<div class="empty"><div class="empty-i">🎯</div><h3>Nessuna scommessa</h3></div>'; return; }
  const minQ = gameConfig?.rules?.minQuota || 1.20;
  el.innerHTML = `<div class="card" style="padding:0;overflow:hidden"><div style="overflow-x:auto"><table><thead><tr>
    <th>Data</th><th>Chi</th><th>Match</th><th>Scelta</th><th>Quota</th><th>Punti</th><th>Esito</th>
  </tr></thead><tbody>` +
  bets.map(b => {
    const chosen = b.choice === 'p1' ? b.p1 : b.p2;
    const pts = b.pointsResult || 0;
    const badge = b.status==='win'?'<span class="badge b-win">✅</span>':b.status==='loss'?'<span class="badge b-loss">❌</span>':'<span class="badge b-pend">⏳</span>';
    return `<tr>
      <td class="mono dim" style="font-size:.76rem">${b.date}</td>
      <td>${b.userName}</td>
      <td style="font-size:.8rem">${b.p1} vs ${b.p2}</td>
      <td>${chosen}</td>
      <td><span class="qp ${b.quotaChosen>=minQ?'ok':'locked'}">${b.quotaChosen}</span></td>
      <td class="mono ${pts>0?'green':pts<0?'red':'dim'}">${pts>0?'+':''}${pts}</td>
      <td>${badge}</td>
    </tr>`;
  }).join('') + '</tbody></table></div></div>';
}

function renderHistAntepost(data) {
  const el = document.getElementById('hist-antepost');
  if (!el) return;
  const isAdmin = userProfile?.isAdmin;
  let antes = [...(data.anteposts||[])];
  if (!isAdmin) antes = antes.filter(a => a.userId === currentUser.uid);
  if (!antes.length) { el.innerHTML = '<div class="empty"><div class="empty-i">📋</div><h3>Nessun antepost</h3></div>'; return; }
  const qs = getActiveContextConfig().anteQuestions || [];
  el.innerHTML = antes.map(a => `
    <div class="mr">
      <div style="flex:1">
        <div class="fw6">${a.tennisPlayer}</div>
        <div class="dim" style="font-size:.76rem">${a.userName} · ${qs[a.questionIdx]?.text || a.questionText || a.type}</div>
        <div style="font-size:.76rem;margin-top:3px"><span class="green">+${a.ptsWin}pt se corretto</span></div>
      </div>
      <span class="badge ${a.status==='win'?'b-win':a.status==='loss'?'b-loss':'b-pend'}">${a.status==='win'?`✅ +${a.ptsWin}pt`:a.status==='loss'?'❌':'⏳'}</span>
    </div>`).join('');
}

// ── ADMIN USERS ──
function renderAdminUsers() {
  const el = document.getElementById('admin-users');
  if (!el) return;
  if (!allUsers.length) { el.innerHTML = '<p class="dim" style="font-size:.84rem">Nessun utente registrato.</p>'; return; }
  el.innerHTML = allUsers.map(u => {
    const uid = u.id || u.uid;
    const pts = totalPts(uid);
    return `<div class="mr">
      <div style="flex:1">
        <div class="fw6">${u.name} ${u.isAdmin ? '<span class="badge b-blue">Admin</span>' : ''}</div>
        <div class="dim" style="font-size:.76rem">${u.email} · <span class="green mono">${pts > 0 ? '+' : ''}${pts} pt</span></div>
      </div>
      <button class="btn btn-g btn-sm" onclick="toggleAdmin('${uid}',${!u.isAdmin})">${u.isAdmin ? '↓ Rimuovi Admin' : '↑ Rendi Admin'}</button>
    </div>`;
  }).join('');
}
window.saveAdminApiKey = async function() {
  const key = document.getElementById('admin-api-key').value.trim();
  if (!key) return toastErr('Inserisci la API key!');
  await updateConfig({ apiKey: key });
  document.getElementById('api-key-status').innerHTML = '<span style="color:var(--accent)">✅ Salvata! Le quote si caricheranno automaticamente.</span>';
  toast('API key salvata! 📡');
};

window.toggleAdmin = async function(uid, makeAdmin) {
  await updateDoc(doc(db, 'users', uid), { isAdmin: makeAdmin });
  toast(makeAdmin ? 'Utente promosso Admin! 🔧' : 'Admin rimosso');
};

async function resetSeason() {
  if (!confirm('Reset della stagione? Le scommesse e gli antepost saranno cancellati.')) return;
  const bSnap = await getDocs(collection(db, 'bets'));
  const aSnap = await getDocs(collection(db, 'anteposts'));
  await Promise.all([...bSnap.docs, ...aSnap.docs].map(d => deleteDoc(d.ref)));
  toast('Stagione resettata! 🔄');
}

// ── ODDS API ──
const SPORT_MAP = {
  aus:'tennis_atp_aus_open',rg:'tennis_atp_french_open',wim:'tennis_atp_wimbledon',
  uso:'tennis_atp_us_open',iw:'tennis_atp_indian_wells',mia:'tennis_atp_miami_open',
  mc:'tennis_atp_monte_carlo_masters',mad:'tennis_atp_madrid_open',rom:'tennis_atp_italian_open',
  can:'tennis_atp_canadian_open',cin:'tennis_atp_western_southern_open',
  sha:'tennis_atp_shanghai_masters',pma:'tennis_atp_paris_masters'
};
// WTA equivalent sports
const WTA_MAP = {
  aus:'tennis_wta_aus_open',rg:'tennis_wta_french_open',wim:'tennis_wta_wimbledon',
  uso:'tennis_wta_us_open',iw:'tennis_wta_indian_wells',mia:'tennis_wta_miami_open',
  mc:'tennis_wta_madrid_open',mad:'tennis_wta_madrid_open',rom:'tennis_wta_italian_open',
  can:'tennis_wta_canadian_open',cin:'tennis_wta_western_southern_open'
};
const REFRESH_INTERVAL = 15 * 60; // 15 minutes in seconds
let oddsData = [];
let selectedOddsTab = 'today';
let countdownSeconds = REFRESH_INTERVAL;
let countdownTimer = null;
let autoRefreshTimer = null;

function toggleApiInput() {
  const p = document.getElementById('api-key-panel');
  p.style.display = p.style.display === 'none' ? 'block' : 'none';
}

function saveApiKey() {
  const key = document.getElementById('api-key').value.trim();
  updateConfig({ apiKey: key });
  if (key) { loadOdds(); }
}

async function fetchOddsForSport(key, sport, markets) {
  const url = `https://api.the-odds-api.com/v4/sports/${sport}/odds/?apiKey=${key}&regions=eu&markets=${markets}&oddsFormat=decimal`;
  let data;
  try {
    const res = await fetch(url, { signal: AbortSignal.timeout(8000) });
    if (!res.ok) throw new Error('direct_fail');
    data = await res.json();
  } catch(e) {
    const proxyUrl = `https://api.allorigins.win/get?url=${encodeURIComponent(url)}`;
    const res2 = await fetch(proxyUrl, { signal: AbortSignal.timeout(10000) });
    if (!res2.ok) throw new Error('proxy_fail');
    const wrapped = await res2.json();
    data = JSON.parse(wrapped.contents);
  }
  if (!Array.isArray(data)) {
    const msg = data?.message || data?.error || 'Risposta non valida';
    throw new Error(msg);
  }
  return data;
}

async function loadOdds(manual = false) {
  const key = gameConfig?.apiKey || document.getElementById('api-key')?.value?.trim();
  if (!key) {
    document.getElementById('odds-list').innerHTML = '<div class="no-matches">🔑 Inserisci la API key per vedere le quote</div>';
    return;
  }
  const tid = gameConfig?.tournament;
  const atpSport = tid && SPORT_MAP[tid] ? SPORT_MAP[tid] : 'tennis_atp_french_open';
  const wtaSport = tid && WTA_MAP[tid] ? WTA_MAP[tid] : null;
  const markets = 'h2h,totals,spreads';
  const btn = document.getElementById('refresh-btn');
  if (btn) btn.classList.add('spinning');
  setSyncStatus('loading');
  try {
    // Fetch ATP + WTA in parallel — ignore WTA errors silently
    const [atpData, wtaData] = await Promise.allSettled([
      fetchOddsForSport(key, atpSport, markets),
      wtaSport ? fetchOddsForSport(key, wtaSport, markets) : Promise.resolve([])
    ]);
    const atp = atpData.status === 'fulfilled' ? atpData.value.map(m => ({...m, _tour:'ATP'})) : [];
    const wta = wtaData.status === 'fulfilled' ? wtaData.value.map(m => ({...m, _tour:'WTA'})) : [];
    oddsData = [...atp, ...wta];
    if (!oddsData.length && atpData.status === 'rejected') throw new Error(atpData.reason?.message || 'Errore API');
    renderOddsPanel();
    setSyncStatus('live');
    resetCountdown();
    if (manual) toast(`${atp.length} partite ATP${wta.length ? ` + ${wta.length} WTA` : ''} caricate! 📡`);
    scheduleAutoRefresh();
  } catch(e) {
    setSyncStatus('error');
    if (manual) toastErr('Errore API: ' + e.message);
  } finally {
    if (btn) btn.classList.remove('spinning');
  }
}

function setSyncStatus(status) {
  const dot = document.getElementById('sync-dot');
  const label = document.getElementById('sync-label');
  if (!dot || !label) return;
  if (status === 'live') {
    dot.classList.add('live');
    const now = new Date().toLocaleTimeString('it', {hour:'2-digit',minute:'2-digit'});
    label.textContent = `Aggiornato alle ${now} · Auto-refresh 15 min`;
  } else if (status === 'loading') {
    dot.classList.remove('live');
    label.textContent = 'Caricamento quote...';
  } else if (status === 'error') {
    dot.classList.remove('live');
    label.textContent = 'Errore caricamento';
  } else {
    dot.classList.remove('live');
    label.textContent = 'In attesa di API key';
  }
}

function resetCountdown() {
  countdownSeconds = REFRESH_INTERVAL;
  if (countdownTimer) clearInterval(countdownTimer);
  countdownTimer = setInterval(() => {
    countdownSeconds--;
    const pct = (countdownSeconds / REFRESH_INTERVAL) * 100;
    const fill = document.getElementById('countdown-fill');
    if (fill) fill.style.width = pct + '%';
    if (countdownSeconds <= 0) { clearInterval(countdownTimer); loadOdds(); }
  }, 1000);
}

function scheduleAutoRefresh() {
  if (autoRefreshTimer) clearTimeout(autoRefreshTimer);
  autoRefreshTimer = setTimeout(() => loadOdds(), REFRESH_INTERVAL * 1000);
}

function renderOddsPanel() {
  const minQ = gameConfig?.rules?.minQuota || 1.20;
  const today = new Date().toISOString().split('T')[0];
  const { today: tToday, tomorrow: tTomorrow, tz: tTz, tournamentName: tName } = getTournamentDates();
  const todayMatches = oddsData.filter(m => getMatchTournamentDate(m.commence_time) === tToday);
  const tomorrowMatches = oddsData.filter(m => getMatchTournamentDate(m.commence_time) === tTomorrow);

  // Sync odds tab with currently selected bet day
  const betDateVal = document.getElementById('bet-date')?.value || tToday;
  if (betDateVal === tomorrow && selectedOddsTab === 'today') selectedOddsTab = 'tomorrow';
  if (betDateVal === today && selectedOddsTab === 'tomorrow') selectedOddsTab = 'today';

  // Render tabs — only today and tomorrow
  const tabsEl = document.getElementById('odds-tabs');
  if (tabsEl) {
    tabsEl.innerHTML = [
      { id:'today', label:`Oggi (${todayMatches.length})` },
      { id:'tomorrow', label:`Domani (${tomorrowMatches.length})` }
    ].map(t => `<button class="odds-tab ${selectedOddsTab===t.id?'active':''}" onclick="switchOddsTab('${t.id}')">${t.label}</button>`).join('');
  }

  // Show only matches for the selected day (= the day user is betting for)
  const matches = selectedOddsTab === 'tomorrow' ? tomorrowMatches : todayMatches;

  const el = document.getElementById('odds-list');
  if (!matches.length) {
    el.innerHTML = `<div class="no-matches">📭 Nessuna partita per ${selectedOddsTab==='today'?'oggi':'domani'} — prova ad aggiornare le quote</div>`;
    return;
  }

  el.innerHTML = matches.slice(0, 40).map(match => {
    const h = match.home_team, a = match.away_team;
    const isWTA = match._tour === 'WTA';
    const book = match.bookmakers?.[0];
    const mkt = book?.markets?.find(m => m.key === 'h2h');
    const q1 = mkt?.outcomes?.find(o => o.name === h)?.price;
    const q2 = mkt?.outcomes?.find(o => o.name === a)?.price;
    const commenceDate = new Date(match.commence_time);
    const isToday = commenceDate.toISOString().split('T')[0] === today;
    const timeStr = commenceDate.toLocaleTimeString('it', {hour:'2-digit',minute:'2-digit'});
    const dateStr = commenceDate.toLocaleDateString('it', {day:'2-digit',month:'short'});
    const q1locked = q1 && q1 < minQ;
    const q2locked = q2 && q2 < minQ;
    const q1fav = q1 && q2 && q1 < q2;
    const q2fav = q2 && q1 && q2 < q1;
    const h_short = h.split(' ').pop();
    const a_short = a.split(' ').pop();
    const hE = h.replace(/'/g,"\\'"); const aE = a.replace(/'/g,"\\'");

    // Over/Under totals
    const totMkt = book?.markets?.find(m => m.key === 'totals');
    const overOuts = (totMkt?.outcomes||[]).filter(o => o.name === 'Over');
    const underOuts = (totMkt?.outcomes||[]).filter(o => o.name === 'Under');
    const totalsHtml = overOuts.length ? `
      <div style="margin-top:8px;border-top:1px solid rgba(255,255,255,.07);padding-top:8px">
        <div style="font-size:.65rem;color:var(--dim);margin-bottom:6px;letter-spacing:1px">OVER / UNDER GAMES</div>
        <div style="display:flex;flex-wrap:wrap;gap:6px">
          ${overOuts.map(o => {
            const uMatch = underOuts.find(u => u.point === o.point);
            const oLocked = o.price < minQ;
            const uLocked = uMatch && uMatch.price < minQ;
            return `<div style="display:flex;align-items:center;gap:4px">
              <span style="font-size:.7rem;color:var(--dim);min-width:32px;text-align:center">${o.point}</span>
              <div class="odd-btn ${oLocked?'locked':''}" style="min-width:64px;padding:6px 8px" onclick="event.stopPropagation();selectOddsOU(this,'${hE}','${aE}',${o.price},${uMatch?.price||0},${o.point},'over')">
                <div class="odd-player" style="font-size:.62rem">Over</div>
                <div class="odd-val ${oLocked?'locked-val':''}">${o.price}</div>
              </div>
              ${uMatch?`<div class="odd-btn ${uLocked?'locked':''}" style="min-width:64px;padding:6px 8px" onclick="event.stopPropagation();selectOddsOU(this,'${hE}','${aE}',${uMatch.price},${o.price},${o.point},'under')">
                <div class="odd-player" style="font-size:.62rem">Under</div>
                <div class="odd-val ${uLocked?'locked-val':''}">${uMatch.price}</div>
              </div>`:''}
            </div>`;
          }).join('')}
        </div>
      </div>` : '';

    // Spreads / Handicap
    const spreadMkt = book?.markets?.find(m => m.key === 'spreads');
    const spreadsHtml = spreadMkt?.outcomes?.length ? `
      <div style="margin-top:8px;border-top:1px solid rgba(255,255,255,.07);padding-top:8px">
        <div style="font-size:.65rem;color:var(--dim);margin-bottom:6px;letter-spacing:1px">HANDICAP</div>
        <div style="display:flex;gap:6px;flex-wrap:wrap">
          ${spreadMkt.outcomes.map(o => {
            const locked = o.price < minQ;
            const sign = o.point > 0 ? '+' : '';
            const labelH = `${o.name} (${sign}${o.point})`;
            const otherO = spreadMkt.outcomes.find(x => x.name !== o.name);
            const otherSign = otherO?.point > 0 ? '+' : '';
            const labelA = otherO ? `${otherO.name} (${otherSign}${otherO.point})` : '';
            return `<div class="odd-btn ${locked?'locked':''}" style="min-width:90px;padding:6px 8px"
              onclick="event.stopPropagation();selectOddsHandicap(this,'${hE}','${aE}','${labelH.replace(/'/g,"\\'")}','${labelA.replace(/'/g,"\\'")}',${o.price},${otherO?.price||0})">
              <div class="odd-player" style="font-size:.62rem">${o.name} ${sign}${o.point}</div>
              <div class="odd-val ${locked?'locked-val':''}">${o.price}</div>
            </div>`;
          }).join('')}
        </div>
      </div>` : '';

    // First set winner (h2h_q4 or alternative_spreads — varies by bookmaker)
    const setMkt = book?.markets?.find(m => m.key === 'h2h_q4' || m.key === 'h2h_h1');
    const setHtml = setMkt?.outcomes?.length ? `
      <div style="margin-top:8px;border-top:1px solid rgba(255,255,255,.07);padding-top:8px">
        <div style="font-size:.65rem;color:var(--dim);margin-bottom:6px;letter-spacing:1px">VINCITORE 1° SET</div>
        <div style="display:flex;gap:6px">
          ${setMkt.outcomes.map(o => {
            const locked = o.price < minQ;
            const isH = o.name === h;
            const other = setMkt.outcomes.find(x => x.name !== o.name);
            return `<div class="odd-btn ${locked?'locked':''}" style="flex:1;padding:6px 8px"
              onclick="event.stopPropagation();selectOddsSet(this,'${hE}','${aE}','${o.name.replace(/'/g,"\\'")}','${(other?.name||'').replace(/'/g,"\\'")}',${o.price},${other?.price||0})">
              <div class="odd-player" style="font-size:.62rem">${o.name.split(' ').pop()}</div>
              <div class="odd-val ${locked?'locked-val':''}">${o.price}</div>
            </div>`;
          }).join('')}
        </div>
      </div>` : '';

    return `
      <div class="match-card" onclick="selectMatchCard(this,'${hE}','${aE}',${q1||0},${q2||0})">
        <div class="match-time">
          ${isToday ? '<span class="today-badge">OGGI</span>' : dateStr}
          <span>${timeStr}</span>
          <span style="margin-left:auto;display:flex;align-items:center;gap:6px">
            ${isWTA ? '<span style="font-size:.62rem;background:rgba(255,100,180,.15);color:#ff80c0;padding:2px 6px;border-radius:8px;font-weight:700">WTA</span>' : '<span style="font-size:.62rem;background:rgba(126,200,80,.1);color:var(--accent);padding:2px 6px;border-radius:8px;font-weight:700">ATP</span>'}
            <span style="font-size:.68rem;color:var(--dim)">${book?.title||'?'}</span>
          </span>
        </div>
        <div class="match-players-row">
          <div class="player-name-big">${h}</div>
          <div class="vs-badge">VS</div>
          <div class="player-name-big" style="text-align:right">${a}</div>
        </div>
        <div style="font-size:.65rem;color:var(--dim);margin-bottom:4px;letter-spacing:1px">VINCENTE</div>
        <div class="odds-row">
          <div class="odd-btn ${q1locked?'locked':''}" onclick="event.stopPropagation();selectOdd(this,'${hE}','${aE}',${q1||0},${q2||0},'p1')">
            <div class="odd-player">${h_short}</div>
            <div class="odd-val ${q1locked?'locked-val':q1fav?'fav':''}">${q1||'?'}</div>
          </div>
          <div class="odd-btn ${q2locked?'locked':''}" onclick="event.stopPropagation();selectOdd(this,'${hE}','${aE}',${q1||0},${q2||0},'p2')">
            <div class="odd-player">${a_short}</div>
            <div class="odd-val ${q2locked?'locked-val':q2fav?'fav':''}">${q2||'?'}</div>
          </div>
        </div>
        ${setHtml}
        ${spreadsHtml}
        ${totalsHtml}
      </div>`;
  }).join('');
}

function switchOddsTab(tab) {
  selectedOddsTab = tab;
  // Sync bet date to match selected tab
  const { today, tomorrow } = getTournamentDates();
  const betDate = document.getElementById('bet-date');
  if (betDate) betDate.value = tab === 'tomorrow' ? tomorrow : today;
  updateBetUI();
  renderOddsPanel();
}

function selectMatchCard(el, p1, p2, q1, q2) {
  document.querySelectorAll('.match-card').forEach(c => c.classList.remove('selected'));
  el.classList.add('selected');
  prefillBet(p1, p2, q1, q2);
}

window.selectOddsOU = function(btn, p1, p2, qChosen, qOther, point, side) {
  const label1 = side === 'over' ? `Over ${point}` : `Under ${point}`;
  const label2 = side === 'over' ? `Under ${point}` : `Over ${point}`;
  document.querySelectorAll('.odd-btn').forEach(b => b.classList.remove('selected-odd'));
  btn.classList.add('selected-odd');
  prefillBet(`${p1} vs ${p2} — ${label1}`, label2, qChosen, qOther);
  const bch = document.getElementById('bch');
  if (bch) { bch.value = 'p1'; updateCalc(); }
  // Switch to manual tab
  switchBetTab('place');
  document.querySelectorAll('#page-bet .tab').forEach(b => { if(b.textContent.includes('Scommetti')) b.classList.add('active'); });
};

window.selectOddsHandicap = function(btn, p1, p2, label1, label2, q1, q2) {
  document.querySelectorAll('.odd-btn').forEach(b => b.classList.remove('selected-odd'));
  btn.classList.add('selected-odd');
  prefillBet(label1, label2, q1, q2);
  const bch = document.getElementById('bch');
  if (bch) { bch.value = 'p1'; updateCalc(); }
  switchBetTab('place');
};

window.selectOddsSet = function(btn, p1, p2, winner, other, q1, q2) {
  document.querySelectorAll('.odd-btn').forEach(b => b.classList.remove('selected-odd'));
  btn.classList.add('selected-odd');
  prefillBet(`${p1} vs ${p2} — 1° set: ${winner}`, `1° set: ${other}`, q1, q2);
  const bch = document.getElementById('bch');
  if (bch) { bch.value = 'p1'; updateCalc(); }
  switchBetTab('place');
};

function selectOdd(btnEl, p1, p2, q1, q2, choice) {
  const minQ = gameConfig?.rules?.minQuota || 1.20;
  const q = choice === 'p1' ? q1 : q2;
  if (q < minQ) { toastErr(`Quota troppo bassa! Min: ${minQ}`); return; }
  // Show confirmation modal
  openBetConfirm(p1, p2, q1, q2, choice);
}

function prefillBet(p1, p2, q1, q2) {
  document.getElementById('bp1').value = p1;
  document.getElementById('bp2').value = p2;
  document.getElementById('bq1').value = q1;
  document.getElementById('bq2').value = q2;
  document.getElementById('bch').value = '';
  document.getElementById('calc-prev').style.display = 'none';
  updateCalc();
}

// ── EXPORT ──
function exportCSV() {
  let csv = 'Data,Giocatore,Match,Scelta,Quota,Punti,Esito\n';
  allBets.forEach(b => {
    const chosen = b.choice === 'p1' ? b.p1 : b.p2;
    csv += `${b.date},"${b.userName}","${b.p1} vs ${b.p2}","${chosen}",${b.quotaChosen},${b.pointsResult},${b.status}\n`;
  });
  dlFile('tennis_fantasy.csv', csv, 'text/csv');
}

function exportJSON() {
  dlFile('tennis_fantasy_backup.json', JSON.stringify({ bets: allBets, anteposts: allAnteposts, config: gameConfig }, null, 2), 'application/json');
}

function dlFile(name, content, type) {
  const a = document.createElement('a');
  a.href = URL.createObjectURL(new Blob([content], { type }));
  a.download = name; a.click();
}

// ── ATP RANKING POINTS ──
const ATP_POINTS = {
  slam:    { 1:2000, 2:1200, 3:720, 4:720, 5:360, 6:360, 7:360, 8:360 },
  masters: { 1:1000, 2:600,  3:360, 4:360, 5:180, 6:180, 7:180, 8:180 }
};

function getAtpPoints(cat, position) {
  const table = cat === 'Grand Slam' ? ATP_POINTS.slam : ATP_POINTS.masters;
  return table[Math.min(position, 8)] || 0;
}

function totalPtsForTournament(uid, tournamentId) {
  const bets = allBets.filter(b => b.userId === uid && b.tournament === tournamentId);
  const antes = allAnteposts.filter(a => a.userId === uid && a.tournament === tournamentId);
  const betPts = bets.reduce((s, b) => s + (b.pointsResult || 0), 0);
  const antePts = antes.reduce((s, a) => s + (a.pointsResult || 0), 0);
  return betPts + antePts;
}

// ── HOME TABS ──
let homeTab = 'torneo';
window.switchHomeTab = function(tab, btn) {
  // Legacy compat
  homeTab = tab;
};

function populateLbSelector() {
  const sel = document.getElementById('lb-selector');
  if (!sel) return;
  const prevValue = sel.value; // preserve current selection
  const history = gameConfig?.tournamentHistory || [];
  const currentId = gameConfig?.tournament;
  const currentT = currentId ? TOURNAMENTS.find(x => x.id === currentId) : null;
  const closedOpts = [...history].filter(h => h.closedAt).reverse().map(h => {
    const t = TOURNAMENTS.find(x => x.id === h.tournamentId);
    return `<option value="${h.tournamentId}">${h.tournamentEmoji||t?.emoji||'🎾'} ${h.tournamentName||t?.name||h.tournamentId}</option>`;
  }).join('');
  const currentOpt = currentT
    ? `<option value="__current__">🎾 ${currentT.emoji} ${currentT.name} (in corso)</option>`
    : `<option value="__current__">🎾 Torneo attivo</option>`;
  sel.innerHTML = currentOpt + `<option value="__season__">📅 Stagione totale</option>` + closedOpts;
  // Restore previous selection if still valid
  if (prevValue && [...sel.options].some(o => o.value === prevValue)) sel.value = prevValue;
}

window.onLbSelectorChange = function() {
  renderLeaderboard();
};

// ── LEADERBOARD LEGA (current user's league) ──
function renderLeaderboardLega() {
  const el = document.getElementById('leaderboard-lega');
  if (!el) return;
  // Find the user's league
  const myLeague = myLeagues[0]; // primary league
  if (!myLeague) {
    el.innerHTML = '<div class="empty"><div class="empty-i">🏅</div><h3>Nessuna lega</h3><p>Non sei in nessuna lega privata. Creane una o unisciti!</p></div>';
    return;
  }
  const members = myLeague.members || [];
  const tid = gameConfig?.tournament || '';
  const sorted = members
    .map(uid => {
      const u = allUsers.find(u => (u.id||u.uid) === uid);
      const betPts = allBets.filter(b => b.userId === uid && b.status !== 'pending' && (!b.tournament || b.tournament === tid)).reduce((s,b) => s+(b.pointsResult||0), 0);
      const antePts = allAnteposts.filter(a => a.userId === uid && a.status !== 'pending' && (!a.tournament || a.tournament === tid)).reduce((s,a) => s+(a.pointsResult||0), 0);
      return { uid, name: u?.name || uid, pts: betPts + antePts };
    })
    .sort((a,b) => b.pts - a.pts);

  const t = TOURNAMENTS.find(x => x.id === tid);
  el.innerHTML = `
    <div class="card" style="margin-bottom:10px;padding:12px 16px">
      <div style="font-family:'Bebas Neue';font-size:1rem;letter-spacing:1px">${myLeague.name}</div>
      <div style="font-size:.75rem;color:var(--dim)">${members.length} membri · ${t?.name || 'Torneo attivo'}</div>
    </div>` +
    sorted.map((p, i) => {
      const mine = p.uid === currentUser.uid;
      const avC = getAvatarColor(p.uid);
      const rn = i===0?'🥇':i===1?'🥈':i===2?'🥉':`${i+1}°`;
      return `<div class="lb-row ${mine?'mine':''}">
        <div class="rn" style="display:flex;align-items:center;gap:8px">
          <div style="width:32px;height:32px;border-radius:50%;background:${avC};display:flex;align-items:center;justify-content:center;font-weight:700;color:#fff">${(p.name||'?').charAt(0).toUpperCase()}</div>
          <span>${rn}</span>
        </div>
        <div><div class="lb-name">${p.name}${mine?' <span style="font-size:.68rem;color:var(--accent)">← tu</span>':''}</div></div>
        <div><div class="lb-pts">${p.pts>0?'+':''}${p.pts}</div><div class="lb-sub tr">pt</div></div>
      </div>`;
    }).join('');
}

// ── STORICO TORNEI CHIUSI ──
function renderStoricoClosed() {
  const el = document.getElementById('storico-tornei-list');
  if (!el) return;
  const history = (gameConfig?.tournamentHistory || []).filter(h => h.closedAt);
  if (!history.length) {
    el.innerHTML = '<div class="empty"><div class="empty-i">📜</div><h3>Nessun torneo chiuso</h3><p>Qui appariranno i tornei dopo che l\'admin li ha dichiarati finiti.</p></div>';
    return;
  }
  el.innerHTML = [...history].reverse().map(h => {
    const t = TOURNAMENTS.find(x => x.id === h.tournamentId);
    const top3 = (h.classifica||[]).slice(0,3);
    return `<div class="card" style="cursor:pointer" onclick="openHistDetail('${h.tournamentId}')">
      <div class="flex jb ic">
        <div>
          <div style="font-size:1.1rem">${h.tournamentEmoji||t?.emoji||'🎾'} <strong>${h.tournamentName||t?.name||h.tournamentId}</strong></div>
          <div class="dim" style="font-size:.78rem;margin-top:2px">${h.dates?.start||'?'} → ${h.dates?.end||'?'}</div>
          <div style="font-size:.78rem;margin-top:5px">${top3.map((p,i)=>`${['🥇','🥈','🥉'][i]} ${p.name} (${p.pts}pt)`).join(' · ')}</div>
        </div>
        <div style="color:var(--dim);font-size:1.2rem">›</div>
      </div>
    </div>`;
  }).join('');
}

function renderRankingGenerale() {
  const el = document.getElementById('ranking-generale');
  if (!el) return;
  // Get all completed tournaments from tournamentHistory in config
  const history = gameConfig?.tournamentHistory || [];
  if (!history.length) {
    el.innerHTML = '<div class="empty"><div class="empty-i">🌍</div><h3>Nessun torneo completato</h3><p>La classifica generale si costruisce torneo dopo torneo!</p></div>';
    return;
  }
  // Calculate ATP points for each user
  const atpPts = {};
  allUsers.forEach(u => { atpPts[u.id||u.uid] = 0; });
  history.forEach(th => {
    const t = TOURNAMENTS.find(x => x.id === th.tournamentId);
    if (!th.ranking) return;
    th.ranking.forEach((uid, i) => {
      const pos = i + 1;
      const pts = getAtpPoints(t?.cat || 'masters', pos);
      atpPts[uid] = (atpPts[uid] || 0) + pts;
    });
  });
  const sorted = [...allUsers].sort((a, b) => (atpPts[b.id||b.uid]||0) - (atpPts[a.id||a.uid]||0));
  el.innerHTML = `
    <div class="card" style="padding:12px 16px;margin-bottom:12px">
      <div style="font-size:.75rem;color:var(--dim)">Tornei completati: <strong>${history.length}</strong> · Punti ATP reali per posizione finale</div>
    </div>` +
    sorted.map((u, i) => {
      const uid = u.id||u.uid;
      const pts = atpPts[uid] || 0;
      const avC = getAvatarColor(uid);
      const mine = uid === currentUser.uid ? 'mine' : '';
      const rn = i===0?'🥇':i===1?'🥈':i===2?'🥉':i+1;
      return `<div class="lb-row ${mine}">
        <div class="rn" style="display:flex;align-items:center;gap:8px">
          <div style="width:32px;height:32px;border-radius:50%;background:${avC};display:flex;align-items:center;justify-content:center;font-weight:700;font-size:.9rem;color:#fff;flex-shrink:0">${u.name.charAt(0).toUpperCase()}</div>
          <span>${typeof rn==='string'?rn:rn+'°'}</span>
        </div>
        <div>
          <div class="lb-name">${u.name}${mine?' <span style="font-size:.68rem;color:var(--accent)">← tu</span>':''}</div>
          <div class="lb-sub">${history.map(th => {
            const idx = th.ranking?.indexOf(uid);
            if (idx === undefined || idx < 0) return '';
            const t = TOURNAMENTS.find(x=>x.id===th.tournamentId);
            const atp = getAtpPoints(t?.cat||'masters', idx+1);
            return `${t?.emoji||'🎾'} ${idx+1}° +${atp}pt`;
          }).filter(Boolean).join(' · ')}</div>
        </div>
        <div>
          <div class="lb-pts ${pts===0?'':''}">+${pts}</div>
          <div class="lb-sub tr">ATP pt</div>
        </div>
      </div>`;
    }).join('');
}

// ── CLOSE TOURNAMENT (admin) ──
window.closeTournament = async function() {
  const t = TOURNAMENTS.find(x => x.id === gameConfig?.tournament);
  if (!t) return toastErr('Nessun torneo attivo!');
  const history = gameConfig?.tournamentHistory || [];
  const alreadyClosed = history.find(h => h.tournamentId === gameConfig.tournament && h.closedAt);
  const msg = alreadyClosed
    ? `Aggiornare lo snapshot di ${t.name}? Sovrascrive il precedente.`
    : `Dichiarare ${t.name} come finito? Lo snapshot verrà salvato.`;
  if (!confirm(msg)) return;
  await saveTournamentSnapshot(gameConfig.tournament, true);
  toast(`🏆 ${t.name} chiuso! Classifica finale salvata.`);
  renderRankingGenerale();
  updateCloseTournamentCard();
};

// ── LIVE PAGE ──
let liveTab = 'bets';
let liveSelectedDate = null;

function isDeadlinePassed(date) {
  // Returns true if the betting deadline for that date has passed
  const deadline = gameConfig?.betDeadline || '23:00';
  const [dh, dm] = deadline.split(':').map(Number);
  const now = new Date();
  const { today, tomorrow } = getTournamentDates();
  if (date < today) return true; // past day always passed
  if (date === today) {
    const dl = new Date(); dl.setHours(dh, dm, 0, 0);
    return now > dl;
  }
  return false; // future date - not passed yet
}

function initLivePage() {
  const tid = gameConfig?.tournament || '';
  const deadline = gameConfig?.betDeadline || '23:00';
  const subEl = document.getElementById('live-sub');
  if (subEl) subEl.textContent = `Scommesse visibili dopo le ${deadline} di ogni giorno · Antepost visibili dopo l'inizio torneo`;

  // Build date list from bets in current tournament
  const { today: lToday } = getTournamentDates();
  const betsInTournament = allBets.filter(b => !b.tournament || b.tournament === tid);
  const dates = [...new Set(betsInTournament.map(b => b.date).filter(Boolean))].sort().reverse();

  // Build date tabs - only show dates where deadline has passed
  const dateTabsEl = document.getElementById('live-date-tabs');
  if (dateTabsEl) {
    const passedDates = dates.filter(d => isDeadlinePassed(d));
    if (!passedDates.length) {
      dateTabsEl.innerHTML = '<span style="font-size:.78rem;color:var(--dim)">Nessuna deadline passata ancora oggi</span>';
      liveSelectedDate = null;
    } else {
      if (!liveSelectedDate || !passedDates.includes(liveSelectedDate)) liveSelectedDate = passedDates[0];
      dateTabsEl.innerHTML = passedDates.map(d => {
        const { today: livToday } = getTournamentDates();
        const label = d === livToday ? 'Oggi' :
          new Date(d+'T12:00:00').toLocaleDateString('it', {day:'2-digit',month:'short'});
        return `<button class="btn btn-sm ${d===liveSelectedDate?'btn-p':'btn-g'}" onclick="selectLiveDate('${d}')">${label}</button>`;
      }).join('');
    }
  }
  renderLiveBets();
  renderLiveAnte();
}
window.initLivePage = initLivePage;

window.selectLiveDate = function(date) {
  liveSelectedDate = date;
  initLivePage();
};

window.switchLiveTab = function(tab, btn) {
  liveTab = tab;
  document.querySelectorAll('#page-live .tab').forEach(b => b.classList.remove('active'));
  if (btn) btn.classList.add('active');
  document.getElementById('live-bets-panel').style.display = tab === 'bets' ? 'block' : 'none';
  document.getElementById('live-ante-panel').style.display = tab === 'ante' ? 'block' : 'none';
};

function renderLiveBets() {
  const el = document.getElementById('live-bets-list');
  if (!el) return;
  const tid = gameConfig?.tournament || '';
  const minQ = gameConfig?.rules?.minQuota || 1.20;

  if (!liveSelectedDate) {
    el.innerHTML = '<div class="empty"><div class="empty-i">⏳</div><h3>Deadline non ancora raggiunta</h3><p>Le scommesse di oggi saranno visibili dopo le ' + (gameConfig?.betDeadline||'23:00') + '</p></div>';
    return;
  }

  const bets = allBets.filter(b => b.date === liveSelectedDate && (!b.tournament || b.tournament === tid));
  if (!bets.length) {
    el.innerHTML = '<div class="empty"><div class="empty-i">🎯</div><h3>Nessuna scommessa</h3><p>Nessuno ha scommesso per questo giorno.</p></div>';
    return;
  }

  // Group by match (p1 vs p2)
  const matches = {};
  bets.forEach(b => {
    const key = [b.p1, b.p2].sort().join(' vs ');
    if (!matches[key]) matches[key] = { p1: b.p1, p2: b.p2, bets: [] };
    matches[key].bets.push(b);
  });

  el.innerHTML = Object.values(matches).map(m => {
    const statusIcon = { pending:'⏳', win:'✅', loss:'❌' };
    return `<div class="card">
      <div style="font-weight:700;margin-bottom:10px;font-size:.9rem">🎾 ${m.p1} <span style="color:var(--dim)">vs</span> ${m.p2}</div>
      ${m.bets.map(b => {
        const chosen = b.choice === 'p1' ? b.p1 : b.p2;
        const avC = getAvatarColor(b.userId);
        const pts = b.pointsResult || 0;
        const isMe = b.userId === currentUser.uid;
        return `<div style="display:flex;align-items:center;gap:10px;padding:7px 0;border-top:1px solid rgba(255,255,255,.05)">
          <div style="width:28px;height:28px;border-radius:50%;background:${avC};display:flex;align-items:center;justify-content:center;font-weight:700;color:#fff;font-size:.75rem;flex-shrink:0">${b.userName?.charAt(0)?.toUpperCase()||'?'}</div>
          <div style="flex:1">
            <div style="font-size:.82rem;font-weight:${isMe?'700':'400'}">${b.userName}${isMe?' <span style="color:var(--accent);font-size:.65rem">← tu</span>':''}</div>
            <div style="font-size:.75rem;color:var(--dim)">su <strong>${chosen}</strong> · quota <span style="color:var(--accent)">${b.quotaChosen}</span></div>
          </div>
          <div style="text-align:right">
            <div style="font-size:.8rem">${statusIcon[b.status]||'⏳'}</div>
            ${b.status!=='pending'?`<div style="font-size:.75rem;color:${pts>0?'var(--accent)':'var(--red)'};font-weight:700">${pts>0?'+':''}${pts}pt</div>`:''}
          </div>
        </div>`;
      }).join('')}
    </div>`;
  }).join('');
}

function renderLiveAnte() {
  const el = document.getElementById('live-ante-list');
  if (!el) return;
  const tid = gameConfig?.tournament || '';
  const open = isAntepostOpen();

  if (open) {
    el.innerHTML = '<div class="empty"><div class="empty-i">🔒</div><h3>Antepost ancora aperti</h3><p>Le scelte degli altri saranno visibili dopo l\'inizio del torneo (' + (gameConfig?.tournamentDates?.start||'?') + ')</p></div>';
    return;
  }

  // Group by question
  const qs = gameConfig?.anteQuestions || [];
  const antes = allAnteposts.filter(a => !a.tournament || a.tournament === tid);

  if (!antes.length) {
    el.innerHTML = '<div class="empty"><div class="empty-i">📋</div><h3>Nessun antepost</h3></div>';
    return;
  }

  el.innerHTML = qs.map((q, qi) => {
    const answers = antes.filter(a => a.questionIdx === qi);
    if (!answers.length) return '';
    // Group by user
    const byUser = {};
    answers.forEach(a => {
      if (!byUser[a.userId]) byUser[a.userId] = { name: a.userName, uid: a.userId, picks: [] };
      byUser[a.userId].picks.push(a);
    });
    const statusIcon = { pending:'⏳', win:'✅', loss:'❌' };
    return `<div class="card">
      <div style="font-weight:700;margin-bottom:10px">${q.text}</div>
      ${Object.values(byUser).map(u => {
        const isMe = u.uid === currentUser.uid;
        const avC = getAvatarColor(u.uid);
        const totalPts = u.picks.reduce((s,a) => s+(a.pointsResult||0), 0);
        return `<div style="display:flex;align-items:flex-start;gap:10px;padding:7px 0;border-top:1px solid rgba(255,255,255,.05)">
          <div style="width:28px;height:28px;border-radius:50%;background:${avC};display:flex;align-items:center;justify-content:center;font-weight:700;color:#fff;font-size:.75rem;flex-shrink:0">${u.name?.charAt(0)?.toUpperCase()||'?'}</div>
          <div style="flex:1">
            <div style="font-size:.82rem;font-weight:${isMe?'700':'400'}">${u.name}${isMe?' <span style="color:var(--accent);font-size:.65rem">← tu</span>':''}</div>
            <div style="font-size:.75rem;color:var(--dim);margin-top:2px">${u.picks.map(a=>`<span style="margin-right:6px">${statusIcon[a.status]||'⏳'} ${a.tennisPlayer}</span>`).join('')}</div>
          </div>
          ${u.picks.some(a=>a.status!=='pending')?`<div style="font-size:.8rem;font-weight:700;color:${totalPts>0?'var(--accent)':'var(--red)'}">${totalPts>0?'+':''}${totalPts}pt</div>`:''}
        </div>`;
      }).join('')}
    </div>`;
  }).filter(Boolean).join('');
}

// ── IMPORT RANKING (CSV/text) ──
window.importRankingFile = async function(input) {
  const file = input.files[0];
  if (!file) return;
  const text = await file.text();
  await processRankingText(text);
  input.value = '';
};

window.importRankingText = async function() {
  const text = document.getElementById('ranking-paste-area')?.value || '';
  if (!text.trim()) return toastErr('Nessun testo inserito!');
  await processRankingText(text);
  document.getElementById('ranking-paste-area').value = '';
  document.getElementById('ranking-paste-area').style.display = 'none';
  document.getElementById('ranking-paste-btn').style.display = 'none';
};

async function processRankingText(text) {
  const lines = text.split('\n').map(l => l.trim()).filter(Boolean);
  const ranking = [];
  for (const line of lines) {
    // Try CSV: rank,name,country,points
    if (line.includes(',')) {
      const parts = line.split(',').map(s => s.trim());
      const rank = parseInt(parts[0]);
      const name = parts[1];
      if (rank && name) ranking.push({ rank, name, country: parts[2]||'', points: parts[3]||'' });
      continue;
    }
    // Try "1. Name Country" or "1 Name Country"
    const m = line.match(/^(\d+)[.\s]+(.+?)\s+([A-Z]{2,3})\s*$/);
    if (m) { ranking.push({ rank: parseInt(m[1]), name: m[2].trim(), country: m[3] }); continue; }
    // Try "1. Name" or "1 Name"
    const m2 = line.match(/^(\d+)[.\s]+(.+)$/);
    if (m2) { ranking.push({ rank: parseInt(m2[1]), name: m2[2].trim() }); }
  }
  if (ranking.length < 3) return toastErr('Nessun dato valido trovato. Controlla il formato.');
  ranking.sort((a,b) => a.rank - b.rank);
  await updateConfig({ atpRanking: ranking, atpRankingUpdated: new Date().toISOString() });
  // Update in-memory gameConfig immediately so renderRankingPage reads fresh data
  if (gameConfig) { gameConfig.atpRanking = ranking; gameConfig.atpRankingUpdated = new Date().toISOString(); }
  toast(`✅ ${ranking.length} giocatori importati!`);
  renderRankingPage();
  loadFixedAnteSettings();
}

// ── ANTEPOST QUESTIONS ──
let anteQuestions = [];

function renderAnteQuestions() {
  // Admin builder
  const el = document.getElementById('ante-q-list');
  if (!el) return;
  const qs = gameConfig?.anteQuestions || [];
  anteQuestions = [...qs];
  if (!qs.length) { el.innerHTML = '<div class="dim" style="font-size:.8rem;margin-bottom:8px">Nessuna domanda ancora.</div>'; return; }
  el.innerHTML = qs.map((q, i) => `
    <div class="mr" style="padding:10px 14px;margin-bottom:6px;align-items:flex-start">
      <div style="flex:1">
        <div style="font-weight:600;font-size:.9rem">${q.text}</div>
        <div style="font-size:.75rem;color:var(--dim);margin-top:3px">${q.type==='multiple'?`${q.count} risposte`:'1 risposta'} · <span style="color:var(--gold)">+${q.pts}pt</span></div>
      </div>
      <button class="btn btn-d btn-sm" onclick="removeAnteQuestion(${i})">✕</button>
    </div>`).join('');
}

window.updateAQForm = function() {
  const t = document.getElementById('aq-type')?.value;
  const w = document.getElementById('aq-count-wrap');
  if (w) w.style.display = t === 'multiple' ? 'block' : 'none';
};

window.addAnteQuestion = function() {
  const text = document.getElementById('aq-text')?.value.trim();
  const type = document.getElementById('aq-type')?.value;
  const count = parseInt(document.getElementById('aq-count')?.value) || 4;
  const pts = parseInt(document.getElementById('aq-pts')?.value) || 20;
  if (!text) return toastErr('Inserisci il testo della domanda!');
  anteQuestions.push({ text, type, count: type==='multiple'?count:1, pts });
  document.getElementById('aq-text').value = '';
  renderAnteQuestions();
};

window.removeAnteQuestion = function(i) {
  anteQuestions.splice(i, 1);
  renderAnteQuestions();
};

window.saveAnteQuestions = async function() {
  if (activeContext !== 'general') {
    // Save to league
    await updateDoc(doc(db, 'leagues', activeContext), { anteQuestions });
    toast('✅ Domande salvate per la lega!');
  } else {
    await updateConfig({ anteQuestions });
    toast('✅ Domande salvate! Vai su Antepost per vederle.');
  }
  renderAntepostQuestions();
};

window.saveFixedAnteQuestions = async function() {
  // If in league context, save to league
  const saveTarget = activeContext !== 'general' ? 'league' : 'global';
  const winnerPts = parseInt(document.getElementById('aq-winner-pts')?.value) || 30;
  const semiPts = parseInt(document.getElementById('aq-semi-pts')?.value) || 10;
  const outsiderRank = parseInt(document.getElementById('aq-outsider-rank')?.value) || 30;
  const outsiderPhase = document.getElementById('aq-outsider-phase')?.value || 'QF';
  const outsiderPts = parseInt(document.getElementById('aq-outsider-pts')?.value) || 25;
  const phaseLabels = { R16:'ottavi', QF:'quarti', SF:'semifinali', F:'finale', W:'vincitore' };
  const phaseLabel = phaseLabels[outsiderPhase] || outsiderPhase;
  const outsiderText = `Chi fuori dal top ${outsiderRank} raggiungerà i ${phaseLabel}?`;
  const atpRanking = gameConfig?.atpRanking || [];
  const fixedQs = [
    { text: 'Chi vincerà il torneo?', type: 'single', count: 1, pts: winnerPts, fixed: 'winner' },
    { text: 'Chi saranno i 4 semifinalisti?', type: 'multiple', count: 4, pts: semiPts, ptsEach: semiPts, fixed: 'semifinal' },
    { text: outsiderText, type: 'single', count: 1, pts: outsiderPts, fixed: 'outsider',
      outsiderRank, outsiderPhase, atpRankingSnapshot: atpRanking.slice(0, 200) }
  ];
  if (activeContext !== 'general') {
    await updateDoc(doc(db, 'leagues', activeContext), { anteQuestions: fixedQs });
    toast(`✅ Domande salvate per la lega!`);
  } else {
    await updateConfig({ anteQuestions: fixedQs });
    toast('✅ Domande salvate! Vai su Antepost.');
  }
  renderAntepostQuestions();
};

// Load fixed question settings into admin form
function loadFixedAnteSettings() {
  const qs = gameConfig?.anteQuestions || [];
  const winner = qs.find(q => q.fixed === 'winner');
  const semi = qs.find(q => q.fixed === 'semifinal');
  const outsider = qs.find(q => q.fixed === 'outsider');
  const wpEl = document.getElementById('aq-winner-pts');
  const spEl = document.getElementById('aq-semi-pts');
  const stEl = document.getElementById('aq-semi-total');
  if (wpEl && winner) wpEl.value = winner.pts || 30;
  if (spEl && semi) { spEl.value = semi.ptsEach || semi.pts || 10; }
  if (stEl && semi) stEl.textContent = ((semi.ptsEach||semi.pts||10) * 4) + ' pt';
  if (outsider) {
    const orEl = document.getElementById('aq-outsider-rank');
    const opEl = document.getElementById('aq-outsider-phase');
    const oEl = document.getElementById('aq-outsider-pts');
    if (orEl) orEl.value = outsider.outsiderRank || 30;
    if (opEl) opEl.value = outsider.outsiderPhase || 'QF';
    if (oEl) oEl.value = outsider.pts || 25;
  }
  // Show ATP ranking status
  const atpRanking = gameConfig?.atpRanking || [];
  const statusEl = document.getElementById('atp-rank-status');
  if (statusEl) {
    if (atpRanking.length) {
      const updated = gameConfig?.atpRankingUpdated ? new Date(gameConfig.atpRankingUpdated).toLocaleDateString('it') : '?';
      statusEl.textContent = `${atpRanking.length} giocatori · aggiornato ${updated}`;
      statusEl.style.color = 'var(--accent)';
    } else {
      statusEl.textContent = 'non caricato';
      statusEl.style.color = 'var(--red)';
    }
  }
}

// ── RANKING PAGE ──
function renderRankingPage() {
  const tbody = document.getElementById('ranking-tbody');
  const statusEl = document.getElementById('ranking-status');
  if (!tbody) return;
  const rawRanking = gameConfig?.atpRanking || [];
  // Deduplicate by rank — keep longest name per rank (fixes old cached bad data)
  const byRank = {};
  rawRanking.forEach(p => {
    if (!p.rank || !p.name) return;
    const isValidName = /[A-Za-zÀ-ÿ]{2,}/.test(p.name) && !/^\d/.test(p.name);
    if (!isValidName) return; // skip entries where name is actually a number
    if (!byRank[p.rank] || p.name.length > byRank[p.rank].name.length) byRank[p.rank] = p;
  });
  const ranking = Object.values(byRank).sort((a,b) => a.rank - b.rank);
  const search = (document.getElementById('ranking-search')?.value || '').toLowerCase();
  const filtered = search ? ranking.filter(p => p.name?.toLowerCase().includes(search)) : ranking;
  const updated = gameConfig?.atpRankingUpdated
    ? new Date(gameConfig.atpRankingUpdated).toLocaleDateString('it', {day:'2-digit',month:'long',year:'numeric'})
    : null;
  if (statusEl) {
    const hasBadData = rawRanking.length > 0 && ranking.length < rawRanking.length * 0.6;
    statusEl.innerHTML = ranking.length
      ? `✅ ${ranking.length} giocatori · aggiornato ${updated||'?'}${hasBadData ? ' <button class="btn btn-g btn-sm" style="margin-left:8px" onclick="loadAtpRanking(true)">🔄 Ricarica (dati corrotti)</button>' : ''}`
      : 'Ranking non ancora caricato — clicca Aggiorna';
    statusEl.style.color = ranking.length ? 'var(--accent)' : 'var(--dim)';
  }
  if (!filtered.length) {
    tbody.innerHTML = '<tr><td colspan="4" style="text-align:center;padding:24px;color:var(--dim)">Nessun giocatore trovato</td></tr>';
    return;
  }
  const outsiderRank = (gameConfig?.anteQuestions||[]).find(q=>q.fixed==='outsider')?.outsiderRank || 0;
  tbody.innerHTML = filtered.slice(0,200).map(p => {
    const isOutsider = outsiderRank && p.rank > outsiderRank;
    return `<tr style="${isOutsider?'opacity:.6':''}">
      <td class="mono" style="font-size:.8rem;color:${isOutsider?'var(--dim)':'var(--accent)'}">${p.rank}</td>
      <td style="font-weight:${p.rank<=10?'700':'400'}">${p.name}${isOutsider?' <span style="font-size:.65rem;color:var(--dim)">outsider</span>':''}</td>
      <td style="font-size:.75rem;color:var(--dim)">${p.country||'—'}</td>
      <td class="mono" style="font-size:.75rem">${p.points||'—'}</td>
    </tr>`;
  }).join('');
}
window.renderRankingPage = renderRankingPage;

// Load ATP ranking via CORS proxy scraping atptour.com
window.loadAtpRanking = async function(fromRankingPage = false) {
  const statusEl = fromRankingPage ? document.getElementById('ranking-status') : document.getElementById('atp-rank-status');
  const previewEl = document.getElementById('atp-rank-preview');
  if (statusEl) { statusEl.textContent = '⏳ Caricamento ranking ATP...'; statusEl.style.color = 'var(--dim)'; }

  try {
    let ranking = [];

    // Source 1: RapidAPI tennis live standings (free tier, no key needed for basic)
    // Source 2: GitHub raw data (community-maintained ATP rankings)
    // Source 3: Wikipedia ATP rankings page via proxy
    const isValidName = txt => /[A-Za-zÀ-ÿ]{2,}/.test(txt) && txt.length >= 3 && txt.length <= 45 && !/^\d/.test(txt);

    const sources = [
      // Source 1: JeffSackmann tennis_atp — merge rankings CSV + players CSV for full names
      // rankings_current.csv: ranking_date,rank,player_id,points
      // atp_players.csv: player_id,first_name,last_name,hand,dob,country
      async () => {
        const base = 'https://raw.githubusercontent.com/JeffSackmann/tennis_atp/master';
        const [rRes, pRes] = await Promise.all([
          fetch(`https://api.allorigins.win/get?url=${encodeURIComponent(base+'/atp_rankings_current.csv')}`, { signal: AbortSignal.timeout(10000) }),
          fetch(`https://api.allorigins.win/get?url=${encodeURIComponent(base+'/atp_players.csv')}`, { signal: AbortSignal.timeout(10000) })
        ]);
        if (!rRes.ok || !pRes.ok) throw new Error('fail');
        const rW = await rRes.json();
        const pW = await pRes.json();

        // Parse players CSV → map id → {name, country}
        const playerMap = {};
        pW.contents.split('\n').slice(1).forEach(line => {
          const cols = line.split(',');
          if (cols.length < 3) return;
          const id = cols[0]?.trim();
          const first = cols[1]?.trim();
          const last = cols[2]?.trim();
          const country = cols[5]?.trim() || '';
          if (id && (first || last)) playerMap[id] = { name: `${first} ${last}`.trim(), country };
        });

        // Parse rankings CSV → get latest date rows only
        const rankLines = rW.contents.split('\n').slice(1).filter(Boolean);
        // Find the most recent date
        let latestDate = '';
        rankLines.forEach(line => {
          const d = line.split(',')[0]?.trim();
          if (d > latestDate) latestDate = d;
        });

        const result = [];
        rankLines.forEach(line => {
          const cols = line.split(',');
          if (cols[0]?.trim() !== latestDate) return;
          const rank = parseInt(cols[1]);
          const playerId = cols[2]?.trim();
          const points = cols[3]?.trim();
          const player = playerMap[playerId];
          if (rank && player?.name && isValidName(player.name)) {
            result.push({ rank, name: player.name, country: player.country, points });
          }
        });

        if (result.length < 50) throw new Error('not enough data');
        return result.sort((a, b) => a.rank - b.rank);
      },
      // Source 2: Wikipedia ATP rankings page (covers ~54, used as partial fallback)
      async () => {
        const res = await fetch(`https://api.allorigins.win/get?url=${encodeURIComponent('https://en.wikipedia.org/wiki/ATP_Rankings')}`, { signal: AbortSignal.timeout(10000) });
        if (!res.ok) throw new Error('fail');
        const w = await res.json();
        const parser = new DOMParser();
        const doc = parser.parseFromString(w.contents, 'text/html');
        const byRank = {};
        doc.querySelectorAll('table.wikitable').forEach(table => {
          table.querySelectorAll('tr').forEach(row => {
            const cells = row.querySelectorAll('td');
            if (cells.length < 3) return;
            const rank = parseInt(cells[0]?.textContent?.trim());
            if (!rank || rank > 500) return;
            const names = [];
            let pts = '';
            for (let i = 1; i < cells.length; i++) {
              const txt = cells[i]?.textContent?.trim().replace(/\s+/g,' ');
              if (isValidName(txt)) names.push(txt);
            }
            for (let i = cells.length-1; i >= 1; i--) {
              const txt = cells[i]?.textContent?.trim();
              if (/^[\d,.']+$/.test(txt) && parseInt(txt.replace(/[,.']/g,'')) > 100) { pts = txt; break; }
            }
            const name = names.reduce((a,b) => b.length > a.length ? b : a, '');
            if (rank && name) {
              if (!byRank[rank] || name.length > byRank[rank].name.length) byRank[rank] = { rank, name, points: pts };
            }
          });
        });
        const result = Object.values(byRank);
        if (result.length < 10) throw new Error('Wikipedia parse failed');
        return result;
      },
      // Source 3: ATP official via corsproxy
      async () => {
        const url = 'https://www.atptour.com/en/rankings/singles';
        const res = await fetch(`https://corsproxy.io/?${encodeURIComponent(url)}`, { signal: AbortSignal.timeout(10000) });
        if (!res.ok) throw new Error('fail');
        const text = await res.text();
        const parser = new DOMParser();
        const doc = parser.parseFromString(text, 'text/html');
        const result = [];
        doc.querySelectorAll('tbody tr, tr').forEach(row => {
          const cells = row.querySelectorAll('td');
          if (cells.length < 3) return;
          const rank = parseInt(cells[0]?.textContent?.trim());
          if (!rank || rank > 500) return;
          let name = '';
          for (let i = 1; i < cells.length; i++) {
            const txt = cells[i]?.textContent?.trim().replace(/\s+/g,' ');
            if (isValidName(txt)) { name = txt; break; }
          }
          if (rank && name) result.push({ rank, name });
        });
        if (result.length < 10) throw new Error('ATP site parse failed');
        return result;
      }
    ];

    for (const trySource of sources) {
      try {
        const result = await trySource();
        if (result && result.length >= 10) { ranking = result; break; }
      } catch(e) { continue; }
    }

    if (ranking.length < 5) {
      // Ultimate fallback: hardcoded ATP top 100 (updated March 2026)
      ranking = [
        {rank:1,name:'Jannik Sinner',country:'ITA',points:'11350'},
        {rank:2,name:'Alexander Zverev',country:'GER',points:'8640'},
        {rank:3,name:'Carlos Alcaraz',country:'ESP',points:'8390'},
        {rank:4,name:'Taylor Fritz',country:'USA',points:'5310'},
        {rank:5,name:'Novak Djokovic',country:'SRB',points:'5010'},
        {rank:6,name:'Casper Ruud',country:'NOR',points:'4785'},
        {rank:7,name:'Daniil Medvedev',country:'RUS',points:'4765'},
        {rank:8,name:'Tommy Paul',country:'USA',points:'4270'},
        {rank:9,name:'Andrey Rublev',country:'RUS',points:'4245'},
        {rank:10,name:'Alex de Minaur',country:'AUS',points:'4210'},
        {rank:11,name:'Stefanos Tsitsipas',country:'GRE',points:'3840'},
        {rank:12,name:'Lorenzo Musetti',country:'ITA',points:'3710'},
        {rank:13,name:'Hubert Hurkacz',country:'POL',points:'3560'},
        {rank:14,name:'Grigor Dimitrov',country:'BUL',points:'3440'},
        {rank:15,name:'Ben Shelton',country:'USA',points:'3115'},
        {rank:16,name:'Holger Rune',country:'DEN',points:'3020'},
        {rank:17,name:'Frances Tiafoe',country:'USA',points:'2710'},
        {rank:18,name:'Ugo Humbert',country:'FRA',points:'2635'},
        {rank:19,name:'Sebastian Baez',country:'ARG',points:'2600'},
        {rank:20,name:'Jack Draper',country:'GBR',points:'2560'},
        {rank:21,name:'Felix Auger-Aliassime',country:'CAN',points:'2415'},
        {rank:22,name:'Karen Khachanov',country:'RUS',points:'2380'},
        {rank:23,name:'Alejandro Davidovich Fokina',country:'ESP',points:'2195'},
        {rank:24,name:'Nicolas Jarry',country:'CHI',points:'2060'},
        {rank:25,name:'Tomas Machac',country:'CZE',points:'2055'},
        {rank:26,name:'Francisco Cerundolo',country:'ARG',points:'2035'},
        {rank:27,name:'Jakub Mensik',country:'CZE',points:'1975'},
        {rank:28,name:'Arthur Fils',country:'FRA',points:'1965'},
        {rank:29,name:'Brandon Nakashima',country:'USA',points:'1855'},
        {rank:30,name:'Flavio Cobolli',country:'ITA',points:'1800'},
        {rank:31,name:'Jiri Lehecka',country:'CZE',points:'1785'},
        {rank:32,name:'Gael Monfils',country:'FRA',points:'1745'},
        {rank:33,name:'Cameron Norrie',country:'GBR',points:'1670'},
        {rank:34,name:'Jordan Thompson',country:'AUS',points:'1620'},
        {rank:35,name:'Jan-Lennard Struff',country:'GER',points:'1595'},
        {rank:36,name:'Alexei Popyrin',country:'AUS',points:'1570'},
        {rank:37,name:'Botic van de Zandschulp',country:'NED',points:'1555'},
        {rank:38,name:'Nuno Borges',country:'POR',points:'1540'},
        {rank:39,name:'Matteo Berrettini',country:'ITA',points:'1520'},
        {rank:40,name:'Denis Shapovalov',country:'CAN',points:'1495'},
        {rank:41,name:'Tallon Griekspoor',country:'NED',points:'1480'},
        {rank:42,name:'Matteo Arnaldi',country:'ITA',points:'1460'},
        {rank:43,name:'Roman Safiullin',country:'RUS',points:'1440'},
        {rank:44,name:'Sebastian Korda',country:'USA',points:'1415'},
        {rank:45,name:'Lorenzo Sonego',country:'ITA',points:'1395'},
        {rank:46,name:'Mariano Navone',country:'ARG',points:'1370'},
        {rank:47,name:'Giovanni Mpetshi Perricard',country:'FRA',points:'1345'},
        {rank:48,name:'Pavel Kotov',country:'RUS',points:'1320'},
        {rank:49,name:'Luciano Darderi',country:'ITA',points:'1295'},
        {rank:50,name:'Miomir Kecmanovic',country:'SRB',points:'1270'},
        {rank:51,name:'Yoshihito Nishioka',country:'JPN',points:'1250'},
        {rank:52,name:'Yannick Hanfmann',country:'GER',points:'1230'},
        {rank:53,name:'Arthur Rinderknech',country:'FRA',points:'1215'},
        {rank:54,name:'Marcos Giron',country:'USA',points:'1200'},
        {rank:55,name:'Taro Daniel',country:'JPN',points:'1185'},
        {rank:56,name:'Quentin Halys',country:'FRA',points:'1165'},
        {rank:57,name:'Fabian Marozsan',country:'HUN',points:'1150'},
        {rank:58,name:'Aleksandar Kovacevic',country:'USA',points:'1135'},
        {rank:59,name:'Christopher Eubanks',country:'USA',points:'1120'},
        {rank:60,name:'Dusan Lajovic',country:'SRB',points:'1100'},
        {rank:61,name:'Daniel Altmaier',country:'GER',points:'1085'},
        {rank:62,name:'Thanasi Kokkinakis',country:'AUS',points:'1070'},
        {rank:63,name:'Gabriel Diallo',country:'CAN',points:'1055'},
        {rank:64,name:'Rinky Hijikata',country:'AUS',points:'1040'},
        {rank:65,name:'Maxime Cressy',country:'USA',points:'1025'},
        {rank:66,name:'Juncheng Shang',country:'CHN',points:'1010'},
        {rank:67,name:'Alex Michelsen',country:'USA',points:'995'},
        {rank:68,name:'Luca Van Assche',country:'FRA',points:'980'},
        {rank:69,name:'Roberto Bautista Agut',country:'ESP',points:'965'},
        {rank:70,name:'Corentin Moutet',country:'FRA',points:'950'},
        {rank:71,name:'Hugo Gaston',country:'FRA',points:'935'},
        {rank:72,name:'Pedro Martinez',country:'ESP',points:'920'},
        {rank:73,name:'Tim van Rijthoven',country:'NED',points:'910'},
        {rank:74,name:'Nicolas Mahut',country:'FRA',points:'895'},
        {rank:75,name:'David Goffin',country:'BEL',points:'880'},
        {rank:76,name:'Camilo Ugo Carabelli',country:'ARG',points:'865'},
        {rank:77,name:'Constant Lestienne',country:'FRA',points:'855'},
        {rank:78,name:'Zizou Bergs',country:'BEL',points:'845'},
        {rank:79,name:'Dominic Thiem',country:'AUT',points:'830'},
        {rank:80,name:'Emilio Nava',country:'USA',points:'820'},
        {rank:81,name:'Wu Yibing',country:'CHN',points:'810'},
        {rank:82,name:'Pedro Cachin',country:'ARG',points:'800'},
        {rank:83,name:'James Duckworth',country:'AUS',points:'790'},
        {rank:84,name:'Gregoire Barrere',country:'FRA',points:'780'},
        {rank:85,name:'Thiago Seyboth Wild',country:'BRA',points:'770'},
        {rank:86,name:'Max Purcell',country:'AUS',points:'760'},
        {rank:87,name:'Hugo Dellien',country:'BOL',points:'750'},
        {rank:88,name:'Vasek Pospisil',country:'CAN',points:'740'},
        {rank:89,name:'Laslo Djere',country:'SRB',points:'730'},
        {rank:90,name:'Christopher O\'Connell',country:'AUS',points:'720'},
        {rank:91,name:'Mackenzie McDonald',country:'USA',points:'710'},
        {rank:92,name:'Alex Bolt',country:'AUS',points:'700'},
        {rank:93,name:'Aleksandar Vukic',country:'AUS',points:'690'},
        {rank:94,name:'Otto Virtanen',country:'FIN',points:'680'},
        {rank:95,name:'Mattia Bellucci',country:'ITA',points:'670'},
        {rank:96,name:'Adrian Mannarino',country:'FRA',points:'660'},
        {rank:97,name:'Giulio Zeppieri',country:'ITA',points:'650'},
        {rank:98,name:'Duje Ajdukovic',country:'CRO',points:'640'},
        {rank:99,name:'Dominic Stricker',country:'SUI',points:'630'},
        {rank:100,name:'Learner Tien',country:'USA',points:'620'},
      ];
      if (statusEl) { statusEl.textContent = '⚠️ Dati offline (marzo 2026) — clicca Aggiorna per provare a ricaricare'; statusEl.style.color = 'var(--gold)'; }
      toast('⚠️ Usati dati offline — aggiorna per dati live');
    }

    // Final dedup: keep only longest name per rank, then sort
    const byRank = {};
    ranking.forEach(p => {
      if (!byRank[p.rank] || (p.name||'').length > (byRank[p.rank].name||'').length) byRank[p.rank] = p;
    });
    ranking = Object.values(byRank);

    // Sort and save
    ranking.sort((a,b) => a.rank - b.rank);
    const sliced = ranking.slice(0,500);
    await updateConfig({ atpRanking: sliced, atpRankingUpdated: new Date().toISOString() });
    // Update in-memory immediately so render reads fresh data
    if (gameConfig) { gameConfig.atpRanking = sliced; gameConfig.atpRankingUpdated = new Date().toISOString(); }

    if (statusEl) { statusEl.textContent = `✅ ${ranking.length} giocatori caricati`; statusEl.style.color = 'var(--accent)'; }
    if (previewEl) {
      const outsiderRank = parseInt(document.getElementById('aq-outsider-rank')?.value) || 30;
      const outsiders = sliced.filter(p => p.rank > outsiderRank).slice(0,5);
      previewEl.textContent = `Esempio outsider (fuori top ${outsiderRank}): ${outsiders.map(p=>`${p.rank}. ${p.name}`).join(' · ')}`;
      previewEl.style.display = 'block';
    }
    toast(`✅ Ranking ATP caricato! ${ranking.length} giocatori`);
    // Render ranking page if currently visible
    if (document.getElementById('page-ranking')?.classList.contains('active')) renderRankingPage();
    loadFixedAnteSettings();
  } catch(e) {
    if (statusEl) { statusEl.textContent = '❌ Errore: ' + e.message; statusEl.style.color = 'var(--red)'; }
    toastErr('Errore caricamento ranking: ' + e.message);
  }
};

// Render antepost questions for users
function isAntepostOpen() {
  // Antepost closes when tournament starts (start date)
  const start = gameConfig?.tournamentDates?.start;
  if (!start) return true;
  const today = new Date().toISOString().split('T')[0];
  return today < start;
}

function renderAntepostQuestions() {
  const el = document.getElementById('ante-questions-list');
  const myEl = document.getElementById('ante-my-answers');
  if (!el) return;
  const qs = gameConfig?.anteQuestions || [];
  const t = gameConfig?.tournament ? TOURNAMENTS.find(x=>x.id===gameConfig.tournament) : null;
  const isAdmin = userProfile?.isAdmin;
  const open = isAntepostOpen(); // true = before tournament start
  const start = gameConfig?.tournamentDates?.start;

  const subEl = document.getElementById('ante-sub');
  if (subEl) {
    if (!open) {
      subEl.textContent = `${t?.emoji||'🎾'} ${t?.name||''} — torneo iniziato, pronostici chiusi!`;
      subEl.style.color = 'var(--red)';
    } else {
      subEl.textContent = t ? `${t.emoji} ${t.name} — rispondi entro il ${start||'inizio torneo'}!` : 'Pronostici fatti prima del torneo.';
      subEl.style.color = '';
    }
  }

  // My existing answers (always visible to myself)
  const myAnswers = allAnteposts.filter(a => a.userId === currentUser.uid && (!a.tournament || a.tournament === (gameConfig?.tournament||'')));

  if (!qs.length && !isAdmin) {
    el.innerHTML = '<div class="empty"><div class="empty-i">📋</div><h3>Nessun pronostico disponibile</h3><p>L\'admin non ha ancora configurato gli antepost per questo torneo.</p></div>';
    if (myEl) myEl.innerHTML = '';
    return;
  }

  if (!qs.length && isAdmin) {
    el.innerHTML = '<div class="card" style="text-align:center;padding:20px;color:var(--dim)">Vai in <strong>Admin → Domande Antepost</strong> per configurare le domande!</div>';
    if (myEl) myEl.innerHTML = '';
    return;
  }

  el.innerHTML = qs.map((q, qi) => {
    const answered = myAnswers.filter(a => a.questionIdx === qi);
    const isAnswered = answered.length >= q.count;
    const isSemi = q.fixed === 'semifinal';
    const isOutsider = q.fixed === 'outsider';
    const ptsLabel = isSemi
      ? `${q.ptsEach||q.pts}pt per ogni azzeccato (max ${(q.ptsEach||q.pts)*4}pt)`
      : isOutsider
        ? `${q.pts}pt se corretto · fuori top ${q.outsiderRank||30}`
        : `${q.pts}pt se corretto`;

    // Count how many users have answered (for admin)
    const allAnswersForQ = allAnteposts.filter(a => a.questionIdx === qi && (!a.tournament || a.tournament === (gameConfig?.tournament||'')));
    const answeredUserCount = [...new Set(allAnswersForQ.map(a=>a.userId))].length;

    // After deadline (tournament started): show others' answers; before: only show "X hanno risposto"
    const othersBlock = isAdmin
      ? `<div style="margin-top:10px;background:rgba(255,255,255,.03);border-radius:8px;padding:10px">
          <div style="font-size:.75rem;color:var(--dim);margin-bottom:6px">RISPOSTE (${answeredUserCount}/${allUsers.length}):</div>
          ${allUsers.map(u => {
            const ans = allAnteposts.filter(a=>a.userId===(u.id||u.uid)&&a.questionIdx===qi&&(!a.tournament||a.tournament===(gameConfig?.tournament||'')));
            return ans.length ? `<div style="font-size:.78rem;margin-bottom:4px"><strong>${u.name}:</strong> ${ans.map(a=>a.tennisPlayer).join(', ')}</div>` : '';
          }).join('')}
        </div>`
      : !open
        ? `<div style="margin-top:8px;font-size:.75rem;color:var(--dim)">👥 ${answeredUserCount} giocatori hanno risposto — vedi tutte le scelte nella sezione <strong>👁️ Live → Antepost</strong></div>`
        : `<div style="margin-top:8px;font-size:.75rem;color:var(--dim)">👥 ${answeredUserCount} giocatori hanno già risposto</div>`;

    return `
      <div class="card">
        <div class="flex jb ic" style="margin-bottom:10px">
          <div>
            <div style="font-weight:700;font-size:.95rem">${q.text}</div>
            <div style="font-size:.75rem;color:var(--dim);margin-top:3px">${q.type==='multiple'?`Nomina ${q.count} tennisti`:q.type==='freetext'?'Risposta libera':'1 tennista'} · <span style="color:var(--gold)">+${ptsLabel}</span></div>
          </div>
          ${isAnswered ? '<span style="color:var(--accent);font-size:.8rem">✅ Risposto</span>' : '<span style="color:var(--gold);font-size:.8rem">⏳ In attesa</span>'}
        </div>
        ${isAdmin ? `<div id="admin-psel-${qi}" style="margin-bottom:8px"><label>Per chi?</label><select id="ante-who-${qi}">${allUsers.map(u=>`<option value="${u.id||u.uid}">${u.name}</option>`).join('')}</select></div>` : ''}
        ${q.type==='multiple' ? 
          Array.from({length:q.count},(_,k)=>`<div style="margin-bottom:6px"><label style="font-size:.75rem;color:var(--dim)">Tennista ${k+1}</label><input type="text" id="ante-ans-${qi}-${k}" placeholder="es. Carlos Alcaraz" value="${answered[k]?.tennisPlayer||''}" ${!open&&!isAdmin?'disabled':''}></div>`).join('') :
          q.type==='freetext' ?
          `<input type="text" id="ante-ans-${qi}-0" placeholder="La tua risposta..." value="${answered[0]?.tennisPlayer||''}" ${!open&&!isAdmin?'disabled':''}>` :
          `<input type="text" id="ante-ans-${qi}-0" placeholder="es. Carlos Alcaraz" value="${answered[0]?.tennisPlayer||''}" ${!open&&!isAdmin?'disabled':''}>`
        }
        <div style="margin-top:10px;display:flex;gap:8px;flex-wrap:wrap;align-items:center">
          ${open || isAdmin
            ? `<button class="btn btn-p btn-sm" onclick="submitAnteAnswer(${qi})">📌 ${isAnswered?'Modifica':'Conferma'}</button>`
            : '<span style="font-size:.75rem;color:var(--red)">🔒 Torneo iniziato — non modificabile</span>'}
          ${isAdmin && answered.length ? `<button class="btn btn-d btn-sm" onclick="resolveAnteQuestion(${qi})">🏁 Risolvi</button>` : ''}
        </div>
        ${othersBlock}
      </div>`;
  }).join('');

  // My answers summary panel
  if (myEl) {
    if (!myAnswers.length) { myEl.innerHTML = ''; return; }
    const resolved = myAnswers.filter(a=>a.status!=='pending');
    if (!resolved.length) { myEl.innerHTML = ''; return; }
    myEl.innerHTML = `<div class="card-title mt3">📊 I tuoi risultati</div>` +
      resolved.map(a => `
        <div class="mr">
          <div style="flex:1">
            <div class="fw6">${qs[a.questionIdx]?.text||'?'}</div>
            <div class="dim" style="font-size:.76rem">${a.tennisPlayer}</div>
          </div>
          <span class="badge ${a.status==='win'?'b-win':'b-loss'}">${a.status==='win'?`✅ +${a.ptsWin}pt`:'❌'}</span>
        </div>`).join('');
  }
}

window.submitAnteAnswer = async function(qi) {
  const isAdmin = userProfile?.isAdmin;
  const userId = isAdmin ? (document.getElementById(`ante-who-${qi}`)?.value || currentUser.uid) : currentUser.uid;
  const userName = allUsers.find(u=>(u.id||u.uid)===userId)?.name || userProfile?.name || '';
  const q = (gameConfig?.anteQuestions||[])[qi];
  if (!q) return;
  const answers = [];
  for (let k=0; k<q.count; k++) {
    const val = document.getElementById(`ante-ans-${qi}-${k}`)?.value.trim();
    if (val) answers.push(val);
  }
  if (!answers.length) return toastErr('Inserisci almeno una risposta!');
  // Delete existing answers for this question+user
  const existing = allAnteposts.filter(a=>a.userId===userId&&a.questionIdx===qi);
  for (const a of existing) await deleteDoc(doc(db,'anteposts',a.id));
  // Add new answers
  for (const ans of answers) {
    const ptsWin = q.fixed === 'semifinal' ? (q.ptsEach || q.pts) : q.pts;
    const anteDoc = {
      userId, userName, tennisPlayer: ans, questionIdx: qi,
      questionText: q.text, type: q.type,
      ptsWin, ptsLose: 0,
      status: 'pending', pointsResult: 0,
      tournament: gameConfig?.tournament || '',
      createdAt: serverTimestamp()
    };
    if (activeContext !== 'general') anteDoc.leagueId = activeContext;
    await addDoc(collection(db,'anteposts'), anteDoc);
  }
  toast(`✅ Risposta salvata! ${answers.join(', ')}`);
};

// ── AUTO RESOLVE via The Odds API ──
async function fetchTournamentResults() {
  const key = gameConfig?.apiKey;
  if (!key) return null;
  const sport = gameConfig?.tournament && SPORT_MAP[gameConfig.tournament] ? SPORT_MAP[gameConfig.tournament] : null;
  if (!sport) return null;
  try {
    // Fetch completed events (scores endpoint)
    const scoresUrl = `https://api.the-odds-api.com/v4/sports/${sport}/scores/?apiKey=${key}&daysFrom=30`;
    let data;
    try {
      const res = await fetch(scoresUrl);
      if (!res.ok) return null;
      data = await res.json();
    } catch(e) {
      try {
        const proxyUrl = `https://api.allorigins.win/get?url=${encodeURIComponent(scoresUrl)}`;
        const res2 = await fetch(proxyUrl);
        if (!res2.ok) return null;
        const wrapped = await res2.json();
        data = JSON.parse(wrapped.contents);
      } catch(e2) { return null; }
    }
    return data;
  } catch(e) { return null; }
}

function extractWinnerFromResults(results) {
  // Find the final match (last completed match)
  if (!results || !results.length) return null;
  const completed = results.filter(r => r.completed);
  if (!completed.length) return null;
  // Sort by date desc, get latest
  completed.sort((a,b) => new Date(b.commence_time) - new Date(a.commence_time));
  const final = completed[0];
  if (!final.scores) return null;
  // Winner is the one with higher score
  const s = final.scores;
  if (!s || s.length < 2) return null;
  const winner = s[0].score > s[1].score ? final.home_team : final.away_team;
  return winner;
}

function extractSemifinalistsFromResults(results) {
  if (!results || !results.length) return [];
  const completed = results.filter(r => r.completed);
  if (completed.length < 4) return [];
  // Sort by date desc, take last 4 unique players
  completed.sort((a,b) => new Date(b.commence_time) - new Date(a.commence_time));
  const semis = completed.slice(0, 2); // last 2 matches are semis
  const players = new Set();
  semis.forEach(m => { players.add(m.home_team); players.add(m.away_team); });
  return [...players].slice(0, 4);
}

window.resolveAnteQuestion = async function(qi) {
  const q = (gameConfig?.anteQuestions||[])[qi];
  if (!q) return;

  // Show resolve modal with options
  const modal = document.getElementById('ante-resolve-modal');
  if (modal) {
    document.getElementById('arm-title').textContent = q.text;
    document.getElementById('arm-qi').value = qi;
    document.getElementById('arm-manual-input').value = '';
    document.getElementById('arm-auto-status').innerHTML = '';
    modal.classList.add('open');

    // Try auto-fetch if question type is winner or semifinal
    const qLower = q.text.toLowerCase();
    const isWinner = qLower.includes('vince') || qLower.includes('winner') || qLower.includes('vincitor');
    const isSemi = qLower.includes('semi') || qLower.includes('semifinal');

    if (isWinner || isSemi) {
      document.getElementById('arm-auto-status').innerHTML = '<span style="color:var(--dim)">🔄 Recupero risultati dall\'API...</span>';
      const results = await fetchTournamentResults();
      if (results && results.length) {
        if (isWinner) {
          const winner = extractWinnerFromResults(results);
          if (winner) {
            document.getElementById('arm-auto-status').innerHTML = `<span style="color:var(--accent)">✅ Vincitore trovato automaticamente:</span><br><strong style="font-size:1.1rem">${winner}</strong>`;
            document.getElementById('arm-manual-input').value = winner;
          } else {
            document.getElementById('arm-auto-status').innerHTML = '<span style="color:var(--gold)">⚠️ Torneo non ancora concluso. Inserisci manualmente.</span>';
          }
        } else if (isSemi) {
          const semis = extractSemifinalistsFromResults(results);
          if (semis.length >= 2) {
            document.getElementById('arm-auto-status').innerHTML = `<span style="color:var(--accent)">✅ Semifinalisti trovati:</span><br><strong>${semis.join(', ')}</strong>`;
            document.getElementById('arm-manual-input').value = semis.join(', ');
          } else {
            document.getElementById('arm-auto-status').innerHTML = '<span style="color:var(--gold)">⚠️ Semifinali non ancora completate. Inserisci manualmente.</span>';
          }
        }
      } else {
        document.getElementById('arm-auto-status').innerHTML = '<span style="color:var(--dim)">ℹ️ Nessun risultato disponibile via API. Inserisci manualmente.</span>';
      }
    } else {
      document.getElementById('arm-auto-status').innerHTML = '<span style="color:var(--dim)">📝 Statistica speciale — inserisci il risultato manualmente.</span>';
    }
    return;
  }

  // Fallback if no modal
  await doResolveAnteQuestion(qi, []);
};

window.confirmAnteResolve = async function() {
  const qi = parseInt(document.getElementById('arm-qi').value);
  const val = document.getElementById('arm-manual-input').value.trim();
  if (!val) return toastErr('Inserisci il risultato!');
  const correctList = val.split(',').map(s=>s.trim().toLowerCase()).filter(Boolean);
  await doResolveAnteQuestion(qi, correctList);
  closeModal('ante-resolve-modal');
};

async function doResolveAnteQuestion(qi, correctList) {
  const q = (gameConfig?.anteQuestions||[])[qi];
  const isSemi = q?.fixed === 'semifinal';
  const ptsEach = q?.ptsEach || q?.pts || 0;
  // Group answers by user for semifinal (partial scoring)
  if (isSemi) {
    // Get all pending answers for this question grouped by user
    const answers = allAnteposts.filter(a => a.questionIdx === qi && a.status === 'pending');
    // Group by userId
    const byUser = {};
    answers.forEach(a => { if (!byUser[a.userId]) byUser[a.userId] = []; byUser[a.userId].push(a); });
    let totalWins = 0;
    for (const [uid, userAnswers] of Object.entries(byUser)) {
      for (const a of userAnswers) {
        const playerLower = (a.tennisPlayer||'').toLowerCase();
        const won = correctList.some(c => playerLower.includes(c) || c.includes(playerLower.split(' ').pop()));
        await updateDoc(doc(db,'anteposts',a.id), {
          status: won?'win':'loss',
          pointsResult: won ? ptsEach : 0
        });
        if (won) totalWins++;
      }
    }
    toast(`✅ Semifinalisti risolti! ${totalWins} risposte corrette (${ptsEach}pt ciascuna).`);
  } else {
    // Standard: winner or custom — full pts or 0
    const answers = allAnteposts.filter(a => a.questionIdx === qi && a.status === 'pending');
    let wins = 0;
    for (const a of answers) {
      const playerLower = (a.tennisPlayer||'').toLowerCase();
      const won = correctList.some(c => playerLower.includes(c) || c.includes(playerLower));
      await updateDoc(doc(db,'anteposts',a.id), {
        status: won?'win':'loss',
        pointsResult: won ? a.ptsWin : 0
      });
      if (won) wins++;
    }
    toast(`✅ "${q?.text}" risolta! ${wins} risposte corrette su ${answers.length}.`);
  }
}

// ── ANTEPOST CONFIG (legacy - keep for compat) ──
let anteConfigItems = [];

function renderAnteConfigList() {
  const el = document.getElementById('ante-config-list');
  if (!el) return;
  // Load from gameConfig
  if (gameConfig?.antepostConfig && !anteConfigItems.length) {
    anteConfigItems = [...(gameConfig.antepostConfig || [])];
  }
  if (!anteConfigItems.length) {
    el.innerHTML = '<div class="dim" style="font-size:.8rem">Nessun antepost configurato. Aggiungine uno!</div>';
    return;
  }
  const typeLabels = {winner:'🏆 Vincitore',semifinal:'🎾 Semifinalista',final:'🥈 Finalista',custom:'✏️ Custom'};
  el.innerHTML = anteConfigItems.map((a, i) => `
    <div class="mr" style="padding:8px 12px;margin-bottom:6px">
      <div style="flex:1;font-size:.88rem">
        <strong>${a.player}</strong> · ${typeLabels[a.type] || a.type}${a.quota ? ` · ${a.quota}x` : ''}
      </div>
      <button class="btn btn-d btn-sm" onclick="removeAnteConfig(${i})">✕</button>
    </div>`).join('');
}

window.addAnteConfig = function() {
  const player = document.getElementById('ante-cfg-player').value.trim();
  const type = document.getElementById('ante-cfg-type').value;
  const quota = parseFloat(document.getElementById('ante-cfg-quota').value) || null;
  if (!player) return toastErr('Inserisci il nome del tennista!');
  anteConfigItems.push({ player, type, quota });
  document.getElementById('ante-cfg-player').value = '';
  document.getElementById('ante-cfg-quota').value = '';
  renderAnteConfigList();
};

window.removeAnteConfig = function(i) {
  anteConfigItems.splice(i, 1);
  renderAnteConfigList();
};

window.saveAnteConfig = async function() {
  await updateConfig({ antepostConfig: anteConfigItems });
  toast('Configurazione antepost salvata! 📋');
};

// ── BET CONFIRM MODAL ──
function openBetConfirm(p1, p2, q1, q2, choice) {
  const chosen = choice === 'p1' ? p1 : p2;
  const q = choice === 'p1' ? q1 : q2;
  const base = gameConfig?.rules?.basePts || 10;
  const pts = Math.round(base * q * 10) / 10;
  const isAdmin = userProfile?.isAdmin;
  const userId = isAdmin ? (document.getElementById('bet-psel')?.value || currentUser.uid) : currentUser.uid;
  const userName = allUsers.find(u => (u.id || u.uid) === userId)?.name || userProfile?.name || '';
  const date = document.getElementById('bet-date')?.value || new Date().toISOString().split('T')[0];

  const { today, tomorrow } = getTournamentDates();
  const isTomorrow = date === tomorrow;
  const isFuture = date > today;
  const dateObj = new Date(date + 'T12:00:00');
  const dayName = dateObj.toLocaleDateString('it', {weekday:'long'}).toUpperCase();
  const dateFormatted = dateObj.toLocaleDateString('it', {day:'2-digit', month:'long', year:'numeric'});
  const dateColor = isTomorrow ? 'var(--gold)' : 'var(--accent)';
  const dateLabel = isTomorrow ? `DOMANI — ${dayName}` : `OGGI — ${dayName}`;

  document.getElementById('bet-confirm-content').innerHTML = `
    ${isFuture ? `<div style="background:rgba(255,180,0,.1);border:1px solid rgba(255,180,0,.3);border-radius:10px;padding:12px 14px;margin-bottom:14px;display:flex;align-items:center;gap:10px">
      <span style="font-size:1.4rem">📅</span>
      <div>
        <div style="font-size:.72rem;color:var(--gold);font-weight:700;letter-spacing:1px">ATTENZIONE — SCOMMESSA PER DOMANI</div>
        <div style="font-size:.82rem;margin-top:2px">Questa giocata vale per <strong>${dayName} ${dateFormatted}</strong>, non per oggi. Non verrà conteggiata per le partite di oggi.</div>
      </div>
    </div>` : ''}
    <div style="background:rgba(255,255,255,.04);border-radius:12px;padding:16px;margin-bottom:14px;">
      <div style="font-size:.75rem;color:var(--dim);margin-bottom:8px;">MATCH</div>
      <div style="font-weight:700;font-size:1.05rem;margin-bottom:4px;">${p1} <span style="color:var(--dim)">vs</span> ${p2}</div>
      <div style="font-size:.85rem;font-weight:700;color:${dateColor}">${dateLabel} · ${dateFormatted}</div>
    </div>
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:14px;">
      <div style="background:rgba(126,200,80,.08);border:1px solid rgba(126,200,80,.2);border-radius:10px;padding:12px;text-align:center;">
        <div style="font-size:.72rem;color:var(--dim);margin-bottom:4px;">SCOMMESSA SU</div>
        <div style="font-weight:700;color:var(--accent)">${chosen}</div>
        <div style="font-family:'DM Mono',monospace;font-size:1.1rem;color:var(--gold);margin-top:4px">${q}x</div>
      </div>
      <div style="background:rgba(255,255,255,.04);border-radius:10px;padding:12px;text-align:center;">
        <div style="font-size:.72rem;color:var(--dim);margin-bottom:4px;">GIOCATORE</div>
        <div style="font-weight:700">${userName}</div>
        <div style="font-size:.78rem;font-weight:600;color:${dateColor};margin-top:4px">${isTomorrow ? '📅 DOMANI' : '🎯 OGGI'}</div>
      </div>
    </div>
    <div style="display:flex;justify-content:space-between;background:rgba(255,255,255,.03);border-radius:10px;padding:12px 16px;">
      <span style="color:var(--dim)">Se vince:</span>
      <strong style="color:var(--accent)">+${pts} pt</strong>
    </div>
    <div style="display:flex;justify-content:space-between;padding:8px 16px;">
      <span style="color:var(--dim)">Se perde:</span>
      <strong style="color:var(--red)">-${pts} pt</strong>
    </div>
  `;
  const btn = document.getElementById('bet-confirm-btn');
  btn.onclick = async () => {
    btn.disabled = true;
    btn.textContent = '⏳ Registrazione...';
    await placeBetDirect(p1, p2, q1, q2, choice);
    btn.disabled = false;
    btn.textContent = '✅ Piazza!';
    closeModal('bet-confirm-modal');
  };
  document.getElementById('bet-confirm-modal').classList.add('open');
}

// ── TOAST ──
let _tt;
function toast(msg, err) {
  const el = document.getElementById('toast');
  el.textContent = msg;
  el.className = 'toast show' + (err ? ' err' : '');
  clearTimeout(_tt); _tt = setTimeout(() => el.classList.remove('show'), 3200);
}
function toastErr(msg) { toast(msg, true); }
window.toastErr = toastErr;

// ── PWA INSTALL ──
let deferredPrompt;
function setupInstallBanner() {
  window.addEventListener('beforeinstallprompt', (e) => {
    e.preventDefault();
    deferredPrompt = e;
    document.getElementById('install-banner').classList.add('show');
  });
  document.getElementById('install-btn').addEventListener('click', async () => {
    if (!deferredPrompt) return;
    deferredPrompt.prompt();
    const { outcome } = await deferredPrompt.userChoice;
    deferredPrompt = null;
    document.getElementById('install-banner').classList.remove('show');
  });
}

