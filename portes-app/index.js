/**
 * LiveDoors — fichier unique.
 *
 * Serveur Express + Socket.io ET page cliente complète (HTML+CSS+JS),
 * renvoyée par res.send() sur "/". Un seul fichier à exécuter :
 *
 *     node index.js
 *
 * Comptes : téléphone + mot de passe (haché avec scrypt, natif à Node,
 * aucune dépendance supplémentaire). Un jeton de session est stocké côté
 * client (localStorage) pour rester connecté après un rafraîchissement.
 *
 * ⚠️ Persistance : les comptes sont sauvegardés dans un fichier JSON local
 *    (livedoors-data.json). Sur un hébergeur comme Render (plan gratuit),
 *    ce fichier survit aux mises en veille et redémarrages, MAIS est
 *    réinitialisé à chaque nouveau déploiement (nouveau code poussé).
 *    Pour une vraie persistance permanente, remplacer par une base de
 *    données (Postgres, etc.).
 *
 * Le serveur NE transporte JAMAIS l'audio/vidéo : il gère les comptes et
 * relaie la signalisation WebRTC (offer/answer/ICE) entre navigateurs, qui
 * établissent ensuite une connexion peer-to-peer directe.
 */

const express = require('express');
const http = require('http');
const fs = require('fs');
const { randomUUID, randomBytes, scryptSync, timingSafeEqual } = require('crypto');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*' } // à restreindre à votre domaine en production
});

// ---------------------------------------------------------------------------
// Comptes persistants (fichier JSON local)
// ---------------------------------------------------------------------------

const DATA_FILE = './livedoors-data.json';

/** phoneKey -> { phoneKey, phone, pseudo, avatarInitials, avatarColor,
 *                salt, hash, token,
 *                contacts: Set<phoneKey>, favorites: Set<phoneKey>,
 *                blocked: Map<phoneKey, phone> } */
const accounts = new Map();

function normalizePhone(phone) {
  return String(phone || '').replace(/\D/g, '');
}

function hashPassword(password) {
  const salt = randomBytes(16).toString('hex');
  const hash = scryptSync(password, salt, 64).toString('hex');
  return { salt, hash };
}
function verifyPassword(password, salt, hash) {
  try {
    const attempt = scryptSync(password, salt, 64).toString('hex');
    return timingSafeEqual(Buffer.from(attempt, 'hex'), Buffer.from(hash, 'hex'));
  } catch (e) {
    return false;
  }
}

function loadAccounts() {
  try {
    const raw = fs.readFileSync(DATA_FILE, 'utf8');
    const parsed = JSON.parse(raw);
    for (const a of parsed) {
      accounts.set(a.phoneKey, {
        ...a,
        contacts: new Set(a.contacts || []),
        favorites: new Set(a.favorites || []),
        blocked: new Map(a.blocked || []),
        closeFriends: new Set(a.closeFriends || []),
        history: a.history || [],
        isPremium: !!a.isPremium,
        theme: a.theme || 'yellow',
        ringtone: a.ringtone || 'classique',
        badge: a.badge || '',
        vipOnly: !!a.vipOnly,
      });
    }
    console.log(`LiveDoors — ${accounts.size} compte(s) rechargé(s) depuis ${DATA_FILE}`);
  } catch (e) {
    // Pas de fichier encore, ou illisible : on démarre avec 0 compte.
  }
}

let saveScheduled = false;
function saveAccounts() {
  if (saveScheduled) return;
  saveScheduled = true;
  setTimeout(() => {
    saveScheduled = false;
    const arr = Array.from(accounts.values()).map((a) => ({
      ...a,
      contacts: Array.from(a.contacts),
      favorites: Array.from(a.favorites),
      blocked: Array.from(a.blocked.entries()),
      closeFriends: Array.from(a.closeFriends),
      history: a.history.slice(-30),
    }));
    fs.writeFile(DATA_FILE, JSON.stringify(arr), () => {});
  }, 300);
}

loadAccounts();

// ---------------------------------------------------------------------------
// Présence en ligne (le temps de la connexion socket)
// ---------------------------------------------------------------------------

/** socketId -> { id, phoneKey, pseudo, avatarInitials, avatarColor, phone,
 *                doorOpen, doorMessage, roomId } */
const users = new Map();

/** roomId -> { hostId, memberIds: Set<socketId> } */
const rooms = new Map();

function publicUser(u) {
  const room = u.roomId ? rooms.get(u.roomId) : null;
  return {
    id: u.id,
    pseudo: u.pseudo,
    avatarInitials: u.avatarInitials,
    avatarColor: u.avatarColor,
    phone: u.phone || null,
    doorOpen: u.doorOpen,
    doorMessage: u.doorMessage || '',
    companions: room ? Math.max(0, room.memberIds.size - 1) : 0,
    badge: u.badge || '',
  };
}

function broadcastFriends() {
  for (const [socketId, viewer] of users) {
    const viewerAccount = accounts.get(viewer.phoneKey);
    if (!viewerAccount) continue;

    const list = Array.from(users.values())
      .filter((u) => {
        if (u.id === viewer.id || !u.phoneKey) return false;
        if (!viewerAccount.contacts.has(u.phoneKey)) return false;
        if (viewerAccount.blocked.has(u.phoneKey)) return false;
        const otherAccount = accounts.get(u.phoneKey);
        if (otherAccount && otherAccount.blocked.has(viewer.phoneKey)) return false;
        return true;
      })
      .map((u) => {
        const otherAccount = accounts.get(u.phoneKey);
        const pub = { ...publicUser(u), badge: otherAccount ? otherAccount.badge : '' };
        // Porte VIP : si l'hôte a restreint sa porte, on la montre "fermée"
        // aux personnes qui ne sont pas dans ses amis proches.
        if (otherAccount && otherAccount.vipOnly && !otherAccount.closeFriends.has(viewer.phoneKey)) {
          pub.doorOpen = false;
          pub.doorMessage = '';
        }
        return {
          ...pub,
          isFavorite: viewerAccount.favorites.has(u.phoneKey),
          isCloseFriend: viewerAccount.closeFriends.has(u.phoneKey),
        };
      });

    io.to(socketId).emit('friends:update', list);
  }
}

function sendBlockedList(socket, account) {
  const list = Array.from(account.blocked.entries()).map(([phoneKey, phone]) => ({ phoneKey, phone }));
  socket.emit('account:blocked', list);
}

// ---------------------------------------------------------------------------
// Connexion Socket.io
// ---------------------------------------------------------------------------

function sendHistory(socket, account) {
  if (!account.isPremium) return;
  socket.emit('account:history', account.history.slice(-30).reverse());
}

function completeLogin(socket, account) {
  const liveUser = {
    id: socket.id,
    phoneKey: account.phoneKey,
    phone: account.phone,
    pseudo: account.pseudo,
    avatarInitials: account.avatarInitials,
    avatarColor: account.avatarColor,
    doorOpen: false,
    doorMessage: '',
    roomId: null,
    pendingHistory: new Map(), // fromSocketId -> index dans account.history
  };
  users.set(socket.id, liveUser);
  socket.emit('registered', {
    ...publicUser(liveUser),
    token: account.token,
    isPremium: account.isPremium,
    theme: account.theme,
    ringtone: account.ringtone,
    badge: account.badge,
    vipOnly: account.vipOnly,
  });
  sendBlockedList(socket, account);
  sendHistory(socket, account);
  broadcastFriends();
}

io.on('connection', (socket) => {

  socket.on('auth:register', ({ pseudo, phone, password }) => {
    const phoneKey = normalizePhone(phone);
    if (!phoneKey || !password || password.length < 4) {
      socket.emit('auth:error', { message: 'Numéro et mot de passe (4 caractères min.) requis.' });
      return;
    }
    if (accounts.has(phoneKey)) {
      socket.emit('auth:error', { message: 'Ce numéro a déjà un compte — connecte-toi plutôt.' });
      return;
    }
    const { salt, hash } = hashPassword(password);
    const palette = ['#ff8a00', '#7c5cff', '#ff3d77', '#00c2a8', '#ffb020', '#4d8bff'];
    const account = {
      phoneKey,
      phone: String(phone).slice(0, 32),
      pseudo: String(pseudo || 'Anonyme').slice(0, 24),
      avatarInitials: String(pseudo || '??').slice(0, 2).toUpperCase(),
      avatarColor: palette[phoneKey.length % palette.length],
      salt,
      hash,
      token: randomUUID(),
      contacts: new Set(),
      favorites: new Set(),
      blocked: new Map(),
      isPremium: false,
      theme: 'yellow',
      ringtone: 'classique',
      badge: '',
      vipOnly: false,
      closeFriends: new Set(),
      history: [], // { phoneKey, phone, pseudo, timestamp, status: 'pending'|'accepted'|'declined'|'missed' }
    };
    accounts.set(phoneKey, account);
    saveAccounts();
    completeLogin(socket, account);
  });

  socket.on('auth:login', ({ phone, password }) => {
    const phoneKey = normalizePhone(phone);
    const account = accounts.get(phoneKey);
    if (!account || !verifyPassword(password || '', account.salt, account.hash)) {
      socket.emit('auth:error', { message: 'Numéro ou mot de passe incorrect.' });
      return;
    }
    if (!account.token) { account.token = randomUUID(); saveAccounts(); }
    completeLogin(socket, account);
  });

  socket.on('auth:token', ({ token }) => {
    const account = Array.from(accounts.values()).find((a) => a.token === token);
    if (!account) {
      socket.emit('auth:token-invalid');
      return;
    }
    completeLogin(socket, account);
  });

  socket.on('contact:add', ({ phone }) => {
    const user = users.get(socket.id);
    const account = user && accounts.get(user.phoneKey);
    if (!user || !account) return;
    const key = normalizePhone(phone);
    if (!key || key === account.phoneKey) return;

    account.contacts.add(key);
    saveAccounts();
    const found = Array.from(users.values()).some((u) => u.id !== user.id && u.phoneKey === key);
    socket.emit('contact:added', { phone, found });
    broadcastFriends();
  });

  socket.on('contact:favorite', ({ phone, favorite }) => {
    const user = users.get(socket.id);
    const account = user && accounts.get(user.phoneKey);
    if (!user || !account) return;
    const key = normalizePhone(phone);
    if (!key) return;

    if (favorite) {
      account.contacts.add(key);
      account.favorites.add(key);
    } else {
      account.favorites.delete(key);
    }
    saveAccounts();
    broadcastFriends();
  });

  socket.on('contact:block', ({ phone, blocked }) => {
    const user = users.get(socket.id);
    const account = user && accounts.get(user.phoneKey);
    if (!user || !account) return;
    const key = normalizePhone(phone);
    if (!key || key === account.phoneKey) return;

    if (blocked) {
      account.blocked.set(key, String(phone));
      account.favorites.delete(key);
    } else {
      account.blocked.delete(key);
    }
    saveAccounts();
    sendBlockedList(socket, account);
    broadcastFriends();
  });

  socket.on('contact:closefriend', ({ phone, closeFriend }) => {
    const user = users.get(socket.id);
    const account = user && accounts.get(user.phoneKey);
    if (!user || !account) return;
    const key = normalizePhone(phone);
    if (!key) return;

    if (closeFriend) {
      account.contacts.add(key);
      account.closeFriends.add(key);
    } else {
      account.closeFriends.delete(key);
    }
    saveAccounts();
    broadcastFriends();
  });

  // ---- Premium : bouton de TEST le temps que le vrai paiement Stripe soit
  // branché. En prod, remplacer ce handler par un webhook Stripe qui met
  // isPremium à jour après un paiement confirmé (ne JAMAIS faire confiance
  // à un simple message du client pour du vrai argent). ----
  socket.on('account:set-premium', ({ premium }) => {
    const user = users.get(socket.id);
    const account = user && accounts.get(user.phoneKey);
    if (!user || !account) return;

    account.isPremium = !!premium;
    if (!account.isPremium) account.vipOnly = false; // pas de VIP sans Premium
    saveAccounts();
    socket.emit('account:updated', { isPremium: account.isPremium, vipOnly: account.vipOnly });
    sendHistory(socket, account);
    broadcastFriends();
  });

  socket.on('account:customize', ({ theme, ringtone, badge }) => {
    const user = users.get(socket.id);
    const account = user && accounts.get(user.phoneKey);
    if (!user || !account || !account.isPremium) return;

    const validThemes = ['yellow', 'violet', 'menthe', 'feu', 'nuit'];
    const validRingtones = ['classique', 'cloche', 'synth', 'cyber'];
    if (theme && validThemes.includes(theme)) account.theme = theme;
    if (ringtone && validRingtones.includes(ringtone)) account.ringtone = ringtone;
    if (typeof badge === 'string') account.badge = badge.slice(0, 4);

    saveAccounts();
    socket.emit('account:updated', { theme: account.theme, ringtone: account.ringtone, badge: account.badge });
    broadcastFriends();
  });

  socket.on('door:vip', ({ vipOnly }) => {
    const user = users.get(socket.id);
    const account = user && accounts.get(user.phoneKey);
    if (!user || !account || !account.isPremium) return;

    account.vipOnly = !!vipOnly;
    saveAccounts();
    socket.emit('account:updated', { vipOnly: account.vipOnly });
    broadcastFriends();
  });

  socket.on('door:open', ({ message } = {}) => {
    const user = users.get(socket.id);
    if (!user || user.doorOpen) return;

    const roomId = randomUUID();
    user.doorOpen = true;
    user.doorMessage = message ? String(message).slice(0, 60) : '';
    user.roomId = roomId;
    rooms.set(roomId, { hostId: socket.id, memberIds: new Set([socket.id]) });
    socket.join(roomId);

    broadcastFriends();
  });

  socket.on('door:message', ({ message }) => {
    const user = users.get(socket.id);
    if (!user) return;
    user.doorMessage = message ? String(message).slice(0, 60) : '';
    broadcastFriends();
  });

  socket.on('door:close', () => {
    closeDoorAndRoom(socket.id);
    broadcastFriends();
  });

  socket.on('call:request', ({ hostId, message }) => {
    const host = users.get(hostId);
    const me = users.get(socket.id);
    if (!host || !me || !host.doorOpen) {
      socket.emit('call:error', { message: "Cette porte n'est plus ouverte." });
      return;
    }
    const hostAccount = accounts.get(host.phoneKey);
    const meAccount = accounts.get(me.phoneKey);
    if ((hostAccount && hostAccount.blocked.has(me.phoneKey)) || (meAccount && meAccount.blocked.has(host.phoneKey))) {
      socket.emit('call:error', { message: "Cette porte n'est plus ouverte." });
      return;
    }
    // Porte VIP : seuls les amis proches de l'hôte peuvent toquer.
    if (hostAccount && hostAccount.vipOnly && !hostAccount.closeFriends.has(me.phoneKey)) {
      socket.emit('call:error', { message: "Cette porte n'est plus ouverte." });
      return;
    }

    if (hostAccount) {
      hostAccount.history.push({
        phoneKey: me.phoneKey, phone: me.phone, pseudo: me.pseudo,
        timestamp: Date.now(), status: 'pending',
      });
      if (hostAccount.history.length > 30) hostAccount.history = hostAccount.history.slice(-30);
      host.pendingHistory.set(socket.id, hostAccount.history.length - 1);
      saveAccounts();
    }

    io.to(hostId).emit('call:incoming-request', {
      ...publicUser(me),
      message: message ? String(message).slice(0, 140) : '',
    });
  });

  socket.on('call:decline', ({ fromId }) => {
    markHistory(socket.id, fromId, 'declined');
    io.to(fromId).emit('call:declined');
  });

  socket.on('call:accept', ({ fromId }) => {
    const host = users.get(socket.id);
    if (!host || !host.doorOpen) return;
    markHistory(socket.id, fromId, 'accepted');
    io.to(fromId).emit('call:accepted', { hostId: socket.id });
  });

  socket.on('call:ready', ({ hostId }) => {
    const host = users.get(hostId);
    const me = users.get(socket.id);
    if (!host || !me || !host.doorOpen || !host.roomId) {
      socket.emit('call:error', { message: "Cette porte n'est plus ouverte." });
      return;
    }

    const room = rooms.get(host.roomId);
    const hostAccount = accounts.get(host.phoneKey);
    const maxMembers = hostAccount && hostAccount.isPremium ? 8 : 2;
    if (room.memberIds.size >= maxMembers) {
      socket.emit('call:error', {
        message: hostAccount && hostAccount.isPremium
          ? 'Ce salon est complet (8 max).'
          : 'Ce salon est complet en version gratuite (2 max) — LiveDoors Plus débloque jusqu\'à 8 personnes.',
      });
      return;
    }
    room.memberIds.add(socket.id);
    me.roomId = host.roomId;
    socket.join(host.roomId);

    socket.to(host.roomId).emit('call:peer-joined', publicUser(me));

    const existingMembers = Array.from(room.memberIds)
      .filter((id) => id !== socket.id)
      .map((id) => publicUser(users.get(id)));

    socket.emit('call:room-state', { roomId: host.roomId, members: existingMembers });

    broadcastFriends();
  });

  socket.on('call:leave', () => {
    leaveCurrentRoom(socket.id);
    broadcastFriends();
  });

  socket.on('webrtc:offer', ({ targetId, offer }) => {
    io.to(targetId).emit('webrtc:offer', { fromId: socket.id, offer });
  });
  socket.on('webrtc:answer', ({ targetId, answer }) => {
    io.to(targetId).emit('webrtc:answer', { fromId: socket.id, answer });
  });
  socket.on('webrtc:ice-candidate', ({ targetId, candidate }) => {
    io.to(targetId).emit('webrtc:ice-candidate', { fromId: socket.id, candidate });
  });

  socket.on('disconnect', () => {
    closeDoorAndRoom(socket.id);
    leaveCurrentRoom(socket.id);
    users.delete(socket.id);
    broadcastFriends();
  });
});

function markHistory(hostSocketId, fromSocketId, status) {
  const host = users.get(hostSocketId);
  if (!host || !host.pendingHistory.has(fromSocketId)) return;
  const account = accounts.get(host.phoneKey);
  const index = host.pendingHistory.get(fromSocketId);
  if (account && account.history[index]) {
    account.history[index].status = status;
    saveAccounts();
  }
  host.pendingHistory.delete(fromSocketId);
}

function closeDoorAndRoom(socketId) {
  const user = users.get(socketId);
  if (!user || !user.doorOpen) return;

  // Toute demande restée sans réponse devient "manquée".
  if (user.pendingHistory && user.pendingHistory.size) {
    const account = accounts.get(user.phoneKey);
    for (const index of user.pendingHistory.values()) {
      if (account && account.history[index] && account.history[index].status === 'pending') {
        account.history[index].status = 'missed';
      }
    }
    user.pendingHistory.clear();
    saveAccounts();
  }

  const room = rooms.get(user.roomId);
  if (room) {
    room.memberIds.forEach((memberId) => {
      if (memberId !== socketId) {
        io.to(memberId).emit('call:ended', { reason: 'host-closed' });
        const member = users.get(memberId);
        if (member) member.roomId = null;
      }
      io.sockets.sockets.get(memberId)?.leave(user.roomId);
    });
    rooms.delete(user.roomId);
  }

  user.doorOpen = false;
  user.doorMessage = '';
  user.roomId = null;
}

function leaveCurrentRoom(socketId) {
  const user = users.get(socketId);
  if (!user || !user.roomId) return;

  const roomId = user.roomId;
  const room = rooms.get(roomId);

  if (room && room.hostId === socketId) {
    closeDoorAndRoom(socketId);
    return;
  }

  if (room) {
    room.memberIds.delete(socketId);
    io.to(roomId).emit('call:peer-left', { id: socketId });
    if (room.memberIds.size === 0) rooms.delete(roomId);
  }

  io.sockets.sockets.get(socketId)?.leave(roomId);
  user.roomId = null;
}

// ---------------------------------------------------------------------------
// Page cliente
// ---------------------------------------------------------------------------

const PAGE_CSS = `
@import url('https://fonts.googleapis.com/css2?family=Baloo+2:wght@600;700;800&family=Nunito:wght@400;600;700;800&family=JetBrains+Mono:wght@500;600&display=swap');

:root{
  --bg:#ffffff; --bg-soft:#f6f6f8; --border:#ececef;
  --yellow:#fffc00; --yellow-deep:#ffe600;
  --ink:#14171a; --ink-soft:#6b7280; --ink-faint:#9aa0ac;
  --grad-1:#fffc00; --grad-2:#ff8a00; --grad-3:#ff3d77;
}
[data-theme="dark"]{
  --bg:#15161a; --bg-soft:#1f2026; --border:#2a2b32;
  --ink:#f2f2f4; --ink-soft:#b7bac2; --ink-faint:#7d818c;
}

*{box-sizing:border-box; margin:0; padding:0;}

body{
  min-height:100vh; background:#efeff2; display:flex; align-items:center;
  justify-content:center; padding:32px 16px; font-family:'Nunito', sans-serif;
}
[data-theme="dark"] body{ background:#0b0b0d; }

.phone{
  width:392px; max-width:100%; height:820px; max-height:92vh;
  background:var(--bg); border-radius:38px; border:1px solid var(--border);
  box-shadow:0 40px 80px -20px rgba(0,0,0,0.25), 0 0 0 8px #050506;
  position:relative; overflow:hidden; display:flex; flex-direction:column;
}

.screen{ height:100%; display:flex; flex-direction:column; }

/* ---------- Login / register screen ---------- */
.login-screen{ align-items:center; justify-content:flex-start; padding:32px; background:var(--yellow); overflow-y:auto; }
.login-inner{ width:100%; margin:auto 0; }
.auth-tabs{ display:flex; gap:8px; margin-bottom:18px; }
.auth-tab{
  flex:1; padding:10px; border-radius:12px; border:none; cursor:pointer;
  font-family:'Baloo 2', sans-serif; font-weight:700; font-size:13px;
  background:rgba(0,0,0,0.08); color:#14171a;
}
.auth-tab.active{ background:#14171a; color:var(--yellow); }
.field-label{
  display:block; font-family:'Baloo 2', sans-serif; font-weight:700;
  font-size:12.5px; color:#14171a; margin:14px 0 6px;
}
.field-input, .field-textarea{
  width:100%; padding:13px 14px; border-radius:12px; border:none;
  font-family:'Nunito', sans-serif; font-size:14px; background:#fff; color:#14171a;
}
.field-textarea{ resize:none; }
.field-input:focus, .field-textarea:focus{ outline:3px solid rgba(0,0,0,0.15); }
.field-hint{ font-size:10.5px; color:rgba(20,23,26,0.55); margin-top:5px; font-weight:600; }
.field-error{ font-size:11.5px; color:#b3003a; margin-top:8px; font-weight:700; display:none; }
.primary-btn{
  width:100%; margin-top:24px; padding:14px; border-radius:14px; border:none;
  background:#14171a; color:var(--yellow); font-family:'Baloo 2', sans-serif;
  font-weight:700; font-size:14.5px; cursor:pointer;
}
.primary-btn:active{ transform:scale(0.98); }
.auth-loading{ text-align:center; font-family:'Baloo 2', sans-serif; font-weight:700; color:#14171a; }

/* ---------- Header ---------- */
.app-header{
  background:var(--yellow); padding:22px 20px 16px; flex-shrink:0;
  display:flex; align-items:center; justify-content:space-between; gap:10px;
}
.app-title{ font-family:'Baloo 2', sans-serif; font-weight:800; font-size:22px; color:#14171a; letter-spacing:-0.3px; display:flex; align-items:center; gap:6px; }
.app-sub{ font-size:11px; color:rgba(20,23,26,0.6); font-weight:700; margin-top:1px; }
.header-right{ display:flex; align-items:center; gap:8px; flex-shrink:0; }
.icon-btn{
  width:34px; height:34px; border-radius:50%; border:none; cursor:pointer;
  background:rgba(0,0,0,0.08); color:#14171a; font-size:14px;
  display:flex; align-items:center; justify-content:center;
}
.header-avatar{
  width:38px; height:38px; border-radius:50%;
  background:linear-gradient(135deg,#ff8a00,#ff3d77);
  display:flex; align-items:center; justify-content:center;
  color:#fff; font-family:'Baloo 2',sans-serif; font-weight:700; font-size:14px;
  border:2px solid rgba(0,0,0,0.08);
}
.premium-badge{
  font-size:9.5px; font-weight:800; color:#14171a; background:linear-gradient(135deg,#ffd700,#ff8a00);
  padding:2px 7px; border-radius:8px; letter-spacing:0.02em; vertical-align:middle;
}

.content{ flex:1; overflow-y:auto; padding:16px 18px 24px; }
.content::-webkit-scrollbar{ width:0; }

/* ---------- My profile row ---------- */
.me-card{ background:var(--bg-soft); border-radius:20px; padding:14px; display:flex; flex-direction:column; gap:10px; margin-bottom:22px; }
.me-card-top{ display:flex; align-items:center; gap:12px; }
.me-avatar-wrap{ position:relative; width:52px; height:52px; flex-shrink:0; }
.me-avatar{
  width:52px; height:52px; border-radius:50%;
  background:linear-gradient(135deg,#ff8a00,#ff3d77);
  display:flex; align-items:center; justify-content:center;
  color:#fff; font-family:'Baloo 2',sans-serif; font-weight:700; font-size:18px;
}
.story-ring{
  position:absolute; inset:-4px; border-radius:50%; padding:3px;
  background:conic-gradient(from 0deg, var(--grad-1), var(--grad-2), var(--grad-3), var(--grad-1));
  -webkit-mask:radial-gradient(farthest-side, transparent calc(100% - 3px), #000 calc(100% - 3px));
  mask:radial-gradient(farthest-side, transparent calc(100% - 3px), #000 calc(100% - 3px));
  opacity:0; transition:opacity .3s ease; animation:spin 3s linear infinite;
}
.story-ring.show{ opacity:1; }
.story-ring.premium-ring{ animation-duration:1.6s; }
@keyframes spin{ to{ transform:rotate(360deg); } }

.me-info{ flex:1; min-width:0; }
.me-name{ font-family:'Baloo 2', sans-serif; font-weight:700; font-size:15.5px; color:var(--ink); }
.me-phone{ font-family:'JetBrains Mono', monospace; font-size:11px; color:var(--ink-soft); margin-top:2px; }
.me-status-line{ font-size:11.5px; font-weight:700; color:var(--ink-faint); margin-top:3px; }
.me-status-line.live{ color:#e08a00; }

.toggle-btn{
  font-family:'Baloo 2', sans-serif; font-weight:700; font-size:13px; color:#14171a;
  background:var(--yellow); border:none; padding:11px 18px; border-radius:14px; cursor:pointer;
  flex-shrink:0; transition:transform .15s ease, background .25s ease, color .25s ease;
  box-shadow:0 6px 14px -6px rgba(255,204,0,0.7);
}
.toggle-btn:active{ transform:scale(0.96); }
.toggle-btn.is-open{ background:var(--ink); color:var(--yellow); box-shadow:0 6px 14px -6px rgba(0,0,0,0.35); }
.toggle-btn:disabled{ opacity:0.5; cursor:not-allowed; }
.toggle-btn.ghost{ background:transparent; border:1px solid var(--border); color:var(--ink); box-shadow:none; }

.status-input-row{ display:flex; gap:8px; }
.status-input{ flex:1; padding:9px 12px; border-radius:12px; border:1px solid var(--border); font-family:'Nunito', sans-serif; font-size:12.5px; background:var(--bg); color:var(--ink); }
.status-set-btn{ font-family:'Baloo 2', sans-serif; font-weight:700; font-size:12px; color:var(--ink); background:transparent; border:1px solid var(--border); padding:8px 12px; border-radius:12px; cursor:pointer; }

/* ---------- Premium panel ---------- */
.premium-card{
  border-radius:20px; padding:16px; margin-bottom:20px; color:#fff;
  background:linear-gradient(135deg,#191a1e,#2b1d09 60%,#3a2400);
  border:1px solid rgba(255,204,0,0.35);
}
.premium-card.is-active{ border-color:rgba(255,204,0,0.7); }
.premium-title{ font-family:'Baloo 2', sans-serif; font-weight:800; font-size:15px; display:flex; align-items:center; gap:6px; margin-bottom:4px; }
.premium-sub{ font-size:11.5px; color:rgba(255,255,255,0.65); margin-bottom:12px; }
.premium-grid{ display:grid; grid-template-columns:1fr 1fr; gap:8px; margin-bottom:12px; }
.premium-feature{ font-size:11px; color:rgba(255,255,255,0.85); display:flex; align-items:center; gap:5px; }
.premium-toggle-btn{
  width:100%; padding:11px; border-radius:12px; border:none; cursor:pointer;
  font-family:'Baloo 2', sans-serif; font-weight:700; font-size:13px;
  background:linear-gradient(135deg,#ffd700,#ff8a00); color:#14171a;
}
.premium-toggle-btn.deactivate{ background:rgba(255,255,255,0.12); color:#fff; }
.premium-note{ font-size:9.5px; color:rgba(255,255,255,0.45); margin-top:8px; text-align:center; }

.customize-row{ display:flex; gap:8px; flex-wrap:wrap; margin-bottom:10px; }
.swatch-btn{ width:34px; height:34px; border-radius:50%; border:2px solid transparent; cursor:pointer; }
.swatch-btn.active{ border-color:var(--ink); }
.chip-btn{
  padding:8px 12px; border-radius:12px; border:1px solid var(--border); background:var(--bg);
  font-family:'Baloo 2', sans-serif; font-weight:700; font-size:12px; color:var(--ink); cursor:pointer;
}
.chip-btn.active{ background:var(--ink); color:var(--yellow); border-color:transparent; }

/* ---------- Section labels ---------- */
.section-label{ font-family:'Baloo 2', sans-serif; font-size:13px; color:var(--ink); font-weight:700; margin:20px 4px 10px; display:flex; align-items:center; gap:7px; }
.section-label .dot{ width:8px; height:8px; border-radius:50%; }
.live-label .dot{ background:linear-gradient(135deg,var(--grad-2),var(--grad-3)); }
.closed-label .dot{ background:#d7d7dc; }
.locked-label{ font-size:10px; color:var(--ink-faint); font-weight:700; margin-left:auto; }

/* ---------- Friend rows ---------- */
.friend-row{ display:flex; align-items:center; gap:8px; padding:9px 8px; border-radius:16px; margin-bottom:2px; }
.friend-row.is-open:hover{ background:var(--bg-soft); }
.avatar-wrap{ position:relative; flex-shrink:0; width:48px; height:48px; }
.avatar{
  width:48px; height:48px; border-radius:50%; display:flex; align-items:center; justify-content:center;
  font-family:'Baloo 2', sans-serif; font-weight:700; font-size:15px; color:#fff; position:relative; z-index:2;
}
.friend-row.is-closed .avatar{ filter:grayscale(1) brightness(0.92); opacity:0.55; }
.friend-info{ flex:1; min-width:0; }
.friend-name{ font-family:'Baloo 2', sans-serif; font-size:14.5px; font-weight:700; color:var(--ink); display:flex; align-items:center; gap:5px; }
.friend-row.is-closed .friend-name{ color:var(--ink-soft); }
.friend-phone{ font-family:'JetBrains Mono', monospace; font-size:10.5px; color:var(--ink-faint); margin-top:1px; }
.friend-meta{ font-size:11px; color:var(--ink-faint); margin-top:2px; font-weight:700; }
.friend-meta.live-meta{ color:#e08a00; }
.friend-status-msg{ font-size:11px; color:var(--ink-soft); margin-top:2px; font-style:italic; }

.row-actions{ display:flex; gap:4px; flex-shrink:0; }
.mini-btn{
  width:28px; height:28px; border-radius:10px; border:1px solid var(--border); background:var(--bg);
  cursor:pointer; font-size:12px; display:flex; align-items:center; justify-content:center; color:var(--ink-soft);
}
.mini-btn.is-fav{ background:var(--yellow); border-color:transparent; color:#14171a; }
.mini-btn.is-vip{ background:#7c5cff; border-color:transparent; color:#fff; }

.join-btn{
  font-family:'Baloo 2', sans-serif; font-weight:700; font-size:12.5px; color:#14171a;
  background:var(--yellow); border:none; padding:9px 14px; border-radius:12px; cursor:pointer;
  flex-shrink:0; transition:transform .15s ease;
}
.join-btn:active{ transform:scale(0.94); }
.join-btn:disabled{ opacity:0.5; cursor:not-allowed; }

.empty-note{ font-size:12px; color:var(--ink-faint); padding:4px 8px; font-weight:600; }

.blocked-row{ display:flex; align-items:center; gap:10px; padding:8px; }
.blocked-phone{ flex:1; font-family:'JetBrains Mono', monospace; font-size:12px; color:var(--ink-soft); }
.unblock-btn{ font-family:'Baloo 2', sans-serif; font-weight:700; font-size:11.5px; color:var(--ink); background:transparent; border:1px solid var(--border); padding:7px 12px; border-radius:10px; cursor:pointer; }

.history-row{ display:flex; align-items:center; gap:10px; padding:8px; border-bottom:1px solid var(--border); }
.history-row:last-child{ border-bottom:none; }
.history-info{ flex:1; min-width:0; }
.history-name{ font-family:'Baloo 2', sans-serif; font-weight:700; font-size:12.5px; color:var(--ink); }
.history-time{ font-size:10px; color:var(--ink-faint); margin-top:1px; }
.history-status{ font-size:10.5px; font-weight:700; padding:3px 8px; border-radius:8px; flex-shrink:0; }
.history-status.accepted{ background:rgba(0,194,168,0.15); color:#00846f; }
.history-status.declined{ background:rgba(255,61,119,0.15); color:#c40050; }
.history-status.missed{ background:rgba(154,160,172,0.2); color:var(--ink-soft); }

/* ---------- Join request modal ---------- */
.modal-backdrop{ position:absolute; inset:0; background:rgba(0,0,0,0.45); z-index:40; display:none; align-items:center; justify-content:center; padding:24px; }
.modal-backdrop.show{ display:flex; }
.modal-card{ background:var(--bg); border-radius:20px; padding:20px; width:100%; font-family:'Nunito', sans-serif; }
.modal-title{ font-family:'Baloo 2', sans-serif; font-weight:700; font-size:15px; color:var(--ink); margin-bottom:10px; }
.modal-actions{ display:flex; gap:10px; margin-top:14px; }
.modal-actions button{ flex:1; }
.modal-cancel-btn{ font-family:'Baloo 2', sans-serif; font-weight:700; font-size:13px; color:var(--ink); background:transparent; border:1px solid var(--border); padding:11px; border-radius:12px; cursor:pointer; }

/* ---------- Call overlay ---------- */
.call-overlay{
  position:absolute; inset:0; background:var(--ink); display:flex; flex-direction:column;
  align-items:center; justify-content:center; gap:16px; transform:translateY(100%);
  transition:transform .5s cubic-bezier(.5,0,.2,1); z-index:20; padding:24px;
}
.call-overlay.active{ transform:translateY(0); }
.call-glow{ position:absolute; width:320px; height:320px; border-radius:50%; background:radial-gradient(circle, rgba(255,252,0,0.14), transparent 70%); animation:breathe 3.2s ease-in-out infinite; }
@keyframes breathe{ 0%,100%{ transform:scale(0.94); opacity:0.7; } 50%{ transform:scale(1.06); opacity:1; } }
.call-avatar{ width:96px; height:96px; border-radius:50%; display:flex; align-items:center; justify-content:center; font-family:'Baloo 2', sans-serif; font-weight:700; font-size:30px; color:#fff; z-index:2; box-shadow:0 0 0 3px rgba(255,255,255,0.12); }
.call-status{ font-size:11px; letter-spacing:0.08em; text-transform:uppercase; color:var(--yellow); font-family:'JetBrains Mono', monospace; font-weight:600; z-index:2; text-align:center; }
.call-name{ font-family:'Baloo 2', sans-serif; font-weight:700; font-size:20px; color:#fff; z-index:2; text-align:center; margin-top:4px; }
.call-timer{ font-family:'JetBrains Mono', monospace; color:rgba(255,255,255,0.55); font-size:13px; z-index:2; text-align:center; margin-top:6px; }

.video-grid{ display:grid; grid-template-columns:1fr 1fr; gap:8px; width:100%; z-index:2; max-height:220px; overflow:hidden; }
.video-tile{ position:relative; border-radius:14px; overflow:hidden; background:#000; aspect-ratio:4/3; display:flex; align-items:center; justify-content:center; }
.video-tile video{ width:100%; height:100%; object-fit:cover; }
.video-tile .video-tile-label{ position:absolute; bottom:4px; left:6px; font-size:10px; color:#fff; font-family:'Baloo 2', sans-serif; font-weight:700; text-shadow:0 1px 3px rgba(0,0,0,0.6); }

.call-controls{ display:flex; gap:8px; z-index:2; margin-top:4px; flex-wrap:wrap; justify-content:center; }
.mute-btn, .leave-btn, .cam-btn, .screen-btn{
  font-family:'Baloo 2', sans-serif; font-weight:700; font-size:12.5px; color:#fff;
  background:rgba(255,255,255,0.1); border:1px solid rgba(255,255,255,0.2);
  padding:11px 16px; border-radius:14px; cursor:pointer; position:relative;
}
.mute-btn.is-muted, .cam-btn.is-on, .screen-btn.is-on{ background:var(--yellow); color:#14171a; border-color:transparent; }
.mute-btn:hover, .leave-btn:hover, .cam-btn:hover, .screen-btn:hover{ background:rgba(255,255,255,0.16); }
.cam-btn.locked, .screen-btn.locked{ opacity:0.5; }
.lock-dot{ font-size:9px; margin-left:3px; }

#incomingRequest{ display:none; z-index:2; text-align:center; background:rgba(255,255,255,0.08); padding:14px 18px; border-radius:16px; margin-top:6px; width:100%; }
.incoming-msg{ font-size:12px; color:rgba(255,255,255,0.75); font-style:italic; margin-bottom:10px; }

/* ---------- Toast ---------- */
.toast-zone{ position:absolute; left:0; right:0; bottom:22px; display:flex; flex-direction:column; align-items:center; gap:8px; pointer-events:none; z-index:30; }
.toast{ background:var(--ink); color:var(--bg); font-size:12.5px; font-weight:700; padding:10px 16px; border-radius:12px; border-left:4px solid var(--yellow); box-shadow:0 10px 24px -8px rgba(0,0,0,0.35); opacity:0; transform:translateY(8px); transition:opacity .3s ease, transform .3s ease; }
.toast.show{ opacity:1; transform:translateY(0); }

@media (prefers-reduced-motion: reduce){
  .story-ring, .call-glow{ animation:none; }
  .call-overlay, .toast{ transition:none; }
}
`;

const PAGE_BODY_HTML = `
<div class="phone" id="phone">

  <!-- ============ ÉCRAN 1 : connexion / inscription ============ -->
  <div class="screen login-screen" id="loginScreen">
    <div class="login-inner">
      <div class="app-title" style="font-size:26px;">LiveDoors</div>
      <div class="field-hint" style="margin-bottom:16px;">Vérification du compte enregistré...</div>
      <div id="authLoading" class="auth-loading" style="display:none;">Connexion...</div>

      <div id="authForm">
        <div class="auth-tabs">
          <button class="auth-tab active" id="tabLogin">Connexion</button>
          <button class="auth-tab" id="tabRegister">Inscription</button>
        </div>

        <div id="pseudoField" style="display:none;">
          <label class="field-label">Pseudo</label>
          <input class="field-input" id="pseudoInput" type="text" placeholder="Ex. Léa" maxlength="24">
        </div>

        <label class="field-label">Numéro de téléphone</label>
        <input class="field-input" id="phoneInput" type="tel" placeholder="06 12 34 56 78">

        <label class="field-label">Mot de passe</label>
        <input class="field-input" id="passwordInput" type="password" placeholder="4 caractères minimum">

        <div class="field-error" id="authError"></div>

        <button class="primary-btn" id="authSubmitBtn">Se connecter</button>
      </div>
    </div>
  </div>

  <!-- ============ ÉCRAN 2 : accueil ============ -->
  <div class="screen home-screen" id="homeScreen" style="display:none;">

    <div class="app-header">
      <div>
        <div class="app-title">LiveDoors <span class="premium-badge" id="headerPremiumBadge" style="display:none;">PLUS</span></div>
        <div class="app-sub" id="connectionState">Connexion...</div>
      </div>
      <div class="header-right">
        <button class="icon-btn" id="themeBtn" title="Changer de thème">🌙</button>
        <button class="icon-btn" id="logoutBtn" title="Se déconnecter">⏻</button>
        <div class="header-avatar" id="headerAvatar">--</div>
      </div>
    </div>

    <div class="content">

      <div class="me-card">
        <div class="me-card-top">
          <div class="me-avatar-wrap">
            <div class="story-ring" id="myRing"></div>
            <div class="me-avatar" id="myAvatar">--</div>
          </div>
          <div class="me-info">
            <div class="me-name" id="myName">Toi</div>
            <div class="me-phone" id="myPhone"></div>
            <div class="me-status-line" id="statusText">Porte fermée</div>
          </div>
          <button class="toggle-btn" id="toggleBtn">Ouvrir</button>
        </div>
        <div class="status-input-row">
          <input class="status-input" id="doorMessageInput" type="text" maxlength="60" placeholder="Petit mot ou emoji (ex: Pause café ☕)">
          <button class="status-set-btn" id="doorMessageBtn">OK</button>
        </div>
        <div class="status-input-row" id="vipRow" style="display:none;">
          <button class="status-set-btn" id="vipToggleBtn" style="flex:1;">💎 Porte VIP : désactivée</button>
        </div>
      </div>

      <!-- ---- Panneau Premium ---- -->
      <div class="premium-card" id="premiumCard">
        <div class="premium-title">✨ LiveDoors <span id="premiumTitleWord">Plus</span></div>
        <div class="premium-sub" id="premiumSub">Salons jusqu'à 8, caméra & partage d'écran, thèmes, sons, portes VIP, historique.</div>
        <div class="premium-grid">
          <div class="premium-feature">👥 Salons jusqu'à 8</div>
          <div class="premium-feature">🎥 Caméra & écran</div>
          <div class="premium-feature">🔇 Anti-bruit avancé</div>
          <div class="premium-feature">💎 Portes VIP</div>
          <div class="premium-feature">🎨 Thèmes & sons</div>
          <div class="premium-feature">🕒 Historique</div>
        </div>
        <button class="premium-toggle-btn" id="premiumToggleBtn">Activer LiveDoors Plus (test) — 2,99€/mois</button>
        <div class="premium-note">Bouton de test en attendant le vrai paiement Stripe. Remplacer par le vrai flux d'achat avant mise en ligne publique.</div>
      </div>

      <!-- ---- Personnalisation (Premium) ---- -->
      <div id="customizeSection" style="display:none;">
        <div class="section-label">🎨 Thème</div>
        <div class="customize-row" id="themeSwatches"></div>

        <div class="section-label">🔔 Son de sonnette</div>
        <div class="customize-row" id="ringtoneChips"></div>

        <div class="section-label">🏷️ Badge</div>
        <div class="customize-row" id="badgeChips"></div>
      </div>

      <div class="section-label">Ajouter un contact</div>
      <div style="display:flex; gap:8px; margin-bottom:20px;">
        <input class="field-input" id="contactPhoneInput" type="tel" placeholder="Numéro de téléphone" style="flex:1;">
        <button class="toggle-btn" id="addContactBtn">Ajouter</button>
      </div>

      <div class="section-label live-label"><span class="dot"></span>En direct maintenant</div>
      <div id="liveList"></div>

      <div class="section-label closed-label"><span class="dot"></span>Portes fermées</div>
      <div id="closedList"></div>

      <div class="section-label">🕒 Historique des passages <span class="locked-label" id="historyLockLabel">Premium</span></div>
      <div id="historyList"></div>

      <div class="section-label">🚫 Numéros bloqués</div>
      <div style="display:flex; gap:8px; margin-bottom:10px;">
        <input class="field-input" id="blockPhoneInput" type="tel" placeholder="Bloquer un numéro" style="flex:1;">
        <button class="toggle-btn" id="blockPhoneBtn">Bloquer</button>
      </div>
      <div id="blockedList"></div>

    </div>

    <div class="modal-backdrop" id="joinModal">
      <div class="modal-card">
        <div class="modal-title" id="joinModalTitle">Rejoindre</div>
        <textarea class="field-textarea" id="joinMessageInput" rows="2" maxlength="140" placeholder="Un petit message (optionnel)..."></textarea>
        <div class="modal-actions">
          <button class="modal-cancel-btn" id="joinModalCancel">Annuler</button>
          <button class="toggle-btn" id="joinModalSend">Toc toc 👋</button>
        </div>
      </div>
    </div>

    <div class="call-overlay" id="callOverlay">
      <div class="call-glow"></div>
      <div class="call-avatar" id="callAvatar"></div>
      <div>
        <div class="call-status" id="callStatusLabel">Connexion...</div>
        <div class="call-name" id="callName"></div>
        <div class="call-timer" id="callTimer">00:00</div>
      </div>

      <div class="video-grid" id="videoGrid" style="display:none;"></div>

      <div class="call-controls">
        <button class="mute-btn" id="muteBtn">Couper le micro</button>
        <button class="cam-btn" id="camBtn">Caméra</button>
        <button class="screen-btn" id="screenBtn">Partager l'écran</button>
        <button class="leave-btn" id="leaveBtn">Quitter</button>
      </div>

      <div id="incomingRequest">
        <div id="incomingRequestName" style="font-family:'Baloo 2', sans-serif; font-weight:700; color:#fff; font-size:14px; margin-bottom:4px;"></div>
        <div class="incoming-msg" id="incomingRequestMsg" style="display:none;"></div>
        <div style="display:flex; gap:10px; justify-content:center;">
          <button class="mute-btn" id="declineRequestBtn" style="background:rgba(255,61,119,0.25); border-color:rgba(255,61,119,0.4);">Refuser</button>
          <button class="mute-btn" id="acceptRequestBtn" style="background:var(--yellow); color:#14171a; border-color:transparent;">Accepter</button>
        </div>
      </div>
    </div>

    <div class="toast-zone" id="toastZone"></div>
    <div id="remoteAudioContainer" style="display:none;"></div>

  </div>

</div>
`;

const PAGE_CLIENT_JS = `
const socket = io();
const ICE_SERVERS = [{ urls: 'stun:stun.l.google.com:19302' }];
const \$ = (id) => document.getElementById(id);

const THEMES = [
  { id: 'yellow', label: 'Jaune', grad2: '#ff8a00', grad3: '#ff3d77' },
  { id: 'violet', label: 'Violet', grad2: '#7c5cff', grad3: '#ff3d77' },
  { id: 'menthe', label: 'Menthe', grad2: '#00c2a8', grad3: '#4d8bff' },
  { id: 'feu', label: 'Feu', grad2: '#ff3d00', grad3: '#ffb020' },
  { id: 'nuit', label: 'Nuit', grad2: '#2b2f77', grad3: '#7c5cff' },
];
const RINGTONES = [
  { id: 'classique', label: 'Classique', notes: [660, 880] },
  { id: 'cloche', label: 'Cloche', notes: [523, 659, 784] },
  { id: 'synth', label: 'Synthé', notes: [220, 440, 330, 550] },
  { id: 'cyber', label: 'Cyber', notes: [200, 800, 200, 800] },
];
const BADGES = ['👑', '💎', '🔥', '🌟', '🚀', '🦋'];

let me = null;
let friends = [];
let localStream = null;
let screenStream = null;
let peers = new Map();
let inCall = false;
let callSeconds = 0;
let callTimerHandle = null;
let camOn = false;
let screenOn = false;

// ---------------------------------------------------------------------------
// Thème clair / sombre (indépendant du thème de couleur Premium)
// ---------------------------------------------------------------------------

function applyDisplayMode(mode) {
  document.documentElement.setAttribute('data-theme', mode);
  \$('themeBtn').textContent = mode === 'dark' ? '☀️' : '🌙';
  localStorage.setItem('livedoors-theme', mode);
}
applyDisplayMode(localStorage.getItem('livedoors-theme') || 'light');
\$('themeBtn').addEventListener('click', () => {
  const current = document.documentElement.getAttribute('data-theme');
  applyDisplayMode(current === 'dark' ? 'light' : 'dark');
});

function applyColorTheme(themeId) {
  const theme = THEMES.find((t) => t.id === themeId) || THEMES[0];
  document.documentElement.style.setProperty('--grad-2', theme.grad2);
  document.documentElement.style.setProperty('--grad-3', theme.grad3);
}

// Petits sons de sonnette synthétisés (aucun fichier audio à héberger).
let audioCtx = null;
function playRingtone(id) {
  const tone = RINGTONES.find((r) => r.id === id) || RINGTONES[0];
  try {
    audioCtx = audioCtx || new (window.AudioContext || window.webkitAudioContext)();
    tone.notes.forEach((freq, i) => {
      const osc = audioCtx.createOscillator();
      const gain = audioCtx.createGain();
      osc.type = 'sine';
      osc.frequency.value = freq;
      gain.gain.setValueAtTime(0.0001, audioCtx.currentTime + i * 0.16);
      gain.gain.linearRampToValueAtTime(0.15, audioCtx.currentTime + i * 0.16 + 0.02);
      gain.gain.exponentialRampToValueAtTime(0.0001, audioCtx.currentTime + i * 0.16 + 0.15);
      osc.connect(gain).connect(audioCtx.destination);
      osc.start(audioCtx.currentTime + i * 0.16);
      osc.stop(audioCtx.currentTime + i * 0.16 + 0.16);
    });
  } catch (e) {}
}

// ---------------------------------------------------------------------------
// Authentification
// ---------------------------------------------------------------------------

let authMode = 'login';

function setAuthMode(mode) {
  authMode = mode;
  \$('tabLogin').classList.toggle('active', mode === 'login');
  \$('tabRegister').classList.toggle('active', mode === 'register');
  \$('pseudoField').style.display = mode === 'register' ? 'block' : 'none';
  \$('authSubmitBtn').textContent = mode === 'register' ? 'Créer mon compte' : 'Se connecter';
  \$('authError').style.display = 'none';
}
\$('tabLogin').addEventListener('click', () => setAuthMode('login'));
\$('tabRegister').addEventListener('click', () => setAuthMode('register'));

\$('authSubmitBtn').addEventListener('click', () => {
  const phone = \$('phoneInput').value.trim();
  const password = \$('passwordInput').value;
  const pseudo = \$('pseudoInput').value.trim();
  \$('authError').style.display = 'none';

  if (!phone || !password) {
    \$('authError').textContent = 'Numéro et mot de passe requis.';
    \$('authError').style.display = 'block';
    return;
  }
  if (authMode === 'register') {
    if (!pseudo) { \$('pseudoInput').focus(); return; }
    socket.emit('auth:register', { pseudo, phone, password });
  } else {
    socket.emit('auth:login', { phone, password });
  }
});

socket.on('auth:error', ({ message }) => {
  \$('authError').textContent = message;
  \$('authError').style.display = 'block';
});

socket.on('auth:token-invalid', () => {
  localStorage.removeItem('livedoors-token');
  \$('authLoading').style.display = 'none';
  \$('authForm').style.display = 'block';
});

const savedToken = localStorage.getItem('livedoors-token');
if (savedToken) {
  \$('authForm').style.display = 'none';
  \$('authLoading').style.display = 'block';
  socket.emit('auth:token', { token: savedToken });
} else {
  \$('authForm').style.display = 'block';
}

\$('logoutBtn').addEventListener('click', () => {
  localStorage.removeItem('livedoors-token');
  window.location.reload();
});

socket.on('registered', (user) => {
  me = user;
  if (user.token) localStorage.setItem('livedoors-token', user.token);

  \$('loginScreen').style.display = 'none';
  \$('homeScreen').style.display = 'flex';

  \$('headerAvatar').textContent = user.avatarInitials;
  \$('headerAvatar').style.background = \`linear-gradient(135deg, \${user.avatarColor}, #ff3d77)\`;
  \$('myAvatar').textContent = user.avatarInitials;
  \$('myAvatar').style.background = \`linear-gradient(135deg, \${user.avatarColor}, #ff3d77)\`;
  \$('myName').textContent = user.pseudo;
  \$('myPhone').textContent = user.phone || '';
  \$('connectionState').textContent = 'Connecté';

  applyColorTheme(user.theme || 'yellow');
  renderPremiumUI();
});

// ---------------------------------------------------------------------------
// Premium — bouton de test (à remplacer par le vrai paiement Stripe)
// ---------------------------------------------------------------------------

\$('premiumToggleBtn').addEventListener('click', () => {
  socket.emit('account:set-premium', { premium: !(me && me.isPremium) });
});

function buildCustomizeUI() {
  \$('themeSwatches').innerHTML = THEMES.map((t) => \`
    <button class="swatch-btn \${me.theme === t.id ? 'active' : ''}" style="background:linear-gradient(135deg,\${t.grad2},\${t.grad3})" onclick="setTheme('\${t.id}')" title="\${t.label}"></button>
  \`).join('');
  \$('ringtoneChips').innerHTML = RINGTONES.map((r) => \`
    <button class="chip-btn \${me.ringtone === r.id ? 'active' : ''}" onclick="setRingtone('\${r.id}')">\${r.label}</button>
  \`).join('');
  \$('badgeChips').innerHTML = BADGES.map((b) => \`
    <button class="chip-btn \${me.badge === b ? 'active' : ''}" onclick="setBadge('\${b}')">\${b}</button>
  \`).join('') + \`<button class="chip-btn \${!me.badge ? 'active' : ''}" onclick="setBadge('')">Aucun</button>\`;
}

function setTheme(id) { socket.emit('account:customize', { theme: id }); }
function setRingtone(id) { socket.emit('account:customize', { ringtone: id }); playRingtone(id); }
function setBadge(b) { socket.emit('account:customize', { badge: b }); }
window.setTheme = setTheme;
window.setRingtone = setRingtone;
window.setBadge = setBadge;

function renderPremiumUI() {
  const isPremium = !!(me && me.isPremium);
  \$('headerPremiumBadge').style.display = isPremium ? 'inline-block' : 'none';
  \$('myRing').classList.toggle('premium-ring', isPremium);
  \$('premiumCard').classList.toggle('is-active', isPremium);
  \$('premiumTitleWord').textContent = isPremium ? 'Plus ✓' : 'Plus';
  \$('premiumSub').textContent = isPremium
    ? "Merci pour ton soutien ! Toutes les fonctionnalités Plus sont actives."
    : "Salons jusqu'à 8, caméra & partage d'écran, thèmes, sons, portes VIP, historique.";
  \$('premiumToggleBtn').textContent = isPremium ? 'Désactiver LiveDoors Plus (test)' : 'Activer LiveDoors Plus (test) — 2,99€/mois';
  \$('premiumToggleBtn').classList.toggle('deactivate', isPremium);

  \$('customizeSection').style.display = isPremium ? 'block' : 'none';
  \$('vipRow').style.display = isPremium ? 'flex' : 'none';
  \$('historyLockLabel').style.display = isPremium ? 'none' : 'inline';

  \$('camBtn').classList.toggle('locked', !isPremium);
  \$('screenBtn').classList.toggle('locked', !isPremium);

  if (isPremium) buildCustomizeUI();
  applyColorTheme((me && me.theme) || 'yellow');
  updateVipButton();
  renderHistory();
}

socket.on('account:updated', (patch) => {
  me = { ...me, ...patch };
  renderPremiumUI();
});

// ---------------------------------------------------------------------------
// Présence — amis en temps réel
// ---------------------------------------------------------------------------

socket.on('friends:update', (list) => {
  friends = list.filter((u) => !me || u.id !== me.id);
  const myUpdated = list.find((u) => me && u.id === me.id);
  if (myUpdated) { me = { ...me, ...myUpdated }; syncMyDoorUI(); }
  render();
});

let blockedList = [];
socket.on('account:blocked', (list) => { blockedList = list; renderBlocked(); });

let historyList = [];
socket.on('account:history', (list) => { historyList = list; renderHistory(); });

function renderHistory() {
  if (!me || !me.isPremium) {
    \$('historyList').innerHTML = \`<div class="empty-note">Passe à LiveDoors Plus pour voir qui est passé pendant ton absence.</div>\`;
    return;
  }
  \$('historyList').innerHTML = historyList.length ? historyList.map((h) => \`
    <div class="history-row">
      <div class="history-info">
        <div class="history-name">\${escapeHtml(h.pseudo)}</div>
        <div class="history-time">\${new Date(h.timestamp).toLocaleString('fr-FR', { day:'2-digit', month:'2-digit', hour:'2-digit', minute:'2-digit' })}</div>
      </div>
      <div class="history-status \${h.status}">\${h.status === 'accepted' ? 'Accepté' : h.status === 'declined' ? 'Refusé' : 'Manqué'}</div>
    </div>
  \`).join('') : \`<div class="empty-note">Aucun passage enregistré pour l'instant.</div>\`;
}

function friendMeta(f) {
  if (f.doorOpen) return f.companions === 0 ? "seul pour l'instant" : \`+\${f.companions} déjà dans l'appel\`;
  return 'porte fermée';
}

function sortFavoritesFirst(list) {
  return [...list].sort((a, b) => (b.isFavorite ? 1 : 0) - (a.isFavorite ? 1 : 0));
}

function render() {
  const live = sortFavoritesFirst(friends.filter((f) => f.doorOpen));
  const closed = sortFavoritesFirst(friends.filter((f) => !f.doorOpen));
  const isPremium = !!(me && me.isPremium);

  \$('liveList').innerHTML = live.length ? live.map((f) => \`
    <div class="friend-row is-open">
      <div class="avatar-wrap">
        <div class="story-ring show"></div>
        <div class="avatar" style="background:\${f.avatarColor}">\${f.avatarInitials}</div>
      </div>
      <div class="friend-info">
        <div class="friend-name">\${f.isFavorite ? '⭐ ' : ''}\${f.badge ? f.badge + ' ' : ''}\${escapeHtml(f.pseudo)}</div>
        <div class="friend-phone">\${f.phone ? escapeHtml(f.phone) : ''}</div>
        <div class="friend-meta live-meta">\${friendMeta(f)}</div>
        \${f.doorMessage ? \`<div class="friend-status-msg">\${escapeHtml(f.doorMessage)}</div>\` : ''}
      </div>
      <div class="row-actions">
        <button class="mini-btn \${f.isFavorite ? 'is-fav' : ''}" onclick="toggleFavorite('\${f.phone}', \${!f.isFavorite})" title="Favori">⭐</button>
        \${isPremium ? \`<button class="mini-btn \${f.isCloseFriend ? 'is-vip' : ''}" onclick="toggleCloseFriend('\${f.phone}', \${!f.isCloseFriend})" title="Ami proche (VIP)">💎</button>\` : ''}
        <button class="mini-btn" onclick="blockContact('\${f.phone}')" title="Bloquer">🚫</button>
      </div>
      <button class="join-btn" onclick="openJoinModal('\${f.id}', '\${escapeAttr(f.pseudo)}')" \${(inCall || pendingRequestHostId) ? 'disabled' : ''}>\${pendingRequestHostId === f.id ? 'Envoyée...' : 'Rejoindre'}</button>
    </div>
  \`).join('') : \`<div class="empty-note">Personne n'a ouvert sa porte pour l'instant.</div>\`;

  \$('closedList').innerHTML = closed.length ? closed.map((f) => \`
    <div class="friend-row is-closed">
      <div class="avatar-wrap">
        <div class="avatar" style="background:\${f.avatarColor}">\${f.avatarInitials}</div>
      </div>
      <div class="friend-info">
        <div class="friend-name">\${f.isFavorite ? '⭐ ' : ''}\${f.badge ? f.badge + ' ' : ''}\${escapeHtml(f.pseudo)}</div>
        <div class="friend-phone">\${f.phone ? escapeHtml(f.phone) : ''}</div>
      </div>
      <div class="row-actions">
        <button class="mini-btn \${f.isFavorite ? 'is-fav' : ''}" onclick="toggleFavorite('\${f.phone}', \${!f.isFavorite})" title="Favori">⭐</button>
        \${isPremium ? \`<button class="mini-btn \${f.isCloseFriend ? 'is-vip' : ''}" onclick="toggleCloseFriend('\${f.phone}', \${!f.isCloseFriend})" title="Ami proche (VIP)">💎</button>\` : ''}
        <button class="mini-btn" onclick="blockContact('\${f.phone}')" title="Bloquer">🚫</button>
      </div>
    </div>
  \`).join('') : \`<div class="empty-note">Aucun autre compte connecté pour le moment.</div>\`;
}

function renderBlocked() {
  \$('blockedList').innerHTML = blockedList.length ? blockedList.map((b) => \`
    <div class="blocked-row">
      <div class="blocked-phone">\${escapeHtml(b.phone)}</div>
      <button class="unblock-btn" onclick="unblockContact('\${b.phone}')">Débloquer</button>
    </div>
  \`).join('') : \`<div class="empty-note">Aucun numéro bloqué.</div>\`;
}

function toggleFavorite(phone, favorite) { socket.emit('contact:favorite', { phone, favorite }); }
window.toggleFavorite = toggleFavorite;

function toggleCloseFriend(phone, closeFriend) {
  socket.emit('contact:closefriend', { phone, closeFriend });
  showToast(closeFriend ? 'Ajouté à tes amis proches (VIP).' : 'Retiré des amis proches.');
}
window.toggleCloseFriend = toggleCloseFriend;

function blockContact(phone) { socket.emit('contact:block', { phone, blocked: true }); showToast('Contact bloqué.'); }
window.blockContact = blockContact;
function unblockContact(phone) { socket.emit('contact:block', { phone, blocked: false }); }
window.unblockContact = unblockContact;

\$('blockPhoneBtn').addEventListener('click', () => {
  const phone = \$('blockPhoneInput').value.trim();
  if (!phone) { \$('blockPhoneInput').focus(); return; }
  socket.emit('contact:block', { phone, blocked: true });
  \$('blockPhoneInput').value = '';
  showToast('Numéro bloqué.');
});

function escapeHtml(str) { const d = document.createElement('div'); d.textContent = str; return d.innerHTML; }
function escapeAttr(str) { return String(str).replace(/'/g, "\\\\'"); }

function showToast(msg) {
  const t = document.createElement('div');
  t.className = 'toast';
  t.textContent = msg;
  \$('toastZone').appendChild(t);
  requestAnimationFrame(() => t.classList.add('show'));
  setTimeout(() => { t.classList.remove('show'); setTimeout(() => t.remove(), 350); }, 2800);
}

// ---------------------------------------------------------------------------
// Ma porte + mon statut + VIP
// ---------------------------------------------------------------------------

function syncMyDoorUI() {
  \$('toggleBtn').textContent = me.doorOpen ? 'Fermer' : 'Ouvrir';
  \$('toggleBtn').classList.toggle('is-open', me.doorOpen);
  \$('myRing').classList.toggle('show', me.doorOpen);
  \$('statusText').textContent = me.doorOpen
    ? (me.companions > 0 ? \`Porte ouverte · \${me.companions} ami(s) dans ton appel\` : "Porte ouverte · en attente d'amis...")
    : 'Porte fermée';
  \$('statusText').classList.toggle('live', me.doorOpen);
}

function updateVipButton() {
  const on = me && me.vipOnly;
  \$('vipToggleBtn').textContent = on ? '💎 Porte VIP : activée (amis proches seulement)' : '💎 Porte VIP : désactivée';
  \$('vipToggleBtn').classList.toggle('is-open', !!on);
}

\$('vipToggleBtn').addEventListener('click', () => {
  socket.emit('door:vip', { vipOnly: !(me && me.vipOnly) });
});

\$('toggleBtn').addEventListener('click', async () => {
  if (!me) return;
  if (!me.doorOpen) {
    try {
      localStream = await navigator.mediaDevices.getUserMedia({
        audio: me.isPremium
          ? { echoCancellation: true, noiseSuppression: true, autoGainControl: true }
          : { echoCancellation: true },
      });
    } catch (err) {
      showToast("Micro refusé — active-le pour ouvrir ta porte.");
      return;
    }
    const message = \$('doorMessageInput').value.trim();
    socket.emit('door:open', { message });
    startCallUI({ id: me.id, pseudo: 'En attente...', avatarInitials: me.avatarInitials, avatarColor: me.avatarColor }, true);
  } else {
    socket.emit('door:close');
    endCall('local-close');
  }
});

\$('doorMessageBtn').addEventListener('click', () => {
  const message = \$('doorMessageInput').value.trim();
  socket.emit('door:message', { message });
  showToast('Statut mis à jour.');
});

// ---------------------------------------------------------------------------
// Rejoindre — modale de message, puis "Toc Toc"
// ---------------------------------------------------------------------------

let pendingRequestHostId = null;

function openJoinModal(hostId, pseudo) {
  if (inCall || pendingRequestHostId) return;
  \$('joinModalTitle').textContent = \`Rejoindre \${pseudo}\`;
  \$('joinMessageInput').value = '';
  \$('joinModal').dataset.hostId = hostId;
  \$('joinModal').dataset.hostPseudo = pseudo;
  \$('joinModal').classList.add('show');
}
window.openJoinModal = openJoinModal;

\$('joinModalCancel').addEventListener('click', () => \$('joinModal').classList.remove('show'));

\$('joinModalSend').addEventListener('click', () => {
  const hostId = \$('joinModal').dataset.hostId;
  const pseudo = \$('joinModal').dataset.hostPseudo;
  const message = \$('joinMessageInput').value.trim();
  \$('joinModal').classList.remove('show');

  const host = friends.find((f) => f.id === hostId);
  if (!host) return;

  pendingRequestHostId = hostId;
  showToast(\`Demande envoyée à \${pseudo}...\`);
  socket.emit('call:request', { hostId, message });
  render();
});

socket.on('call:declined', () => { showToast('Ta demande a été refusée.'); pendingRequestHostId = null; render(); });

socket.on('call:accepted', async ({ hostId }) => {
  const host = friends.find((f) => f.id === hostId);
  pendingRequestHostId = null;
  if (!host) return;
  try {
    localStream = await navigator.mediaDevices.getUserMedia({
      audio: me.isPremium
        ? { echoCancellation: true, noiseSuppression: true, autoGainControl: true }
        : { echoCancellation: true },
    });
  } catch (err) { showToast("Impossible d'accéder au micro."); return; }
  startCallUI(host, false);
  socket.emit('call:ready', { hostId });
});

// ---------------------------------------------------------------------------
// WebRTC
// ---------------------------------------------------------------------------

function isPolite(peerId) { return socket.id < peerId; }

function createPeerConnection(peerId) {
  const pc = new RTCPeerConnection({ iceServers: ICE_SERVERS });
  pc.makingOffer = false;
  pc.ignoreOffer = false;

  if (localStream) localStream.getTracks().forEach((track) => pc.addTrack(track, localStream));

  pc.onnegotiationneeded = async () => {
    try {
      pc.makingOffer = true;
      const offer = await pc.createOffer();
      await pc.setLocalDescription(offer);
      socket.emit('webrtc:offer', { targetId: peerId, offer: pc.localDescription });
    } catch (err) {} finally { pc.makingOffer = false; }
  };

  pc.onicecandidate = (event) => {
    if (event.candidate) socket.emit('webrtc:ice-candidate', { targetId: peerId, candidate: event.candidate });
  };

  pc.ontrack = (event) => attachRemoteTrack(peerId, event.track, event.streams[0]);

  pc.onconnectionstatechange = () => {
    if (['failed', 'disconnected', 'closed'].includes(pc.connectionState)) removePeer(peerId);
  };

  peers.set(peerId, pc);
  return pc;
}

function attachRemoteTrack(peerId, track, stream) {
  if (track.kind === 'video') {
    ensureVideoTile(peerId, stream);
  } else {
    let audioEl = document.getElementById(\`audio-\${peerId}\`);
    if (!audioEl) {
      audioEl = document.createElement('audio');
      audioEl.id = \`audio-\${peerId}\`;
      audioEl.autoplay = true;
      \$('remoteAudioContainer').appendChild(audioEl);
    }
    audioEl.srcObject = stream;
  }
}

function ensureVideoTile(peerId, stream) {
  \$('videoGrid').style.display = 'grid';
  let tile = document.getElementById(\`videotile-\${peerId}\`);
  if (!tile) {
    tile = document.createElement('div');
    tile.className = 'video-tile';
    tile.id = \`videotile-\${peerId}\`;
    const video = document.createElement('video');
    video.autoplay = true;
    video.playsInline = true;
    const label = document.createElement('div');
    label.className = 'video-tile-label';
    const peerInfo = friends.find((f) => f.id === peerId);
    label.textContent = peerInfo ? peerInfo.pseudo : (peerId === 'me' ? 'Toi' : 'Participant');
    tile.appendChild(video);
    tile.appendChild(label);
    \$('videoGrid').appendChild(tile);
  }
  tile.querySelector('video').srcObject = stream;
}

function removeVideoTile(peerId) {
  document.getElementById(\`videotile-\${peerId}\`)?.remove();
  if (!\$('videoGrid').children.length) \$('videoGrid').style.display = 'none';
}

function removePeer(peerId) {
  const pc = peers.get(peerId);
  if (pc) { pc.close(); peers.delete(peerId); }
  document.getElementById(\`audio-\${peerId}\`)?.remove();
  removeVideoTile(peerId);
}

socket.on('call:room-state', async ({ members }) => { for (const member of members) createPeerConnection(member.id); });

socket.on('call:peer-joined', (peer) => {
  showToast(\`\${peer.pseudo} a rejoint l'appel\`);
  if (me && me.isPremium) playRingtone(me.ringtone || 'classique');
  updateCallStatus();
});

\$('addContactBtn').addEventListener('click', () => {
  const phone = \$('contactPhoneInput').value.trim();
  if (!phone) { \$('contactPhoneInput').focus(); return; }
  socket.emit('contact:add', { phone });
  \$('contactPhoneInput').value = '';
});

socket.on('contact:added', ({ phone, found }) => {
  showToast(found ? 'Contact ajouté !' : "Contact ajouté, il apparaîtra dès qu'il sera connecté.");
});

let incomingRequestFromId = null;

socket.on('call:incoming-request', (from) => {
  incomingRequestFromId = from.id;
  \$('incomingRequestName').textContent = \`\${from.pseudo} veut rejoindre\`;
  if (from.message) { \$('incomingRequestMsg').textContent = \`"\${from.message}"\`; \$('incomingRequestMsg').style.display = 'block'; }
  else { \$('incomingRequestMsg').style.display = 'none'; }
  \$('incomingRequest').style.display = 'block';
  if (me && me.isPremium) playRingtone(me.ringtone || 'classique');
});

\$('acceptRequestBtn').addEventListener('click', () => {
  if (!incomingRequestFromId) return;
  socket.emit('call:accept', { fromId: incomingRequestFromId });
  \$('incomingRequest').style.display = 'none';
  incomingRequestFromId = null;
});

\$('declineRequestBtn').addEventListener('click', () => {
  if (!incomingRequestFromId) return;
  socket.emit('call:decline', { fromId: incomingRequestFromId });
  \$('incomingRequest').style.display = 'none';
  incomingRequestFromId = null;
});

socket.on('webrtc:offer', async ({ fromId, offer }) => {
  const pc = peers.get(fromId) || createPeerConnection(fromId);
  const offerCollision = offer.type === 'offer' && (pc.makingOffer || pc.signalingState !== 'stable');
  pc.ignoreOffer = !isPolite(fromId) && offerCollision;
  if (pc.ignoreOffer) return;
  await pc.setRemoteDescription(offer);
  const answer = await pc.createAnswer();
  await pc.setLocalDescription(answer);
  socket.emit('webrtc:answer', { targetId: fromId, answer: pc.localDescription });
});

socket.on('webrtc:answer', async ({ fromId, answer }) => { const pc = peers.get(fromId); if (pc) await pc.setRemoteDescription(answer); });
socket.on('webrtc:ice-candidate', async ({ fromId, candidate }) => { const pc = peers.get(fromId); if (pc) { try { await pc.addIceCandidate(candidate); } catch (_) {} } });
socket.on('call:peer-left', ({ id }) => { removePeer(id); updateCallStatus(); });
socket.on('call:ended', () => { showToast("L'hôte a fermé sa porte."); endCall('host-closed'); });
socket.on('call:error', ({ message }) => showToast(message));

// ---------------------------------------------------------------------------
// UI de l'écran d'appel
// ---------------------------------------------------------------------------

function startCallUI(target, isHosting) {
  inCall = true;
  callSeconds = 0;
  \$('callAvatar').style.background = target.avatarColor;
  \$('callAvatar').textContent = target.avatarInitials;
  \$('callName').textContent = isHosting ? 'Ta porte est ouverte' : target.pseudo;
  \$('callStatusLabel').textContent = isHosting ? 'En attente' : 'Connexion...';
  \$('callTimer').textContent = '00:00';
  \$('callOverlay').classList.add('active');
  \$('muteBtn').classList.remove('is-muted'); \$('muteBtn').textContent = 'Couper le micro';
  \$('camBtn').classList.remove('is-on'); \$('camBtn').textContent = 'Caméra';
  \$('screenBtn').classList.remove('is-on'); \$('screenBtn').textContent = "Partager l'écran";
  camOn = false; screenOn = false;

  callTimerHandle = setInterval(() => {
    callSeconds++;
    const m = String(Math.floor(callSeconds / 60)).padStart(2, '0');
    const s = String(callSeconds % 60).padStart(2, '0');
    \$('callTimer').textContent = \`\${m}:\${s}\`;
  }, 1000);

  render();
}

function updateCallStatus() {
  const count = peers.size;
  \$('callStatusLabel').textContent = count === 0 ? 'En attente' : \`\${count} personne(s) connectée(s)\`;
}

\$('muteBtn').addEventListener('click', () => {
  if (!localStream) return;
  const track = localStream.getAudioTracks()[0];
  if (!track) return;
  track.enabled = !track.enabled;
  \$('muteBtn').classList.toggle('is-muted', !track.enabled);
  \$('muteBtn').textContent = track.enabled ? 'Couper le micro' : 'Réactiver le micro';
});

\$('camBtn').addEventListener('click', async () => {
  if (!inCall) return;
  if (!me || !me.isPremium) { showToast('Caméra réservée à LiveDoors Plus ✨'); return; }
  if (!camOn) {
    try {
      const camStream = await navigator.mediaDevices.getUserMedia({ video: { width: 1280, height: 720 } });
      const videoTrack = camStream.getVideoTracks()[0];
      localStream.addTrack(videoTrack);
      ensureVideoTile('me', new MediaStream([videoTrack]));
      peers.forEach((pc) => pc.addTrack(videoTrack, localStream));
      camOn = true;
      \$('camBtn').classList.add('is-on'); \$('camBtn').textContent = 'Caméra active';
    } catch (err) { showToast('Caméra refusée ou indisponible.'); }
  } else {
    const videoTrack = localStream.getVideoTracks()[0];
    if (videoTrack) {
      peers.forEach((pc) => { const sender = pc.getSenders().find((s) => s.track === videoTrack); if (sender) pc.removeTrack(sender); });
      videoTrack.stop();
      localStream.removeTrack(videoTrack);
    }
    removeVideoTile('me');
    camOn = false;
    \$('camBtn').classList.remove('is-on'); \$('camBtn').textContent = 'Caméra';
  }
});

\$('screenBtn').addEventListener('click', async () => {
  if (!inCall) return;
  if (!me || !me.isPremium) { showToast("Partage d'écran réservé à LiveDoors Plus ✨"); return; }
  if (!screenOn) {
    try {
      screenStream = await navigator.mediaDevices.getDisplayMedia({ video: true });
      const screenTrack = screenStream.getVideoTracks()[0];
      const existingVideoTrack = localStream.getVideoTracks()[0];
      if (existingVideoTrack) {
        peers.forEach((pc) => { const sender = pc.getSenders().find((s) => s.track === existingVideoTrack); if (sender) sender.replaceTrack(screenTrack); });
        existingVideoTrack.stop();
        localStream.removeTrack(existingVideoTrack);
        localStream.addTrack(screenTrack);
      } else {
        localStream.addTrack(screenTrack);
        peers.forEach((pc) => pc.addTrack(screenTrack, localStream));
      }
      ensureVideoTile('me', new MediaStream([screenTrack]));
      screenOn = true;
      \$('screenBtn').classList.add('is-on'); \$('screenBtn').textContent = "Écran partagé";
      screenTrack.addEventListener('ended', stopScreenShare);
    } catch (err) { showToast("Partage d'écran refusé ou indisponible."); }
  } else {
    stopScreenShare();
  }
});

function stopScreenShare() {
  const screenTrack = localStream.getVideoTracks()[0];
  if (screenTrack) {
    peers.forEach((pc) => { const sender = pc.getSenders().find((s) => s.track === screenTrack); if (sender) pc.removeTrack(sender); });
    screenTrack.stop();
    localStream.removeTrack(screenTrack);
  }
  if (screenStream) { screenStream.getTracks().forEach((t) => t.stop()); screenStream = null; }
  removeVideoTile('me');
  screenOn = false;
  \$('screenBtn').classList.remove('is-on'); \$('screenBtn').textContent = "Partager l'écran";
}

\$('leaveBtn').addEventListener('click', () => {
  const wasHost = me && me.doorOpen;
  socket.emit(wasHost ? 'door:close' : 'call:leave');
  endCall('local-leave');
});

function endCall(reason) {
  inCall = false;
  clearInterval(callTimerHandle);
  \$('callOverlay').classList.remove('active');
  \$('videoGrid').innerHTML = '';
  \$('videoGrid').style.display = 'none';

  peers.forEach((pc, id) => { pc.close(); document.getElementById(\`audio-\${id}\`)?.remove(); });
  peers.clear();

  if (screenStream) { screenStream.getTracks().forEach((t) => t.stop()); screenStream = null; }
  if (localStream) { localStream.getTracks().forEach((t) => t.stop()); localStream = null; }
  camOn = false; screenOn = false;

  render();
}
`;

const PAGE_HTML = '<!DOCTYPE html>\n' +
  '<html lang="fr">\n' +
  '<head>\n' +
  '<meta charset="UTF-8">\n' +
  '<meta name="viewport" content="width=device-width, initial-scale=1">\n' +
  '<title>LiveDoors</title>\n' +
  '<link rel="manifest" href="/manifest.json">\n' +
  '<meta name="theme-color" content="#fffc00">\n' +
  '<link rel="apple-touch-icon" href="/icons/icon-180.png">\n' +
  '<meta name="mobile-web-app-capable" content="yes">\n' +
  '<meta name="apple-mobile-web-app-capable" content="yes">\n' +
  '<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">\n' +
  '<meta name="apple-mobile-web-app-title" content="LiveDoors">\n' +
  '<style>\n' + PAGE_CSS + '\n</style>\n' +
  '</head>\n' +
  '<body>\n' +
  PAGE_BODY_HTML + '\n' +
  '<script src="/socket.io/socket.io.js"></script>\n' +
  '<script>\n' + PAGE_CLIENT_JS + '\n</script>\n' +
  '<script>' + "if (\"serviceWorker\" in navigator) { window.addEventListener(\"load\", () => navigator.serviceWorker.register(\"/sw.js\").catch(() => {})); }" + '</script>\n' +
  '</body>\n' +
  '</html>';

app.get('/', (req, res) => {
  res.send(PAGE_HTML);
});

// ---------------------------------------------------------------------------
// PWA — manifest, service worker et icônes
// ---------------------------------------------------------------------------

const ICON_180_B64 = "iVBORw0KGgoAAAANSUhEUgAAALQAAAC0CAYAAAA9zQYyAAADEUlEQVR4nO3cQU4bQRRF0XLEFjyB/S8NJmwByRlFihQSN4md9r99ztBiUHp9VVg24nT5WJcFEd/2PgDckqBJETQpgiZF0KQImhRBkyJoUgRNiqBJETQpgiZF0KQImhRBkyJoUgRNiqBJETQpgiZF0KQImhRBkyJoUgRNiqBJefr7APdwfnne+whjvL++7X2EmzpV/rediP9dIe7EWw4x30Zhx/FBFx7CI5m+5+igp4//qCbvOjboyaNPMHXfkUFPHXuaiTuPC3riyJNN2zv5OfRajY+g7m1arFuMu6G3EPM2xZ1GBb3lRik+pHvastekm3xU0NeI+e+UdksFDYImRdCkCJoUQZMiaFIETYqgSRE0KYImRdCkCJoUQZMiaFIETYqgSRE0KYImRdCkCJoUQZMiaFIETYqgSRE0KYImRdCkCJoUQZMiaFIETYqgSRE0KYImRdCkCJoUQZPytPcBuJ3zy/Mvr72/vu1wkv24oSM+i/lPr1cJOuBatEeKWtDDbY31KFELerCvRnqEqAVNiqBJETQpgiZF0IN99UuTI3zJIujhtkZ6hJjXEnTCtViPEvNags74XbRHinktf5yUcrR4P+OGJkXQpAiaFEGTImhSBE2KoEkRNCmCJkXQpAiaFEGTImhSBE2KoEkRNCmCJkXQpAiaFEGTImhSBE2KoEkRNCmCJkXQpAiaFEGTImhSBE2KoEkRNCmCJkXQpAiaFEGTImhSBE2KoEkRNCmCJkXQpAiaFEGTImhSUkGfX573PsJIpd1GBf3++nb1Z0oP53/YsteW3R/FqKC3EvU2xZ2e9j7AvRQfFteNu6En/formLb3uKDXmjfyVBN3Hhn0WjPHnmTqvmODXmvu6I9u8q6jg15r9viPaPqe44Nea/5DeBSFHU+Xj3XZ+xC35iO77QoR/ywZNMeVeMsBPwiaFEGTImhSBE2KoEkRNCmCJkXQpAiaFEGTImhSBE2KoEkRNCmCJkXQpAiaFEGTImhSBE2KoEkRNCmCJkXQpHwH5ZBzVz6sVUAAAAAASUVORK5CYII=";
const ICON_192_B64 = "iVBORw0KGgoAAAANSUhEUgAAAMAAAADACAYAAABS3GwHAAADT0lEQVR4nO3cQU4bQRBA0SHiCmzg/keDDVdAclaWEHGIMcHjrv/eErJoVffvGcsod4e37bBB1K+9FwB7EgBpAiBNAKQJgDQBkCYA0gRAmgBIEwBpAiBNAKQJgDQBkCYA0gRAmgBIEwBpAiBNAKQJgDQBkCYA0gRAmgBIEwBpAiDtfu8FXMPD0+PeS1jW6/PL3kv4UXdT/3Nch/7/mxjDyFcgh/9nTJzrqCfAxA26VVOeBmOeAA7/dU2Z94gApmzGaibMfUQAcKnlA5hwC61s9fkv/SH4K8Of8qHtmgrzHf9F2KobcwuOs1v9lv/M8q9An3H4/4/Jc1w2gMm30opW3Y9lA/iXybfWHqbOc2wAcA4BkCYA0gRAmgBIEwBpAiBNAKQJgDQBkCYA0gRAmgBIEwBpAiBNAKQJgDQBkCYA0gRAmgBIEwBpAiBNAKQJgDQBkCYA0gRAmgBIEwBpAiBNAKQJgDQBkCYA0gRAmgBIEwBpAiBNAKQJgDQBkCYA0gRAmgBIEwBpAiDtfu8FcB0PT49//Oz1+WWHldwWAQx36uB//F05BK9Ag312+C/5dxMJYKivHupqBAIgTQADXXqbF58CAiBNAKQJgDQBkCaAgS79Yqv4hZgASBPAUF+9zYu3/7YJYLRzD3X18G+bP4Yb73i4/TXoaQKIcNhP8wpEmgBIEwBpAiBNAKQJgDQBkCYA0gRAmgBIEwBpAiBNAKQJgDQBkCYA0gRAmgBIEwBpAiBNAKQJgDQBkCYA0gRAmgBIEwBpAiBNAKQJgDQBkCYA0gRAmgBIEwBpAiBNAKQJgDQBkCYA0sYG8PD0uPcSRpk6z2UDeH1+2XsJvLPqfiwbwDmm3lrXNnmO93sv4KcdN2/VG2pPkw/+0d3hbTvsvYjvKGzSrVv5cln+FWjl4U+w+vyXDwC+Y0QAq99Cq5ow9xEBbNuMzVjJlHkv/yH4FB+Mf86Ug3805gnw3rRNuhUT5zryCfCRJ8LlJh769xIBwN+MfAWCcwmANAGQJgDSBECaAEgTAGkCIE0ApAmANAGQJgDSBECaAEgTAGkCIE0ApAmANAGQJgDSBECaAEgTAGkCIE0ApAmANAGQ9hvM7YUNXMU/3gAAAABJRU5ErkJggg==";
const ICON_512_B64 = "iVBORw0KGgoAAAANSUhEUgAAAgAAAAIACAYAAAD0eNT6AAAKzUlEQVR4nO3dS1IjRxRA0cLBFpjA/pcGE7ZAhDySQ00jWUj1SdU9Z9xB5+Tlu5WNw0+Hr+kwAQAp/2x9AABgfQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIIEAAAECQAACBIAABAkAAAgCABAABBAoC7ef6H9Zk77iUAACBIAABAkAAAgCABAABBAoC/Xf4/rmzz6+8AAAAASUVORK5CYII=";
const MANIFEST_JSON = "{\n  \"name\": \"LiveDoors\",\n  \"short_name\": \"LiveDoors\",\n  \"description\": \"Zéro sonnerie. Si c'est ouvert, tu rentres.\",\n  \"start_url\": \"/\",\n  \"scope\": \"/\",\n  \"display\": \"standalone\",\n  \"background_color\": \"#fffc00\",\n  \"theme_color\": \"#fffc00\",\n  \"orientation\": \"portrait\",\n  \"icons\": [\n    { \"src\": \"/icons/icon-192.png\", \"sizes\": \"192x192\", \"type\": \"image/png\", \"purpose\": \"any maskable\" },\n    { \"src\": \"/icons/icon-512.png\", \"sizes\": \"512x512\", \"type\": \"image/png\", \"purpose\": \"any maskable\" }\n  ]\n}";
const SERVICE_WORKER_JS = "const CACHE_NAME = 'livedoors-shell-v4';\nconst SHELL_URLS = ['/', '/manifest.json'];\nself.addEventListener('install', (event) => {\n  event.waitUntil(caches.open(CACHE_NAME).then((cache) => cache.addAll(SHELL_URLS)));\n  self.skipWaiting();\n});\nself.addEventListener('activate', (event) => {\n  event.waitUntil(caches.keys().then((keys) => Promise.all(keys.filter((k) => k !== CACHE_NAME).map((k) => caches.delete(k)))));\n  self.clients.claim();\n});\nself.addEventListener('fetch', (event) => {\n  const url = new URL(event.request.url);\n  if (url.pathname.startsWith('/socket.io/')) return;\n  if (event.request.method !== 'GET') return;\n  event.respondWith(\n    fetch(event.request).then((response) => {\n      if (response.ok) { const clone = response.clone(); caches.open(CACHE_NAME).then((cache) => cache.put(event.request, clone)); }\n      return response;\n    }).catch(() => caches.match(event.request))\n  );\n});\n";

app.get('/manifest.json', (req, res) => res.type('application/manifest+json').send(MANIFEST_JSON));
app.get('/sw.js', (req, res) => res.type('application/javascript').send(SERVICE_WORKER_JS));
app.get('/icons/icon-180.png', (req, res) => res.type('png').send(Buffer.from(ICON_180_B64, 'base64')));
app.get('/icons/icon-192.png', (req, res) => res.type('png').send(Buffer.from(ICON_192_B64, 'base64')));
app.get('/icons/icon-512.png', (req, res) => res.type('png').send(Buffer.from(ICON_512_B64, 'base64')));

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`LiveDoors — serveur tout-en-un sur http://localhost:${PORT}`);
});
