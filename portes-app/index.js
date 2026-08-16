/**
 * LiveDoors — fichier unique.
 *
 * Contient tout le projet : le serveur Express + Socket.io (comptes,
 * contacts, ouverture/fermeture de "porte", demandes d'entrée avec
 * validation, relais de signalisation WebRTC) ET la page cliente complète
 * (HTML + CSS + JS), renvoyée telle quelle par res.send() sur la route "/".
 * Un seul fichier à exécuter :
 *
 *     node index.js
 *
 * Ce serveur NE transporte JAMAIS l'audio/vidéo : il sert uniquement à
 * (1) garder la liste des comptes connectés et leur statut
 * (2) faire office de "central téléphonique" qui relaie les messages
 *     d'appairage WebRTC (offer / answer / ICE candidates) entre deux
 *     navigateurs, qui établissent ensuite une connexion peer-to-peer
 *     directe pour le son et l'image.
 *
 * ⚠️ Stockage en mémoire (Map) : tout est perdu au redémarrage du serveur.
 *    Pour une vraie prod, remplacer `users` par une vraie base de données
 *    et ajouter une authentification (JWT, session...).
 */

const express = require('express');
const http = require('http');
const { randomUUID } = require('crypto');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*' } // à restreindre à votre domaine en production
});

// ---------------------------------------------------------------------------
// État en mémoire
// ---------------------------------------------------------------------------

/** socketId -> { id, pseudo, avatarInitials, avatarColor, phone, phoneKey,
 *                doorOpen, doorMessage, roomId, contacts: Set<phoneKey> } */
const users = new Map();

/** roomId -> { hostId, memberIds: Set<socketId> } */
const rooms = new Map();

function normalizePhone(phone) {
  return String(phone || '').replace(/\D/g, '');
}

function publicUser(u) {
  const room = u.roomId ? rooms.get(u.roomId) : null;
  return {
    id: u.id,
    pseudo: u.pseudo,
    username: u.username || '',
    avatarInitials: u.avatarInitials,
    avatarColor: u.avatarColor,
    avatarConfig: u.avatarConfig || '',
    avatarPhoto: u.avatarPhoto || '',
    phone: u.phone || null,
    doorOpen: u.doorOpen,
    doorMessage: u.doorMessage || '',
    companions: room ? Math.max(0, room.memberIds.size - 1) : 0,
  };
}

// L'avatar composé voyage sous forme de petite recette du genre "3-1-0-5-2-4-1".
// On ne garde que des chiffres et des tirets : impossible d'y glisser autre chose.
function cleanAvatarConfig(value) {
  const raw = String(value || '').slice(0, 60);
  let out = '';
  for (const ch of raw) {
    if ((ch >= '0' && ch <= '9') || ch === '-') out += ch;
  }
  return out;
}

// Le nom d'utilisateur sert à se faire ajouter sans donner son numéro.
// Seulement des lettres, des chiffres, un point ou un tiret bas.
function cleanUsername(value) {
  const raw = String(value || '').toLowerCase().slice(0, 20);
  let out = '';
  for (const ch of raw) {
    const ok = (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch === '_' || ch === '.';
    if (ok) out += ch;
  }
  return out;
}

function findByUsername(username) {
  const key = cleanUsername(username);
  if (key.length < 3) return null;
  return Array.from(users.values()).find((u) => u.username === key) || null;
}

// La photo de profil est déjà réduite à 128 px par le navigateur avant l'envoi.
// On vérifie quand même : uniquement une image encodée, et pas plus de 40 Ko,
// sinon un seul compte pourrait saturer la liste de tous ses contacts.
const PHOTO_MAX = 40000;
function cleanPhoto(value) {
  const raw = String(value || '');
  if (!raw) return '';
  if (raw.length > PHOTO_MAX) return '';
  const ok = raw.indexOf('data:image/jpeg;base64,') === 0
    || raw.indexOf('data:image/png;base64,') === 0
    || raw.indexOf('data:image/webp;base64,') === 0;
  return ok ? raw : '';
}

// N'envoie à chaque compte QUE les comptes qu'il a ajoutés en contact.
// Un blocage vaut dans les DEUX sens : la personne bloquée ne voit plus
// la porte de celui qui l'a bloquée, et inversement.
function broadcastFriends() {
  for (const [socketId, viewer] of users) {
    const list = Array.from(users.values())
      .filter((u) => u.id !== viewer.id && u.phoneKey && viewer.contacts.has(u.phoneKey))
      .filter((u) => !viewer.blocked.has(u.phoneKey))
      .filter((u) => !(viewer.phoneKey && u.blocked.has(viewer.phoneKey)))
      .map(publicUser);
    io.to(socketId).emit('friends:update', list);
  }
}

// ---------------------------------------------------------------------------
// Connexion Socket.io
// ---------------------------------------------------------------------------

io.on('connection', (socket) => {

  socket.on('register', ({ pseudo, username, avatarInitials, avatarColor, avatarConfig, avatarPhoto, phone, contacts, blocked }) => {
    // Se réenregistrer sert aussi à modifier son profil : on referme d'abord
    // proprement porte et appel en cours, sinon une room fantôme resterait
    // ouverte côté serveur avec des participants coincés dedans.
    if (users.has(socket.id)) {
      closeDoorAndRoom(socket.id);
      leaveCurrentRoom(socket.id);
    }

    const user = {
      id: socket.id,
      pseudo: String(pseudo || 'Anonyme').slice(0, 24),
      username: cleanUsername(username),
      // slice(0, 4) et pas de toUpperCase : un emoji d'avatar occupe 2 "cases"
      // en JS et se ferait couper/abîmer par l'ancienne version.
      avatarInitials: String(avatarInitials || pseudo || '??').slice(0, 4),
      avatarColor: avatarColor || '#ff8a00',
      avatarConfig: cleanAvatarConfig(avatarConfig),
      avatarPhoto: cleanPhoto(avatarPhoto),
      phone: phone ? String(phone).slice(0, 32) : null,
      phoneKey: phone ? normalizePhone(phone) : null,
      doorOpen: false,
      doorMessage: '',
      roomId: null,
      contacts: new Set(),
      blocked: new Set(),
    };

    // Le carnet de contacts est gardé côté navigateur (localStorage) et renvoyé
    // à chaque connexion : le serveur, lui, oublie tout dès qu'il redémarre.
    if (Array.isArray(contacts)) {
      contacts.slice(0, 300).forEach((p) => {
        const key = normalizePhone(p);
        if (key) user.contacts.add(key);
      });
    }

    if (Array.isArray(blocked)) {
      blocked.slice(0, 300).forEach((p) => {
        const key = normalizePhone(p);
        if (key) user.blocked.add(key);
      });
    }

    users.set(socket.id, user);
    socket.emit('registered', publicUser(user));
    broadcastFriends();
  });

  socket.on('contact:add', ({ phone }) => {
    const user = users.get(socket.id);
    if (!user) return;
    const key = normalizePhone(phone);
    if (!key) return;

    user.contacts.add(key);
    const found = Array.from(users.values()).some((u) => u.id !== user.id && u.phoneKey === key);
    socket.emit('contact:added', { phone, found });
    broadcastFriends();
  });

  // Ajout par nom d'utilisateur. Le carnet reste rangé par numéro, donc on
  // retrouve d'abord la personne pour récupérer le sien.
  socket.on('contact:addByUsername', ({ username }) => {
    const user = users.get(socket.id);
    if (!user) return;

    const key = cleanUsername(username);
    if (key.length < 3) {
      socket.emit('contact:error', { message: 'Nom trop court (3 caractères minimum).' });
      return;
    }
    if (user.username && key === user.username) {
      socket.emit('contact:error', { message: "C'est toi 🙂" });
      return;
    }

    const target = findByUsername(key);
    if (!target || !target.phoneKey) {
      socket.emit('contact:error', {
        message: "Personne trouvée avec ce nom. Il doit être connecté au moins une fois pour être ajouté comme ça.",
      });
      return;
    }

    user.contacts.add(target.phoneKey);
    socket.emit('contact:added', { phone: target.phone, found: true, username: target.username });
    broadcastFriends();
  });

  // Ouvrir sa porte, avec un petit statut optionnel (ex: "Pause café ☕").
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

  // Changer le statut sans rouvrir/refermer la porte.
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

  // Demande d'entrée ("Toc Toc"), avec un petit message optionnel.
  socket.on('call:request', ({ hostId, message }) => {
    const host = users.get(hostId);
    const me = users.get(socket.id);
    if (!host || !me || !host.doorOpen) {
      socket.emit('call:error', { message: "Cette porte n'est plus ouverte." });
      return;
    }
    // Personne bloquée : on répond exactement comme une porte fermée, sans
    // révéler qu'il y a eu un blocage.
    if ((me.phoneKey && host.blocked.has(me.phoneKey))
      || (host.phoneKey && me.blocked.has(host.phoneKey))) {
      socket.emit('call:error', { message: "Cette porte n'est plus ouverte." });
      return;
    }
    io.to(hostId).emit('call:incoming-request', {
      ...publicUser(me),
      message: message ? String(message).slice(0, 140) : '',
    });
  });

  socket.on('call:decline', ({ fromId }) => {
    io.to(fromId).emit('call:declined');
  });

  socket.on('call:accept', ({ fromId }) => {
    const host = users.get(socket.id);
    if (!host || !host.doorOpen) return;
    io.to(fromId).emit('call:accepted', { hostId: socket.id });
  });

  // Le demandeur a son micro prêt -> il rejoint effectivement la room de l'hôte.
  socket.on('call:ready', ({ hostId }) => {
    const host = users.get(hostId);
    const me = users.get(socket.id);
    if (!host || !me || !host.doorOpen || !host.roomId) {
      socket.emit('call:error', { message: "Cette porte n'est plus ouverte." });
      return;
    }

    const room = rooms.get(host.roomId);
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

  // ---- Relais pur de signalisation WebRTC (audio, vidéo, partage d'écran :
  // le serveur ne comprend pas le contenu, il relaie juste le SDP/ICE) ----
  socket.on('webrtc:offer', ({ targetId, offer }) => {
    io.to(targetId).emit('webrtc:offer', { fromId: socket.id, offer });
  });
  socket.on('webrtc:answer', ({ targetId, answer }) => {
    io.to(targetId).emit('webrtc:answer', { fromId: socket.id, answer });
  });
  socket.on('webrtc:ice-candidate', ({ targetId, candidate }) => {
    io.to(targetId).emit('webrtc:ice-candidate', { fromId: socket.id, candidate });
  });

  // ---- Tchat écrit pendant l'appel ----
  // Le message n'est envoyé qu'aux gens présents dans la même room, et
  // uniquement si l'expéditeur y est lui-même. Rien n'est stocké : quand
  // l'appel se termine, le tchat disparaît avec lui.
  socket.on('chat:message', ({ text }) => {
    const user = users.get(socket.id);
    if (!user || !user.roomId) return;

    const clean = String(text || '').trim().slice(0, 200);
    if (!clean) return;

    io.to(user.roomId).emit('chat:message', {
      fromId: user.id,
      pseudo: user.pseudo,
      avatarInitials: user.avatarInitials,
      avatarColor: user.avatarColor,
      avatarConfig: user.avatarConfig || '',
      text: clean,
      at: Date.now(),
    });
  });

  socket.on('disconnect', () => {
    handleDisconnect(socket.id);
    users.delete(socket.id);
    broadcastFriends();
  });
});

// Quand quelqu'un ferme l'appli ou perd le réseau, on ne coupe PAS l'appel des
// autres. S'il restait au moins deux personnes dans la pièce, la conversation
// continue et l'hôte est repris par quelqu'un d'autre. La pièce n'est fermée
// que s'il ne reste plus personne à qui parler.
function handleDisconnect(socketId) {
  const user = users.get(socketId);
  if (!user) return;

  const roomId = user.roomId;
  const room = roomId ? rooms.get(roomId) : null;

  if (room) {
    room.memberIds.delete(socketId);
    io.sockets.sockets.get(socketId)?.leave(roomId);

    if (room.memberIds.size >= 2) {
      if (room.hostId === socketId) {
        const newHostId = Array.from(room.memberIds)[0];
        const newHost = users.get(newHostId);
        room.hostId = newHostId;
        if (newHost) {
          newHost.doorOpen = true;
          newHost.doorMessage = user.doorMessage || '';
          newHost.roomId = roomId;
        }
        io.to(roomId).emit('call:host-changed', { hostId: newHostId });
      }
      io.to(roomId).emit('call:peer-left', { id: socketId });
    } else {
      room.memberIds.forEach((memberId) => {
        io.to(memberId).emit('call:ended', { reason: 'host-closed' });
        const member = users.get(memberId);
        if (member) { member.roomId = null; member.doorOpen = false; member.doorMessage = ''; }
        io.sockets.sockets.get(memberId)?.leave(roomId);
      });
      rooms.delete(roomId);
    }
  }

  user.doorOpen = false;
  user.doorMessage = '';
  user.roomId = null;
}

function closeDoorAndRoom(socketId) {
  const user = users.get(socketId);
  if (!user || !user.doorOpen) return;

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
  --bg:#ffffff;
  --bg-soft:#f6f6f8;
  --border:#ececef;
  --yellow:#fffc00;
  --yellow-deep:#ffe600;
  --ink:#14171a;
  --ink-soft:#6b7280;
  --ink-faint:#9aa0ac;
  --grad-1:#fffc00;
  --grad-2:#ff8a00;
  --grad-3:#ff3d77;
  --overlay-ink: rgba(0,0,0,0.08);
}

[data-theme="dark"]{
  --bg:#15161a;
  --bg-soft:#1f2026;
  --border:#2a2b32;
  --ink:#f2f2f4;
  --ink-soft:#b7bac2;
  --ink-faint:#7d818c;
  --overlay-ink: rgba(255,255,255,0.08);
}

*{box-sizing:border-box; margin:0; padding:0;}

body{
  min-height:100vh;
  background:#efeff2;
  display:flex;
  align-items:center;
  justify-content:center;
  padding:32px 16px;
  font-family:'Nunito', sans-serif;
}
[data-theme="dark"] body{ background:#0b0b0d; }

.phone{
  width:392px;
  max-width:100%;
  height:820px;
  max-height:92vh;
  background:var(--bg);
  border-radius:38px;
  border:1px solid var(--border);
  box-shadow:0 40px 80px -20px rgba(0,0,0,0.25), 0 0 0 8px #050506;
  position:relative;
  overflow:hidden;
  display:flex;
  flex-direction:column;
}

.screen{ height:100%; display:flex; flex-direction:column; }

/* Sur un vrai téléphone, on ne veut pas d'un "faux téléphone" dessiné au
   milieu de l'écran : l'appli occupe toute la surface. 100dvh (et pas 100vh)
   pour que la barre d'adresse qui apparaît/disparaît ne coupe pas le bas. */
@media (max-width: 600px), (pointer: coarse){
  body{ display:block; padding:0; background:var(--bg); }
  .phone{
    width:100%; max-width:none;
    height:100vh; height:100dvh; max-height:none;
    border:none; border-radius:0; box-shadow:none;
  }
  .app-header{ padding-top:calc(18px + env(safe-area-inset-top)); }
  .content{ padding-bottom:calc(24px + env(safe-area-inset-bottom)); }
  .call-overlay{ padding-bottom:calc(24px + env(safe-area-inset-bottom)); }
  .chat-panel{ padding-bottom:calc(14px + env(safe-area-inset-bottom)); }
}

/* ---------- Login screen ---------- */
.login-screen{ align-items:center; justify-content:center; padding:32px; background:var(--yellow); }
.login-inner{ width:100%; }
.field-label{
  display:block; font-family:'Baloo 2', sans-serif; font-weight:700;
  font-size:12.5px; color:var(--ink); margin:14px 0 6px;
}
.field-input, .field-textarea{
  width:100%; padding:13px 14px; border-radius:12px; border:none;
  font-family:'Nunito', sans-serif; font-size:14px; background:#fff; color:#14171a;
}
.field-textarea{ resize:none; }
.field-input:focus, .field-textarea:focus{ outline:3px solid rgba(0,0,0,0.15); }
.field-hint{ font-size:10.5px; color:rgba(20,23,26,0.55); margin-top:5px; font-weight:600; }
.primary-btn{
  width:100%; margin-top:24px; padding:14px; border-radius:14px; border:none;
  background:var(--ink); color:var(--yellow); font-family:'Baloo 2', sans-serif;
  font-weight:700; font-size:14.5px; cursor:pointer;
}
[data-theme="dark"] .login-screen .primary-btn{ background:#14171a; color:var(--yellow); }
.primary-btn:active{ transform:scale(0.98); }

/* ---------- Header ---------- */
.app-header{
  background:var(--yellow);
  padding:22px 20px 16px;
  flex-shrink:0;
  display:flex;
  align-items:center;
  justify-content:space-between;
  gap:10px;
}
.app-title{
  font-family:'Baloo 2', sans-serif;
  font-weight:800;
  font-size:22px;
  color:#14171a;
  letter-spacing:-0.3px;
}
.app-sub{
  font-size:11px;
  color:rgba(20,23,26,0.6);
  font-weight:700;
  margin-top:1px;
}
.header-right{ display:flex; align-items:center; gap:8px; flex-shrink:0; }
.theme-btn{
  width:36px; height:36px; border-radius:50%; border:none; cursor:pointer;
  background:rgba(0,0,0,0.08); color:#14171a; font-size:16px;
  display:flex; align-items:center; justify-content:center;
}
.header-avatar{
  width:38px; height:38px;
  border-radius:50%;
  background:linear-gradient(135deg,#ff8a00,#ff3d77);
  display:flex; align-items:center; justify-content:center;
  color:#fff; font-family:'Baloo 2',sans-serif; font-weight:700; font-size:14px;
  border:2px solid rgba(0,0,0,0.08);
}

.content{ flex:1; overflow-y:auto; padding:16px 18px 24px; }
.content::-webkit-scrollbar{ width:0; }

/* ---------- My profile row ---------- */
.me-card{
  background:var(--bg-soft);
  border-radius:20px;
  padding:14px;
  display:flex;
  flex-direction:column;
  gap:10px;
  margin-bottom:22px;
}
.me-card-top{ display:flex; align-items:center; gap:12px; }
.me-avatar-wrap{ position:relative; width:52px; height:52px; flex-shrink:0; }
.me-avatar{
  width:52px; height:52px;
  border-radius:50%;
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
.toggle-btn.is-open{ background:#14171a; color:var(--yellow); box-shadow:0 6px 14px -6px rgba(0,0,0,0.35); }
.toggle-btn:disabled{ opacity:0.5; cursor:not-allowed; }

.status-input-row{ display:flex; gap:8px; }
.status-input{
  flex:1; padding:9px 12px; border-radius:12px; border:1px solid var(--border);
  font-family:'Nunito', sans-serif; font-size:12.5px; background:var(--bg); color:var(--ink);
}
.status-set-btn{
  font-family:'Baloo 2', sans-serif; font-weight:700; font-size:12px; color:var(--ink);
  background:transparent; border:1px solid var(--border); padding:8px 12px; border-radius:12px; cursor:pointer;
}

/* ---------- Section labels ---------- */
.section-label{
  font-family:'Baloo 2', sans-serif; font-size:13px; color:var(--ink); font-weight:700;
  margin:20px 4px 10px; display:flex; align-items:center; gap:7px;
}
.section-label .dot{ width:8px; height:8px; border-radius:50%; }
.live-label .dot{ background:linear-gradient(135deg,var(--grad-2),var(--grad-3)); }
.closed-label .dot{ background:#d7d7dc; }
.offline-label .dot{ background:transparent; border:2px solid #d7d7dc; box-sizing:border-box; }

.avatar-photo{ width:100%; height:100%; object-fit:cover; display:block; }

.contact-actions{ display:flex; gap:4px; margin-top:5px; }
.contact-btn{
  width:26px; height:26px; border-radius:8px; cursor:pointer; padding:0;
  border:1px solid var(--border); background:transparent; font-size:11.5px; line-height:1;
}
.contact-btn:hover{ background:var(--bg-soft); }
.contact-btn.on{ border-color:#e0a800; }
.contact-btn.danger{ border-color:#c0143c; }

/* Zone photo de profil */
.photo-row{ display:flex; gap:6px; margin-bottom:8px; }
.photo-btn{
  flex:1; border:none; cursor:pointer; border-radius:10px; padding:9px;
  background:#14171a; color:var(--yellow);
  font-family:'Baloo 2', sans-serif; font-weight:700; font-size:12px;
}
.photo-btn.secondary{ background:rgba(0,0,0,0.12); color:#14171a; }
.modal-card .photo-btn.secondary{ background:var(--bg-soft); color:var(--ink); }

/* ---------- Friend rows ---------- */
.friend-row{ display:flex; align-items:center; gap:12px; padding:9px 8px; border-radius:16px; margin-bottom:2px; }
.friend-row.is-open:hover{ background:var(--bg-soft); }

.avatar-wrap{ position:relative; flex-shrink:0; width:48px; height:48px; }
.avatar{
  width:48px; height:48px; border-radius:50%; display:flex; align-items:center; justify-content:center;
  font-family:'Baloo 2', sans-serif; font-weight:700; font-size:15px; color:#fff; position:relative; z-index:2;
}
.friend-row.is-closed .avatar{ filter:grayscale(1) brightness(0.92); opacity:0.55; }

.friend-info{ flex:1; min-width:0; }
.friend-name{ font-family:'Baloo 2', sans-serif; font-size:14.5px; font-weight:700; color:var(--ink); }
.friend-row.is-closed .friend-name{ color:var(--ink-soft); }
.friend-phone{ font-family:'JetBrains Mono', monospace; font-size:10.5px; color:var(--ink-faint); margin-top:1px; }
.friend-meta{ font-size:11px; color:var(--ink-faint); margin-top:2px; font-weight:700; }
.friend-meta.live-meta{ color:#e08a00; }
.friend-status-msg{ font-size:11px; color:var(--ink-soft); margin-top:2px; font-style:italic; }

.join-btn{
  font-family:'Baloo 2', sans-serif; font-weight:700; font-size:12.5px; color:#14171a;
  background:var(--yellow); border:none; padding:9px 16px; border-radius:12px; cursor:pointer;
  flex-shrink:0; transition:transform .15s ease;
}
.join-btn:active{ transform:scale(0.94); }
.join-btn:disabled{ opacity:0.5; cursor:not-allowed; }

.empty-note{ font-size:12px; color:var(--ink-faint); padding:4px 8px; font-weight:600; }

/* ---------- Join request modal (message avant d'appeler) ---------- */
.modal-backdrop{
  position:absolute; inset:0; background:rgba(0,0,0,0.45); z-index:40;
  display:none; align-items:center; justify-content:center; padding:24px;
}
.modal-backdrop.show{ display:flex; }
.modal-card{
  background:var(--bg); border-radius:20px; padding:20px; width:100%;
  font-family:'Nunito', sans-serif;
}
.modal-title{ font-family:'Baloo 2', sans-serif; font-weight:700; font-size:15px; color:var(--ink); margin-bottom:10px; }
.modal-actions{ display:flex; gap:10px; margin-top:14px; }
.modal-actions button{ flex:1; }
.modal-cancel-btn{
  font-family:'Baloo 2', sans-serif; font-weight:700; font-size:13px; color:var(--ink);
  background:transparent; border:1px solid var(--border); padding:11px; border-radius:12px; cursor:pointer;
}

/* ---------- Call overlay ---------- */
.call-overlay{
  /* #14171a en dur, PAS var(--ink) : en thème sombre --ink devient presque
     blanc, ce qui donnait un écran d'appel blanc avec du texte blanc dessus. */
  position:absolute; inset:0; background:#14171a; display:flex; flex-direction:column;
  align-items:center; justify-content:center; gap:16px; transform:translateY(100%);
  transition:transform .5s cubic-bezier(.5,0,.2,1); z-index:20; padding:24px;
}
.call-overlay.active{ transform:translateY(0); }
.call-glow{
  position:absolute; width:320px; height:320px; border-radius:50%;
  background:radial-gradient(circle, rgba(255,252,0,0.14), transparent 70%);
  animation:breathe 3.2s ease-in-out infinite;
}
@keyframes breathe{ 0%,100%{ transform:scale(0.94); opacity:0.7; } 50%{ transform:scale(1.06); opacity:1; } }
.call-avatar{
  width:96px; height:96px; border-radius:50%; display:flex; align-items:center; justify-content:center;
  font-family:'Baloo 2', sans-serif; font-weight:700; font-size:30px; color:#fff; z-index:2;
  box-shadow:0 0 0 3px rgba(255,255,255,0.12);
}
.call-status{
  font-size:11px; letter-spacing:0.08em; text-transform:uppercase; color:var(--yellow);
  font-family:'JetBrains Mono', monospace; font-weight:600; z-index:2; text-align:center;
}
.call-name{ font-family:'Baloo 2', sans-serif; font-weight:700; font-size:20px; color:#fff; z-index:2; text-align:center; margin-top:4px; }
.call-timer{ font-family:'JetBrains Mono', monospace; color:rgba(255,255,255,0.55); font-size:13px; z-index:2; text-align:center; margin-top:6px; }

.video-grid{
  display:grid; grid-template-columns:1fr 1fr; gap:8px; width:100%; z-index:2;
  max-height:220px; overflow:hidden;
}
.video-tile{
  position:relative; border-radius:14px; overflow:hidden; background:#000;
  aspect-ratio:4/3; display:flex; align-items:center; justify-content:center;
}
.video-tile video{ width:100%; height:100%; object-fit:cover; }
.video-tile .video-tile-label{
  position:absolute; bottom:4px; left:6px; font-size:10px; color:#fff;
  font-family:'Baloo 2', sans-serif; font-weight:700; text-shadow:0 1px 3px rgba(0,0,0,0.6);
}

.call-controls{ display:flex; gap:8px; z-index:2; margin-top:4px; flex-wrap:wrap; justify-content:center; }
.mute-btn, .leave-btn, .cam-btn, .screen-btn{
  font-family:'Baloo 2', sans-serif; font-weight:700; font-size:12.5px; color:#fff;
  background:rgba(255,255,255,0.1); border:1px solid rgba(255,255,255,0.2);
  padding:11px 16px; border-radius:14px; cursor:pointer;
}
.mute-btn.is-muted, .cam-btn.is-on, .screen-btn.is-on{ background:var(--yellow); color:#14171a; border-color:transparent; }
.mute-btn:hover, .leave-btn:hover, .cam-btn:hover, .screen-btn:hover{ background:rgba(255,255,255,0.16); }

/* ---------- Incoming request card ---------- */
#incomingRequest{
  display:none; z-index:2; text-align:center; background:rgba(255,255,255,0.08);
  padding:14px 18px; border-radius:16px; margin-top:6px; width:100%;
}
.incoming-msg{
  font-size:12px; color:rgba(255,255,255,0.75); font-style:italic; margin-bottom:10px;
}

/* ---------- Toast ---------- */
.toast-zone{
  position:absolute; left:0; right:0; bottom:22px; display:flex; flex-direction:column;
  align-items:center; gap:8px; pointer-events:none; z-index:30;
}
.toast{
  background:var(--ink); color:var(--bg); font-size:12.5px; font-weight:700; padding:10px 16px;
  border-radius:12px; border-left:4px solid var(--yellow); box-shadow:0 10px 24px -8px rgba(0,0,0,0.35);
  opacity:0; transform:translateY(8px); transition:opacity .3s ease, transform .3s ease;
}
.toast.show{ opacity:1; transform:translateY(0); }

/* ---------- Écran de déverrouillage (code secret) ---------- */
.lock-screen{ align-items:center; justify-content:center; padding:32px; background:var(--yellow); }

/* Les écrans profil et déverrouillage restent jaunes dans les deux thèmes :
   on y force donc une encre foncée, sinon le texte devient blanc sur jaune. */
[data-theme="dark"] .login-screen,
[data-theme="dark"] .lock-screen{
  --ink:#14171a;
  --ink-soft:#3c4148;
  --ink-faint:#5a6069;
}
.lock-avatar{
  width:88px; height:88px; border-radius:50%; margin:0 auto 14px;
  display:flex; align-items:center; justify-content:center;
  font-family:'Baloo 2', sans-serif; font-weight:700; font-size:34px; color:#fff;
  box-shadow:0 10px 24px -10px rgba(0,0,0,0.45);
}
.pin-input{ text-align:center; letter-spacing:0.5em; font-size:22px; font-weight:800; }
.link-btn{
  display:block; width:100%; margin-top:12px; background:transparent; border:none; cursor:pointer;
  font-family:'Nunito', sans-serif; font-size:12px; font-weight:700; color:rgba(0,0,0,0.55);
  text-decoration:underline;
}
[data-theme="dark"] .link-btn{ color:rgba(0,0,0,0.6); }
.pin-error{ color:#c0143c; font-weight:800; }

/* ---------- Créateur d'avatar ---------- */
.avatar-preview-row{ display:flex; align-items:center; gap:12px; margin-bottom:10px; }
.avatar-preview{
  width:74px; height:74px; border-radius:50%; flex:none; overflow:hidden; background:#fff;
  box-shadow:0 8px 18px -10px rgba(0,0,0,0.5);
}
.avatar-preview-hint{ font-size:11.5px; font-weight:700; color:rgba(0,0,0,0.55); line-height:1.35; }
.builder{ background:rgba(255,255,255,0.65); border-radius:14px; padding:8px; }
/* Les onglets passent à la ligne : un défilement horizontal au doigt était
   quasi impossible à attraper sur téléphone. */
.builder-tabs{ display:flex; flex-wrap:wrap; gap:4px; margin-bottom:8px; }
.builder-tab{
  flex:none; border:none; cursor:pointer; border-radius:10px; padding:7px 11px;
  background:rgba(0,0,0,0.07); color:#14171a;
  font-family:'Baloo 2', sans-serif; font-weight:700; font-size:11.5px;
}
.builder-tab.active{ background:#14171a; color:var(--yellow); }
.builder-options{ display:grid; grid-template-columns:repeat(6, 1fr); gap:5px; }
.builder-option{
  aspect-ratio:1; padding:0; overflow:hidden; cursor:pointer; background:#fff;
  border:2px solid transparent; border-radius:10px; display:block;
}
.builder-option.selected{ border-color:#14171a; }
.builder-random{
  width:100%; margin-top:8px; border:none; cursor:pointer; border-radius:10px; padding:9px;
  background:#14171a; color:var(--yellow);
  font-family:'Baloo 2', sans-serif; font-weight:700; font-size:12.5px;
}

/* Les avatars sont des SVG : ils doivent remplir leur pastille ronde. */
.avatar svg, .me-avatar svg, .header-avatar svg, .call-avatar svg,
.lock-avatar svg, .avatar-preview svg, .builder-option svg{
  width:100%; height:100%; display:block;
}
.header-avatar{ overflow:hidden; padding:0; cursor:pointer; }
.me-avatar, .avatar, .call-avatar, .lock-avatar{ overflow:hidden; }

/* ---------- Paramètres ---------- */
.segmented{ display:flex; gap:5px; }
.segment{
  flex:1; cursor:pointer; border-radius:11px; padding:10px 4px;
  border:1px solid var(--border); background:transparent; color:var(--ink);
  font-family:'Baloo 2', sans-serif; font-weight:700; font-size:11.5px;
}
.segment.active{ background:var(--yellow); color:#14171a; border-color:transparent; }
.settings-action{
  width:100%; margin-top:6px; cursor:pointer; text-align:left;
  border:1px solid var(--border); background:transparent; color:var(--ink);
  border-radius:11px; padding:11px 13px;
  font-family:'Baloo 2', sans-serif; font-weight:700; font-size:12.5px;
}
.settings-action:hover{ background:var(--bg-soft); }
.settings-action.danger{ color:#c0143c; }

/* ---------- Fenêtre "Modifier mon profil" ---------- */
.modal-card.tall{ max-height:82%; overflow-y:auto; }
.modal-card .field-hint{ color:var(--ink-faint); }
.modal-card .avatar-preview-hint{ color:var(--ink-soft); }
.modal-card .field-input{ border:1px solid var(--border); }

/* ---------- Tchat pendant l'appel ---------- */
.chat-btn{
  font-family:'Baloo 2', sans-serif; font-weight:700; font-size:12.5px; color:#fff;
  background:rgba(255,255,255,0.1); border:1px solid rgba(255,255,255,0.2);
  padding:11px 16px; border-radius:14px; cursor:pointer; position:relative;
}
.chat-btn.is-on{ background:var(--yellow); color:#14171a; border-color:transparent; }
.chat-btn:hover{ background:rgba(255,255,255,0.16); }
.chat-badge{
  display:none; position:absolute; top:-6px; right:-6px; min-width:18px; height:18px;
  border-radius:9px; background:#ff3d77; color:#fff; font-size:10px; line-height:18px;
  padding:0 5px; font-family:'Baloo 2', sans-serif; font-weight:700;
}
.chat-badge.show{ display:block; }

.chat-panel{
  position:absolute; left:0; right:0; bottom:0; height:66%; z-index:6;
  background:rgba(20,23,26,0.94); border-top:1px solid rgba(255,255,255,0.12);
  border-radius:22px 22px 0 0; padding:12px 14px 14px;
  display:none; flex-direction:column; gap:8px;
}
.chat-panel.show{ display:flex; }
.chat-head{ display:flex; align-items:center; justify-content:space-between; }
.chat-title{ font-family:'Baloo 2', sans-serif; font-weight:700; font-size:13px; color:var(--yellow); }
.chat-close{
  background:transparent; border:none; color:rgba(255,255,255,0.7); font-size:16px; cursor:pointer;
  padding:2px 6px;
}
.chat-messages{
  flex:1; overflow-y:auto; display:flex; flex-direction:column; gap:7px; padding-right:2px;
}
.chat-empty{ font-size:12px; color:rgba(255,255,255,0.4); text-align:center; margin:auto 0; font-style:italic; }
.chat-msg{ max-width:82%; display:flex; flex-direction:column; gap:2px; }
.chat-msg.mine{ align-self:flex-end; align-items:flex-end; }
.chat-msg-author{ font-size:10px; font-weight:800; color:rgba(255,255,255,0.5); padding:0 4px; }
.chat-bubble{
  background:rgba(255,255,255,0.12); color:#fff; font-size:13px; font-weight:600;
  padding:8px 12px; border-radius:14px; border-bottom-left-radius:4px; word-break:break-word;
}
.chat-msg.mine .chat-bubble{
  background:var(--yellow); color:#14171a; border-bottom-left-radius:14px; border-bottom-right-radius:4px;
}
.chat-bubble.big-emoji{ background:transparent; font-size:32px; padding:2px 6px; }
/* Panneau d'émojis : une grille qui se déroule vers le haut, avec un vrai
   défilement vertical. L'ancienne barre horizontale était impossible à
   faire glisser au doigt. */
.emoji-panel{
  display:none; background:rgba(255,255,255,0.08); border-radius:12px; padding:6px;
  max-height:150px; overflow-y:auto; -webkit-overflow-scrolling:touch;
}
.emoji-panel.show{ display:block; }
.emoji-grid-chat{ display:grid; grid-template-columns:repeat(7, 1fr); gap:3px; }
.emoji-grid-chat button{
  aspect-ratio:1; background:transparent; border:none; border-radius:9px;
  font-size:19px; cursor:pointer; line-height:1; padding:0;
}
.emoji-grid-chat button:active{ transform:scale(0.88); background:rgba(255,255,255,0.15); }
.emoji-toggle{
  flex:none; width:42px; border-radius:12px; border:1px solid rgba(255,255,255,0.18);
  background:rgba(255,255,255,0.08); font-size:18px; cursor:pointer; line-height:1;
}
.emoji-toggle.is-on{ background:var(--yellow); border-color:transparent; }
.chat-input-row{ display:flex; gap:7px; }
.chat-input{
  flex:1; padding:11px 13px; border-radius:12px; border:1px solid rgba(255,255,255,0.18);
  background:rgba(255,255,255,0.08); color:#fff; font-family:'Nunito', sans-serif;
  font-size:13.5px; font-weight:600;
}
.chat-input::placeholder{ color:rgba(255,255,255,0.4); }
.chat-input:focus{ outline:2px solid var(--yellow); }
.chat-send{
  background:var(--yellow); border:none; border-radius:12px; padding:0 16px; cursor:pointer;
  font-family:'Baloo 2', sans-serif; font-weight:700; font-size:12.5px; color:#14171a;
}

/* ---------- Émojis qui s'envolent ---------- */
.reaction-zone{ position:absolute; inset:0; pointer-events:none; z-index:7; overflow:hidden; }
.floating-emoji{
  position:absolute; bottom:80px; font-size:34px; animation:floatUp 2.6s ease-out forwards;
}
@keyframes floatUp{
  0%{ transform:translateY(0) scale(0.6); opacity:0; }
  15%{ transform:translateY(-30px) scale(1.15); opacity:1; }
  100%{ transform:translateY(-380px) scale(1); opacity:0; }
}

@media (prefers-reduced-motion: reduce){
  .story-ring, .call-glow{ animation:none; }
  .call-overlay, .toast{ transition:none; }
  .floating-emoji{ animation-duration:0.9s; }
}
`;

const PAGE_BODY_HTML = `
<div class="phone" id="phone">

  <!-- ============ ÉCRAN 0 : déverrouillage (compte déjà enregistré) ============ -->
  <div class="screen lock-screen" id="lockScreen" style="display:none;">
    <div class="login-inner" style="text-align:center;">
      <div class="lock-avatar" id="lockAvatar">--</div>
      <div class="app-title" style="font-size:22px;" id="lockName">Bon retour !</div>
      <div class="app-sub" style="margin-bottom:18px;">Entre ton code secret pour ouvrir ton compte</div>

      <input class="field-input pin-input" id="pinInput" type="password" inputmode="numeric" maxlength="6" placeholder="••••">
      <div class="field-hint" id="pinHint">Le code que tu as choisi en créant ton profil.</div>

      <button class="primary-btn" id="unlockBtn">Ouvrir mon compte</button>
      <button class="link-btn" id="forgetBtn">Ce n'est pas moi — changer de compte</button>
    </div>
  </div>

  <!-- ============ ÉCRAN 1 : création de profil / connexion ============ -->
  <div class="screen login-screen" id="loginScreen">
    <div class="login-inner">
      <div class="app-title" style="font-size:26px;">LiveDoors</div>
      <div class="app-sub" style="margin-bottom:20px;">Crée ton profil pour voir tes amis en direct</div>

      <label class="field-label">Pseudo</label>
      <input class="field-input" id="pseudoInput" type="text" placeholder="Ex. Léa" maxlength="24">

      <label class="field-label">Nom d'utilisateur</label>
      <input class="field-input" id="usernameInput" type="text" maxlength="20" placeholder="ex. gino72" autocapitalize="none">
      <div class="field-hint">Lettres, chiffres, _ et . — c'est ce que tes amis taperont pour t'ajouter sans ton numéro.</div>

      <label class="field-label">Numéro de téléphone</label>
      <input class="field-input" id="phoneInput" type="tel" placeholder="06 12 34 56 78">
      <div class="field-hint">Sert à te retrouver auprès de tes vrais contacts. Non vérifié dans cette démo.</div>

      <label class="field-label">Ton avatar</label>
      <div class="avatar-preview-row">
        <div class="avatar-preview" id="avatarPreview"></div>
        <div class="avatar-preview-hint">Compose ta tête 👇<br>Coiffure, yeux, bouche, accessoire…</div>
      </div>
      <div class="photo-row">
        <button class="photo-btn" type="button" id="photoBtn">📷 Mettre une photo</button>
        <button class="photo-btn secondary" type="button" id="photoClearBtn">Retirer</button>
      </div>
      <input type="file" id="photoInput" accept="image/*" style="display:none">
      <div class="builder">
        <div class="builder-tabs" id="builderTabs"></div>
        <div class="builder-options" id="builderOptions"></div>
        <button class="builder-random" type="button" id="builderRandom">🎲 Au hasard</button>
      </div>

      <label class="field-label">Code secret (4 à 6 chiffres)</label>
      <input class="field-input pin-input" id="newPinInput" type="password" inputmode="numeric" maxlength="6" placeholder="••••">
      <div class="field-hint">Il protège ton compte sur cet appareil. Ne le donne à personne.</div>

      <button class="primary-btn" id="registerBtn">Créer mon profil</button>
    </div>
  </div>

  <!-- ============ ÉCRAN 2 : accueil (liste des amis) ============ -->
  <div class="screen home-screen" id="homeScreen" style="display:none;">

    <div class="app-header">
      <div>
        <div class="app-title">LiveDoors</div>
        <div class="app-sub" id="connectionState">Connexion...</div>
      </div>
      <div class="header-right">
        <button class="theme-btn" id="settingsBtn" title="Paramètres">⚙️</button>
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
      </div>

      <div class="section-label">Ajouter un contact</div>
      <div style="display:flex; gap:8px;">
        <input class="field-input" id="contactPhoneInput" type="text" placeholder="Numéro ou nom d'utilisateur" style="flex:1;" autocapitalize="none">
        <button class="toggle-btn" id="addContactBtn">Ajouter</button>
      </div>
      <div class="field-hint" style="margin-bottom:20px;">Ex : 06 12 34 56 78 — ou bien gino72</div>

      <div class="section-label live-label"><span class="dot"></span>En direct maintenant</div>
      <div id="liveList"></div>

      <div class="section-label closed-label"><span class="dot"></span>Portes fermées</div>
      <div id="closedList"></div>

      <div class="section-label offline-label"><span class="dot"></span>Hors ligne</div>
      <div id="offlineList"></div>

    </div>

    <!-- ---- Modale : paramètres ---- -->
    <div class="modal-backdrop" id="settingsModal">
      <div class="modal-card">
        <div class="modal-title">Paramètres</div>

        <label class="field-label">Apparence</label>
        <div class="segmented" id="themeChoice">
          <button class="segment" type="button" data-theme-mode="light">☀️ Clair</button>
          <button class="segment" type="button" data-theme-mode="dark">🌙 Sombre</button>
          <button class="segment" type="button" data-theme-mode="auto">📱 Auto</button>
        </div>
        <div class="field-hint">« Auto » suit le réglage de ton téléphone : il passe en sombre le soir si ton téléphone le fait.</div>

        <label class="field-label">Mon compte</label>
        <button class="settings-action" type="button" id="settingsLock">🔒 Verrouiller maintenant</button>
        <button class="settings-action danger" type="button" id="settingsForget">🚪 Changer de compte</button>

        <div class="modal-actions">
          <button class="toggle-btn" id="settingsClose">Fermer</button>
        </div>
      </div>
    </div>

    <!-- ---- Modale : modifier mon profil ---- -->
    <div class="modal-backdrop" id="profileModal">
      <div class="modal-card tall">
        <div class="modal-title">Modifier mon profil</div>

        <label class="field-label">Pseudo</label>
        <input class="field-input" id="editPseudo" type="text" maxlength="24">

        <label class="field-label">Nom d'utilisateur</label>
        <input class="field-input" id="editUsername" type="text" maxlength="20" autocapitalize="none">

        <label class="field-label">Numéro de téléphone</label>
        <input class="field-input" id="editPhone" type="tel">
        <div class="field-hint">Si tu le changes, tes amis devront t'ajouter avec le nouveau numéro.</div>

        <label class="field-label">Mon avatar</label>
        <div class="avatar-preview-row">
          <div class="avatar-preview" id="editAvatarPreview"></div>
          <div class="avatar-preview-hint">Change ce que tu veux 👇</div>
        </div>
        <div class="photo-row">
          <button class="photo-btn" type="button" id="editPhotoBtn">📷 Mettre une photo</button>
          <button class="photo-btn secondary" type="button" id="editPhotoClearBtn">Retirer</button>
        </div>
        <input type="file" id="editPhotoInput" accept="image/*" style="display:none">
        <div class="builder">
          <div class="builder-tabs" id="editBuilderTabs"></div>
          <div class="builder-options" id="editBuilderOptions"></div>
          <button class="builder-random" type="button" id="editBuilderRandom">🎲 Au hasard</button>
        </div>

        <label class="field-label">Nouveau code secret</label>
        <input class="field-input pin-input" id="editPin" type="password" inputmode="numeric" maxlength="6" placeholder="••••">
        <div class="field-hint">Laisse vide pour garder ton code actuel.</div>

        <label class="field-label">Code actuel</label>
        <input class="field-input pin-input" id="editPinCurrent" type="password" inputmode="numeric" maxlength="6" placeholder="••••">
        <div class="field-hint">Demandé seulement si tu changes ton code ou ton numéro.</div>

        <div class="modal-actions">
          <button class="modal-cancel-btn" id="profileCancel">Annuler</button>
          <button class="toggle-btn" id="profileSave">Enregistrer</button>
        </div>
      </div>
    </div>

    <!-- ---- Modale : message optionnel avant d'envoyer une demande ---- -->
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
        <button class="chat-btn" id="chatBtn">💬 Tchat<span class="chat-badge" id="chatBadge"></span></button>
        <button class="leave-btn" id="leaveBtn">Quitter</button>
      </div>

      <!-- ---- Tchat écrit, visible uniquement pendant l'appel ---- -->
      <div class="chat-panel" id="chatPanel">
        <div class="chat-head">
          <div class="chat-title">💬 Tchat de l'appel</div>
          <button class="chat-close" id="chatCloseBtn">✕</button>
        </div>
        <div class="chat-messages" id="chatMessages"></div>
        <div class="emoji-panel" id="emojiPanel"><div class="emoji-grid-chat" id="emojiGrid"></div></div>
        <div class="chat-input-row">
          <button class="emoji-toggle" type="button" id="emojiToggle">😊</button>
          <input class="chat-input" id="chatInput" type="text" maxlength="200" placeholder="Ton message…" autocomplete="off">
          <button class="chat-send" id="chatSendBtn">Envoyer</button>
        </div>
      </div>

      <div class="reaction-zone" id="reactionZone"></div>

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

    <!-- Éléments audio distants (invisibles, un par participant sans vidéo) -->
    <div id="remoteAudioContainer" style="display:none;"></div>

  </div>

</div>
`;

const PAGE_CLIENT_JS = `
/**
 * Client LiveDoors.
 * Aucune donnée n'est simulée ici : tout vient du serveur (présence réelle
 * des comptes connectés) et l'audio/vidéo passe par de vraies connexions
 * WebRTC peer-to-peer entre navigateurs, le serveur ne servant qu'à la
 * signalisation (voir index.js, partie serveur).
 */

const socket = io();

const palette = ['#ff8a00', '#7c5cff', '#ff3d77', '#00c2a8', '#ffb020', '#4d8bff'];
const ICE_SERVERS = [
  { urls: 'stun:stun.l.google.com:19302' },
  // TODO prod: ajouter un serveur TURN (ex. coturn, Twilio, Xirsys) —
  // le STUN seul ne suffit pas dès qu'un des deux réseaux est restrictif.
];

const $ = (id) => document.getElementById(id);

let me = null;
let friends = [];
let localStream = null;        // flux micro (+ caméra si activée) local
let screenStream = null;       // flux de partage d'écran, si actif
let peers = new Map();         // peerId -> RTCPeerConnection
let inCall = false;
let callSeconds = 0;
let callTimerHandle = null;
let camOn = false;
let screenOn = false;

// ---------------------------------------------------------------------------
// Thème clair / sombre
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Apparence : clair, sombre, ou "auto" (le réglage du téléphone)
//
// En mode auto, on écoute prefers-color-scheme : si le téléphone bascule en
// sombre le soir, l'appli suit toute seule, sans avoir à toucher à rien.
// ---------------------------------------------------------------------------

const darkQuery = window.matchMedia ? window.matchMedia('(prefers-color-scheme: dark)') : null;
let themeMode = 'auto';

function applyTheme(mode) {
  themeMode = (mode === 'light' || mode === 'dark') ? mode : 'auto';
  const systemDark = !!(darkQuery && darkQuery.matches);
  const dark = themeMode === 'dark' || (themeMode === 'auto' && systemDark);

  document.documentElement.setAttribute('data-theme', dark ? 'dark' : 'light');
  try { localStorage.setItem('livedoors-theme', themeMode); } catch (e) {}

  const box = document.getElementById('themeChoice');
  if (box) {
    Array.from(box.children).forEach((b) => {
      b.classList.toggle('active', b.getAttribute('data-theme-mode') === themeMode);
    });
  }
}

applyTheme(localStorage.getItem('livedoors-theme') || 'auto');

if (darkQuery) {
  const onSystemChange = () => { if (themeMode === 'auto') applyTheme('auto'); };
  if (darkQuery.addEventListener) darkQuery.addEventListener('change', onSystemChange);
  else if (darkQuery.addListener) darkQuery.addListener(onSystemChange); // vieux navigateurs
}

Array.from(\$('themeChoice').children).forEach((b) => {
  b.addEventListener('click', () => applyTheme(b.getAttribute('data-theme-mode')));
});

// ---- Ouverture / fermeture des paramètres ----
\$('settingsBtn').addEventListener('click', () => {
  applyTheme(themeMode); // remet le bon bouton en surbrillance
  \$('settingsModal').classList.add('show');
});
\$('settingsClose').addEventListener('click', () => \$('settingsModal').classList.remove('show'));

// ---------------------------------------------------------------------------
// L'avatar composé
//
// L'avatar est un petit dessin SVG construit en couches (fond, peau, coiffure,
// yeux, bouche, accessoire). On ne transporte jamais le dessin lui-même, juste
// une "recette" du genre "3-1-0-5-2-4-1" : chaque chiffre dit quelle variante
// utiliser. C'est minuscule à envoyer et chacun redessine de son côté.
// ---------------------------------------------------------------------------

const AV_BG = ['#ffd166','#8ecae6','#ffadad','#a7e8a0','#bdb2ff','#ffc6ff','#9bf6ff','#ffb4a2','#f7ede2','#c8d5b9'];
const AV_SKIN = ['#ffe0c9','#fdd0ae','#f0b98d','#dda15e','#c68642','#a3673f','#7d4b26','#4f2f18'];
const AV_HAIR_COLOR = ['#2b2b2b','#4a3728','#6b4423','#a9662a','#c98b3a','#ece2b0','#c0392b','#7b4fd6'];
const AV_SHIRT = ['#457b9d','#e63946','#2a9d8f','#f4a261','#6a4c93','#264653','#ff8fab','#3d5a80'];

const AV_PARTS = ['bg','skin','hair','hairColor','brows','eyes','nose','mouth','beard','glasses','hat','shirt'];
const AV_COUNTS = {
  bg:10, skin:8, hair:12, hairColor:8, brows:5, eyes:8,
  nose:4, mouth:8, beard:5, glasses:5, hat:6, shirt:8,
};
const AV_LABELS = {
  hair:'Coiffure', hairColor:'Couleur', eyes:'Yeux', brows:'Sourcils', nose:'Nez',
  mouth:'Bouche', beard:'Barbe', glasses:'Lunettes', hat:'Chapeau', shirt:'Haut',
  skin:'Peau', bg:'Fond',
};
const AV_TAB_ORDER = ['hair','hairColor','eyes','brows','nose','mouth','beard','glasses','hat','shirt','skin','bg'];

const AV_DARK = '#20242a';

function parseAvatarConfig(str) {
  const bits = String(str || '').split('-');
  const out = {};
  AV_PARTS.forEach((key, i) => {
    const n = parseInt(bits[i], 10);
    out[key] = (isNaN(n) || n < 0 || n >= AV_COUNTS[key]) ? 0 : n;
  });
  return out;
}
function stringifyAvatarConfig(cfg) {
  return AV_PARTS.map((key) => cfg[key]).join('-');
}
function randomAvatarConfig() {
  const cfg = {};
  AV_PARTS.forEach((key) => { cfg[key] = Math.floor(Math.random() * AV_COUNTS[key]); });
  return cfg;
}

function avHeart(cx, cy, s, color) {
  return '<path d="M' + cx + ' ' + (cy + s * 0.8)
    + ' C' + (cx - s * 1.2) + ' ' + (cy - s * 0.1) + ', ' + (cx - s * 0.55) + ' ' + (cy - s * 1) + ', ' + cx + ' ' + (cy - s * 0.25)
    + ' C' + (cx + s * 0.55) + ' ' + (cy - s * 1) + ', ' + (cx + s * 1.2) + ' ' + (cy - s * 0.1) + ', ' + cx + ' ' + (cy + s * 0.8)
    + ' Z" fill="' + color + '"/>';
}

// ---- Cheveux : couche arrière (derrière la tête) ----
function avHairBack(style, color) {
  const f = ' fill="' + color + '"';
  if (style === 4) return '<rect x="20" y="26" width="60" height="54" rx="26"' + f + '/>';
  if (style === 5) return '<circle cx="50" cy="38" r="33"' + f + '/>';
  if (style === 8) return '<circle cx="17" cy="48" r="11"' + f + '/><circle cx="83" cy="48" r="11"' + f + '/>';
  if (style === 11) return '<path d="M72 30 Q92 40 86 64 Q84 72 76 70 Q84 50 66 40 Z"' + f + '/>';
  return '';
}

// ---- Cheveux : couche avant (sur le crâne) ----
function avHairFront(style, color) {
  const f = ' fill="' + color + '"';
  const cap = '<path d="M25 44 Q25 16 50 16 Q75 16 75 44 Q68 29 50 29 Q32 29 25 44 Z"' + f + '/>';

  if (style === 0) return '';
  if (style === 1) return cap;
  if (style === 2) return '<path d="M25 42 Q25 15 50 15 Q75 15 75 42 L75 33 Q50 26 25 33 Z"' + f + '/>';
  if (style === 3) return '<path d="M25 38 L30 21 L35 34 L41 15 L47 32 L53 13 L59 32 L65 16 L71 34 L75 24 L75 42 Q50 30 25 42 Z"' + f + '/>';
  if (style === 4) return cap;
  if (style === 5) return cap;
  if (style === 6) return '<circle cx="50" cy="11" r="9.5"' + f + '/>' + cap;
  if (style === 7) return '<path d="M43 20 Q50 1 57 20 L57 31 L43 31 Z"' + f + '/>';
  if (style === 8) return cap;
  if (style === 9) return '<path d="M25 42 Q25 15 50 15 Q75 15 75 42 Q70 25 43 27 Q33 29 25 42 Z"' + f + '/>';
  if (style === 10) {
    let s = '';
    [[31,32],[40,23],[50,20],[60,23],[69,32],[35,26],[65,26]].forEach((p) => {
      s += '<circle cx="' + p[0] + '" cy="' + p[1] + '" r="9"' + f + '/>';
    });
    return s;
  }
  return cap;
}

// ---- Sourcils ----
function avBrows(style) {
  const st = ' stroke="' + AV_DARK + '" fill="none" stroke-linecap="round"';
  if (style === 0) return '<path d="M32 34 L45 32 M55 32 L68 34"' + st + ' stroke-width="2.4"/>';
  if (style === 1) return '<rect x="31" y="30" width="15" height="4.5" rx="2.2" fill="' + AV_DARK + '"/><rect x="54" y="30" width="15" height="4.5" rx="2.2" fill="' + AV_DARK + '"/>';
  if (style === 2) return '<path d="M32 34 Q39 28 46 34 M54 34 Q61 28 68 34"' + st + ' stroke-width="2.6"/>';
  if (style === 3) return '<path d="M32 30 L46 35 M54 35 L68 30"' + st + ' stroke-width="2.8"/>';
  return '<path d="M32 29 Q39 25 46 29 M54 29 Q61 25 68 29"' + st + ' stroke-width="2.4"/>';
}

// ---- Yeux ----
function avEyes(style) {
  const d = AV_DARK;
  if (style === 0) return '<circle cx="39" cy="43" r="3.4" fill="' + d + '"/><circle cx="61" cy="43" r="3.4" fill="' + d + '"/>';
  if (style === 1) {
    return '<ellipse cx="39" cy="43" rx="6.2" ry="7" fill="#fff"/><ellipse cx="61" cy="43" rx="6.2" ry="7" fill="#fff"/>'
      + '<circle cx="39" cy="44" r="3.2" fill="' + d + '"/><circle cx="61" cy="44" r="3.2" fill="' + d + '"/>'
      + '<circle cx="40.6" cy="41.6" r="1.2" fill="#fff"/><circle cx="62.6" cy="41.6" r="1.2" fill="#fff"/>';
  }
  if (style === 2) return '<path d="M33 45 Q39 37 45 45 M55 45 Q61 37 67 45" stroke="' + d + '" stroke-width="3" fill="none" stroke-linecap="round"/>';
  if (style === 3) return '<path d="M33 44 Q39 37 45 44" stroke="' + d + '" stroke-width="3" fill="none" stroke-linecap="round"/><circle cx="61" cy="43" r="4.2" fill="' + d + '"/>';
  if (style === 4) return '<path d="M33 42 Q39 48 45 42 M55 42 Q61 48 67 42" stroke="' + d + '" stroke-width="3" fill="none" stroke-linecap="round"/>';
  if (style === 5) {
    return '<circle cx="39" cy="43" r="6.5" fill="#fff"/><circle cx="61" cy="43" r="6.5" fill="#fff"/>'
      + '<circle cx="39" cy="43" r="2.4" fill="' + d + '"/><circle cx="61" cy="43" r="2.4" fill="' + d + '"/>';
  }
  if (style === 6) {
    return '<ellipse cx="39" cy="43" rx="6.2" ry="7" fill="#fff"/><ellipse cx="61" cy="43" rx="6.2" ry="7" fill="#fff"/>'
      + '<circle cx="39" cy="44" r="3.2" fill="' + d + '"/><circle cx="61" cy="44" r="3.2" fill="' + d + '"/>'
      + '<path d="M32 38 L29 35 M39 36 L39 32 M46 38 L49 35 M54 38 L51 35 M61 36 L61 32 M68 38 L71 35" stroke="' + d + '" stroke-width="2" stroke-linecap="round"/>';
  }
  return avHeart(39, 43, 6, '#ff3d77') + avHeart(61, 43, 6, '#ff3d77');
}

// ---- Nez ----
function avNose(style) {
  if (style === 0) return '<path d="M50 46 L49 52 L54 52" stroke="rgba(0,0,0,0.35)" stroke-width="2" fill="none" stroke-linecap="round" stroke-linejoin="round"/>';
  if (style === 1) return '<circle cx="50" cy="51" r="2" fill="rgba(0,0,0,0.32)"/>';
  if (style === 2) return '<ellipse cx="50" cy="51" rx="4.5" ry="3.2" fill="rgba(0,0,0,0.16)"/>';
  return '';
}

// ---- Bouche ----
function avMouth(style) {
  const d = AV_DARK;
  if (style === 0) return '<path d="M42 57 Q50 65 58 57" stroke="' + d + '" stroke-width="3" fill="none" stroke-linecap="round"/>';
  if (style === 1) return '<path d="M39 55 Q50 70 61 55 Z" fill="' + d + '"/><path d="M46 64 Q50 69 54 64 Z" fill="#ff6b81"/>';
  if (style === 2) return '<path d="M43 59 L57 59" stroke="' + d + '" stroke-width="3" stroke-linecap="round"/>';
  if (style === 3) return '<ellipse cx="50" cy="59" rx="4.5" ry="5.5" fill="' + d + '"/>';
  if (style === 4) return '<path d="M42 56 Q50 63 58 56" stroke="' + d + '" stroke-width="3" fill="none" stroke-linecap="round"/><path d="M46 61 Q50 69 54 61 Z" fill="#ff6b81"/>';
  if (style === 5) return '<path d="M41 55 Q50 66 59 55 Z" fill="' + d + '"/><rect x="43.5" y="55" width="13" height="4.5" rx="1" fill="#fff"/>';
  if (style === 6) return '<path d="M42 62 Q50 55 58 62" stroke="' + d + '" stroke-width="3" fill="none" stroke-linecap="round"/>';
  return '<path d="M42 58 Q50 64 59 54" stroke="' + d + '" stroke-width="3" fill="none" stroke-linecap="round"/>';
}

// ---- Barbe ----
function avBeard(style, color) {
  if (style === 0) return '';
  const full = '<path d="M25 40 Q25 71 50 71 Q75 71 75 40 Q70 57 50 57 Q30 57 25 40 Z"';
  if (style === 1) return full + ' fill="' + color + '"/>';
  if (style === 2) return '<ellipse cx="50" cy="65" rx="7.5" ry="6.5" fill="' + color + '"/><path d="M40 53 Q50 49 60 53 Q50 57 40 53 Z" fill="' + color + '"/>';
  if (style === 3) return '<path d="M39 53 Q50 48 61 53 Q50 58 39 53 Z" fill="' + color + '"/>';
  return full + ' fill="' + color + '" opacity="0.32"/>';
}

// ---- Lunettes ----
function avGlasses(style) {
  const d = AV_DARK;
  if (style === 0) return '';
  if (style === 1) {
    return '<circle cx="39" cy="43" r="8.5" fill="#fff" fill-opacity="0.25" stroke="' + d + '" stroke-width="2.4"/>'
      + '<circle cx="61" cy="43" r="8.5" fill="#fff" fill-opacity="0.25" stroke="' + d + '" stroke-width="2.4"/>'
      + '<path d="M47.5 43 L52.5 43 M30.5 43 L24 41 M69.5 43 L76 41" stroke="' + d + '" stroke-width="2.4"/>';
  }
  if (style === 2) {
    return '<rect x="29" y="36" width="19" height="13" rx="3" fill="#fff" fill-opacity="0.25" stroke="' + d + '" stroke-width="2.4"/>'
      + '<rect x="52" y="36" width="19" height="13" rx="3" fill="#fff" fill-opacity="0.25" stroke="' + d + '" stroke-width="2.4"/>'
      + '<path d="M48 42 L52 42" stroke="' + d + '" stroke-width="2.4"/>';
  }
  if (style === 3) {
    return '<rect x="28" y="36" width="20" height="13" rx="4" fill="' + d + '"/>'
      + '<rect x="52" y="36" width="20" height="13" rx="4" fill="' + d + '"/>'
      + '<path d="M48 41 L52 41" stroke="' + d + '" stroke-width="3"/>';
  }
  return '<circle cx="38" cy="43" r="11" fill="#fff" fill-opacity="0.3" stroke="#7b4fd6" stroke-width="3"/>'
    + '<circle cx="62" cy="43" r="11" fill="#fff" fill-opacity="0.3" stroke="#7b4fd6" stroke-width="3"/>'
    + '<path d="M49 43 L51 43" stroke="#7b4fd6" stroke-width="3"/>';
}

// ---- Chapeau / accessoire de tête ----
function avHat(style) {
  if (style === 0) return '';
  if (style === 1) return '<path d="M24 29 Q50 6 76 29 Z" fill="#e63946"/><ellipse cx="50" cy="30" rx="32" ry="4.5" fill="#c1121f"/>';
  if (style === 2) return '<path d="M25 32 Q25 10 50 10 Q75 10 75 32 Z" fill="#2a9d8f"/><rect x="23" y="29" width="54" height="7" rx="3.5" fill="#264653"/><circle cx="50" cy="8" r="5" fill="#e9edc9"/>';
  if (style === 3) return '<path d="M31 26 L38 12 L44 22 L50 7 L56 22 L62 12 L69 26 Z" fill="#ffd166" stroke="#e0a800" stroke-width="2" stroke-linejoin="round"/>';
  if (style === 4) {
    return '<path d="M22 46 Q22 14 50 14 Q78 14 78 46" stroke="#3a3f47" stroke-width="5" fill="none"/>'
      + '<rect x="15" y="41" width="13" height="20" rx="6.5" fill="#3a3f47"/>'
      + '<rect x="72" y="41" width="13" height="20" rx="6.5" fill="#3a3f47"/>';
  }
  return '<path d="M25 30 Q50 20 75 30 L75 36 Q50 26 25 36 Z" fill="#e76f51"/>';
}

function avatarSvg(config) {
  const c = parseAvatarConfig(config);
  const skin = AV_SKIN[c.skin];
  const hair = AV_HAIR_COLOR[c.hairColor];

  return '<svg viewBox="0 0 100 100" xmlns="http://www.w3.org/2000/svg">'
    + '<rect width="100" height="100" fill="' + AV_BG[c.bg] + '"/>'
    + avHairBack(c.hair, hair)
    + '<rect x="43" y="60" width="14" height="22" fill="' + skin + '"/>'
    + '<path d="M12 100 L12 93 Q12 84 30 80 L70 80 Q88 84 88 93 L88 100 Z" fill="' + AV_SHIRT[c.shirt] + '"/>'
    + '<path d="M43 80 L50 88 L57 80 Z" fill="' + skin + '"/>'
    + '<ellipse cx="24" cy="47" rx="4.5" ry="6.5" fill="' + skin + '"/>'
    + '<ellipse cx="76" cy="47" rx="4.5" ry="6.5" fill="' + skin + '"/>'
    + '<ellipse cx="50" cy="44" rx="25" ry="27" fill="' + skin + '"/>'
    + avHairFront(c.hair, hair)
    + avBrows(c.brows)
    + avEyes(c.eyes)
    + avNose(c.nose)
    + avBeard(c.beard, hair)
    + avMouth(c.mouth)
    + avGlasses(c.glasses)
    + avHat(c.hat)
    + '</svg>';
}

// -- Le constructeur d'avatar (réutilisé sur 2 écrans) -----------------------
function createAvatarBuilder(tabsId, optionsId, previewId, randomId) {
  let cfg = randomAvatarConfig();
  let activeTab = 'hair';

  function drawPreview() {
    \$(previewId).innerHTML = avatarSvg(stringifyAvatarConfig(cfg));
  }

  function drawOptions() {
    const box = \$(optionsId);
    box.innerHTML = '';
    for (let i = 0; i < AV_COUNTS[activeTab]; i++) {
      const test = Object.assign({}, cfg);
      test[activeTab] = i;
      const b = document.createElement('button');
      b.type = 'button';
      b.className = (cfg[activeTab] === i) ? 'builder-option selected' : 'builder-option';
      b.innerHTML = avatarSvg(stringifyAvatarConfig(test));
      b.addEventListener('click', () => {
        cfg[activeTab] = i;
        drawOptions();
        drawPreview();
      });
      box.appendChild(b);
    }
  }

  function drawTabs() {
    const box = \$(tabsId);
    box.innerHTML = '';
    AV_TAB_ORDER.forEach((key) => {
      const b = document.createElement('button');
      b.type = 'button';
      b.className = (key === activeTab) ? 'builder-tab active' : 'builder-tab';
      b.textContent = AV_LABELS[key];
      b.addEventListener('click', () => { activeTab = key; drawTabs(); drawOptions(); });
      box.appendChild(b);
    });
  }

  \$(randomId).addEventListener('click', () => {
    cfg = randomAvatarConfig();
    drawOptions();
    drawPreview();
  });

  drawTabs();
  drawOptions();
  drawPreview();

  return {
    get: () => stringifyAvatarConfig(cfg),
    set: (str) => { cfg = parseAvatarConfig(str); drawOptions(); drawPreview(); },
  };
}

// ---------------------------------------------------------------------------
// Photo de profil
//
// La photo choisie est redessinée dans un carré de 128 px avant d'être
// enregistrée : une photo de téléphone fait plusieurs Mo, ce qui serait
// impossible à envoyer à tous les contacts à chaque changement. Après
// réduction, elle pèse quelques dizaines de Ko.
// ---------------------------------------------------------------------------

const PHOTO_SIZE = 128;

function shrinkPhoto(file) {
  return new Promise((resolve, reject) => {
    if (!file || file.type.indexOf('image/') !== 0) { reject(new Error('pas une image')); return; }

    const reader = new FileReader();
    reader.onerror = () => reject(new Error('lecture impossible'));
    reader.onload = () => {
      const img = new Image();
      img.onerror = () => reject(new Error('image illisible'));
      img.onload = () => {
        const canvas = document.createElement('canvas');
        canvas.width = PHOTO_SIZE;
        canvas.height = PHOTO_SIZE;
        const ctx = canvas.getContext('2d');

        // On recadre au centre pour garder un carré sans déformer le visage.
        const side = Math.min(img.width, img.height);
        const sx = (img.width - side) / 2;
        const sy = (img.height - side) / 2;
        ctx.drawImage(img, sx, sy, side, side, 0, 0, PHOTO_SIZE, PHOTO_SIZE);

        resolve(canvas.toDataURL('image/jpeg', 0.72));
      };
      img.src = reader.result;
    };
    reader.readAsDataURL(file);
  });
}

function isSafePhoto(value) {
  const v = String(value || '');
  return v.indexOf('data:image/jpeg;base64,') === 0
    || v.indexOf('data:image/png;base64,') === 0
    || v.indexOf('data:image/webp;base64,') === 0;
}

// ---------------------------------------------------------------------------
// Écran 1 — profil, avatar, code secret et mémorisation du compte
//
// Le profil (pseudo, téléphone, avatar, empreinte du code) est gardé dans le
// localStorage du navigateur : plus besoin de le retaper à chaque ouverture.
// Le carnet de contacts y est gardé aussi, parce que le serveur, lui, oublie
// tout dès qu'il redémarre — il est renvoyé automatiquement à chaque connexion.
// ---------------------------------------------------------------------------

const PROFILE_KEY = 'livedoors-profile';
const CONTACTS_KEY = 'livedoors-contacts';
const SESSION_KEY = 'livedoors-session';
const STATUS_KEY = 'livedoors-status';
const SESSION_DAYS = 30;
const STATUS_HOURS = 24;

// -- Petit mot / emoji : conservé 24 h ---------------------------------------
// Le serveur oublie tout à chaque redémarrage, donc le statut est gardé sur
// l'appareil avec son heure de création et renvoyé automatiquement tant qu'il
// a moins de 24 heures.
function loadStatus() {
  try {
    const saved = JSON.parse(localStorage.getItem(STATUS_KEY) || 'null');
    if (!saved || !saved.text) return '';
    if (Date.now() - saved.at > STATUS_HOURS * 3600000) {
      localStorage.removeItem(STATUS_KEY);
      return '';
    }
    return saved.text;
  } catch (e) { return ''; }
}
function saveStatus(text) {
  try {
    if (text) localStorage.setItem(STATUS_KEY, JSON.stringify({ text, at: Date.now() }));
    else localStorage.removeItem(STATUS_KEY);
  } catch (e) {}
}

let profile = null;         // profil complet gardé sur l'appareil
let onlineProfile = null;   // profil utilisé pour parler au serveur
let signupBuilder = null;   // constructeur d'avatar de l'écran de création
let editBuilder = null;     // constructeur d'avatar de la fenêtre "modifier"
let pinTries = 0;

function loadProfile() {
  try { return JSON.parse(localStorage.getItem(PROFILE_KEY) || 'null'); } catch (e) { return null; }
}
function saveProfile(p) {
  try { localStorage.setItem(PROFILE_KEY, JSON.stringify(p)); } catch (e) {}
}

// -- Session : le code n'est plus redemandé à chaque ouverture ---------------
// Une fois le bon code tapé, l'appareil est considéré comme "de confiance"
// pendant 30 jours. Le cadenas 🔒 permet de refermer tout de suite.
function sessionValid() {
  try {
    const until = parseInt(localStorage.getItem(SESSION_KEY) || '0', 10);
    return !isNaN(until) && Date.now() < until;
  } catch (e) { return false; }
}
function openSession() {
  try { localStorage.setItem(SESSION_KEY, String(Date.now() + SESSION_DAYS * 86400000)); } catch (e) {}
}
function clearSession() {
  try { localStorage.removeItem(SESSION_KEY); } catch (e) {}
}

// -- Carnet de contacts -----------------------------------------------------
// On garde le numéro MAIS AUSSI le dernier pseudo et le dernier avatar vus.
// Comme ça un contact déconnecté reste affiché dans la liste "Hors ligne"
// au lieu de disparaître complètement, ce qui donnait l'impression que le
// carnet n'était pas sauvegardé.
function normalizePhoneLocal(phone) {
  let out = '';
  for (const ch of String(phone || '')) {
    if (ch >= '0' && ch <= '9') out += ch;
  }
  return out.length > 9 ? out.slice(-9) : out;
}

function loadContacts() {
  try {
    const list = JSON.parse(localStorage.getItem(CONTACTS_KEY) || '[]');
    if (!Array.isArray(list)) return [];
    // Ancien format : un simple tableau de numéros. On le convertit au vol.
    return list.map((item) => (typeof item === 'string' ? { phone: item } : item))
      .filter((item) => item && item.phone);
  } catch (e) { return []; }
}
function saveContacts(list) {
  try { localStorage.setItem(CONTACTS_KEY, JSON.stringify(list.slice(0, 300))); } catch (e) {}
}
function contactPhones() {
  return loadContacts().map((c) => c.phone);
}
function rememberContact(phone) {
  const list = loadContacts();
  const key = normalizePhoneLocal(phone);
  if (!list.some((c) => normalizePhoneLocal(c.phone) === key)) {
    list.push({ phone });
    saveContacts(list);
  }
}
function forgetContact(phone) {
  const key = normalizePhoneLocal(phone);
  saveContacts(loadContacts().filter((c) => normalizePhoneLocal(c.phone) !== key));
  render();
  showToast('Contact retiré.');
}

// -- Renommer / favori / bloquer --------------------------------------------
function findContact(phone) {
  const key = normalizePhoneLocal(phone);
  return loadContacts().find((c) => normalizePhoneLocal(c.phone) === key) || null;
}
function updateContact(phone, changes) {
  const list = loadContacts();
  const key = normalizePhoneLocal(phone);
  let card = list.find((c) => normalizePhoneLocal(c.phone) === key);
  if (!card) { card = { phone }; list.push(card); }
  Object.assign(card, changes);
  saveContacts(list);
  return card;
}
// Le nom affiché : celui que TU as choisi en priorité, sinon son pseudo.
function displayName(user) {
  const card = user.phone ? findContact(user.phone) : null;
  if (card && card.alias) return card.alias;
  return user.pseudo || 'Contact';
}
function isFavorite(phone) {
  const card = findContact(phone);
  return !!(card && card.favorite);
}
function blockedPhones() {
  return loadContacts().filter((c) => c.blocked).map((c) => c.phone);
}

function renameContact(phone) {
  const card = findContact(phone);
  const current = (card && card.alias) || (card && card.pseudo) || '';
  const value = prompt('Quel nom veux-tu lui donner ?', current);
  if (value === null) return;
  updateContact(phone, { alias: value.trim().slice(0, 24) });
  render();
  showToast('Contact renommé.');
}

function toggleFavorite(phone) {
  const now = !isFavorite(phone);
  updateContact(phone, { favorite: now });
  render();
  showToast(now ? 'Ajouté aux favoris ⭐' : 'Retiré des favoris.');
}

function toggleBlocked(phone) {
  const card = findContact(phone);
  const now = !(card && card.blocked);
  if (now && !confirm('Bloquer ce contact ? Vous ne verrez plus vos portes respectives.')) return;
  updateContact(phone, { blocked: now, favorite: now ? false : (card && card.favorite) });
  sendRegister(); // le serveur applique le blocage des deux côtés
  render();
  showToast(now ? 'Contact bloqué 🚫' : 'Contact débloqué.');
}
// Dès qu'un contact est vu en ligne, on met à jour sa fiche locale.
function refreshContactCards(list) {
  const saved = loadContacts();
  let changed = false;
  list.forEach((u) => {
    if (!u.phone) return;
    const key = normalizePhoneLocal(u.phone);
    const found = saved.find((c) => normalizePhoneLocal(c.phone) === key);
    if (found && (found.pseudo !== u.pseudo || found.avatarConfig !== u.avatarConfig)) {
      found.pseudo = u.pseudo;
      found.avatarConfig = u.avatarConfig || '';
      changed = true;
    }
  });
  if (changed) saveContacts(saved);
}

function colorForPseudo(pseudo) {
  let hash = 0;
  for (const ch of pseudo) hash = (hash * 31 + ch.charCodeAt(0)) % palette.length;
  return palette[Math.abs(hash) % palette.length];
}

function initialsFor(pseudo) {
  return String(pseudo || '??').slice(0, 2).toUpperCase();
}

// Mêmes règles que le serveur : minuscules, lettres, chiffres, _ et .
function cleanUsernameLocal(value) {
  const raw = String(value || '').toLowerCase().slice(0, 20);
  let out = '';
  for (const ch of raw) {
    const ok = (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch === '_' || ch === '.';
    if (ok) out += ch;
  }
  return out;
}
function avatarTextFor(p) {
  return p.avatarEmoji || initialsFor(p.pseudo);
}

// Affiche l'avatar d'un compte : la photo si elle existe, sinon le dessin,
// sinon les initiales à l'ancienne (comptes créés avant la mise à jour).
function paintAvatarFor(el, user) {
  if (user && user.avatarPhoto && isSafePhoto(user.avatarPhoto)) {
    el.style.background = 'transparent';
    el.innerHTML = '';
    const img = document.createElement('img');
    img.className = 'avatar-photo';
    img.src = user.avatarPhoto;
    img.alt = '';
    el.appendChild(img);
  } else if (user && user.avatarConfig) {
    el.style.background = 'transparent';
    el.innerHTML = avatarSvg(user.avatarConfig);
  } else {
    el.innerHTML = '';
    paintAvatar(el, (user && user.avatarInitials) || '--', (user && user.avatarColor) || '#ff8a00');
  }
}

// Même chose, mais en texte HTML (pour les listes construites d'un bloc).
function avatarMarkup(user) {
  if (user.avatarPhoto && isSafePhoto(user.avatarPhoto)) {
    return '<img class="avatar-photo" alt="" src="' + user.avatarPhoto + '">';
  }
  if (user.avatarConfig) return avatarSvg(user.avatarConfig);
  return escapeHtml(user.avatarInitials || '--');
}

function paintAvatar(el, text, color) {
  el.textContent = text;
  el.style.background = \`linear-gradient(135deg, \${color}, #ff3d77)\`;
}

// -- Code secret : on ne garde jamais le code, seulement son empreinte --------
function isDigits(s) {
  if (!s) return false;
  for (const c of s) { if (c < '0' || c > '9') return false; }
  return true;
}
function simpleHash(str) {
  let h = 5381;
  for (let i = 0; i < str.length; i++) h = ((h * 33) ^ str.charCodeAt(i)) >>> 0;
  return 'x' + h.toString(16);
}
async function hashPin(pin, salt) {
  const raw = 'livedoors|' + salt + '|' + pin;
  try {
    if (window.crypto && crypto.subtle) {
      const buf = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(raw));
      return Array.from(new Uint8Array(buf)).map((b) => b.toString(16).padStart(2, '0')).join('');
    }
  } catch (e) {}
  return simpleHash(raw);
}

// -- Choix de la photo sur les deux écrans -----------------------------------
let signupPhoto = '';
let editPhoto = '';

function wirePhotoPicker(btnId, inputId, clearId, previewId, onChange) {
  \$(btnId).addEventListener('click', () => \$(inputId).click());

  \$(inputId).addEventListener('change', async (e) => {
    const file = e.target.files && e.target.files[0];
    e.target.value = ''; // pour pouvoir rechoisir la même photo ensuite
    if (!file) return;
    try {
      const data = await shrinkPhoto(file);
      onChange(data);
      \$(previewId).innerHTML = '<img class="avatar-photo" alt="" src="' + data + '">';
      showToast('Photo ajoutée 📷');
    } catch (err) {
      showToast("Impossible de lire cette image.");
    }
  });

  \$(clearId).addEventListener('click', () => {
    onChange('');
    showToast('Photo retirée — retour au personnage.');
  });
}

wirePhotoPicker('photoBtn', 'photoInput', 'photoClearBtn', 'avatarPreview', (d) => {
  signupPhoto = d;
  if (!d) signupBuilder.set(signupBuilder.get());
});
wirePhotoPicker('editPhotoBtn', 'editPhotoInput', 'editPhotoClearBtn', 'editAvatarPreview', (d) => {
  editPhoto = d;
  if (!d) editBuilder.set(editBuilder.get());
});

// -- Création du profil ------------------------------------------------------
\$('registerBtn').addEventListener('click', async () => {
  const pseudo = \$('pseudoInput').value.trim();
  const username = cleanUsernameLocal(\$('usernameInput').value);
  const phone = \$('phoneInput').value.trim();
  const pin = \$('newPinInput').value.trim();

  if (!pseudo) { \$('pseudoInput').focus(); return; }
  if (username && username.length < 3) {
    showToast("Nom d'utilisateur trop court (3 caractères minimum).");
    \$('usernameInput').focus();
    return;
  }
  if (!phone) { \$('phoneInput').focus(); return; }
  if (!isDigits(pin) || pin.length < 4) {
    showToast('Choisis un code secret de 4 à 6 chiffres.');
    \$('newPinInput').focus();
    return;
  }

  const p = {
    pseudo,
    username,
    phone,
    avatarConfig: signupBuilder.get(),
    avatarPhoto: signupPhoto,
    avatarColor: colorForPseudo(pseudo),
    pinHash: await hashPin(pin, phone),
  };

  profile = p;
  saveProfile(p);
  openSession();
  \$('newPinInput').value = '';
  goOnline(p);
});

// ---------------------------------------------------------------------------
// Modifier son profil (pseudo, téléphone, avatar, code secret)
// ---------------------------------------------------------------------------

function openProfileModal() {
  if (!profile) return;
  if (inCall) { showToast("Termine l'appel avant de modifier ton profil."); return; }

  \$('editPseudo').value = profile.pseudo;
  \$('editUsername').value = profile.username || '';
  \$('editPhone').value = profile.phone || '';
  \$('editPin').value = '';
  \$('editPinCurrent').value = '';
  editPhoto = profile.avatarPhoto || '';
  editBuilder.set(profile.avatarConfig || stringifyAvatarConfig(randomAvatarConfig()));
  if (editPhoto) {
    \$('editAvatarPreview').innerHTML = '<img class="avatar-photo" alt="" src="' + editPhoto + '">';
  }
  \$('profileModal').classList.add('show');
}

\$('headerAvatar').addEventListener('click', openProfileModal);
\$('profileCancel').addEventListener('click', () => \$('profileModal').classList.remove('show'));

\$('profileSave').addEventListener('click', async () => {
  const pseudo = \$('editPseudo').value.trim();
  const username = cleanUsernameLocal(\$('editUsername').value);
  const phone = \$('editPhone').value.trim();
  const newPin = \$('editPin').value.trim();
  const currentPin = \$('editPinCurrent').value.trim();

  if (!pseudo) { showToast('Il faut un pseudo.'); \$('editPseudo').focus(); return; }
  if (!phone) { showToast('Il faut un numéro.'); \$('editPhone').focus(); return; }
  if (newPin && (!isDigits(newPin) || newPin.length < 4)) {
    showToast('Le nouveau code doit faire 4 à 6 chiffres.');
    \$('editPin').focus();
    return;
  }

  // Changer le code ou le numéro touche à la sécurité du compte : on vérifie
  // que la personne connaît bien le code actuel avant de laisser passer.
  const sensitive = newPin || phone !== profile.phone;
  if (sensitive) {
    const check = await hashPin(currentPin, profile.phone);
    if (check !== profile.pinHash) {
      showToast('Code actuel incorrect.');
      \$('editPinCurrent').focus();
      return;
    }
  }

  const updated = {
    pseudo,
    username,
    phone,
    avatarConfig: editBuilder.get(),
    avatarPhoto: editPhoto,
    avatarColor: profile.avatarColor || colorForPseudo(pseudo),
    // L'empreinte du code dépend du numéro : si le numéro change, il faut la
    // recalculer, sinon le code ne serait plus reconnu au prochain démarrage.
    pinHash: newPin
      ? await hashPin(newPin, phone)
      : (phone === profile.phone ? profile.pinHash : await hashPin(currentPin, phone)),
  };

  profile = updated;
  saveProfile(updated);
  \$('editPin').value = '';
  \$('editPinCurrent').value = '';
  \$('profileModal').classList.remove('show');
  goOnline(updated);
  showToast('Profil mis à jour ✅');
});

// -- Écran de déverrouillage -------------------------------------------------
async function tryUnlock() {
  if (!profile) return;
  const pin = \$('pinInput').value.trim();
  const hash = await hashPin(pin, profile.phone);

  if (hash === profile.pinHash) {
    pinTries = 0;
    \$('pinInput').value = '';
    \$('pinHint').classList.remove('pin-error');
    \$('pinHint').textContent = 'Le code que tu as choisi en créant ton profil.';
    openSession();
    goOnline(profile);
  } else {
    pinTries++;
    \$('pinInput').value = '';
    \$('pinHint').classList.add('pin-error');
    \$('pinHint').textContent = pinTries >= 3
      ? 'Toujours pas le bon code (' + pinTries + ' essais). Si tu l’as oublié, utilise le lien en dessous.'
      : 'Code incorrect, réessaie.';
  }
}

\$('unlockBtn').addEventListener('click', tryUnlock);
\$('pinInput').addEventListener('keydown', (e) => { if (e.key === 'Enter') { e.preventDefault(); tryUnlock(); } });

\$('settingsForget').addEventListener('click', forgetAccount);
\$('forgetBtn').addEventListener('click', forgetAccount);

function forgetAccount() {
  const ok = confirm('Effacer ce compte de cet appareil ? Les contacts enregistrés seront perdus.');
  if (!ok) return;
  try { localStorage.removeItem(PROFILE_KEY); localStorage.removeItem(CONTACTS_KEY); } catch (e) {}
  clearSession();
  location.reload();
}

\$('settingsLock').addEventListener('click', () => {
  if (inCall) { showToast("Termine l'appel avant de verrouiller."); return; }
  clearSession(); // le code sera redemandé au prochain lancement
  location.reload();
});

// -- Connexion au serveur (et reconnexion automatique) -----------------------
function goOnline(p) {
  onlineProfile = p;
  sendRegister();
}

function sendRegister() {
  if (!onlineProfile || !socket.connected) return;
  socket.emit('register', {
    pseudo: onlineProfile.pseudo,
    phone: onlineProfile.phone,
    avatarInitials: avatarTextFor(onlineProfile),
    avatarColor: onlineProfile.avatarColor,
    avatarConfig: onlineProfile.avatarConfig || '',
    username: onlineProfile.username || '',
    avatarPhoto: onlineProfile.avatarPhoto || '',
    contacts: contactPhones(),
    blocked: blockedPhones(),
  });
}

socket.on('connect', () => {
  \$('connectionState').textContent = 'Connecté';
  sendRegister(); // remet le compte en ligne après un redémarrage du serveur
});

socket.on('disconnect', () => {
  \$('connectionState').textContent = 'Reconnexion…';
  // On ne coupe PLUS l'appel ici : la voix passe en direct d'un téléphone à
  // l'autre (WebRTC), elle continue même si le serveur est injoignable un
  // moment. Seule une vraie coupure entre les deux participants arrête tout.
  if (inCall) showToast('Connexion au serveur perdue — l\\'appel continue.');
});

socket.on('registered', (user) => {
  me = user;
  \$('lockScreen').style.display = 'none';
  \$('loginScreen').style.display = 'none';
  \$('homeScreen').style.display = 'flex';

  paintAvatarFor(\$('headerAvatar'), user);
  paintAvatarFor(\$('myAvatar'), user);
  \$('myName').textContent = user.pseudo;
  \$('myPhone').textContent = (user.username ? '@' + user.username + ' · ' : '') + (user.phone || '');
  \$('connectionState').textContent = 'Connecté';

  // Petit mot encore valable : on le remet en place tout seul.
  const keptStatus = loadStatus();
  if (keptStatus) {
    \$('doorMessageInput').value = keptStatus;
    if (!me.doorMessage) socket.emit('door:message', { message: keptStatus });
  }
});

// -- Démarrage de l'appli ----------------------------------------------------
function boot() {
  signupBuilder = createAvatarBuilder('builderTabs', 'builderOptions', 'avatarPreview', 'builderRandom');
  editBuilder = createAvatarBuilder('editBuilderTabs', 'editBuilderOptions', 'editAvatarPreview', 'editBuilderRandom');
  buildEmojiBar();
  clearChat();

  profile = loadProfile();

  // Session encore valable : on entre directement, sans redemander le code.
  if (profile && profile.pinHash && sessionValid()) {
    \$('loginScreen').style.display = 'none';
    goOnline(profile);
    return;
  }

  if (profile && profile.pinHash) {
    \$('loginScreen').style.display = 'none';
    \$('lockScreen').style.display = 'flex';
    \$('lockName').textContent = 'Salut ' + profile.pseudo + ' !';
    paintAvatarFor(\$('lockAvatar'), {
      avatarConfig: profile.avatarConfig,
      avatarInitials: avatarTextFor(profile),
      avatarColor: profile.avatarColor,
    });
    setTimeout(() => \$('pinInput').focus(), 300);
  }
}

// ---------------------------------------------------------------------------
// Présence — liste des amis en temps réel (poussée par le serveur)
// ---------------------------------------------------------------------------

socket.on('friends:update', (list) => {
  friends = list.filter((u) => !me || u.id !== me.id);
  refreshContactCards(friends);
  const myUpdated = list.find((u) => me && u.id === me.id);
  if (myUpdated) {
    me = myUpdated;
    syncMyDoorUI();
  }
  render();
});

function friendMeta(f) {
  if (f.doorOpen) return f.companions === 0 ? "seul pour l'instant" : \`+\${f.companions} déjà dans l'appel\`;
  return 'porte fermée';
}

function contactActions(phone) {
  if (!phone) return '';
  const card = findContact(phone);
  const fav = card && card.favorite;
  const blocked = card && card.blocked;
  const p = escapeAttr(phone);
  return \`<div class="contact-actions">
      <button class="contact-btn\${fav ? ' on' : ''}" onclick="toggleFavorite('\${p}')" title="Favori">\${fav ? '⭐' : '☆'}</button>
      <button class="contact-btn" onclick="renameContact('\${p}')" title="Renommer">✏️</button>
      <button class="contact-btn\${blocked ? ' danger' : ''}" onclick="toggleBlocked('\${p}')" title="\${blocked ? 'Débloquer' : 'Bloquer'}">\${blocked ? '🚫' : '⛔'}</button>
      <button class="contact-btn" onclick="forgetContact('\${p}')" title="Retirer">✕</button>
    </div>\`;
}

// Les favoris remontent toujours en haut de leur section.
function byFavorite(a, b) {
  const fa = a.phone && isFavorite(a.phone) ? 0 : 1;
  const fb = b.phone && isFavorite(b.phone) ? 0 : 1;
  return fa - fb;
}

function render() {
  const live = friends.filter((f) => f.doorOpen).sort(byFavorite);
  const closed = friends.filter((f) => !f.doorOpen).sort(byFavorite);

  \$('liveList').innerHTML = live.length ? live.map((f) => \`
    <div class="friend-row is-open">
      <div class="avatar-wrap">
        <div class="story-ring show"></div>
        <div class="avatar">\${avatarMarkup(f)}</div>
      </div>
      <div class="friend-info">
        <div class="friend-name">\${f.phone && isFavorite(f.phone) ? '⭐ ' : ''}\${escapeHtml(displayName(f))}</div>
        <div class="friend-phone">\${f.username ? '@' + escapeHtml(f.username) : escapeHtml(f.phone || '')}</div>
        <div class="friend-meta live-meta">\${friendMeta(f)}</div>
        \${f.doorMessage ? \`<div class="friend-status-msg">\${escapeHtml(f.doorMessage)}</div>\` : ''}
        \${contactActions(f.phone)}
      </div>
      <button class="join-btn" onclick="openJoinModal('\${f.id}', '\${escapeAttr(displayName(f))}')" \${(inCall || pendingRequestHostId) ? 'disabled' : ''}>\${pendingRequestHostId === f.id ? 'Envoyée...' : 'Rejoindre'}</button>
    </div>
  \`).join('') : \`<div class="empty-note">Personne n'a ouvert sa porte pour l'instant.</div>\`;

  \$('closedList').innerHTML = closed.length ? closed.map((f) => \`
    <div class="friend-row is-closed">
      <div class="avatar-wrap">
        <div class="avatar">\${avatarMarkup(f)}</div>
      </div>
      <div class="friend-info">
        <div class="friend-name">\${f.phone && isFavorite(f.phone) ? '⭐ ' : ''}\${escapeHtml(displayName(f))}</div>
        <div class="friend-phone">\${f.username ? '@' + escapeHtml(f.username) : escapeHtml(f.phone || '')}</div>
        \${contactActions(f.phone)}
      </div>
    </div>
  \`).join('') : \`<div class="empty-note">Aucun autre compte connecté pour le moment.</div>\`;

  // Contacts enregistrés sur cet appareil qui ne sont pas connectés là.
  // Sans cette section ils disparaissaient totalement de l'écran.
  const onlineKeys = friends.map((f) => normalizePhoneLocal(f.phone));
  const offline = loadContacts()
    .filter((c) => onlineKeys.indexOf(normalizePhoneLocal(c.phone)) === -1)
    .sort((a, b) => (a.favorite ? 0 : 1) - (b.favorite ? 0 : 1));

  \$('offlineList').innerHTML = offline.length ? offline.map((c) => \`
    <div class="friend-row is-closed">
      <div class="avatar-wrap">
        <div class="avatar">\${avatarMarkup({ avatarConfig: c.avatarConfig || '', avatarPhoto: c.avatarPhoto || '', avatarInitials: (c.alias || c.pseudo || '?').slice(0, 2).toUpperCase() })}</div>
      </div>
      <div class="friend-info">
        <div class="friend-name">\${c.favorite ? '⭐ ' : ''}\${escapeHtml(c.alias || c.pseudo || 'Contact')}</div>
        <div class="friend-phone">\${escapeHtml(c.phone)}</div>
        <div class="friend-meta">\${c.blocked ? 'Bloqué 🚫' : 'Pas connecté'}</div>
        \${contactActions(c.phone)}
      </div>
    </div>
  \`).join('') : \`<div class="empty-note">Ton carnet est vide. Ajoute un contact avec son numéro.</div>\`;
}

function escapeHtml(str) {
  const d = document.createElement('div');
  d.textContent = str;
  return d.innerHTML;
}
function escapeAttr(str) {
  return String(str).replace(/'/g, "\\\\'");
}

function showToast(msg) {
  const t = document.createElement('div');
  t.className = 'toast';
  t.textContent = msg;
  \$('toastZone').appendChild(t);
  requestAnimationFrame(() => t.classList.add('show'));
  setTimeout(() => {
    t.classList.remove('show');
    setTimeout(() => t.remove(), 350);
  }, 2800);
}

// ---------------------------------------------------------------------------
// Ma porte + mon statut
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

\$('toggleBtn').addEventListener('click', async () => {
  if (!me) return;
  if (!me.doorOpen) {
    try {
      localStream = await navigator.mediaDevices.getUserMedia({ audio: true });
    } catch (err) {
      showToast("Micro refusé — active-le pour ouvrir ta porte.");
      return;
    }
    const message = \$('doorMessageInput').value.trim();
    saveStatus(message);
    socket.emit('door:open', { message });
    startCallUI({ id: me.id, pseudo: 'En attente...', avatarInitials: me.avatarInitials, avatarColor: me.avatarColor, avatarConfig: me.avatarConfig, avatarPhoto: me.avatarPhoto }, true);
  } else {
    socket.emit('door:close');
    endCall('local-close');
  }
});

\$('doorMessageBtn').addEventListener('click', () => {
  const message = \$('doorMessageInput').value.trim();
  saveStatus(message);
  socket.emit('door:message', { message });
  showToast(message ? 'Statut gardé 24 h ✅' : 'Statut effacé.');
});

// ---------------------------------------------------------------------------
// Rejoindre la porte d'un ami — modale de message, puis demande "Toc Toc"
// ---------------------------------------------------------------------------

let pendingRequestHostId = null;
let pendingRequestHostPseudo = null;

function openJoinModal(hostId, pseudo) {
  if (inCall || pendingRequestHostId) return;
  pendingRequestHostId = null; // pas encore envoyée
  \$('joinModalTitle').textContent = \`Rejoindre \${pseudo}\`;
  \$('joinMessageInput').value = '';
  \$('joinModal').dataset.hostId = hostId;
  \$('joinModal').dataset.hostPseudo = pseudo;
  \$('joinModal').classList.add('show');
}
window.openJoinModal = openJoinModal;

\$('joinModalCancel').addEventListener('click', () => {
  \$('joinModal').classList.remove('show');
});

\$('joinModalSend').addEventListener('click', () => {
  const hostId = \$('joinModal').dataset.hostId;
  const pseudo = \$('joinModal').dataset.hostPseudo;
  const message = \$('joinMessageInput').value.trim();
  \$('joinModal').classList.remove('show');

  const host = friends.find((f) => f.id === hostId);
  if (!host) return;

  pendingRequestHostId = hostId;
  pendingRequestHostPseudo = pseudo;
  showToast(\`Demande envoyée à \${pseudo}...\`);
  socket.emit('call:request', { hostId, message });
  render();
});

socket.on('call:declined', () => {
  showToast('Ta demande a été refusée.');
  pendingRequestHostId = null;
  render();
});

socket.on('call:accepted', async ({ hostId }) => {
  const host = friends.find((f) => f.id === hostId);
  pendingRequestHostId = null;
  if (!host) return;

  try {
    localStream = await navigator.mediaDevices.getUserMedia({ audio: true });
  } catch (err) {
    showToast("Impossible d'accéder au micro.");
    return;
  }

  startCallUI(host, false);
  socket.emit('call:ready', { hostId });
});

// ---------------------------------------------------------------------------
// WebRTC — connexions peer-to-peer (audio + vidéo optionnelle)
// ---------------------------------------------------------------------------

function isPolite(peerId) {
  // Politesse déterministe basée sur la comparaison des ids, pour éviter
  // les collisions ("glare") quand les deux côtés renégocient en même temps.
  return socket.id < peerId;
}

function createPeerConnection(peerId) {
  const pc = new RTCPeerConnection({ iceServers: ICE_SERVERS });
  pc.makingOffer = false;
  pc.ignoreOffer = false;

  if (localStream) {
    localStream.getTracks().forEach((track) => pc.addTrack(track, localStream));
  }

  pc.onnegotiationneeded = async () => {
    try {
      pc.makingOffer = true;
      const offer = await pc.createOffer();
      await pc.setLocalDescription(offer);
      socket.emit('webrtc:offer', { targetId: peerId, offer: pc.localDescription });
    } catch (err) {
      // silencieux : une renégociation ratée n'est pas bloquante
    } finally {
      pc.makingOffer = false;
    }
  };

  pc.onicecandidate = (event) => {
    if (event.candidate) {
      socket.emit('webrtc:ice-candidate', { targetId: peerId, candidate: event.candidate });
    }
  };

  pc.ontrack = (event) => {
    attachRemoteTrack(peerId, event.track, event.streams[0]);
  };

  pc.onconnectionstatechange = () => {
    if (['failed', 'disconnected', 'closed'].includes(pc.connectionState)) {
      removePeer(peerId);
    }
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
    label.textContent = peerInfo ? peerInfo.pseudo : 'Participant';
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

// -- Signalisation entrante --

socket.on('call:room-state', async ({ members }) => {
  for (const member of members) {
    createPeerConnection(member.id); // onnegotiationneeded envoie l'offre
  }
});

socket.on('call:peer-joined', (peer) => {
  showToast(\`\${peer.pseudo} a rejoint l'appel\`);
  updateCallStatus();
});

// L'utilisateur tape ce qu'il veut : si l'entrée contient surtout des
// chiffres, c'est un numéro ; sinon c'est un nom d'utilisateur.
function looksLikePhone(value) {
  let digits = 0;
  let letters = 0;
  for (const ch of value) {
    if (ch >= '0' && ch <= '9') digits++;
    else if (ch !== ' ' && ch !== '+' && ch !== '.' && ch !== '-') letters++;
  }
  return digits >= 6 && letters === 0;
}

\$('addContactBtn').addEventListener('click', () => {
  const value = \$('contactPhoneInput').value.trim();
  if (!value) { \$('contactPhoneInput').focus(); return; }

  if (looksLikePhone(value)) socket.emit('contact:add', { phone: value });
  else socket.emit('contact:addByUsername', { username: value });

  \$('contactPhoneInput').value = '';
});

\$('contactPhoneInput').addEventListener('keydown', (e) => {
  if (e.key === 'Enter') { e.preventDefault(); \$('addContactBtn').click(); }
});

socket.on('contact:added', ({ phone, found }) => {
  rememberContact(phone); // gardé sur l'appareil : le carnet survit aux redémarrages
  render();
  showToast(found ? 'Contact ajouté !' : "Contact ajouté, il apparaîtra dès qu'il sera connecté.");
});

socket.on('contact:error', ({ message }) => showToast(message));

// ---------------------------------------------------------------------------
// Demandes d'appel entrantes (côté hôte : accepter / refuser)
// ---------------------------------------------------------------------------

let incomingRequestFromId = null;

socket.on('call:incoming-request', (from) => {
  incomingRequestFromId = from.id;
  \$('incomingRequestName').textContent = \`\${from.pseudo} veut rejoindre\`;
  if (from.message) {
    \$('incomingRequestMsg').textContent = \`"\${from.message}"\`;
    \$('incomingRequestMsg').style.display = 'block';
  } else {
    \$('incomingRequestMsg').style.display = 'none';
  }
  \$('incomingRequest').style.display = 'block';
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

socket.on('webrtc:answer', async ({ fromId, answer }) => {
  const pc = peers.get(fromId);
  if (pc) await pc.setRemoteDescription(answer);
});

socket.on('webrtc:ice-candidate', async ({ fromId, candidate }) => {
  const pc = peers.get(fromId);
  if (pc) { try { await pc.addIceCandidate(candidate); } catch (_) {} }
});

socket.on('call:peer-left', ({ id }) => {
  removePeer(id);
  updateCallStatus();
});

socket.on('call:ended', () => {
  showToast("L'hôte a fermé sa porte.");
  endCall('host-closed');
});

socket.on('call:error', ({ message }) => {
  showToast(message);
});

// ---------------------------------------------------------------------------
// UI de l'écran d'appel
// ---------------------------------------------------------------------------

function startCallUI(target, isHosting) {
  inCall = true;
  callSeconds = 0;
  paintAvatarFor(\$('callAvatar'), target);
  \$('callName').textContent = isHosting ? 'Ta porte est ouverte' : target.pseudo;
  \$('callStatusLabel').textContent = isHosting ? 'En attente' : 'Connexion...';
  \$('callTimer').textContent = '00:00';
  \$('callOverlay').classList.add('active');
  \$('muteBtn').classList.remove('is-muted');
  \$('muteBtn').textContent = 'Couper le micro';
  \$('camBtn').classList.remove('is-on');
  \$('camBtn').textContent = 'Caméra';
  \$('screenBtn').classList.remove('is-on');
  \$('screenBtn').textContent = "Partager l'écran";
  camOn = false;
  screenOn = false;

  clearChat();
  closeChat();

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

// -- Caméra : ajoute/retire une piste vidéo locale, renégociée automatiquement --
\$('camBtn').addEventListener('click', async () => {
  if (!inCall) return;
  if (!camOn) {
    try {
      const camStream = await navigator.mediaDevices.getUserMedia({ video: true });
      const videoTrack = camStream.getVideoTracks()[0];
      localStream.addTrack(videoTrack);
      ensureVideoTile('me', new MediaStream([videoTrack]));
      peers.forEach((pc) => pc.addTrack(videoTrack, localStream));
      camOn = true;
      \$('camBtn').classList.add('is-on');
      \$('camBtn').textContent = 'Caméra active';
    } catch (err) {
      showToast('Caméra refusée ou indisponible.');
    }
  } else {
    const videoTrack = localStream.getVideoTracks()[0];
    if (videoTrack) {
      peers.forEach((pc) => {
        const sender = pc.getSenders().find((s) => s.track === videoTrack);
        if (sender) pc.removeTrack(sender);
      });
      videoTrack.stop();
      localStream.removeTrack(videoTrack);
    }
    removeVideoTile('me');
    camOn = false;
    \$('camBtn').classList.remove('is-on');
    \$('camBtn').textContent = 'Caméra';
  }
});

// -- Partage d'écran : remplace la piste vidéo envoyée par le flux d'écran --
\$('screenBtn').addEventListener('click', async () => {
  if (!inCall) return;
  if (!screenOn) {
    try {
      screenStream = await navigator.mediaDevices.getDisplayMedia({ video: true });
      const screenTrack = screenStream.getVideoTracks()[0];

      const existingVideoTrack = localStream.getVideoTracks()[0];
      if (existingVideoTrack) {
        // Remplace la piste caméra par la piste d'écran sur chaque connexion.
        peers.forEach((pc) => {
          const sender = pc.getSenders().find((s) => s.track === existingVideoTrack);
          if (sender) sender.replaceTrack(screenTrack);
        });
        existingVideoTrack.stop();
        localStream.removeTrack(existingVideoTrack);
        localStream.addTrack(screenTrack);
      } else {
        localStream.addTrack(screenTrack);
        peers.forEach((pc) => pc.addTrack(screenTrack, localStream));
      }

      ensureVideoTile('me', new MediaStream([screenTrack]));
      screenOn = true;
      \$('screenBtn').classList.add('is-on');
      \$('screenBtn').textContent = "Écran partagé";

      screenTrack.addEventListener('ended', stopScreenShare);
    } catch (err) {
      showToast("Partage d'écran refusé ou indisponible.");
    }
  } else {
    stopScreenShare();
  }
});

function stopScreenShare() {
  const screenTrack = localStream.getVideoTracks()[0];
  if (screenTrack) {
    peers.forEach((pc) => {
      const sender = pc.getSenders().find((s) => s.track === screenTrack);
      if (sender) pc.removeTrack(sender);
    });
    screenTrack.stop();
    localStream.removeTrack(screenTrack);
  }
  if (screenStream) { screenStream.getTracks().forEach((t) => t.stop()); screenStream = null; }
  removeVideoTile('me');
  screenOn = false;
  \$('screenBtn').classList.remove('is-on');
  \$('screenBtn').textContent = "Partager l'écran";
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
  if (localStream) {
    localStream.getTracks().forEach((t) => t.stop());
    localStream = null;
  }
  camOn = false;
  screenOn = false;

  closeChat();
  clearChat();
  render();
}

// ---------------------------------------------------------------------------
// Tchat écrit pendant l'appel (+ émojis)
//
// Les messages ne circulent qu'entre les personnes présentes dans l'appel, et
// rien n'est conservé : quand l'appel se termine, le tchat est vidé.
// ---------------------------------------------------------------------------

const CHAT_EMOJIS = [
  '😀','😃','😄','😁','😆','😅','🤣','😂',
  '🙂','😉','😊','😍','🥰','😘','😜','🤪',
  '🤔','🤨','😐','😴','😢','😭','😤','😡',
  '🥳','😎','🤩','🥺','😳','🤯','🤐','🤑',
  '👍','👎','👏','🙏','🤝','💪','👋','✌️',
  '❤️','🔥','💯','⭐','🎉','🎁','⚡','🌈',
  '🍕','🍔','🍟','🍩','⚽','🏀','🎮','🎧',
];

let chatOpen = false;
let emojiPanelOpen = false;
let unreadCount = 0;

function buildEmojiBar() {
  const grid = \$('emojiGrid');
  grid.innerHTML = '';
  CHAT_EMOJIS.forEach((emo) => {
    const b = document.createElement('button');
    b.type = 'button';
    b.textContent = emo;
    b.addEventListener('click', () => {
      const input = \$('chatInput');
      input.value = (input.value + emo).slice(0, 200);
      input.focus();
    });
    grid.appendChild(b);
  });
}

function toggleEmojiPanel(force) {
  emojiPanelOpen = (typeof force === 'boolean') ? force : !emojiPanelOpen;
  \$('emojiPanel').classList.toggle('show', emojiPanelOpen);
  \$('emojiToggle').classList.toggle('is-on', emojiPanelOpen);
}

function clearChat() {
  \$('chatMessages').innerHTML = '<div class="chat-empty">Pas encore de message. Lance la discussion 👋</div>';
  unreadCount = 0;
  updateChatBadge();
}

function updateChatBadge() {
  const badge = \$('chatBadge');
  if (unreadCount > 0 && !chatOpen) {
    badge.textContent = unreadCount > 9 ? '9+' : String(unreadCount);
    badge.classList.add('show');
  } else {
    badge.classList.remove('show');
  }
}

function openChat() {
  chatOpen = true;
  unreadCount = 0;
  updateChatBadge();
  \$('chatPanel').classList.add('show');
  \$('chatBtn').classList.add('is-on');
  const box = \$('chatMessages');
  box.scrollTop = box.scrollHeight;
  setTimeout(() => \$('chatInput').focus(), 80);
}

function closeChat() {
  chatOpen = false;
  toggleEmojiPanel(false);
  \$('chatPanel').classList.remove('show');
  \$('chatBtn').classList.remove('is-on');
  updateChatBadge();
}

function sendChat() {
  const input = \$('chatInput');
  const text = input.value.trim();
  if (!text) return;
  if (!inCall) { showToast("Le tchat ne marche que pendant un appel."); return; }
  socket.emit('chat:message', { text });
  input.value = '';
  input.focus();
}

\$('chatBtn').addEventListener('click', () => { chatOpen ? closeChat() : openChat(); });
\$('emojiToggle').addEventListener('click', () => toggleEmojiPanel());
\$('chatCloseBtn').addEventListener('click', closeChat);
\$('chatSendBtn').addEventListener('click', sendChat);
\$('chatInput').addEventListener('keydown', (e) => {
  if (e.key === 'Enter') { e.preventDefault(); sendChat(); }
});

socket.on('chat:message', (msg) => {
  addChatMessage(msg);
  if (isEmojiOnly(msg.text)) floatEmoji(msg.text);

  const fromMe = me && msg.fromId === me.id;
  if (!chatOpen && !fromMe) {
    unreadCount++;
    updateChatBadge();
    showToast(msg.pseudo + ' : ' + msg.text);
  }
});

function addChatMessage(msg) {
  const box = \$('chatMessages');
  const empty = box.querySelector('.chat-empty');
  if (empty) empty.remove();

  const mine = me && msg.fromId === me.id;

  const wrap = document.createElement('div');
  wrap.className = mine ? 'chat-msg mine' : 'chat-msg';

  const author = document.createElement('div');
  author.className = 'chat-msg-author';
  author.textContent = mine ? 'Toi' : msg.pseudo;

  const bubble = document.createElement('div');
  bubble.className = isEmojiOnly(msg.text) ? 'chat-bubble big-emoji' : 'chat-bubble';
  bubble.textContent = msg.text; // textContent : impossible d'injecter du HTML

  wrap.appendChild(author);
  wrap.appendChild(bubble);
  box.appendChild(wrap);
  box.scrollTop = box.scrollHeight;
}

// Un message composé uniquement d'émojis s'affiche en grand et s'envole.
function isEmojiOnly(text) {
  const t = String(text).trim();
  if (!t || t.length > 12) return false;
  for (const ch of t) {
    const code = ch.codePointAt(0);
    if (code < 0x2000 && code !== 0x20) return false;
  }
  return true;
}

function floatEmoji(text) {
  const zone = \$('reactionZone');
  for (const ch of String(text).trim()) {
    const code = ch.codePointAt(0);
    if (code < 0x2000 || code === 0xFE0F || code === 0x200D) continue;
    const el = document.createElement('div');
    el.className = 'floating-emoji';
    el.textContent = ch;
    el.style.left = (10 + Math.random() * 75) + '%';
    el.style.animationDelay = (Math.random() * 0.4).toFixed(2) + 's';
    zone.appendChild(el);
    setTimeout(() => el.remove(), 3400);
  }
}

boot();
`;

const PAGE_HTML = '<!DOCTYPE html>\n' +
  '<html lang="fr">\n' +
  '<head>\n' +
  '<meta charset="UTF-8">\n' +
  '<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">\n' +
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
// PWA — manifest, service worker et icônes (installable sur Android et iOS)
// ---------------------------------------------------------------------------

const ICON_180_B64 = "iVBORw0KGgoAAAANSUhEUgAAALQAAAC0CAYAAAA9zQYyAAADEUlEQVR4nO3cQU4bQRRF0XLEFjyB/S8NJmwByRlFihQSN4md9r99ztBiUHp9VVg24nT5WJcFEd/2PgDckqBJETQpgiZF0KQImhRBkyJoUgRNiqBJETQpgiZF0KQImhRBkyJoUgRNiqBJETQpgiZF0KQImhRBkyJoUgRNiqBJefr7APdwfnne+whjvL++7X2EmzpV/rediP9dIe7EWw4x30Zhx/FBFx7CI5m+5+igp4//qCbvOjboyaNPMHXfkUFPHXuaiTuPC3riyJNN2zv5OfRajY+g7m1arFuMu6G3EPM2xZ1GBb3lRik+pHvastekm3xU0NeI+e+UdksFDYImRdCkCJoUQZMiaFIETYqgSRE0KYImRdCkCJoUQZMiaFIETYqgSRE0KYImRdCkCJoUQZMiaFIETYqgSRE0KYImRdCkCJoUQZMiaFIETYqgSRE0KYImRdCkCJoUQZMiaFIETYqgSRE0KYImRdCkCJoUQZPytPcBuJ3zy/Mvr72/vu1wkv24oSM+i/lPr1cJOuBatEeKWtDDbY31KFELerCvRnqEqAVNiqBJETQpgiZF0IN99UuTI3zJIujhtkZ6hJjXEnTCtViPEvNags74XbRHinktf5yUcrR4P+OGJkXQpAiaFEGTImhSBE2KoEkRNCmCJkXQpAiaFEGTImhSBE2KoEkRNCmCJkXQpAiaFEGTImhSBE2KoEkRNCmCJkXQpAiaFEGTImhSBE2KoEkRNCmCJkXQpAiaFEGTImhSBE2KoEkRNCmCJkXQpAiaFEGTImhSUkGfX573PsJIpd1GBf3++nb1Z0oP53/YsteW3R/FqKC3EvU2xZ2e9j7AvRQfFteNu6En/formLb3uKDXmjfyVBN3Hhn0WjPHnmTqvmODXmvu6I9u8q6jg15r9viPaPqe44Nea/5DeBSFHU+Xj3XZ+xC35iO77QoR/ywZNMeVeMsBPwiaFEGTImhSBE2KoEkRNCmCJkXQpAiaFEGTImhSBE2KoEkRNCmCJkXQpAiaFEGTImhSBE2KoEkRNCmCJkXQpHwH5ZBzVz6sVUAAAAAASUVORK5CYII=";
const ICON_192_B64 = "iVBORw0KGgoAAAANSUhEUgAAAMAAAADACAYAAABS3GwHAAADT0lEQVR4nO3cQU4bQRBA0SHiCmzg/keDDVdAclaWEHGIMcHjrv/eErJoVffvGcsod4e37bBB1K+9FwB7EgBpAiBNAKQJgDQBkCYA0gRAmgBIEwBpAiBNAKQJgDQBkCYA0gRAmgBIEwBpAiBNAKQJgDQBkCYA0gRAmgBIEwBpAiDtfu8FXMPD0+PeS1jW6/PL3kv4UXdT/3Nch/7/mxjDyFcgh/9nTJzrqCfAxA26VVOeBmOeAA7/dU2Z94gApmzGaibMfUQAcKnlA5hwC61s9fkv/SH4K8Of8qHtmgrzHf9F2KobcwuOs1v9lv/M8q9An3H4/4/Jc1w2gMm30opW3Y9lA/iXybfWHqbOc2wAcA4BkCYA0gRAmgBIEwBpAiBNAKQJgDQBkCYA0gRAmgBIEwBpAiBNAKQJgDQBkCYA0gRAmgBIEwBpAiBNAKQJgDQBkCYA0gRAmgBIEwBpAiBNAKQJgDQBkCYA0gRAmgBIEwBpAiBNAKQJgDQBkCYA0gRAmgBIEwBpAiDtfu8FcB0PT49//Oz1+WWHldwWAQx36uB//F05BK9Ag312+C/5dxMJYKivHupqBAIgTQADXXqbF58CAiBNAKQJgDQBkCaAgS79Yqv4hZgASBPAUF+9zYu3/7YJYLRzD3X18G+bP4Yb73i4/TXoaQKIcNhP8wpEmgBIEwBpAiBNAKQJgDQBkCYA0gRAmgBIEwBpAiBNAKQJgDQBkCYA0gRAmgBIEwBpAiBNAKQJgDQBkCYA0gRAmgBIEwBpAiBNAKQJgDQBkCYA0gRAmgBIEwBpAiBNAKQJgDQBkCYA0gRAmgBIEwBpAiBNAKQJgDQBkCYA0sYG8PD0uPcSRpk6z2UDeH1+2XsJvLPqfiwbwDmm3lrXNnmO93sv4KcdN2/VG2pPkw/+0d3hbTvsvYjvKGzSrVv5cln+FWjl4U+w+vyXDwC+Y0QAq99Cq5ow9xEBbNuMzVjJlHkv/yH4FB+Mf86Ug3805gnw3rRNuhUT5zryCfCRJ8LlJh769xIBwN+MfAWCcwmANAGQJgDSBECaAEgTAGkCIE0ApAmANAGQJgDSBECaAEgTAGkCIE0ApAmANAGQJgDSBECaAEgTAGkCIE0ApAmANAGQ9hvM7YUNXMU/3gAAAABJRU5ErkJggg==";
const ICON_512_B64 = "iVBORw0KGgoAAAANSUhEUgAAAgAAAAIACAYAAAD0eNT6AAAKzUlEQVR4nO3dS1IjRxRA0cLBFpjA/pcGE7ZAhDySQ00jWUj1SdU9Z9xB5+Tlu5WNw0+Hr+kwAQAp/2x9AABgfQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIKetz4A+/fy9rr1EeAhfb5/bH0Eduzp8DUdtj4E+2LhwzIEAXMSAMzC0od1iQHuJQC4i8UP2xIC3EoAcBOLH8YiBPgtAcCvWPwwNiHAtfxngFzN8ofxmVOu5QWA/+VCgcfkNYBLvABwkeUPj8v8cokA4CyXBzw+c8w5AoAfuTRgP8wzPxEA/MVlAftjrvlOAPAHlwTsl/nmlADgPy4H2D9zzpEAYJomlwKUmHemSQAwuQygyNwjAAAgSADE+QqALvPfJgDCDD/gHugSAAAQJACiVD9w5D5oEgAAECQAgtQ+8J17oed56wPQ4v9PDpdZxKzl6fA1HbY+BOvZ4nKx9OE25pUleQFgMS4SuM9xhrwKsAS/A8AiLH+Yj3liCQIgZK2vCJcVzG+tufLa0CEAmJXlD8sxX8xJADAblxMsz5wxFwEAAEECIGLpf9fzVQLrWXre/B5AgwAAgCABAABBAoC7ef6H9Zk77iUAACBIAABAkAAAgCABAABBAgAAggQAAAQJAAAIEgAAECQAACBIAABAkAAAgCABAABBAgAAggQAAAQJAAAIEgAAECQAACBIAABAkAAAgCABAABBAgAAggQAAAQJAAAIEgAAECQAACBIAABAkAAAgCABAABBAgAAggQAAAQJAAAIEgAAECQAACBIAABAkAAAgCABAABBAgAAggQAAAQJAAAIEgAAECQAACBIAABAkAAAgCABAABBAgAAggQAAAQJAAAIEgAAECQAACBIAABAkAAAgCABAABBAgAAggQAAAQJAAAIEgAAECQAACBIAABAkAAAgCABAABBAgAAggQAAAQJAAAIEgAAECQAACBIAABAkAAAgCABAABBAgAAggQAAAQJAAAIEgAAECQAACBIAABAkAAAgCABAABBAgAAggQAAAQJAAAIEgAAECQAACBIAABAkAAAgCABAABBAgAAggQAAAQJAAAIEgAAECQAACBIAABAkAAAgCABAABBAgAAggQAAAQJAAAIEgAAECQAACBIAABAkAAAgCABAABBAgAAggQAAAQJAAAIEgAAECQAACBIAABAkAAAgCABAABBAgAAggQAAAQJAAAIEgAAECQAACBIAABAkAAAgCABAABBAgAAggQAAAQJAAAIEgAAECQAACBIAABAkAAAgCABAABBAgAAggQAAAQJAAAIEgAAECQAACBIAABAkAAAgCABAABBAgAAggQAAAQJAAAIEgAAECQAACBIAABAkAAAgCABAABBAgAAggQAAAQJAAAIEgAAECQAACBIAABAkAAAgCABAABBAgAAggQAAAQJAAAIEgAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQf8CTAZBuv+vmHQAAAAASUVORK5CYII=";

const MANIFEST_JSON = "{\n  \"name\": \"LiveDoors\",\n  \"short_name\": \"LiveDoors\",\n  \"description\": \"Zéro sonnerie. Si c'est ouvert, tu rentres.\",\n  \"start_url\": \"/\",\n  \"scope\": \"/\",\n  \"display\": \"standalone\",\n  \"background_color\": \"#fffc00\",\n  \"theme_color\": \"#fffc00\",\n  \"orientation\": \"portrait\",\n  \"icons\": [\n    {\n      \"src\": \"/icons/icon-192.png\",\n      \"sizes\": \"192x192\",\n      \"type\": \"image/png\",\n      \"purpose\": \"any maskable\"\n    },\n    {\n      \"src\": \"/icons/icon-512.png\",\n      \"sizes\": \"512x512\",\n      \"type\": \"image/png\",\n      \"purpose\": \"any maskable\"\n    }\n  ]\n}";

const SERVICE_WORKER_JS = "const CACHE_NAME = 'livedoors-shell-v2';\nconst SHELL_URLS = ['/', '/manifest.json'];\n\n// Ce service worker met seulement en cache la \"coquille\" de l'appli\n// (HTML/CSS/JS/manifest/icônes) pour un démarrage instantané hors-ligne.\n// Il ne touche jamais à Socket.io ni aux appels WebRTC.\n\nself.addEventListener('install', (event) => {\n  event.waitUntil(\n    caches.open(CACHE_NAME).then((cache) => cache.addAll(SHELL_URLS))\n  );\n  self.skipWaiting();\n});\n\nself.addEventListener('activate', (event) => {\n  event.waitUntil(\n    caches.keys().then((keys) =>\n      Promise.all(keys.filter((k) => k !== CACHE_NAME).map((k) => caches.delete(k)))\n    )\n  );\n  self.clients.claim();\n});\n\nself.addEventListener('fetch', (event) => {\n  const url = new URL(event.request.url);\n\n  if (url.pathname.startsWith('/socket.io/')) return;\n  if (event.request.method !== 'GET') return;\n\n  // Réseau en priorité, cache seulement en secours (hors-ligne) : ça évite\n  // de voir une version périmée de l'appli après une mise à jour.\n  event.respondWith(\n    fetch(event.request)\n      .then((response) => {\n        if (response.ok) {\n          const clone = response.clone();\n          caches.open(CACHE_NAME).then((cache) => cache.put(event.request, clone));\n        }\n        return response;\n      })\n      .catch(() => caches.match(event.request))\n  );\n});\n";

app.get('/manifest.json', (req, res) => {
  res.type('application/manifest+json').send(MANIFEST_JSON);
});

app.get('/sw.js', (req, res) => {
  res.type('application/javascript').send(SERVICE_WORKER_JS);
});

app.get('/icons/icon-180.png', (req, res) => {
  res.type('png').send(Buffer.from(ICON_180_B64, 'base64'));
});
app.get('/icons/icon-192.png', (req, res) => {
  res.type('png').send(Buffer.from(ICON_192_B64, 'base64'));
});
app.get('/icons/icon-512.png', (req, res) => {
  res.type('png').send(Buffer.from(ICON_512_B64, 'base64'));
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`LiveDoors — serveur tout-en-un sur http://localhost:${PORT}`);
});
