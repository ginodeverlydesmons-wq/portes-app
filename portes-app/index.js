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
    avatarInitials: u.avatarInitials,
    avatarColor: u.avatarColor,
    phone: u.phone || null,
    doorOpen: u.doorOpen,
    doorMessage: u.doorMessage || '',
    companions: room ? Math.max(0, room.memberIds.size - 1) : 0,
  };
}

// N'envoie à chaque compte QUE les comptes qu'il a ajoutés en contact.
function broadcastFriends() {
  for (const [socketId, viewer] of users) {
    const list = Array.from(users.values())
      .filter((u) => u.id !== viewer.id && u.phoneKey && viewer.contacts.has(u.phoneKey))
      .map(publicUser);
    io.to(socketId).emit('friends:update', list);
  }
}

// ---------------------------------------------------------------------------
// Connexion Socket.io
// ---------------------------------------------------------------------------

io.on('connection', (socket) => {

  socket.on('register', ({ pseudo, avatarInitials, avatarColor, phone }) => {
    const user = {
      id: socket.id,
      pseudo: String(pseudo || 'Anonyme').slice(0, 24),
      avatarInitials: String(avatarInitials || pseudo || '??').slice(0, 2).toUpperCase(),
      avatarColor: avatarColor || '#ff8a00',
      phone: phone ? String(phone).slice(0, 32) : null,
      phoneKey: phone ? normalizePhone(phone) : null,
      doorOpen: false,
      doorMessage: '',
      roomId: null,
      contacts: new Set(),
    };
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

  socket.on('disconnect', () => {
    closeDoorAndRoom(socket.id);
    leaveCurrentRoom(socket.id);
    users.delete(socket.id);
    broadcastFriends();
  });
});

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
.toggle-btn.is-open{ background:var(--ink); color:var(--yellow); box-shadow:0 6px 14px -6px rgba(0,0,0,0.35); }
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
  position:absolute; inset:0; background:var(--ink); display:flex; flex-direction:column;
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

@media (prefers-reduced-motion: reduce){
  .story-ring, .call-glow{ animation:none; }
  .call-overlay, .toast{ transition:none; }
}
`;

const PAGE_BODY_HTML = `
<div class="phone" id="phone">

  <!-- ============ ÉCRAN 1 : création de profil / connexion ============ -->
  <div class="screen login-screen" id="loginScreen">
    <div class="login-inner">
      <div class="app-title" style="font-size:26px;">LiveDoors</div>
      <div class="app-sub" style="margin-bottom:24px;">Crée ton profil pour voir tes amis en direct</div>

      <label class="field-label">Pseudo</label>
      <input class="field-input" id="pseudoInput" type="text" placeholder="Ex. Léa" maxlength="24">

      <label class="field-label">Numéro de téléphone</label>
      <input class="field-input" id="phoneInput" type="tel" placeholder="06 12 34 56 78">
      <div class="field-hint">Sert à te retrouver auprès de tes vrais contacts. Non vérifié dans cette démo.</div>

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
        <button class="theme-btn" id="themeBtn" title="Changer de thème">🌙</button>
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
      <div style="display:flex; gap:8px; margin-bottom:20px;">
        <input class="field-input" id="contactPhoneInput" type="tel" placeholder="Numéro de téléphone" style="flex:1;">
        <button class="toggle-btn" id="addContactBtn">Ajouter</button>
      </div>

      <div class="section-label live-label"><span class="dot"></span>En direct maintenant</div>
      <div id="liveList"></div>

      <div class="section-label closed-label"><span class="dot"></span>Portes fermées</div>
      <div id="closedList"></div>

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

function applyTheme(theme) {
  document.documentElement.setAttribute('data-theme', theme);
  \$('themeBtn').textContent = theme === 'dark' ? '☀️' : '🌙';
  localStorage.setItem('livedoors-theme', theme);
}
applyTheme(localStorage.getItem('livedoors-theme') || 'light');

\$('themeBtn').addEventListener('click', () => {
  const current = document.documentElement.getAttribute('data-theme');
  applyTheme(current === 'dark' ? 'light' : 'dark');
});

// ---------------------------------------------------------------------------
// Écran 1 — création de profil
// ---------------------------------------------------------------------------

function colorForPseudo(pseudo) {
  let hash = 0;
  for (const ch of pseudo) hash = (hash * 31 + ch.charCodeAt(0)) % palette.length;
  return palette[Math.abs(hash) % palette.length];
}

\$('registerBtn').addEventListener('click', () => {
  const pseudo = \$('pseudoInput').value.trim();
  const phone = \$('phoneInput').value.trim();
  if (!pseudo) { \$('pseudoInput').focus(); return; }
  if (!phone) { \$('phoneInput').focus(); return; }

  socket.emit('register', {
    pseudo,
    phone,
    avatarInitials: pseudo.slice(0, 2).toUpperCase(),
    avatarColor: colorForPseudo(pseudo),
  });
});

socket.on('registered', (user) => {
  me = user;
  \$('loginScreen').style.display = 'none';
  \$('homeScreen').style.display = 'flex';

  \$('headerAvatar').textContent = user.avatarInitials;
  \$('headerAvatar').style.background = \`linear-gradient(135deg, \${user.avatarColor}, #ff3d77)\`;
  \$('myAvatar').textContent = user.avatarInitials;
  \$('myAvatar').style.background = \`linear-gradient(135deg, \${user.avatarColor}, #ff3d77)\`;
  \$('myName').textContent = user.pseudo;
  \$('myPhone').textContent = user.phone || '';
  \$('connectionState').textContent = 'Connecté';
});

// ---------------------------------------------------------------------------
// Présence — liste des amis en temps réel (poussée par le serveur)
// ---------------------------------------------------------------------------

socket.on('friends:update', (list) => {
  friends = list.filter((u) => !me || u.id !== me.id);
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

function render() {
  const live = friends.filter((f) => f.doorOpen);
  const closed = friends.filter((f) => !f.doorOpen);

  \$('liveList').innerHTML = live.length ? live.map((f) => \`
    <div class="friend-row is-open">
      <div class="avatar-wrap">
        <div class="story-ring show"></div>
        <div class="avatar" style="background:\${f.avatarColor}">\${f.avatarInitials}</div>
      </div>
      <div class="friend-info">
        <div class="friend-name">\${escapeHtml(f.pseudo)}</div>
        <div class="friend-phone">\${f.phone ? escapeHtml(f.phone) : ''}</div>
        <div class="friend-meta live-meta">\${friendMeta(f)}</div>
        \${f.doorMessage ? \`<div class="friend-status-msg">\${escapeHtml(f.doorMessage)}</div>\` : ''}
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
        <div class="friend-name">\${escapeHtml(f.pseudo)}</div>
        <div class="friend-phone">\${f.phone ? escapeHtml(f.phone) : ''}</div>
      </div>
    </div>
  \`).join('') : \`<div class="empty-note">Aucun autre compte connecté pour le moment.</div>\`;
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

\$('addContactBtn').addEventListener('click', () => {
  const phone = \$('contactPhoneInput').value.trim();
  if (!phone) { \$('contactPhoneInput').focus(); return; }
  socket.emit('contact:add', { phone });
  \$('contactPhoneInput').value = '';
});

socket.on('contact:added', ({ phone, found }) => {
  showToast(found ? 'Contact ajouté !' : "Contact ajouté, il apparaîtra dès qu'il sera connecté.");
});

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
  \$('callAvatar').style.background = target.avatarColor;
  \$('callAvatar').textContent = target.avatarInitials;
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
