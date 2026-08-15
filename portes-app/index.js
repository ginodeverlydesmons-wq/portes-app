/**
 * Porte Ouverte — fichier unique.
 *
 * Contient tout le projet : le serveur Express + Socket.io (présence des
 * comptes, ouverture/fermeture de "porte", relais de signalisation WebRTC)
 * ET la page cliente complète (HTML + CSS + JS), renvoyée telle quelle par
 * res.send() sur la route "/". Un seul fichier à exécuter :
 *
 *     node index.js
 *
 * Ce serveur NE transporte JAMAIS l'audio : il sert uniquement à
 * (1) garder la liste des comptes connectés et de leur statut (porte ouverte/fermée)
 * (2) faire office de "central téléphonique" qui relaie les messages d'appairage
 *     WebRTC (offer / answer / ICE candidates) entre deux navigateurs, qui
 *     établissent ensuite une connexion peer-to-peer directe pour le son.
 *
 * ⚠️ Stockage en mémoire (Map) : tout est perdu au redémarrage du serveur.
 *    Pour une vraie prod, remplacer `users` par une vraie base de données
 *    (Postgres/Mongo/Redis) et ajouter une authentification (JWT, session...).
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
// Page cliente (HTML + CSS + JS inline), servie directement par res.send()
// ---------------------------------------------------------------------------
// Le CSS, le JS client et le corps HTML sont stockés via JSON.stringify pour
// que tout caractère spécial (backtick, guillemet, ${...}) soit échappé
// automatiquement et sans risque, plutôt qu'à la main.

const PAGE_CSS = "@import url('https://fonts.googleapis.com/css2?family=Baloo+2:wght@600;700;800&family=Nunito:wght@400;600;700;800&family=JetBrains+Mono:wght@500;600&display=swap');\n\n:root{\n  --bg:#ffffff;\n  --bg-soft:#f6f6f8;\n  --border:#ececef;\n  --yellow:#fffc00;\n  --yellow-deep:#ffe600;\n  --ink:#14171a;\n  --ink-soft:#6b7280;\n  --ink-faint:#9aa0ac;\n  --grad-1:#fffc00;\n  --grad-2:#ff8a00;\n  --grad-3:#ff3d77;\n}\n\n*{box-sizing:border-box; margin:0; padding:0;}\n\nbody{\n  min-height:100vh;\n  background:#efeff2;\n  display:flex;\n  align-items:center;\n  justify-content:center;\n  padding:32px 16px;\n  font-family:'Nunito', sans-serif;\n}\n\n.phone{\n  width:392px;\n  max-width:100%;\n  height:820px;\n  max-height:92vh;\n  background:var(--bg);\n  border-radius:38px;\n  border:1px solid #dcdce2;\n  box-shadow:0 40px 80px -20px rgba(0,0,0,0.25), 0 0 0 8px #050506;\n  position:relative;\n  overflow:hidden;\n  display:flex;\n  flex-direction:column;\n}\n\n.screen{ height:100%; display:flex; flex-direction:column; }\n\n/* ---------- Login screen ---------- */\n.login-screen{ align-items:center; justify-content:center; padding:32px; background:var(--yellow); }\n.login-inner{ width:100%; }\n.field-label{\n  display:block; font-family:'Baloo 2', sans-serif; font-weight:700;\n  font-size:12.5px; color:var(--ink); margin:14px 0 6px;\n}\n.field-input{\n  width:100%; padding:13px 14px; border-radius:12px; border:none;\n  font-family:'Nunito', sans-serif; font-size:14px; background:#fff; color:var(--ink);\n}\n.field-input:focus{ outline:3px solid rgba(0,0,0,0.15); }\n.field-hint{ font-size:10.5px; color:rgba(20,23,26,0.55); margin-top:5px; font-weight:600; }\n.primary-btn{\n  width:100%; margin-top:24px; padding:14px; border-radius:14px; border:none;\n  background:var(--ink); color:var(--yellow); font-family:'Baloo 2', sans-serif;\n  font-weight:700; font-size:14.5px; cursor:pointer;\n}\n.primary-btn:active{ transform:scale(0.98); }\n\n/* ---------- Header ---------- */\n.app-header{\n  background:var(--yellow);\n  padding:22px 20px 16px;\n  flex-shrink:0;\n  display:flex;\n  align-items:center;\n  justify-content:space-between;\n}\n.app-title{\n  font-family:'Baloo 2', sans-serif;\n  font-weight:800;\n  font-size:22px;\n  color:var(--ink);\n  letter-spacing:-0.3px;\n}\n.app-sub{\n  font-size:11px;\n  color:rgba(20,23,26,0.6);\n  font-weight:700;\n  margin-top:1px;\n}\n.header-avatar{\n  width:38px; height:38px;\n  border-radius:50%;\n  background:linear-gradient(135deg,#ff8a00,#ff3d77);\n  display:flex; align-items:center; justify-content:center;\n  color:#fff; font-family:'Baloo 2',sans-serif; font-weight:700; font-size:14px;\n  border:2px solid rgba(0,0,0,0.08);\n}\n\n.content{ flex:1; overflow-y:auto; padding:16px 18px 24px; }\n.content::-webkit-scrollbar{ width:0; }\n\n/* ---------- My profile row ---------- */\n.me-card{\n  background:var(--bg-soft);\n  border-radius:20px;\n  padding:14px;\n  display:flex;\n  align-items:center;\n  gap:12px;\n  margin-bottom:22px;\n}\n.me-avatar-wrap{ position:relative; width:52px; height:52px; flex-shrink:0; }\n.me-avatar{\n  width:52px; height:52px;\n  border-radius:50%;\n  background:linear-gradient(135deg,#ff8a00,#ff3d77);\n  display:flex; align-items:center; justify-content:center;\n  color:#fff; font-family:'Baloo 2',sans-serif; font-weight:700; font-size:18px;\n}\n.story-ring{\n  position:absolute; inset:-4px; border-radius:50%; padding:3px;\n  background:conic-gradient(from 0deg, var(--grad-1), var(--grad-2), var(--grad-3), var(--grad-1));\n  -webkit-mask:radial-gradient(farthest-side, transparent calc(100% - 3px), #000 calc(100% - 3px));\n  mask:radial-gradient(farthest-side, transparent calc(100% - 3px), #000 calc(100% - 3px));\n  opacity:0; transition:opacity .3s ease; animation:spin 3s linear infinite;\n}\n.story-ring.show{ opacity:1; }\n@keyframes spin{ to{ transform:rotate(360deg); } }\n\n.me-info{ flex:1; min-width:0; }\n.me-name{ font-family:'Baloo 2', sans-serif; font-weight:700; font-size:15.5px; color:var(--ink); }\n.me-phone{ font-family:'JetBrains Mono', monospace; font-size:11px; color:var(--ink-soft); margin-top:2px; }\n.me-status-line{ font-size:11.5px; font-weight:700; color:var(--ink-faint); margin-top:3px; }\n.me-status-line.live{ color:#e08a00; }\n\n.toggle-btn{\n  font-family:'Baloo 2', sans-serif; font-weight:700; font-size:13px; color:var(--ink);\n  background:var(--yellow); border:none; padding:11px 18px; border-radius:14px; cursor:pointer;\n  flex-shrink:0; transition:transform .15s ease, background .25s ease, color .25s ease;\n  box-shadow:0 6px 14px -6px rgba(255,204,0,0.7);\n}\n.toggle-btn:active{ transform:scale(0.96); }\n.toggle-btn.is-open{ background:var(--ink); color:var(--yellow); box-shadow:0 6px 14px -6px rgba(0,0,0,0.35); }\n.toggle-btn:disabled{ opacity:0.5; cursor:not-allowed; }\n\n/* ---------- Section labels ---------- */\n.section-label{\n  font-family:'Baloo 2', sans-serif; font-size:13px; color:var(--ink); font-weight:700;\n  margin:20px 4px 10px; display:flex; align-items:center; gap:7px;\n}\n.section-label .dot{ width:8px; height:8px; border-radius:50%; }\n.live-label .dot{ background:linear-gradient(135deg,var(--grad-2),var(--grad-3)); }\n.closed-label .dot{ background:#d7d7dc; }\n\n/* ---------- Friend rows ---------- */\n.friend-row{ display:flex; align-items:center; gap:12px; padding:9px 8px; border-radius:16px; margin-bottom:2px; }\n.friend-row.is-open:hover{ background:var(--bg-soft); }\n\n.avatar-wrap{ position:relative; flex-shrink:0; width:48px; height:48px; }\n.avatar{\n  width:48px; height:48px; border-radius:50%; display:flex; align-items:center; justify-content:center;\n  font-family:'Baloo 2', sans-serif; font-weight:700; font-size:15px; color:#fff; position:relative; z-index:2;\n}\n.friend-row.is-closed .avatar{ filter:grayscale(1) brightness(0.92); opacity:0.55; }\n\n.friend-info{ flex:1; min-width:0; }\n.friend-name{ font-family:'Baloo 2', sans-serif; font-size:14.5px; font-weight:700; color:var(--ink); }\n.friend-row.is-closed .friend-name{ color:var(--ink-soft); }\n.friend-phone{ font-family:'JetBrains Mono', monospace; font-size:10.5px; color:var(--ink-faint); margin-top:1px; }\n.friend-meta{ font-size:11px; color:var(--ink-faint); margin-top:2px; font-weight:700; }\n.friend-meta.live-meta{ color:#e08a00; }\n\n.join-btn{\n  font-family:'Baloo 2', sans-serif; font-weight:700; font-size:12.5px; color:var(--ink);\n  background:var(--yellow); border:none; padding:9px 16px; border-radius:12px; cursor:pointer;\n  flex-shrink:0; transition:transform .15s ease;\n}\n.join-btn:active{ transform:scale(0.94); }\n.join-btn:disabled{ opacity:0.5; cursor:not-allowed; }\n\n.empty-note{ font-size:12px; color:var(--ink-faint); padding:4px 8px; font-weight:600; }\n\n/* ---------- Call overlay ---------- */\n.call-overlay{\n  position:absolute; inset:0; background:var(--ink); display:flex; flex-direction:column;\n  align-items:center; justify-content:center; gap:20px; transform:translateY(100%);\n  transition:transform .5s cubic-bezier(.5,0,.2,1); z-index:20;\n}\n.call-overlay.active{ transform:translateY(0); }\n.call-glow{\n  position:absolute; width:320px; height:320px; border-radius:50%;\n  background:radial-gradient(circle, rgba(255,252,0,0.14), transparent 70%);\n  animation:breathe 3.2s ease-in-out infinite;\n}\n@keyframes breathe{ 0%,100%{ transform:scale(0.94); opacity:0.7; } 50%{ transform:scale(1.06); opacity:1; } }\n.call-avatar{\n  width:108px; height:108px; border-radius:50%; display:flex; align-items:center; justify-content:center;\n  font-family:'Baloo 2', sans-serif; font-weight:700; font-size:32px; color:#fff; z-index:2;\n  box-shadow:0 0 0 3px rgba(255,255,255,0.12);\n}\n.call-status{\n  font-size:11px; letter-spacing:0.08em; text-transform:uppercase; color:var(--yellow);\n  font-family:'JetBrains Mono', monospace; font-weight:600; z-index:2; text-align:center;\n}\n.call-name{ font-family:'Baloo 2', sans-serif; font-weight:700; font-size:22px; color:#fff; z-index:2; text-align:center; margin-top:4px; }\n.call-timer{ font-family:'JetBrains Mono', monospace; color:rgba(255,255,255,0.55); font-size:13px; z-index:2; text-align:center; margin-top:8px; }\n.call-controls{ display:flex; gap:10px; z-index:2; margin-top:8px; }\n.mute-btn, .leave-btn{\n  font-family:'Baloo 2', sans-serif; font-weight:700; font-size:13px; color:#fff;\n  background:rgba(255,255,255,0.1); border:1px solid rgba(255,255,255,0.2);\n  padding:12px 20px; border-radius:14px; cursor:pointer;\n}\n.mute-btn.is-muted{ background:var(--yellow); color:var(--ink); border-color:transparent; }\n.mute-btn:hover, .leave-btn:hover{ background:rgba(255,255,255,0.16); }\n\n/* ---------- Toast ---------- */\n.toast-zone{\n  position:absolute; left:0; right:0; bottom:22px; display:flex; flex-direction:column;\n  align-items:center; gap:8px; pointer-events:none; z-index:30;\n}\n.toast{\n  background:var(--ink); color:#fff; font-size:12.5px; font-weight:700; padding:10px 16px;\n  border-radius:12px; border-left:4px solid var(--yellow); box-shadow:0 10px 24px -8px rgba(0,0,0,0.35);\n  opacity:0; transform:translateY(8px); transition:opacity .3s ease, transform .3s ease;\n}\n.toast.show{ opacity:1; transform:translateY(0); }\n\n@media (prefers-reduced-motion: reduce){\n  .story-ring, .call-glow{ animation:none; }\n  .call-overlay, .toast{ transition:none; }\n}\n";

const PAGE_CLIENT_JS = "/**\n * Client \"Porte Ouverte\".\n * Aucune donnée n'est simulée ici : tout vient du serveur (présence réelle\n * des comptes connectés) et l'audio passe par de vraies connexions WebRTC\n * peer-to-peer entre navigateurs, le serveur ne servant qu'à la\n * signalisation (voir server.js).\n */\n\nconst socket = io();\n\nconst palette = ['#ff8a00', '#7c5cff', '#ff3d77', '#00c2a8', '#ffb020', '#4d8bff'];\nconst ICE_SERVERS = [\n  { urls: 'stun:stun.l.google.com:19302' },\n  // TODO prod: ajouter un serveur TURN (ex. coturn, Twilio, Xirsys) —\n  // le STUN seul ne suffit pas dès qu'un des deux réseaux est restrictif\n  // (NAT symétrique, 4G, wifi d'entreprise...).\n];\n\nconst $ = (id) => document.getElementById(id);\n\nlet me = null;                 // profil renvoyé par le serveur après 'registered'\nlet friends = [];              // dernière liste reçue via 'friends:update'\nlet localStream = null;        // flux micro local\nlet peers = new Map();         // peerId -> RTCPeerConnection\nlet inCall = false;\nlet callSeconds = 0;\nlet callTimerHandle = null;\n\n// ---------------------------------------------------------------------------\n// Écran 1 — création de profil\n// ---------------------------------------------------------------------------\n\nfunction colorForPseudo(pseudo) {\n  let hash = 0;\n  for (const ch of pseudo) hash = (hash * 31 + ch.charCodeAt(0)) % palette.length;\n  return palette[Math.abs(hash) % palette.length];\n}\n\n$('registerBtn').addEventListener('click', () => {\n  const pseudo = $('pseudoInput').value.trim();\n  const phone = $('phoneInput').value.trim();\n  if (!pseudo) { $('pseudoInput').focus(); return; }\n\n  socket.emit('register', {\n    pseudo,\n    phone: phone || null,\n    avatarInitials: pseudo.slice(0, 2).toUpperCase(),\n    avatarColor: colorForPseudo(pseudo),\n  });\n});\n\nsocket.on('registered', (user) => {\n  me = user;\n  $('loginScreen').style.display = 'none';\n  $('homeScreen').style.display = 'flex';\n\n  $('headerAvatar').textContent = user.avatarInitials;\n  $('headerAvatar').style.background = `linear-gradient(135deg, ${user.avatarColor}, #ff3d77)`;\n  $('myAvatar').textContent = user.avatarInitials;\n  $('myAvatar').style.background = `linear-gradient(135deg, ${user.avatarColor}, #ff3d77)`;\n  $('myName').textContent = user.pseudo;\n  $('myPhone').textContent = user.phone || '';\n  $('connectionState').textContent = 'Connecté';\n});\n\n// ---------------------------------------------------------------------------\n// Présence — liste des amis en temps réel (poussée par le serveur)\n// ---------------------------------------------------------------------------\n\nsocket.on('friends:update', (list) => {\n  friends = list.filter((u) => !me || u.id !== me.id);\n  const myUpdated = list.find((u) => me && u.id === me.id);\n  if (myUpdated) {\n    me = myUpdated;\n    syncMyDoorUI();\n  }\n  render();\n});\n\nfunction friendMeta(f) {\n  if (f.doorOpen) return f.companions === 0 ? 'seul pour l\\'instant' : `+${f.companions} déjà dans l'appel`;\n  return 'porte fermée';\n}\n\nfunction render() {\n  const live = friends.filter((f) => f.doorOpen);\n  const closed = friends.filter((f) => !f.doorOpen);\n\n  $('liveList').innerHTML = live.length ? live.map((f) => `\n    <div class=\"friend-row is-open\">\n      <div class=\"avatar-wrap\">\n        <div class=\"story-ring show\"></div>\n        <div class=\"avatar\" style=\"background:${f.avatarColor}\">${f.avatarInitials}</div>\n      </div>\n      <div class=\"friend-info\">\n        <div class=\"friend-name\">${escapeHtml(f.pseudo)}</div>\n        <div class=\"friend-phone\">${f.phone ? escapeHtml(f.phone) : ''}</div>\n        <div class=\"friend-meta live-meta\">${friendMeta(f)}</div>\n      </div>\n      <button class=\"join-btn\" onclick=\"joinCall('${f.id}')\" ${inCall ? 'disabled' : ''}>Rejoindre</button>\n    </div>\n  `).join('') : `<div class=\"empty-note\">Personne n'a ouvert sa porte pour l'instant.</div>`;\n\n  $('closedList').innerHTML = closed.length ? closed.map((f) => `\n    <div class=\"friend-row is-closed\">\n      <div class=\"avatar-wrap\">\n        <div class=\"avatar\" style=\"background:${f.avatarColor}\">${f.avatarInitials}</div>\n      </div>\n      <div class=\"friend-info\">\n        <div class=\"friend-name\">${escapeHtml(f.pseudo)}</div>\n        <div class=\"friend-phone\">${f.phone ? escapeHtml(f.phone) : ''}</div>\n      </div>\n    </div>\n  `).join('') : `<div class=\"empty-note\">Aucun autre compte connecté pour le moment.</div>`;\n}\n\nfunction escapeHtml(str) {\n  const d = document.createElement('div');\n  d.textContent = str;\n  return d.innerHTML;\n}\n\nfunction showToast(msg) {\n  const t = document.createElement('div');\n  t.className = 'toast';\n  t.textContent = msg;\n  $('toastZone').appendChild(t);\n  requestAnimationFrame(() => t.classList.add('show'));\n  setTimeout(() => {\n    t.classList.remove('show');\n    setTimeout(() => t.remove(), 350);\n  }, 2800);\n}\n\n// ---------------------------------------------------------------------------\n// Ma porte (ouvrir = créer ma propre session d'appel côté serveur)\n// ---------------------------------------------------------------------------\n\nfunction syncMyDoorUI() {\n  $('toggleBtn').textContent = me.doorOpen ? 'Fermer' : 'Ouvrir';\n  $('toggleBtn').classList.toggle('is-open', me.doorOpen);\n  $('myRing').classList.toggle('show', me.doorOpen);\n  $('statusText').textContent = me.doorOpen\n    ? (me.companions > 0 ? `Porte ouverte · ${me.companions} ami(s) dans ton appel` : \"Porte ouverte · en attente d'amis...\")\n    : 'Porte fermée';\n  $('statusText').classList.toggle('live', me.doorOpen);\n}\n\n$('toggleBtn').addEventListener('click', async () => {\n  if (!me) return;\n  if (!me.doorOpen) {\n    // Ouvrir sa porte = héberger sa propre room + être prêt à recevoir des appels.\n    try {\n      localStream = await navigator.mediaDevices.getUserMedia({ audio: true });\n    } catch (err) {\n      showToast(\"Micro refusé — active-le pour ouvrir ta porte.\");\n      return;\n    }\n    socket.emit('door:open');\n    startCallUI({ id: me.id, pseudo: 'En attente...', avatarInitials: me.avatarInitials, avatarColor: me.avatarColor }, true);\n  } else {\n    socket.emit('door:close');\n    endCall('local-close');\n  }\n});\n\n// ---------------------------------------------------------------------------\n// Rejoindre la porte d'un ami — flux WebRTC \"mesh\" (une connexion par pair)\n// ---------------------------------------------------------------------------\n\nasync function joinCall(hostId) {\n  if (inCall) return;\n  const host = friends.find((f) => f.id === hostId);\n  if (!host) return;\n\n  try {\n    localStream = await navigator.mediaDevices.getUserMedia({ audio: true });\n  } catch (err) {\n    showToast('Impossible d\\'accéder au micro.');\n    return;\n  }\n\n  startCallUI(host, false);\n  socket.emit('call:join', { hostId });\n}\nwindow.joinCall = joinCall; // exposé pour les boutons générés dynamiquement\n\nfunction createPeerConnection(peerId) {\n  const pc = new RTCPeerConnection({ iceServers: ICE_SERVERS });\n\n  localStream.getTracks().forEach((track) => pc.addTrack(track, localStream));\n\n  pc.onicecandidate = (event) => {\n    if (event.candidate) {\n      socket.emit('webrtc:ice-candidate', { targetId: peerId, candidate: event.candidate });\n    }\n  };\n\n  pc.ontrack = (event) => {\n    attachRemoteAudio(peerId, event.streams[0]);\n  };\n\n  pc.onconnectionstatechange = () => {\n    if (['failed', 'disconnected', 'closed'].includes(pc.connectionState)) {\n      removePeer(peerId);\n    }\n  };\n\n  peers.set(peerId, pc);\n  return pc;\n}\n\nfunction attachRemoteAudio(peerId, stream) {\n  let audioEl = document.getElementById(`audio-${peerId}`);\n  if (!audioEl) {\n    audioEl = document.createElement('audio');\n    audioEl.id = `audio-${peerId}`;\n    audioEl.autoplay = true;\n    $('remoteAudioContainer').appendChild(audioEl);\n  }\n  audioEl.srcObject = stream;\n}\n\nfunction removePeer(peerId) {\n  const pc = peers.get(peerId);\n  if (pc) { pc.close(); peers.delete(peerId); }\n  document.getElementById(`audio-${peerId}`)?.remove();\n}\n\n// -- Signalisation entrante --\n\n// Je viens de rejoindre : je connais déjà les membres présents -> je les appelle.\nsocket.on('call:room-state', async ({ members }) => {\n  for (const member of members) {\n    const pc = createPeerConnection(member.id);\n    const offer = await pc.createOffer();\n    await pc.setLocalDescription(offer);\n    socket.emit('webrtc:offer', { targetId: member.id, offer });\n  }\n});\n\n// Quelqu'un vient de rejoindre ma room : j'attends son offre.\nsocket.on('call:peer-joined', (peer) => {\n  showToast(`${peer.pseudo} a rejoint l'appel`);\n  updateCallStatus();\n});\n\nsocket.on('webrtc:offer', async ({ fromId, offer }) => {\n  const pc = peers.get(fromId) || createPeerConnection(fromId);\n  await pc.setRemoteDescription(offer);\n  const answer = await pc.createAnswer();\n  await pc.setLocalDescription(answer);\n  socket.emit('webrtc:answer', { targetId: fromId, answer });\n});\n\nsocket.on('webrtc:answer', async ({ fromId, answer }) => {\n  const pc = peers.get(fromId);\n  if (pc) await pc.setRemoteDescription(answer);\n});\n\nsocket.on('webrtc:ice-candidate', async ({ fromId, candidate }) => {\n  const pc = peers.get(fromId);\n  if (pc) { try { await pc.addIceCandidate(candidate); } catch (_) {} }\n});\n\nsocket.on('call:peer-left', ({ id }) => {\n  removePeer(id);\n  updateCallStatus();\n});\n\nsocket.on('call:ended', () => {\n  showToast(\"L'hôte a fermé sa porte.\");\n  endCall('host-closed');\n});\n\nsocket.on('call:error', ({ message }) => {\n  showToast(message);\n});\n\n// ---------------------------------------------------------------------------\n// UI de l'écran d'appel\n// ---------------------------------------------------------------------------\n\nfunction startCallUI(target, isHosting) {\n  inCall = true;\n  callSeconds = 0;\n  $('callAvatar').style.background = target.avatarColor;\n  $('callAvatar').textContent = target.avatarInitials;\n  $('callName').textContent = isHosting ? 'Ta porte est ouverte' : target.pseudo;\n  $('callStatusLabel').textContent = isHosting ? 'En attente' : 'Connexion...';\n  $('callTimer').textContent = '00:00';\n  $('callOverlay').classList.add('active');\n  $('muteBtn').classList.remove('is-muted');\n  $('muteBtn').textContent = 'Couper le micro';\n\n  callTimerHandle = setInterval(() => {\n    callSeconds++;\n    const m = String(Math.floor(callSeconds / 60)).padStart(2, '0');\n    const s = String(callSeconds % 60).padStart(2, '0');\n    $('callTimer').textContent = `${m}:${s}`;\n  }, 1000);\n\n  render();\n}\n\nfunction updateCallStatus() {\n  const count = peers.size;\n  $('callStatusLabel').textContent = count === 0 ? 'En attente' : `${count} personne(s) connectée(s)`;\n}\n\n$('muteBtn').addEventListener('click', () => {\n  if (!localStream) return;\n  const track = localStream.getAudioTracks()[0];\n  track.enabled = !track.enabled;\n  $('muteBtn').classList.toggle('is-muted', !track.enabled);\n  $('muteBtn').textContent = track.enabled ? 'Couper le micro' : 'Réactiver le micro';\n});\n\n$('leaveBtn').addEventListener('click', () => {\n  const wasHost = me && me.doorOpen;\n  socket.emit(wasHost ? 'door:close' : 'call:leave');\n  endCall('local-leave');\n});\n\nfunction endCall(reason) {\n  inCall = false;\n  clearInterval(callTimerHandle);\n  $('callOverlay').classList.remove('active');\n\n  peers.forEach((pc, id) => { pc.close(); document.getElementById(`audio-${id}`)?.remove(); });\n  peers.clear();\n\n  if (localStream) {\n    localStream.getTracks().forEach((t) => t.stop());\n    localStream = null;\n  }\n\n  render();\n}\n";

const PAGE_BODY_HTML = "<div class=\"phone\" id=\"phone\">\n\n  <!-- ============ ÉCRAN 1 : création de profil / connexion ============ -->\n  <div class=\"screen login-screen\" id=\"loginScreen\">\n    <div class=\"login-inner\">\n      <div class=\"app-title\" style=\"font-size:26px;\">Portes</div>\n      <div class=\"app-sub\" style=\"margin-bottom:24px;\">Crée ton profil pour voir tes amis en direct</div>\n\n      <label class=\"field-label\">Pseudo</label>\n      <input class=\"field-input\" id=\"pseudoInput\" type=\"text\" placeholder=\"Ex. Léa\" maxlength=\"24\">\n\n      <label class=\"field-label\">Numéro de téléphone</label>\n      <input class=\"field-input\" id=\"phoneInput\" type=\"tel\" placeholder=\"06 12 34 56 78\">\n      <div class=\"field-hint\">Sert à te retrouver auprès de tes vrais contacts. Non vérifié dans cette démo.</div>\n\n      <button class=\"primary-btn\" id=\"registerBtn\">Créer mon profil</button>\n    </div>\n  </div>\n\n  <!-- ============ ÉCRAN 2 : accueil (liste des amis) ============ -->\n  <div class=\"screen home-screen\" id=\"homeScreen\" style=\"display:none;\">\n\n    <div class=\"app-header\">\n      <div>\n        <div class=\"app-title\">Portes</div>\n        <div class=\"app-sub\" id=\"connectionState\">Connexion...</div>\n      </div>\n      <div class=\"header-avatar\" id=\"headerAvatar\">--</div>\n    </div>\n\n    <div class=\"content\">\n\n      <div class=\"me-card\">\n        <div class=\"me-avatar-wrap\">\n          <div class=\"story-ring\" id=\"myRing\"></div>\n          <div class=\"me-avatar\" id=\"myAvatar\">--</div>\n        </div>\n        <div class=\"me-info\">\n          <div class=\"me-name\" id=\"myName\">Toi</div>\n          <div class=\"me-phone\" id=\"myPhone\"></div>\n          <div class=\"me-status-line\" id=\"statusText\">Porte fermée</div>\n        </div>\n        <button class=\"toggle-btn\" id=\"toggleBtn\">Ouvrir</button>\n      </div>\n\n      <div class=\"section-label live-label\"><span class=\"dot\"></span>En direct maintenant</div>\n      <div id=\"liveList\"></div>\n\n      <div class=\"section-label closed-label\"><span class=\"dot\"></span>Portes fermées</div>\n      <div id=\"closedList\"></div>\n\n    </div>\n\n    <div class=\"call-overlay\" id=\"callOverlay\">\n      <div class=\"call-glow\"></div>\n      <div class=\"call-avatar\" id=\"callAvatar\"></div>\n      <div>\n        <div class=\"call-status\" id=\"callStatusLabel\">Connexion...</div>\n        <div class=\"call-name\" id=\"callName\"></div>\n        <div class=\"call-timer\" id=\"callTimer\">00:00</div>\n      </div>\n      <div class=\"call-controls\">\n        <button class=\"mute-btn\" id=\"muteBtn\">Couper le micro</button>\n        <button class=\"leave-btn\" id=\"leaveBtn\">Quitter l'appel</button>\n      </div>\n    </div>\n\n    <div class=\"toast-zone\" id=\"toastZone\"></div>\n\n    <!-- Éléments audio distants (invisibles, un par participant) -->\n    <div id=\"remoteAudioContainer\" style=\"display:none;\"></div>\n\n  </div>\n\n</div>\n";

const PAGE_HTML = '<!DOCTYPE html>\n' +
  '<html lang="fr">\n' +
  '<head>\n' +
  '<meta charset="UTF-8">\n' +
  '<meta name="viewport" content="width=device-width, initial-scale=1">\n' +
  '<title>Portes</title>\n' +
  '<link rel="manifest" href="/manifest.json">\n' +
  '<meta name="theme-color" content="#fffc00">\n' +
  '<link rel="apple-touch-icon" href="/icons/icon-180.png">\n' +
  '<meta name="mobile-web-app-capable" content="yes">\n' +
  '<meta name="apple-mobile-web-app-capable" content="yes">\n' +
  '<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">\n' +
  '<meta name="apple-mobile-web-app-title" content="Portes">\n' +
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

const ICON_180_B64 = "iVBORw0KGgoAAAANSUhEUgAAALQAAAC0CAYAAAA9zQYyAAADEUlEQVR4nO3cQU4bQRRF0XLEFjyB/S8NJmwByRlFihQSN4md9r99ztBiUHp9VVg24nT5WJcFEd/2PgDckqBJETQpgiZF0KQImhRBkyJoUgRNiqBJETQpgiZF0KQImhRBkyJoUgRNiqBJETQpgiZF0KQImhRBkyJoUgRNiqBJedr7APdwfnne+whjvL++7X2EmzpV/rediP9dIe7EWw4x30Zhx/FBFx7CI5m+5+igp4//qCbvOjboyaNPMHXfkUFPHXuaiTuPC3riyJNN2zv5OfRajY+g7m1arFuMu6G3EPM2xZ1GBb3lRik+pHvastekm3xU0NeI+e+UdksFDYImRdCkCJoUQZMiaFIETYqgSRE0KYImRdCkCJoUQZMiaFIETYqgSRE0KYImRdCkCJoUQZMiaFIETYqgSRE0KYImRdCkCJoUQZMiaFIETYqgSRE0KYImRdCkCJoUQZMiaFIETYqgSRE0KYImRdCkCJoUQZPytPcBuJ3zy/Mvr72/vu1wkv24oSM+i/lPr1cJOuBatEeKWtDDbY31KFELerCvRnqEqAVNiqBJETQpgiZF0IN99UuTI3zJIujhtkZ6hJjXEnTCtViPEvNags74XbRHinktf5yUcrR4P+OGJkXQpAiaFEGTImhSBE2KoEkRNCmCJkXQpAiaFEGTImhSBE2KoEkRNCmCJkXQpAiaFEGTImhSBE2KoEkRNCmCJkXQpAiaFEGTImhSBE2KoEkRNCmCJkXQpAiaFEGTImhSBE2KoEkRNCmCJkXQpAiaFEGTImhSUkGfX573PsJIpd1GBf3++nb1Z0oP53/YsteW3R/FqKC3EvU2xZ2e9j7AvRQfFteNu6En/formLb3uKDXmjfyVBN3Hhn0WjPHnmTqvmODXmvu6I9u8q6jg15r9viPaPqe44Nea/5DeBSFHU+Xj3XZ+xC35iO77QoR/ywZNMeVeMsBPwiaFEGTImhSBE2KoEkRNCmCJkXQpAiaFEGTImhSBE2KoEkRNCmCJkXQpAiaFEGTImhSBE2KoEkRNCmCJkXQpHwH5ZBzVz6sVUAAAAAASUVORK5CYII=";
const ICON_192_B64 = "iVBORw0KGgoAAAANSUhEUgAAAMAAAADACAYAAABS3GwHAAADT0lEQVR4nO3cQU4bQRBA0SHiCmzg/keDDVdAclaWEHGIMcHjrv/eErJoVffvGcsod4e37bBB1K+9FwB7EgBpAiBNAKQJgDQBkCYA0gRAmgBIEwBpAiBNAKQJgDQBkCYA0gRAmgBIEwBpAiBNAKQJgDQBkCYA0gRAmgBIEwBpAiDtfu8FXMPD0+PeS1jW6/PL3kv4UXdT/3Nch/7/mxjDyFcgh/9nTJzrqCfAxA26VVOeBmOeAA7/dU2Z94gApmzGaibMfUQAcKnlA5hwC61s9fkv/SH4K8Of8qHtmgrzHf9F2KobcwuOs1v9lv/M8q9An3H4/4/Jc1w2gMm30opW3Y9lA/iXybfWHqbOc2wAcA4BkCYA0gRAmgBIEwBpAiBNAKQJgDQBkCYA0gRAmgBIEwBpAiBNAKQJgDQBkCYA0gRAmgBIEwBpAiBNAKQJgDQBkCYA0gRAmgBIEwBpAiBNAKQJgDQBkCYA0gRAmgBIEwBpAiBNAKQJgDQBkCYA0gRAmgBIEwBpAiDtfu8FcB0PT49//Oz1+WWHldwWAQx36uB//F05BK9Ag312+C/5dxMJYKivHupqBAIgTQADXXqbF58CAiBNAKQJgDQBkCaAgS79Yqv4hZgASBPAUF+9zYu3/7YJYLRzD3X18G+bP4Yb73i4/TXoaQKIcNhP8wpEmgBIEwBpAiBNAKQJgDQBkCYA0gRAmgBIEwBpAiBNAKQJgDQBkCYA0gRAmgBIEwBpAiBNAKQJgDQBkCYA0gRAmgBIEwBpAiBNAKQJgDQBkCYA0gRAmgBIEwBpAiBNAKQJgDQBkCYA0gRAmgBIEwBpAiBNAKQJgDQBkCYA0sYG8PD0uPcSRpk6z2UDeH1+2XsJvLPqfiwbwDmm3lrXNnmO93sv4KcdN2/VG2pPkw/+0d3hbTvsvYjvKGzSrVv5cln+FWjl4U+w+vyXDwC+Y0QAq99Cq5ow9xEBbNuMzVjJlHkv/yH4FB+Mf86Ug3805gnw3rRNuhUT5zryCfCRJ8LlJh769xIBwN+MfAWCcwmANAGQJgDSBECaAEgTAGkCIE0ApAmANAGQJgDSBECaAEgTAGkCIE0ApAmANAGQJgDSBECaAEgTAGkCIE0ApAmANAGQ9hvM7YUNXMU/3gAAAABJRU5ErkJggg==";
const ICON_512_B64 = "iVBORw0KGgoAAAANSUhEUgAAAgAAAAIACAYAAAD0eNT6AAAKzUlEQVR4nO3dS1IjRxRA0cLBFpjA/pcGE7ZAhDySQ00jWUj1SdU9Z9xB5+Tlu5WNw0+Hr+kwAQAp/2x9AABgfQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIKetz4A+/fy9rr1EeAhfb5/bH0Eduzp8DUdtj4E+2LhwzIEAXMSAMzC0od1iQHuJQC4i8UP2xIC3EoAcBOLH8YiBPgtAcCvWPwwNiHAtfxngFzN8ofxmVOu5QWA/+VCgcfkNYBLvABwkeUPj8v8cokA4CyXBzw+c8w5AoAfuTRgP8wzPxEA/MVlAftjrvlOAPAHlwTsl/nmlADgPy4H2D9zzpEAYJomlwKUmHemSQAwuQygyNwjAAAgSADE+QqALvPfJgDCDD/gHugSAAAQJACiVD9w5D5oEgAAECQAgtQ+8J17oed56wPQ4v9PDpdZxKzl6fA1HbY+BOvZ4nKx9OE25pUleQFgMS4SuM9xhrwKsAS/A8AiLH+Yj3liCQIgZK2vCJcVzG+tufLa0CEAmJXlD8sxX8xJADAblxMsz5wxFwEAAEECIGLpf9fzVQLrWXre/B5AgwAAgCABAABBAoC7ef6H9Zk77iUAACBIAABAkAAAgCABAABBAgAAggQAAAQJAAAIEgAAECQAACBIAABAkAAAgCABAABBAgAAggQAAAQJAAAIEgAAECQAACBIAABAkAAAgCABAABBAgAAggQAAAQJAAAIEgAAECQAACBIAABAkAAAgCABAABBAgAAggQAAAQJAAAIEgAAECQAACBIAABAkAAAgCABAABBAgAAggQAAAQJAAAIEgAAECQAACBIAABAkAAAgCABAABBAgAAggQAAAQJAAAIEgAAECQAACBIAABAkAAAgCABAABBAgAAggQAAAQJAAAIEgAAECQAACBIAABAkAAAgCABAABBAgAAggQAAAQJAAAIEgAAECQAACBIAABAkAAAgCABAABBAgAAggQAAAQJAAAIEgAAECQAACBIAABAkAAAgCABAABBAgAAggQAAAQJAAAIEgAAECQAACBIAABAkAAAgCABAABBAgAAggQAAAQJAAAIEgAAECQAACBIAABAkAAAgCABAABBAgAAggQAAAQJAAAIEgAAECQAACBIAABAkAAAgCABAABBAgAAggQAAAQJAAAIEgAAECQAACBIAABAkAAAgCABAABBAgAAggQAAAQJAAAIEgAAECQAACBIAABAkAAAgCABAABBAgAAggQAAAQJAAAIEgAAECQAACBIAABAkAAAgCABAABBAgAAggQAAAQ9b30AgFG9vL3+75/5fP9Y4SQwPwEAcOKapX/uz4sBHokAAJh+v/gv/QwhwCPwOwBA3hzLf8mfB0vwAgBkLbmovQYwOi8AQNJaX+leAxiVAABy1l7KIoARCQAgZatlLAIYjQAAgCABAGRs/RW+9d8PpwQAkDDK8h3lHCAAACBIAAC7N9pX92jnoUkAAECQAACAIAEA7Nqoz+2jnosOAQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQDs2uf7x9ZH+NGo56JDAABAkAAAgCABAOzeaM/to52HJgEAAEECAEgY5at7lHOAAAAytl6+W//9cEoAAECQAABStvoK9/XPaAQAkLP2Mrb8GZEAAJLWWsqWP6N63voAAFs5LueXt9fFfjaMygsAkDf3srb8eQReAACmeV4DLH4eiQAAOHG6xK+JAUufRyUAAM6w3NkzvwMAAEECAACCBAAABAkAAAgSAAAQJAAAIEgAAECQAACAIAEAAEECAACCBAAABAkAAAgSAAAQJAAAIEgAAECQAACAIAEAAEECAACCBAAABAkAAAgSAAAQJAAAIEgAAECQAACAIAEAAEECAACCBAAABAkAAAgSAAAQJAAAIEgAAECQAACAIAEAAEECAACCBAAABAkAAAgSAAAQJAAAIEgAAECQAACAIAEAAEECAACCBAAABAkAAAgSAAAQJAAAIEgAAECQAACAIAEAAEECAACCBAAABAkAAAgSAAAQJAAAIEgAAECQAACAIAEAAEECAACCBAAABAkAAAgSAAAQJAAAIEgAAECQAACAIAEAAEECAACCBAAABAkAAAgSAAAQJAAAIEgAAECQAACAIAEAAEECAACCBAAABAkAAAgSAAAQJAAAIEgAAECQAACAIAEAAEECAACCBAAABAkAAAgSAAAQJAAAIEgAAECQAACAIAEAAEECAACCBAAABAkAAAgSAAAQJAAAIEgAAECQAACAIAEAAEECAACCBAAABAkAAAgSAAAQJAAAIEgAAECQAACAIAEAAEECAACCBAAABAkAAAgSAAAQJAAAIEgAAECQAACAIAEAAEECAACCBAAABAkAAAgSAAAQJAAAIEgAAECQAACAIAEAAEECAACCBAAABAkAAAgSAAAQJAAAIEgAAECQAOBuL2+vWx8Bcswd9xIAABAkAAAgSABEfL5/LPrzPUfCepaet6XvC8YgAAAgSAAwG68AsDxzxlwEALNyOcFyzBdzEgAha/27nksK5rfWXPn3/w4BwCJEAMzHPLGE560PwH4dLy1fFHAbi58lPR2+psPWh2BdW14qYgAuM5+sxQsAq/JFAzAGvwMQpPKB79wLPQIAAIIEQJTaB47cB00CAACCBECY6gfcA10CIM7wQ5f5bxMAABAkAPAVAEHmHgHANE0uAygx70yTAOCESwH2z5xzJAD4g8sB9st8c0oA8BeXBOyPueY7AcCPXBawH+aZnwgAznJpwOMzx5wjALjI5QGPy/xyydPhazpsfQgew8vb69ZHAK5g8XMNLwBczaUC4zOnXMsLADfxGgBjsfj5LQHAXYQAbMvi51YCgFkIAViXxc+9BACzEwOwDEufOQkAFicI4DYWPksSAAAQ5D8DBIAgAQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQf8CTAZBuv+vmHQAAAAASUVORK5CYII=";

const MANIFEST_JSON = "{\n  \"name\": \"Portes\",\n  \"short_name\": \"Portes\",\n  \"description\": \"Zéro sonnerie. Si c'est ouvert, tu rentres.\",\n  \"start_url\": \"/\",\n  \"scope\": \"/\",\n  \"display\": \"standalone\",\n  \"background_color\": \"#fffc00\",\n  \"theme_color\": \"#fffc00\",\n  \"orientation\": \"portrait\",\n  \"icons\": [\n    {\n      \"src\": \"/icons/icon-192.png\",\n      \"sizes\": \"192x192\",\n      \"type\": \"image/png\",\n      \"purpose\": \"any maskable\"\n    },\n    {\n      \"src\": \"/icons/icon-512.png\",\n      \"sizes\": \"512x512\",\n      \"type\": \"image/png\",\n      \"purpose\": \"any maskable\"\n    }\n  ]\n}";

const SERVICE_WORKER_JS = "const CACHE_NAME = 'portes-shell-v1';\nconst SHELL_URLS = ['/', '/manifest.json'];\n\n// Ce service worker met seulement en cache la \"coquille\" de l'appli\n// (HTML/CSS/JS/manifest/icônes) pour un démarrage instantané hors-ligne.\n// Il ne touche jamais à Socket.io ni aux appels WebRTC : la présence des\n// amis et l'audio ont besoin d'une vraie connexion réseau en direct.\n\nself.addEventListener('install', (event) => {\n  event.waitUntil(\n    caches.open(CACHE_NAME).then((cache) => cache.addAll(SHELL_URLS))\n  );\n  self.skipWaiting();\n});\n\nself.addEventListener('activate', (event) => {\n  event.waitUntil(\n    caches.keys().then((keys) =>\n      Promise.all(keys.filter((k) => k !== CACHE_NAME).map((k) => caches.delete(k)))\n    )\n  );\n  self.clients.claim();\n});\n\nself.addEventListener('fetch', (event) => {\n  const url = new URL(event.request.url);\n\n  // Jamais de cache pour Socket.io (polling/handshake) : il faut du direct.\n  if (url.pathname.startsWith('/socket.io/')) return;\n  if (event.request.method !== 'GET') return;\n\n  event.respondWith(\n    caches.match(event.request).then((cached) => {\n      const network = fetch(event.request)\n        .then((response) => {\n          if (response.ok) {\n            const clone = response.clone();\n            caches.open(CACHE_NAME).then((cache) => cache.put(event.request, clone));\n          }\n          return response;\n        })\n        .catch(() => cached);\n      return cached || network;\n    })\n  );\n});\n";

app.get('/manifest.json', (req, res) => {
  res.type('application/manifest+json').send(MANIFEST_JSON);
});

app.get('/sw.js', (req, res) => {
  // Un service worker doit être servi depuis la racine (scope "/") pour
  // pouvoir contrôler toute l'appli.
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

// ---------------------------------------------------------------------------
// État en mémoire
// ---------------------------------------------------------------------------

/** socketId -> { id, pseudo, avatarInitials, avatarColor, phone, doorOpen, roomId } */
const users = new Map();

/** roomId -> { hostId, memberIds: Set<socketId> } */
const rooms = new Map();

/** Version "publique" d'un user (ce qu'on envoie aux autres clients) */
function publicUser(u) {
  const room = u.roomId ? rooms.get(u.roomId) : null;
  return {
    id: u.id,
    pseudo: u.pseudo,
    avatarInitials: u.avatarInitials,
    avatarColor: u.avatarColor,
    // NB: en prod, réfléchir à qui a le droit de voir le numéro en clair
    // (ici on l'envoie tel quel pour préparer un vrai carnet de contacts).
    phone: u.phone || null,
    doorOpen: u.doorOpen,
    companions: room ? Math.max(0, room.memberIds.size - 1) : 0,
  };
}

function broadcastFriends() {
  io.emit('friends:update', Array.from(users.values()).map(publicUser));
}

// ---------------------------------------------------------------------------
// Connexion Socket.io
// ---------------------------------------------------------------------------

io.on('connection', (socket) => {

  // 1) Création / identification du compte pour cette session.
  //    TODO prod: remplacer par une vraie auth (compte persistant, login,
  //    vérification du numéro par SMS/OTP...). Ici le "compte" ne vit que
  //    le temps de la connexion socket.
  socket.on('register', ({ pseudo, avatarInitials, avatarColor, phone }) => {
    const user = {
      id: socket.id,
      pseudo: String(pseudo || 'Anonyme').slice(0, 24),
      avatarInitials: String(avatarInitials || pseudo || '??').slice(0, 2).toUpperCase(),
      avatarColor: avatarColor || '#ff8a00',
      phone: phone ? String(phone).slice(0, 32) : null,
      doorOpen: false,
      roomId: null,
    };
    users.set(socket.id, user);
    socket.emit('registered', publicUser(user));
    broadcastFriends();
  });

  // 2) L'utilisateur ouvre sa porte -> il devient l'hôte d'une nouvelle room.
  socket.on('door:open', () => {
    const user = users.get(socket.id);
    if (!user || user.doorOpen) return;

    const roomId = randomUUID();
    user.doorOpen = true;
    user.roomId = roomId;
    rooms.set(roomId, { hostId: socket.id, memberIds: new Set([socket.id]) });
    socket.join(roomId);

    broadcastFriends();
  });

  // 3) L'utilisateur ferme sa porte -> tout le monde dans sa room est éjecté.
  socket.on('door:close', () => {
    closeDoorAndRoom(socket.id);
    broadcastFriends();
  });

  // 4) Un ami clique sur "Rejoindre" -> il rejoint la room de l'hôte.
  socket.on('call:join', ({ hostId }) => {
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

    // Les membres déjà présents attendent une offre WebRTC du nouvel arrivant.
    socket.to(host.roomId).emit('call:peer-joined', publicUser(me));

    // Le nouvel arrivant reçoit la liste des membres déjà présents pour
    // initier une RTCPeerConnection vers chacun d'eux (topologie "mesh").
    const existingMembers = Array.from(room.memberIds)
      .filter((id) => id !== socket.id)
      .map((id) => publicUser(users.get(id)));

    socket.emit('call:room-state', { roomId: host.roomId, members: existingMembers });

    broadcastFriends();
  });

  // 5) Un membre (hôte ou invité) quitte l'appel.
  socket.on('call:leave', () => {
    leaveCurrentRoom(socket.id);
    broadcastFriends();
  });

  // ---- Relais pur de signalisation WebRTC (le serveur ne comprend pas le contenu) ----
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

// ---------------------------------------------------------------------------
// Aides internes
// ---------------------------------------------------------------------------

/** Ferme la porte d'un hôte : dissout sa room et éjecte tout le monde. */
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
  user.roomId = null;
}

/** Fait quitter un membre (invité ou hôte) de la room où il se trouve. */
function leaveCurrentRoom(socketId) {
  const user = users.get(socketId);
  if (!user || !user.roomId) return;

  const roomId = user.roomId;
  const room = rooms.get(roomId);

  if (room && room.hostId === socketId) {
    // Quitter en tant qu'hôte = fermer sa porte.
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

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Porte Ouverte — serveur tout-en-un sur http://localhost:${PORT}`);
});
