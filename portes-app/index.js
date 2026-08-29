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
    premium: !!u.premium,
    avatarInitials: u.avatarInitials,
    avatarColor: u.avatarColor,
    avatarPhoto: u.avatarPhoto || '',
    phone: u.phone || null,
    doorOpen: u.doorOpen,
    doorMessage: u.doorMessage || '',
    companions: room ? Math.max(0, room.memberIds.size - 1) : 0,
  };
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

// ---------------------------------------------------------------------------
// Offre Premium
//
// ⚠️ Pour l'instant le compte dit lui-même s'il est Premium : c'est une
// maquette. Le jour où il y aura un vrai paiement, ce sera au serveur de
// vérifier auprès du prestataire (Stripe & co) — jamais au navigateur, qu'on
// peut trafiquer en deux clics.
// ---------------------------------------------------------------------------

// Les salons de groupe sont ouverts à tout le monde. La limite qui reste est
// purement technique : chaque personne envoie son son à TOUTES les autres
// (connexions directes, sans serveur au milieu). À 12, cela fait déjà 11
// connexions par téléphone, et ça devient lourd pour les appareils modestes.
const ROOM_MAX = 12;

function roomLimitFor() {
  return ROOM_MAX;
}

// Fonds d'écran de salon : réservés à l'hôte abonné. Le serveur ne stocke
// qu'un numéro, chaque appareil dessine le fond correspondant de son côté.
const WALLPAPER_COUNT = 10;
function cleanWallpaper(value) {
  const n = parseInt(value, 10);
  return (!isNaN(n) && n >= 0 && n < WALLPAPER_COUNT) ? n : 0;
}

// La photo de profil est déjà réduite à 128 px par le navigateur avant l'envoi.
// On vérifie quand même : uniquement une image encodée, et pas plus de 40 Ko,
// sinon un seul compte pourrait saturer la liste de tous ses contacts.
const PHOTO_MAX = 40000;
const WALLPAPER_PHOTO_MAX = 90000; // fond de salon : plus grand, mais borné
const STICKER_MAX = 30000;         // sticker perso
function cleanPhoto(value) {
  return cleanImage(value, PHOTO_MAX);
}

// Même contrôle pour toutes les images envoyées par les navigateurs : un vrai
// format d'image, et pas plus lourd que la limite prévue pour cet usage.
function cleanImage(value, maxLength) {
  const raw = String(value || '');
  if (!raw) return '';
  if (raw.length > maxLength) return '';
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
      .map((u) => {
        const card = publicUser(u);
        // Mode discret : seuls les amis proches voient que la porte est
        // ouverte. Pour les autres, la porte a simplement l'air fermée.
        const visible = !u.discreet || (viewer.phoneKey && u.vip.has(viewer.phoneKey));
        if (!visible) {
          card.doorOpen = false;
          card.doorMessage = '';
          card.companions = 0;
        }
        return card;
      });
    io.to(socketId).emit('friends:update', list);
  }
}

// ---------------------------------------------------------------------------
// Connexion Socket.io
// ---------------------------------------------------------------------------

io.on('connection', (socket) => {

  socket.on('register', ({ pseudo, username, avatarInitials, avatarColor, avatarPhoto, phone, contacts, blocked, premium, vipOnly, vip, discreet }) => {
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
      avatarPhoto: cleanPhoto(avatarPhoto),
      phone: phone ? String(phone).slice(0, 32) : null,
      phoneKey: phone ? normalizePhone(phone) : null,
      doorOpen: false,
      doorMessage: '',
      roomId: null,
      premium: !!premium,
      vipOnly: !!vipOnly,
      discreet: !!premium && !!discreet, // le mode discret fait partie de l'abonnement
      contacts: new Set(),
      blocked: new Set(),
      vip: new Set(),
    };

    if (Array.isArray(vip)) {
      vip.slice(0, 300).forEach((p) => {
        const key = normalizePhone(p);
        if (key) user.vip.add(key);
      });
    }

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
  socket.on('door:open', ({ message, wallpaper } = {}) => {
    const user = users.get(socket.id);
    if (!user || user.doorOpen) return;

    const roomId = randomUUID();
    user.doorOpen = true;
    user.doorMessage = message ? String(message).slice(0, user.premium ? 140 : 60) : '';
    // Le fond n'est retenu que si l'hôte est abonné : sinon on reste sur 0.
    user.wallpaper = user.premium ? cleanWallpaper(wallpaper) : 0;
    user.roomId = roomId;
    rooms.set(roomId, { hostId: socket.id, memberIds: new Set([socket.id]) });
    socket.join(roomId);

    broadcastFriends();
  });

  // Changer le fond du salon en cours de route. Seul l'hôte abonné peut le
  // faire, et le changement est envoyé à toute la pièce.
  socket.on('door:wallpaper', ({ wallpaper, photo }) => {
    const user = users.get(socket.id);
    if (!user || !user.roomId) return;

    const room = rooms.get(user.roomId);
    if (!room || room.hostId !== socket.id) {
      socket.emit('call:error', { message: "Seul l'hôte peut changer le fond." });
      return;
    }
    if (!user.premium) {
      socket.emit('call:error', { message: 'Les fonds de salon font partie de LiveDoors Plus.' });
      return;
    }

    // Une image perso l'emporte sur le fond prédéfini ; on peut revenir aux
    // dégradés en renvoyant un fond sans photo.
    user.wallpaperPhoto = photo ? cleanImage(photo, WALLPAPER_PHOTO_MAX) : '';
    user.wallpaper = user.wallpaperPhoto ? 0 : cleanWallpaper(wallpaper);

    io.to(user.roomId).emit('call:wallpaper', {
      wallpaper: user.wallpaper,
      photo: user.wallpaperPhoto,
    });
  });

  // État micro / caméra / partage d'écran, partagé avec le salon pour que
  // chacun voie qui parle, qui s'est coupé le micro, qui a la caméra.
  socket.on('call:state', ({ muted, cam, screen }) => {
    const user = users.get(socket.id);
    if (!user || !user.roomId) return;
    user.callState = { muted: !!muted, cam: !!cam, screen: !!screen };
    io.to(user.roomId).emit('call:state', {
      id: socket.id,
      pseudo: user.pseudo,
      ...user.callState,
    });
  });

  // Effets de fête (confettis, cœurs...) : réservés aux abonnés, visibles de
  // tout le salon. On ne transmet qu'un mot-clé, chaque appareil l'anime.
  socket.on('call:effect', ({ effect }) => {
    const user = users.get(socket.id);
    if (!user || !user.roomId) return;
    if (!user.premium) {
      socket.emit('call:error', { message: 'Les effets font partie de LiveDoors Plus.' });
      return;
    }
    const allowed = ['confetti', 'hearts', 'fireworks', 'rain'];
    const key = allowed.indexOf(String(effect)) !== -1 ? String(effect) : 'confetti';
    io.to(user.roomId).emit('call:effect', { effect: key, from: user.pseudo });
  });

  // Changer le statut sans rouvrir/refermer la porte.
  socket.on('door:message', ({ message }) => {
    const user = users.get(socket.id);
    if (!user) return;
    user.doorMessage = message ? String(message).slice(0, user.premium ? 140 : 60) : '';
    broadcastFriends();
  });

  // Fermer sa porte. L'hôte peut soit tout arrêter, soit passer la main à
  // quelqu'un qui est déjà dans l'appel : la conversation continue sans lui.
  socket.on('door:close', ({ transferTo } = {}) => {
    const user = users.get(socket.id);
    if (!user) return;

    const room = user.roomId ? rooms.get(user.roomId) : null;
    const heir = transferTo ? users.get(transferTo) : null;
    const canTransfer = room
      && room.hostId === socket.id
      && heir
      && heir.id !== socket.id
      && room.memberIds.has(heir.id);

    if (canTransfer) {
      const roomId = user.roomId;

      room.hostId = heir.id;
      heir.doorOpen = true;
      heir.doorMessage = user.doorMessage || '';
      heir.wallpaper = heir.premium ? (user.wallpaper || 0) : 0;
      heir.wallpaperPhoto = heir.premium ? (user.wallpaperPhoto || '') : '';
      heir.roomId = roomId;

      // L'ancien hôte sort de la pièce, les autres restent entre eux.
      room.memberIds.delete(socket.id);
      socket.leave(roomId);
      user.doorOpen = false;
      user.roomId = null; // le petit mot, lui, reste affiché

      io.to(roomId).emit('call:host-changed', { hostId: heir.id });
      io.to(roomId).emit('call:wallpaper', {
        wallpaper: heir.wallpaper,
        photo: heir.wallpaperPhoto,
      });
      io.to(roomId).emit('call:peer-left', { id: socket.id });

      socket.emit('door:transferred', { pseudo: heir.pseudo });
      broadcastFriends();
      return;
    }

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
    // Porte privée (Premium) : réservée aux amis proches de l'hôte.
    if (host.vipOnly && !(me.phoneKey && host.vip.has(me.phoneKey))) {
      socket.emit('call:error', { message: 'Cette porte est en mode privé.' });
      return;
    }
    // Salon complet : 2 personnes en gratuit, 8 avec l'abonnement de l'hôte.
    const room = host.roomId ? rooms.get(host.roomId) : null;
    if (room && room.memberIds.size >= roomLimitFor()) {
      socket.emit('call:error', { message: 'Ce salon est complet (' + ROOM_MAX + ' personnes).' });
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

    // On revérifie ici : entre la demande et l'entrée, quelqu'un d'autre a pu
    // prendre la dernière place.
    if (room.memberIds.size >= roomLimitFor()) {
      socket.emit('call:error', { message: 'Ce salon est complet.' });
      return;
    }

    room.memberIds.add(socket.id);
    me.roomId = host.roomId;
    socket.join(host.roomId);

    socket.to(host.roomId).emit('call:peer-joined', publicUser(me));

    const existingMembers = Array.from(room.memberIds)
      .filter((id) => id !== socket.id)
      .map((id) => {
        const u = users.get(id);
        return { ...publicUser(u), callState: u.callState || { muted: false, cam: false, screen: false } };
      });

    socket.emit('call:room-state', {
      roomId: host.roomId,
      members: existingMembers,
      wallpaper: host.wallpaper || 0,
      wallpaperPhoto: host.wallpaperPhoto || '',
    });

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
  socket.on('chat:message', ({ text, sticker }) => {
    const user = users.get(socket.id);
    if (!user || !user.roomId) return;

    // Sticker perso : image, réservée aux abonnés et bornée en taille.
    const image = user.premium ? cleanImage(sticker, STICKER_MAX) : '';
    const clean = String(text || '').trim().slice(0, 200);
    if (!clean && !image) return;

    io.to(user.roomId).emit('chat:message', {
      fromId: user.id,
      pseudo: user.pseudo,
      avatarInitials: user.avatarInitials,
      avatarColor: user.avatarColor,
      premium: !!user.premium,
      text: clean,
      sticker: image,
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
          newHost.wallpaper = newHost.premium ? (user.wallpaper || 0) : 0;
          newHost.wallpaperPhoto = newHost.premium ? (user.wallpaperPhoto || '') : '';
          newHost.roomId = roomId;
        }
        io.to(roomId).emit('call:host-changed', { hostId: newHostId });
        io.to(roomId).emit('call:wallpaper', {
          wallpaper: newHost && newHost.premium ? (newHost.wallpaper || 0) : 0,
          photo: newHost && newHost.premium ? (newHost.wallpaperPhoto || '') : '',
        });
      }
      io.to(roomId).emit('call:peer-left', { id: socketId });
    } else {
      room.memberIds.forEach((memberId) => {
        io.to(memberId).emit('call:ended', { reason: 'host-closed' });
        const member = users.get(memberId);
        if (member) { member.roomId = null; member.doorOpen = false; }
        io.sockets.sockets.get(memberId)?.leave(roomId);
      });
      rooms.delete(roomId);
    }
  }

  user.doorOpen = false;
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

  // On ferme la porte mais on GARDE le petit mot : il doit rester visible
  // pour les contacts tant que la personne est connectée.
  user.doorOpen = false;
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
/* overflow-y:auto indispensable : le formulaire a beaucoup grandi (nom
   d'utilisateur, photo, créateur de personnage, code secret) et dépasse la
   hauteur de l'écran. Sans ça, le bas devenait tout simplement inaccessible.
   justify-content:center seulement quand ça tient, sinon on part du haut. */
.login-screen{
  align-items:center; padding:32px 32px 48px;
  background:var(--yellow); overflow-y:auto; -webkit-overflow-scrolling:touch;
}
/* margin:auto centre verticalement quand ça tient, et laisse défiler sinon. */
.login-inner{ width:100%; margin:auto 0; flex-shrink:0; }
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

.contact-actions{ display:flex; gap:5px; margin-top:6px; }
.fav-star{ color:#e0a800; margin-right:4px; vertical-align:-1px; }
.fav-star svg{ fill:currentColor; }
.contact-btn{
  width:28px; height:28px; border-radius:9px; cursor:pointer; padding:0;
  border:1px solid var(--border); background:transparent; color:var(--ink-faint);
  display:flex; align-items:center; justify-content:center;
}
.contact-btn svg{ fill:none; }
.contact-btn:hover{ background:var(--bg-soft); color:var(--ink); }
.contact-btn.on{ color:#e0a800; border-color:#e0a800; }
.contact-btn.on svg{ fill:currentColor; }
.contact-btn.close-on{ color:#e6398b; border-color:#e6398b; }
.contact-btn.close-on svg{ fill:currentColor; }
.contact-btn.danger{ color:#c0143c; border-color:#c0143c; }
.bell-grid{ display:grid; grid-template-columns:repeat(2, 1fr); gap:5px; }

/* Zone photo de profil */
.photo-row{ display:flex; gap:6px; margin-bottom:8px; }
.photo-btn{
  flex:1; border:none; cursor:pointer; border-radius:10px; padding:9px;
  background:#14171a; color:var(--yellow);
  font-family:'Baloo 2', sans-serif; font-weight:700; font-size:12px;
}
.photo-btn{ display:flex; align-items:center; justify-content:center; gap:7px; }
.photo-btn .btn-ic{ color:inherit; }
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
/* Porte fermée mais personne connectée : l'avatar reste net et en couleur.
   Le grisé est réservé aux contacts vraiment hors ligne. */
.friend-row.is-closed .avatar{ filter:none; opacity:1; }
.friend-row.is-offline .avatar{ filter:grayscale(1) brightness(0.92); opacity:0.55; }

.friend-info{ flex:1; min-width:0; }
.friend-name{ font-family:'Baloo 2', sans-serif; font-size:14.5px; font-weight:700; color:var(--ink); }
.friend-row.is-closed .friend-name{ color:var(--ink-soft); }
.friend-phone{ font-family:'JetBrains Mono', monospace; font-size:10.5px; color:var(--ink-faint); margin-top:1px; }
.friend-meta{ font-size:11px; color:var(--ink-faint); margin-top:2px; font-weight:700; }
.friend-meta.live-meta{ color:#e08a00; }
.friend-status-msg{ font-size:11px; color:var(--ink-soft); margin-top:2px; font-style:italic; }

/* Petit mot des abonnés : texte en dégradé animé, comme le badge PLUS. */
.friend-status-msg.is-premium{
  font-style:normal; font-weight:800;
  background:linear-gradient(90deg,#ff3d77,#ff8a00,#ffc400,#26de81,#45aaf2,#a55eea,#ff3d77);
  background-size:400% 100%;
  -webkit-background-clip:text; background-clip:text; color:transparent;
  animation:rgbSlide 5s linear infinite;
}
/* Mon propre champ : bordure colorée quand l'abonnement est actif */
.status-input.is-premium{
  border:1.5px solid transparent;
  background:
    linear-gradient(var(--bg), var(--bg)) padding-box,
    linear-gradient(90deg,#ff3d77,#ff8a00,#ffc400,#26de81,#45aaf2,#a55eea,#ff3d77) border-box;
  background-size:auto, 400% 100%;
  animation:rgbSlide 5s linear infinite;
  font-weight:700;
}

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
  display:none; align-items:center; justify-content:center;
  padding:18px; overflow-y:auto; -webkit-overflow-scrolling:touch;
}
.modal-backdrop.show{ display:flex; }
/* Sans max-height + overflow, une fenêtre plus haute que l'écran (paramètres,
   profil) débordait sans qu'on puisse atteindre les boutons du bas. */
.modal-card{
  background:var(--bg); border-radius:20px; padding:20px; width:100%;
  font-family:'Nunito', sans-serif;
  max-height:calc(100% - 20px); overflow-y:auto; -webkit-overflow-scrolling:touch;
  margin:auto 0; flex-shrink:0;
}
.modal-card.tall{ max-height:calc(100% - 20px); }
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
  cursor:pointer;
}
.video-tile video{ width:100%; height:100%; object-fit:cover; }
.video-tile .video-tile-label{
  position:absolute; bottom:4px; left:6px; font-size:10px; color:#fff;
  font-family:'Baloo 2', sans-serif; font-weight:700; text-shadow:0 1px 3px rgba(0,0,0,0.6);
}
.video-zoom{
  position:absolute; top:5px; right:5px; width:28px; height:28px; border-radius:9px;
  background:rgba(0,0,0,0.45); border:1px solid rgba(255,255,255,0.2); color:#fff;
  cursor:pointer; display:flex; align-items:center; justify-content:center; padding:0;
}
.video-zoom:hover{ background:rgba(0,0,0,0.7); }

/* Plein écran : la vignette sort de la grille et couvre tout l'appel.
   La grille doit AUSSI monter d'un cran, sinon les boutons de la barre
   d'appel se dessinent par-dessus la vidéo. */
.video-grid.has-full{ max-height:none; z-index:30; }
.video-tile.is-full{
  position:fixed; inset:0; z-index:30; border-radius:0; aspect-ratio:auto;
  background:#000;
}
.video-tile.is-full video{ object-fit:contain; }
.video-tile.is-full .video-tile-label{ bottom:16px; left:16px; font-size:13px; }
.video-tile.is-full .video-zoom{
  top:auto; bottom:14px; right:14px; width:44px; height:44px; border-radius:50%;
}

/* Barre d'appel : une grille régulière de 4 colonnes. Largeur fixe et
   libellés sur une seule ligne — c'est ce qui manquait : "Qui est là"
   passait sur trois lignes et décalait tout le rang. */
.call-controls{
  display:grid; grid-template-columns:repeat(4, 68px); justify-content:center;
  gap:14px 6px; z-index:2; margin-top:10px;
}
.call-btn{
  width:68px; padding:0; border:none; background:transparent; cursor:pointer;
  display:flex; flex-direction:column; align-items:center; gap:6px;
  position:relative; color:#fff;
}
.call-ic{
  width:50px; height:50px; border-radius:50%;
  background:rgba(255,255,255,0.13); border:1px solid rgba(255,255,255,0.14);
  display:flex; align-items:center; justify-content:center; color:#fff;
  transition:background .15s, transform .1s, color .15s;
}
.call-lb{
  font-family:'Nunito', sans-serif; font-weight:700; font-size:10px;
  color:rgba(255,255,255,0.72); white-space:nowrap; line-height:1;
}
.call-btn:hover .call-ic{ background:rgba(255,255,255,0.22); }
.call-btn:active .call-ic{ transform:scale(0.92); }

/* Chaque fonction a sa couleur quand elle est active : d'un coup d'œil on
   sait ce qui tourne, au lieu de trois ronds jaunes identiques. */
.call-btn.is-on .call-ic, .call-btn.is-muted .call-ic{
  border-color:transparent; color:#fff;
}
#muteBtn.is-muted .call-ic{ background:#e63946; }              /* micro coupé : rouge */
#camBtn.is-on .call-ic{ background:#2d9cdb; }                  /* caméra : bleu */
#screenBtn.is-on .call-ic{ background:#9b51e0; }               /* écran : violet */
#chatBtn.is-on .call-ic{ background:var(--yellow); color:#14171a; }
#wallBtn.is-on .call-ic{ background:#f2994a; }                 /* fond : orange */
#fxBtn.is-on .call-ic{ background:#eb5fa8; }                   /* effets : rose */

#muteBtn.is-muted .call-lb{ color:#ff8b95; }
#camBtn.is-on .call-lb{ color:#7fc9f0; }
#screenBtn.is-on .call-lb{ color:#c79cf2; }
#chatBtn.is-on .call-lb{ color:var(--yellow); }
#wallBtn.is-on .call-lb{ color:#f7bd8a; }
#fxBtn.is-on .call-lb{ color:#f5a3cd; }

/* Raccrocher : rond rouge plein */
.call-btn.hangup .call-ic{ background:#e63946; border-color:transparent; }
.call-btn.hangup:hover .call-ic{ background:#c1121f; }

.chat-badge{
  display:none; position:absolute; top:-3px; right:6px; min-width:18px; height:18px;
  border-radius:9px; background:#ff3d77; color:#fff; font-size:10px; line-height:18px;
  padding:0 5px; font-family:'Baloo 2', sans-serif; font-weight:700; text-align:center;
}
.chat-badge.show{ display:block; }

/* ---------- Incoming request card ---------- */
#incomingRequest{
  display:none; z-index:3; width:100%; max-width:320px; margin-top:14px;
  background:rgba(255,255,255,0.10); border:1px solid rgba(255,255,255,0.14);
  border-radius:20px; padding:14px;
  backdrop-filter:blur(10px);
  animation:reqIn 0.3s cubic-bezier(.2,1.3,.4,1);
}
@keyframes reqIn{
  0%{ transform:translateY(14px) scale(0.96); opacity:0; }
  100%{ transform:translateY(0) scale(1); opacity:1; }
}
.req-head{ display:flex; align-items:center; gap:11px; text-align:left; }
.req-avatar{
  width:44px; height:44px; border-radius:50%; overflow:hidden; flex:none;
  background:rgba(255,255,255,0.12);
  display:flex; align-items:center; justify-content:center;
  font-family:'Baloo 2', sans-serif; font-weight:700; color:#fff; font-size:15px;
}
.req-avatar svg, .req-avatar img{ width:100%; height:100%; object-fit:cover; display:block; }
.req-texts{ min-width:0; }
.req-name{
  font-family:'Baloo 2', sans-serif; font-weight:700; color:#fff; font-size:15px;
  overflow:hidden; text-overflow:ellipsis; white-space:nowrap;
}
.req-sub{ font-size:11.5px; color:rgba(255,255,255,0.6); font-weight:600; }
.incoming-msg{
  font-size:12.5px; color:rgba(255,255,255,0.85); font-style:italic;
  background:rgba(255,255,255,0.07); border-radius:12px; padding:8px 11px; margin-top:10px;
  text-align:left;
}
.req-actions{ display:flex; gap:9px; margin-top:12px; }
.req-btn{
  flex:1; cursor:pointer; border:none; border-radius:13px; padding:12px 0;
  font-family:'Baloo 2', sans-serif; font-weight:700; font-size:13.5px;
}
.req-btn:active{ transform:scale(0.97); }
.req-no{ background:rgba(255,255,255,0.12); color:#fff; border:1px solid rgba(255,255,255,0.18); }
.req-no:hover{ background:rgba(230,57,70,0.35); border-color:rgba(230,57,70,0.6); }
.req-yes{ background:#26de81; color:#0c2c1c; }
.req-yes:hover{ background:#20c974; }

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
.lock-screen{
  align-items:center; padding:32px 32px 48px;
  background:var(--yellow); overflow-y:auto; -webkit-overflow-scrolling:touch;
}

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
  display:flex; align-items:center; justify-content:center;
  font-family:'Baloo 2', sans-serif; font-weight:700; font-size:26px; color:#fff;
}
.avatar-preview-hint{ font-size:11.5px; font-weight:700; color:rgba(0,0,0,0.55); line-height:1.35; }

/* Les avatars sont des SVG : ils doivent remplir leur pastille ronde. */
.avatar svg, .me-avatar svg, .header-avatar svg, .call-avatar svg,
.lock-avatar svg, .avatar-preview svg{
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
.settings-action{ display:flex; align-items:center; gap:9px; }
.settings-action:hover{ background:var(--bg-soft); }
.btn-ic{ display:flex; flex:none; color:var(--ink-soft); }
.settings-action.danger .btn-ic{ color:#c0143c; }
.settings-action.on .btn-ic{ color:inherit; }
.seg-ic{ display:flex; }
.segment{ display:flex; align-items:center; justify-content:center; gap:5px; }
.chip-ic{ display:inline-flex; vertical-align:-2px; margin-right:4px; color:var(--ink-soft); }
.premium-chip{ display:inline-flex; align-items:center; }
.settings-action.danger{ color:#c0143c; }
/* Chaque réglage Premium actif a sa propre couleur */
.settings-action.on{ border-color:var(--grad-2); }
#premiumToggle.on{ border-color:#26de81; color:#1c9c5d; }
#vipToggle.on{ border-color:#9b51e0; color:#7b3bc4; }
#discreetToggle.on{ border-color:#2d9cdb; color:#1f7fb4; }
[data-theme="dark"] #premiumToggle.on{ color:#5be0a0; }
[data-theme="dark"] #vipToggle.on{ color:#c79cf2; }
[data-theme="dark"] #discreetToggle.on{ color:#7fc9f0; }

.heir-btn{
  width:100%; margin-bottom:6px; cursor:pointer; text-align:left;
  display:flex; align-items:center; gap:10px;
  border:1px solid var(--border); background:transparent; color:var(--ink);
  border-radius:12px; padding:9px 11px;
  font-family:'Baloo 2', sans-serif; font-weight:700; font-size:13px;
}
.heir-btn:hover{ background:var(--bg-soft); }
.heir-btn .person-avatar{ width:32px; height:32px; font-size:12px; }

/* ---------- Premium ---------- */
.premium-box{
  border:1px solid var(--border); border-radius:14px; padding:12px;
  background:linear-gradient(160deg, rgba(255,252,0,0.14), rgba(255,61,119,0.10));
}
.premium-list{ display:flex; flex-wrap:wrap; gap:5px; margin-bottom:10px; }
.premium-chip{
  font-size:11px; font-weight:700; color:var(--ink);
  background:var(--bg-soft); border:1px solid var(--border);
  border-radius:999px; padding:5px 9px;
}

/* Badge animé : le dégradé glisse en boucle sous le texte découpé. */
.premium-badge{
  display:inline-block; margin-left:5px; vertical-align:middle;
  font-family:'Baloo 2', sans-serif; font-weight:700; font-size:9.5px;
  letter-spacing:0.06em; padding:2px 6px; border-radius:999px;
  color:#fff; background:linear-gradient(90deg,#ff3d77,#ff8a00,#ffe600,#26de81,#45aaf2,#a55eea,#ff3d77);
  background-size:400% 100%; animation:rgbSlide 4s linear infinite;
  box-shadow:0 2px 8px -3px rgba(0,0,0,0.4);
}
.premium-badge.big{ font-size:11px; padding:3px 9px; margin-left:6px; }
@keyframes rgbSlide{
  0%{ background-position:0% 50%; }
  100%{ background-position:400% 50%; }
}

.history-row{
  display:flex; align-items:center; gap:10px; padding:9px 0;
  border-bottom:1px solid var(--border);
}
.history-row:last-child{ border-bottom:none; }
.history-name{ font-family:'Baloo 2', sans-serif; font-weight:700; font-size:13px; color:var(--ink); }
.history-when{ font-size:11px; color:var(--ink-faint); }
.history-main{ min-width:0; flex:1; }
.history-tag{ margin-left:auto; font-size:11px; font-weight:700; flex-shrink:0; }
.history-tag.ok{ color:#00a884; }
.history-tag.no{ color:#c0143c; }
.history-add{
  margin-left:auto; flex-shrink:0; cursor:pointer; border:none; border-radius:10px;
  padding:7px 11px; background:var(--yellow); color:#14171a;
  font-family:'Baloo 2', sans-serif; font-weight:700; font-size:11.5px;
}

/* ---------- Fenêtre "Modifier mon profil" ---------- */
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
  background:transparent; border:none; color:rgba(255,255,255,0.7); cursor:pointer;
  padding:2px 6px; display:flex; align-items:center;
}
.chat-close:hover{ color:#fff; }
.chat-messages{
  flex:1; overflow-y:auto; display:flex; flex-direction:column; gap:7px; padding-right:2px;
}
.chat-empty{ font-size:12px; color:rgba(255,255,255,0.4); text-align:center; margin:auto 0; font-style:italic; }
.chat-system-line{
  align-self:center; text-align:center; font-size:11px; font-weight:700;
  color:rgba(255,255,255,0.55); background:rgba(255,255,255,0.07);
  border-radius:999px; padding:4px 12px; margin:2px 0;
}
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
  max-height:190px; overflow-y:auto; -webkit-overflow-scrolling:touch;
}
.emoji-panel.show{ display:block; }
.panel-tabs{ display:flex; gap:4px; margin-bottom:6px; position:sticky; top:-6px; }
.panel-tab{
  flex:1; border:none; cursor:pointer; border-radius:9px; padding:7px;
  background:rgba(255,255,255,0.08); color:rgba(255,255,255,0.75);
  font-family:'Baloo 2', sans-serif; font-weight:700; font-size:11.5px;
}
.panel-tab.active{ background:var(--yellow); color:#14171a; }
.emoji-grid-chat{ display:grid; grid-template-columns:repeat(7, 1fr); gap:3px; }
.emoji-grid-chat button{
  aspect-ratio:1; background:transparent; border:none; border-radius:9px;
  font-size:19px; cursor:pointer; line-height:1; padding:0;
}
.emoji-grid-chat button:active{ transform:scale(0.88); background:rgba(255,255,255,0.15); }
.emoji-grid-chat button{ position:relative; }
.emoji-grid-chat button.has-move::after{
  content:''; position:absolute; top:3px; right:3px;
  width:6px; height:6px; border-radius:50%; background:var(--yellow);
  box-shadow:0 0 5px rgba(255,252,0,0.9);
}
.emoji-legend{
  font-size:9.5px; color:rgba(255,255,255,0.5); font-weight:700;
  display:flex; align-items:center; gap:5px; margin-top:6px;
}
.emoji-legend i{
  width:6px; height:6px; border-radius:50%; background:var(--yellow); display:inline-block;
}

.sticker-grid{ display:grid; grid-template-columns:repeat(4, 1fr); gap:6px; }
.sticker-choice{
  aspect-ratio:1; border:none; border-radius:14px; cursor:pointer;
  font-size:26px; line-height:1; padding:0;
}
.sticker-choice:active{ transform:scale(0.9); }
.sticker{
  width:88px; height:88px; border-radius:20px; display:flex;
  align-items:center; justify-content:center; font-size:46px;
  animation:stickerPop 0.45s cubic-bezier(.2,1.6,.4,1);
}
@keyframes stickerPop{
  0%{ transform:scale(0.4) rotate(-12deg); opacity:0; }
  100%{ transform:scale(1) rotate(0); opacity:1; }
}

/* Émojis animés : de vraies petites animations dessinées, chargées à la
   demande. */
.emoji-anim-row{ display:flex; gap:5px; align-items:center; padding:2px 4px; }
.emoji-anim{ width:64px; height:64px; display:block; }
.emoji-anim-fallback{ font-size:46px; line-height:1; display:inline-block; }

/* Repli quand l'animation n'existe pas : le mouvement CSS d'avant, appliqué
   à l'émoji entier. Chacun bouge à sa manière. */
.emoji-anim-fallback.fx-wink{ animation:emWink 2.2s ease-in-out infinite; }
.emoji-anim-fallback.fx-beat{ animation:emBeat 1.1s ease-in-out infinite; }
.emoji-anim-fallback.fx-laugh{ animation:emLaugh 0.7s ease-in-out infinite; }
.emoji-anim-fallback.fx-sleep{ animation:emSleep 3s ease-in-out infinite; }
.emoji-anim-fallback.fx-flame{ animation:emFlame 0.5s ease-in-out infinite; }
.emoji-anim-fallback.fx-spin{ animation:emSpin 2.4s ease-in-out infinite; }
.emoji-anim-fallback.fx-jump{ animation:emJump 1.2s ease-in-out infinite; }
.emoji-anim-fallback.fx-shake{ animation:emShake 0.45s ease-in-out infinite; }
/* Chaque émoji bouge à sa manière : le clin d'œil fait un clin d'œil, le
   cœur bat, le rire secoue, le dormeur respire. Une animation générique
   pour tous n'avait aucun sens. */
.chat-bubble.big-emoji.fx-wink{ animation:emWink 2.2s ease-in-out infinite; }
.chat-bubble.big-emoji.fx-beat{ animation:emBeat 1.1s ease-in-out infinite; }
.chat-bubble.big-emoji.fx-laugh{ animation:emLaugh 0.7s ease-in-out infinite; }
.chat-bubble.big-emoji.fx-sleep{ animation:emSleep 3s ease-in-out infinite; }
.chat-bubble.big-emoji.fx-flame{ animation:emFlame 0.5s ease-in-out infinite; }
.chat-bubble.big-emoji.fx-spin{ animation:emSpin 2.4s ease-in-out infinite; }
.chat-bubble.big-emoji.fx-jump{ animation:emJump 1.2s ease-in-out infinite; }
.chat-bubble.big-emoji.fx-shake{ animation:emShake 0.45s ease-in-out infinite; }
.chat-bubble.big-emoji.fx-pop{ animation:emojiPop 0.55s cubic-bezier(.2,1.5,.4,1) 1; }

/* Le clin d'œil : l'émoji se ferme à moitié une fraction de seconde,
   comme une paupière qui tombe. */
@keyframes emWink{
  0%, 62%, 100%{ transform:scaleY(1) rotate(0); }
  70%{ transform:scaleY(0.55) rotate(-6deg); }
  78%{ transform:scaleY(1) rotate(-3deg); }
}
@keyframes emBeat{
  0%, 100%{ transform:scale(1); }
  14%{ transform:scale(1.28); }
  28%{ transform:scale(1); }
  42%{ transform:scale(1.18); }
}
@keyframes emLaugh{
  0%, 100%{ transform:rotate(-7deg) translateY(0); }
  50%{ transform:rotate(7deg) translateY(-5px); }
}
@keyframes emSleep{
  0%, 100%{ transform:scale(1) rotate(-4deg); opacity:0.85; }
  50%{ transform:scale(1.08) rotate(4deg); opacity:1; }
}
@keyframes emFlame{
  0%, 100%{ transform:scale(1) skewX(0); }
  30%{ transform:scale(1.12) skewX(-6deg); }
  60%{ transform:scale(0.96) skewX(5deg); }
}
@keyframes emSpin{
  0%, 70%, 100%{ transform:rotate(0); }
  85%{ transform:rotate(360deg); }
}
@keyframes emJump{
  0%, 60%, 100%{ transform:translateY(0); }
  30%{ transform:translateY(-12px); }
  45%{ transform:translateY(0) scale(1.06, 0.94); }
}
@keyframes emShake{
  0%, 100%{ transform:translateX(0); }
  25%{ transform:translateX(-4px) rotate(-5deg); }
  75%{ transform:translateX(4px) rotate(5deg); }
}
@keyframes emojiPop{
  0%{ transform:scale(0.5) rotate(-8deg); }
  60%{ transform:scale(1.25) rotate(4deg); }
  100%{ transform:scale(1) rotate(0); }
}
.emoji-toggle{
  flex:none; width:42px; border-radius:12px; border:1px solid rgba(255,255,255,0.18);
  background:rgba(255,255,255,0.08); cursor:pointer; color:#fff;
  display:flex; align-items:center; justify-content:center;
}
.emoji-toggle.is-on{ background:var(--yellow); border-color:transparent; color:#14171a; }
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

/* ---------- Fonds de salon ---------- */
.wall-panel{
  position:absolute; left:0; right:0; bottom:0; z-index:6;
  background:rgba(20,23,26,0.94); border-top:1px solid rgba(255,255,255,0.12);
  border-radius:22px 22px 0 0; padding:12px 14px 18px;
  display:none; flex-direction:column; gap:10px; max-height:60%; overflow-y:auto;
}
.wall-panel.show{ display:flex; }
.wall-grid{ display:grid; grid-template-columns:repeat(3, 1fr); gap:8px; }
.wall-choice{
  aspect-ratio:9/14; border-radius:12px; cursor:pointer; padding:0;
  border:2px solid transparent; position:relative; overflow:hidden;
}
.wall-choice.selected{ border-color:var(--yellow); }
.wall-choice span{
  position:absolute; left:0; right:0; bottom:0; padding:3px;
  background:rgba(0,0,0,0.45); color:#fff; font-size:9.5px; font-weight:700;
  font-family:'Baloo 2', sans-serif;
}

.person-row{
  display:flex; align-items:center; gap:10px; padding:9px 2px;
  border-bottom:1px solid rgba(255,255,255,0.08);
}
.person-row:last-child{ border-bottom:none; }
.person-avatar{
  width:36px; height:36px; border-radius:50%; overflow:hidden; flex:none;
  display:flex; align-items:center; justify-content:center;
  font-family:'Baloo 2', sans-serif; font-weight:700; font-size:13px; color:#fff;
}
.person-name{
  font-family:'Baloo 2', sans-serif; font-weight:700; font-size:13px; color:#fff;
  flex:1; min-width:0; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;
}
.person-icons{ display:flex; gap:8px; flex:none; align-items:center; }
.person-icons span{ display:flex; }
.person-icons .live{ color:#fff; }
.person-icons .off{ color:rgba(255,255,255,0.28); }

.fx-grid{ display:grid; grid-template-columns:repeat(4, 1fr); gap:8px; }
.fx-choice{
  aspect-ratio:1; border:1px solid rgba(255,255,255,0.18); border-radius:14px;
  background:rgba(255,255,255,0.08); cursor:pointer; font-size:24px;
  display:flex; flex-direction:column; align-items:center; justify-content:center; gap:3px;
}
.fx-choice span{
  font-size:9.5px; color:rgba(255,255,255,0.75);
  font-family:'Baloo 2', sans-serif; font-weight:700;
}
.fx-choice:active{ transform:scale(0.92); }
/* Aperçu miniature dans le sélecteur, pour reconnaître l'effet */
.fx-prev{ display:block; width:22px; height:22px; position:relative; }
.fx-prev-confetti{
  background:
    linear-gradient(#ff3d77,#ff3d77) 2px 2px/5px 8px no-repeat,
    linear-gradient(#ffe600,#ffe600) 11px 0/5px 8px no-repeat,
    linear-gradient(#26de81,#26de81) 6px 12px/5px 8px no-repeat,
    linear-gradient(#45aaf2,#45aaf2) 15px 11px/5px 8px no-repeat;
}
.fx-prev-rain{
  background:
    linear-gradient(rgba(160,220,255,0),rgba(160,220,255,0.95)) 4px 0/2px 12px no-repeat,
    linear-gradient(rgba(160,220,255,0),rgba(160,220,255,0.95)) 11px 5px/2px 12px no-repeat,
    linear-gradient(rgba(160,220,255,0),rgba(160,220,255,0.95)) 17px 1px/2px 12px no-repeat;
}

/* Particules des effets de fête */
.fx-bit{ position:absolute; top:-30px; pointer-events:none; }
.fx-confetti{ border-radius:2px; }
.fx-drop{
  width:2px; border-radius:2px;
  background:linear-gradient(to bottom, rgba(160,220,255,0), rgba(160,220,255,0.95));
}
/* Le confetti tombe en tournoyant et en dérivant sur le côté */
@keyframes fxTumble{
  0%{ transform:translate(0,0) rotate(0) scale(1); opacity:1; }
  50%{ transform:translate(22px,340px) rotate(320deg) scale(0.95); opacity:1; }
  100%{ transform:translate(-14px,720px) rotate(700deg) scale(0.9); opacity:0; }
}
/* La pluie tombe droit et vite, avec une très légère inclinaison */
@keyframes fxRain{
  0%{ transform:translate(0,0); opacity:0; }
  10%{ opacity:1; }
  100%{ transform:translate(-26px,760px); opacity:0; }
}
@keyframes fxFall{
  0%{ transform:translateY(0) rotate(0); opacity:1; }
  100%{ transform:translateY(700px) rotate(540deg); opacity:0; }
}
@keyframes fxRise{
  0%{ transform:translateY(0) scale(0.6); opacity:0; }
  20%{ opacity:1; }
  100%{ transform:translateY(-620px) scale(1.1); opacity:0; }
}

/* Stickers persos et fond de tchat */
.sticker-img{ width:88px; height:88px; border-radius:20px; object-fit:cover; animation:stickerPop 0.45s cubic-bezier(.2,1.6,.4,1); }
.sticker-choice.custom{ background-size:cover; background-position:center; }
.sticker-add{
  aspect-ratio:1; border:2px dashed rgba(255,255,255,0.35); border-radius:14px;
  background:transparent; color:rgba(255,255,255,0.6); font-size:22px; cursor:pointer;
}
.chat-panel.has-bg .chat-messages{ border-radius:12px; }

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
        <div class="avatar-preview-hint">Ajoute une photo, ou garde tes initiales.</div>
      </div>
      <div class="photo-row">
        <button class="photo-btn" type="button" id="photoBtn"><span class="btn-ic" id="icPhoto1"></span>Mettre une photo</button>
        <button class="photo-btn secondary" type="button" id="photoClearBtn">Retirer</button>
      </div>
      <input type="file" id="photoInput" accept="image/*" style="display:none">

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
        <div class="header-avatar" id="headerAvatar">--</div>
        <button class="theme-btn" id="settingsBtn" title="Paramètres"></button>
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

    <!-- ---- Modale : l'hôte ferme sa porte alors qu'il y a du monde ---- -->
    <div class="modal-backdrop" id="closeModal">
      <div class="modal-card">
        <div class="modal-title">Tu quittes, et les autres ?</div>
        <div class="field-hint" style="margin-bottom:10px;">Tu peux passer la main à quelqu'un pour que la conversation continue sans toi.</div>

        <label class="field-label">Passer la main à</label>
        <div id="heirList"></div>

        <div class="modal-actions">
          <button class="modal-cancel-btn" id="closeCancel">Annuler</button>
          <button class="toggle-btn" id="closeEveryone">Terminer pour tous</button>
        </div>
      </div>
    </div>

    <!-- ---- Modale : paramètres ---- -->
    <div class="modal-backdrop" id="settingsModal">
      <div class="modal-card">
        <div class="modal-title">Paramètres</div>

        <label class="field-label">Apparence</label>
        <div class="segmented" id="themeChoice">
          <button class="segment" type="button" data-theme-mode="light"><span class="seg-ic" id="icSun"></span>Clair</button>
          <button class="segment" type="button" data-theme-mode="dark"><span class="seg-ic" id="icMoon"></span>Sombre</button>
          <button class="segment" type="button" data-theme-mode="auto"><span class="seg-ic" id="icDevice"></span>Auto</button>
        </div>
        <div class="field-hint">« Auto » suit le réglage de ton téléphone : il passe en sombre le soir si ton téléphone le fait.</div>

        <label class="field-label">Mon compte</label>
        <button class="settings-action" type="button" id="settingsHistory"><span class="btn-ic" id="icHistory"></span>Historique des toc-toc</button>
        <button class="settings-action" type="button" id="settingsLock"><span class="btn-ic" id="icLock"></span>Verrouiller maintenant</button>
        <button class="settings-action danger" type="button" id="settingsForget"><span class="btn-ic" id="icForget"></span>Changer de compte</button>

        <label class="field-label">LiveDoors Plus <span class="premium-badge">PLUS</span></label>
        <div class="premium-box">
          <div class="premium-list">
            <span class="premium-chip" id="chipVideo">Vidéo</span>
            <span class="premium-chip" id="chipScreen">Partage d'écran</span>
            <span class="premium-chip" id="chipPrivate">Porte privée</span>
            <span class="premium-chip" id="chipBells">8 sonneries</span>
            <span class="premium-chip" id="chipStickers">Stickers</span>
            <span class="premium-chip" id="chipEmoji">Émojis animés</span>
            <span class="premium-chip" id="chipStatus">Statut long</span>
          </div>
          <button class="settings-action" type="button" id="premiumToggle">Activer l'essai</button>
          <div class="field-hint">Maquette : aucun paiement n'est branché pour l'instant.</div>
        </div>

        <div id="premiumOptions" style="display:none;">
          <label class="field-label">Sonnerie quand on frappe</label>
          <div class="bell-grid" id="bellChoice">
            <button class="segment" type="button" data-bell="0">Doux</button>
            <button class="segment" type="button" data-bell="1">Carillon</button>
            <button class="segment" type="button" data-bell="2">Arcade</button>
            <button class="segment" type="button" data-bell="3">Toc-toc</button>
            <button class="segment" type="button" data-bell="4">Goutte</button>
            <button class="segment" type="button" data-bell="5">Fanfare</button>
            <button class="segment" type="button" data-bell="6">Rétro</button>
            <button class="segment" type="button" data-bell="7">Aucune</button>
          </div>

          <label class="field-label">Porte privée</label>
          <button class="settings-action" type="button" id="vipToggle">🔓 Ouverte à tous mes contacts</button>
          <div class="field-hint">En mode privé, seuls tes amis proches peuvent frapper.</div>

          <label class="field-label">Mode discret</label>
          <button class="settings-action" type="button" id="discreetToggle">👁️ Tout le monde voit ma porte</button>
          <div class="field-hint">En mode discret, seuls tes amis proches voient que tu es en appel. Pour les autres, ta porte a l'air fermée.</div>

          <label class="field-label">Mes images</label>
          <button class="settings-action" type="button" id="wallPhotoBtn"><span class="btn-ic" id="icWallPhoto"></span>Fond de salon depuis mes photos</button>
          <button class="settings-action" type="button" id="chatBgBtn"><span class="btn-ic" id="icChatBg"></span>Fond du tchat depuis mes photos</button>
          <button class="settings-action" type="button" id="myStickerBtn"><span class="btn-ic" id="icSticker"></span>Ajouter un sticker perso</button>
          <button class="settings-action danger" type="button" id="clearImagesBtn"><span class="btn-ic" id="icReset"></span>Tout remettre par défaut</button>
          <input type="file" id="wallPhotoInput" accept="image/*" style="display:none">
          <input type="file" id="chatBgInput" accept="image/*" style="display:none">
          <input type="file" id="myStickerInput" accept="image/*" style="display:none">
          <div class="field-hint">Le fond du tchat ne se voit que chez toi. Le fond de salon se voit par tout l'appel.</div>
        </div>

        <div class="modal-actions">
          <button class="toggle-btn" id="settingsClose">Fermer</button>
        </div>
      </div>
    </div>

    <!-- ---- Modale : historique des toc-toc ---- -->
    <div class="modal-backdrop" id="historyModal">
      <div class="modal-card tall">
        <div class="modal-title">Historique des toc-toc</div>
        <div id="historyList"></div>
        <div class="modal-actions">
          <button class="modal-cancel-btn" id="historyClear">Tout effacer</button>
          <button class="toggle-btn" id="historyClose">Fermer</button>
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
          <div class="avatar-preview-hint">Ta photo, ou tes initiales.</div>
        </div>
        <div class="photo-row">
          <button class="photo-btn" type="button" id="editPhotoBtn"><span class="btn-ic" id="icPhoto2"></span>Mettre une photo</button>
          <button class="photo-btn secondary" type="button" id="editPhotoClearBtn">Retirer</button>
        </div>
        <input type="file" id="editPhotoInput" accept="image/*" style="display:none">

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
        <button class="call-btn" id="muteBtn"><span class="call-ic" id="muteIc"></span><span class="call-lb">Micro</span></button>
        <button class="call-btn" id="camBtn"><span class="call-ic" id="camIc"></span><span class="call-lb">Caméra</span></button>
        <button class="call-btn" id="screenBtn"><span class="call-ic" id="screenIc"></span><span class="call-lb">Écran</span></button>
        <button class="call-btn" id="chatBtn"><span class="call-ic" id="chatIc"></span><span class="call-lb">Tchat</span><span class="chat-badge" id="chatBadge"></span></button>
        <button class="call-btn" id="peopleBtn"><span class="call-ic" id="peopleIc"></span><span class="call-lb">Membres</span><span class="chat-badge" id="peopleCount"></span></button>
        <button class="call-btn" id="wallBtn" style="display:none;"><span class="call-ic" id="wallIc"></span><span class="call-lb">Fond</span></button>
        <button class="call-btn" id="fxBtn" style="display:none;"><span class="call-ic" id="fxIc"></span><span class="call-lb">Effet</span></button>
        <button class="call-btn hangup" id="leaveBtn"><span class="call-ic" id="leaveIc"></span><span class="call-lb">Quitter</span></button>
      </div>

      <!-- ---- Fonds de salon : visible seulement pour l'hôte abonné ---- -->
      <div class="wall-panel" id="wallPanel">
        <div class="chat-head">
          <div class="chat-title">Fond du salon</div>
          <button class="chat-close" id="wallCloseBtn">✕</button>
        </div>
        <div class="wall-grid" id="wallGrid"></div>
      </div>

      <!-- ---- Tchat écrit, visible uniquement pendant l'appel ---- -->
      <div class="chat-panel" id="chatPanel">
        <div class="chat-head">
          <div class="chat-title">Tchat de l'appel</div>
          <button class="chat-close" id="chatCloseBtn">✕</button>
        </div>
        <div class="chat-messages" id="chatMessages"></div>
        <div class="emoji-panel" id="emojiPanel">
          <div class="panel-tabs">
            <button class="panel-tab active" type="button" id="tabEmoji">Émojis</button>
            <button class="panel-tab" type="button" id="tabSticker">Stickers</button>
          </div>
          <div class="emoji-grid-chat" id="emojiGrid"></div>
          <div class="emoji-legend"><i></i> Ces émojis s'animent pour de vrai (abonnés)</div>
          <div class="sticker-grid" id="stickerGrid" style="display:none;"></div>
        </div>
        <div class="chat-input-row">
          <button class="emoji-toggle" type="button" id="emojiToggle">😊</button>
          <input class="chat-input" id="chatInput" type="text" maxlength="200" placeholder="Ton message…" autocomplete="off">
          <button class="chat-send" id="chatSendBtn">Envoyer</button>
        </div>
      </div>

      <div class="wall-panel" id="peoplePanel">
        <div class="chat-head">
          <div class="chat-title">Dans l'appel</div>
          <button class="chat-close" id="peopleCloseBtn">✕</button>
        </div>
        <div id="peopleList"></div>
      </div>

      <div class="wall-panel" id="fxPanel">
        <div class="chat-head">
          <div class="chat-title">Envoyer un effet</div>
          <button class="chat-close" id="fxCloseBtn">✕</button>
        </div>
        <div class="fx-grid">
          <button class="fx-choice" type="button" data-fx="confetti"><i class="fx-prev fx-prev-confetti"></i><span>Confettis</span></button>
          <button class="fx-choice" type="button" data-fx="hearts">💖<span>Cœurs</span></button>
          <button class="fx-choice" type="button" data-fx="fireworks">✨<span>Étincelles</span></button>
          <button class="fx-choice" type="button" data-fx="rain"><i class="fx-prev fx-prev-rain"></i><span>Pluie</span></button>
        </div>
      </div>

      <div class="reaction-zone" id="reactionZone"></div>

      <div id="incomingRequest">
        <div class="req-head">
          <div class="req-avatar" id="incomingRequestAvatar"></div>
          <div class="req-texts">
            <div class="req-name" id="incomingRequestName"></div>
            <div class="req-sub">veut rejoindre ton appel</div>
          </div>
        </div>
        <div class="incoming-msg" id="incomingRequestMsg" style="display:none;"></div>
        <div class="req-actions">
          <button class="req-btn req-no" id="declineRequestBtn">Refuser</button>
          <button class="req-btn req-yes" id="acceptRequestBtn">Accepter</button>
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
// LiveDoors Plus — maquette d'abonnement
//
// L'état est gardé sur l'appareil et renvoyé au serveur, qui applique les
// limites (taille du salon, porte privée). C'est volontairement une maquette :
// tant qu'il n'y a pas de vrai paiement vérifié côté serveur, n'importe qui
// pourrait se déclarer abonné.
// ---------------------------------------------------------------------------

const PREMIUM_KEY = 'livedoors-premium';
const BELL_KEY = 'livedoors-bell';
const VIP_KEY = 'livedoors-viponly';
const HISTORY_KEY = 'livedoors-history';

function isPremium() {
  try { return localStorage.getItem(PREMIUM_KEY) === '1'; } catch (e) { return false; }
}
function setPremium(on) {
  try { localStorage.setItem(PREMIUM_KEY, on ? '1' : '0'); } catch (e) {}
}
function bellChoice() {
  try { return parseInt(localStorage.getItem(BELL_KEY) || '0', 10) || 0; } catch (e) { return 0; }
}
function vipOnly() {
  try { return isPremium() && localStorage.getItem(VIP_KEY) === '1'; } catch (e) { return false; }
}

// -- Sonneries : fabriquées à la volée, aucun fichier à charger --------------
// Chaque sonnerie = une liste de notes [fréquence, départ, durée] + un timbre.
const BELLS = [
  { name: 'Doux',     wave: 'sine',     notes: [[440, 0, 0.4], [550, 0.16, 0.45]] },
  { name: 'Carillon', wave: 'sine',     notes: [[660, 0, 0.5], [880, 0.16, 0.5], [1170, 0.32, 0.6]] },
  { name: 'Arcade',   wave: 'square',   notes: [[520, 0, 0.1], [780, 0.1, 0.1], [1040, 0.2, 0.1], [1560, 0.3, 0.25]] },
  { name: 'Toc-toc',  wave: 'triangle', notes: [[180, 0, 0.09], [180, 0.18, 0.09], [150, 0.36, 0.14]] },
  { name: 'Goutte',   wave: 'sine',     notes: [[1200, 0, 0.12], [700, 0.08, 0.3]] },
  { name: 'Fanfare',  wave: 'sawtooth', notes: [[392, 0, 0.16], [523, 0.16, 0.16], [659, 0.32, 0.16], [784, 0.48, 0.4]] },
  { name: 'Rétro',    wave: 'square',   notes: [[330, 0, 0.08], [440, 0.08, 0.08], [330, 0.16, 0.08], [660, 0.24, 0.3]] },
  { name: 'Aucune',   wave: 'sine',     notes: [] },
];

function playBell() {
  const bell = BELLS[bellChoice()] || BELLS[0];
  if (!bell.notes.length) return;
  try {
    const Ctx = window.AudioContext || window.webkitAudioContext;
    if (!Ctx) return;
    const ctx = new Ctx();

    bell.notes.forEach(([freq, delay, dur]) => {
      const osc = ctx.createOscillator();
      const gain = ctx.createGain();
      const start = ctx.currentTime + delay;
      osc.type = bell.wave;
      osc.frequency.value = freq;
      gain.gain.setValueAtTime(0.0001, start);
      gain.gain.exponentialRampToValueAtTime(0.22, start + 0.025);
      gain.gain.exponentialRampToValueAtTime(0.0001, start + dur);
      osc.connect(gain).connect(ctx.destination);
      osc.start(start);
      osc.stop(start + dur + 0.05);
    });

    setTimeout(() => ctx.close(), 2200);
  } catch (e) {}
}

// -- Historique des toc-toc (gardé sur l'appareil, 50 derniers) --------------
function loadHistory() {
  try {
    const list = JSON.parse(localStorage.getItem(HISTORY_KEY) || '[]');
    return Array.isArray(list) ? list : [];
  } catch (e) { return []; }
}
function addHistory(entry) {
  const list = loadHistory();
  list.unshift(entry);
  try { localStorage.setItem(HISTORY_KEY, JSON.stringify(list.slice(0, 50))); } catch (e) {}
}
function whenLabel(at) {
  const mins = Math.floor((Date.now() - at) / 60000);
  if (mins < 1) return "à l'instant";
  if (mins < 60) return 'il y a ' + mins + ' min';
  const hours = Math.floor(mins / 60);
  if (hours < 24) return 'il y a ' + hours + ' h';
  return 'il y a ' + Math.floor(hours / 24) + ' j';
}

// Quelqu'un peut frapper sans être dans ton carnet : on propose de l'ajouter
// directement depuis l'historique.
function addFromHistory(phone, username) {
  if (phone) socket.emit('contact:add', { phone });
  else if (username) socket.emit('contact:addByUsername', { username });
  else { showToast('Pas assez d\\'infos pour ajouter cette personne.'); return; }
  setTimeout(renderHistory, 600); // le temps que le serveur réponde
}

function renderHistory() {
  const list = loadHistory();
  \$('historyList').innerHTML = list.length ? list.map((h) => {
    const known = h.phone && findContact(h.phone);
    const canAdd = !known && (h.phone || h.username);
    return \`
    <div class="history-row">
      <div class="history-main">
        <div class="history-name">\${escapeHtml(h.pseudo || 'Quelqu\\'un')}\${h.username ? ' <span class="history-when">@' + escapeHtml(h.username) + '</span>' : ''}</div>
        <div class="history-when">\${whenLabel(h.at)}\${h.message ? ' · ' + escapeHtml(h.message) : ''}</div>
      </div>
      \${canAdd
        ? \`<button class="history-add" onclick="addFromHistory('\${escapeAttr(h.phone || '')}', '\${escapeAttr(h.username || '')}')">+ Ajouter</button>\`
        : \`<div class="history-tag \${h.answer === 'ok' ? 'ok' : 'no'}">\${h.answer === 'ok' ? 'Accepté' : (h.answer === 'no' ? 'Refusé' : 'Sans réponse')}</div>\`}
    </div>\`;
  }).join('') : '<div class="empty-note">Personne n\\'a frappé pour l\\'instant.</div>';
}

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
  refreshPremiumUI();
  \$('settingsModal').classList.add('show');
});
\$('settingsClose').addEventListener('click', () => \$('settingsModal').classList.remove('show'));

// ---- Réglages Premium ----
function refreshPremiumUI() {
  const on = isPremium();
  \$('premiumToggle').innerHTML = on
    ? '<span class="btn-ic">' + icon('star', 16) + '</span>Abonnement actif — désactiver'
    : "Activer l'essai";
  \$('premiumToggle').classList.toggle('on', on);
  \$('premiumOptions').style.display = on ? 'block' : 'none';

  // Le petit mot devient coloré avec l'abonnement.
  \$('doorMessageInput').classList.toggle('is-premium', on);
  \$('doorMessageInput').maxLength = on ? 140 : 60;
  \$('doorMessageInput').placeholder = on
    ? 'Petit mot ou emoji (140 caractères)'
    : 'Petit mot ou emoji (ex: Pause café)';

  Array.from(\$('bellChoice').children).forEach((b) => {
    b.classList.toggle('active', parseInt(b.getAttribute('data-bell'), 10) === bellChoice());
  });

  const vip = vipOnly();
  \$('vipToggle').innerHTML = '<span class="btn-ic">' + icon(vip ? 'lock' : 'unlock', 16) + '</span>'
    + (vip ? 'Privée : amis proches seulement' : 'Ouverte à tous mes contacts');
  \$('vipToggle').classList.toggle('on', vip);

  const discreet = discreetMode();
  \$('discreetToggle').innerHTML = '<span class="btn-ic">' + icon(discreet ? 'eyeOff' : 'eye', 16) + '</span>'
    + (discreet ? 'Discret : amis proches seulement' : 'Tout le monde voit ma porte');
  \$('discreetToggle').classList.toggle('on', discreet);

}

\$('premiumToggle').addEventListener('click', () => {
  const on = !isPremium();
  setPremium(on);
  refreshPremiumUI();
  refreshWallButton();

  // Tout ce qui dépendait de l'abonnement est remis à plat immédiatement :
  // fonds personnalisés, stickers perso, couleur du statut.
  buildEmojiBar();
  applyChatBackground();
  if (inCall) applyWallpaper(iAmHost && on ? wallpaperChoice() : 0, wallpaperPhoto());
  render();

  sendRegister(); // le serveur doit connaître le nouveau statut
  showToast(on ? 'LiveDoors Plus activé.' : 'Retour à la version gratuite.');
});

Array.from(\$('bellChoice').children).forEach((b) => {
  b.addEventListener('click', () => {
    try { localStorage.setItem(BELL_KEY, b.getAttribute('data-bell')); } catch (e) {}
    refreshPremiumUI();
    playBell(); // pour l'entendre tout de suite
  });
});

\$('vipToggle').addEventListener('click', () => {
  if (!isPremium()) { showToast('Réservé à LiveDoors Plus.'); return; }
  const next = vipOnly() ? '0' : '1';
  try { localStorage.setItem(VIP_KEY, next); } catch (e) {}
  refreshPremiumUI();
  sendRegister();
  showToast(next === '1' ? 'Porte privée : amis proches seulement.' : 'Porte ouverte à tous tes contacts.');
});

\$('discreetToggle').addEventListener('click', () => {
  if (!isPremium()) { showToast('Réservé à LiveDoors Plus.'); return; }
  const next = discreetMode() ? '0' : '1';
  try { localStorage.setItem(DISCREET_KEY, next); } catch (e) {}
  refreshPremiumUI();
  sendRegister(); // c'est le serveur qui cache la porte aux autres
  showToast(next === '1' ? 'Mode discret activé.' : 'Tout le monde revoit ta porte.');
});

// ---- Images personnelles : fond de salon, fond de tchat, stickers ----
function removeMySticker(index) {
  const list = myStickers();
  list.splice(index, 1);
  saveMyStickers(list);
  buildEmojiBar();
  showToast('Sticker supprimé.');
}

\$('wallPhotoBtn').addEventListener('click', () => {
  if (!isPremium()) { showToast('Réservé à LiveDoors Plus.'); return; }
  \$('wallPhotoInput').click();
});
\$('chatBgBtn').addEventListener('click', () => {
  if (!isPremium()) { showToast('Réservé à LiveDoors Plus.'); return; }
  \$('chatBgInput').click();
});
\$('myStickerBtn').addEventListener('click', () => {
  if (!isPremium()) { showToast('Réservé à LiveDoors Plus.'); return; }
  \$('myStickerInput').click();
});

// Le fond de salon est vu par tout l'appel : il peut être plus grand.
\$('wallPhotoInput').addEventListener('change', async (e) => {
  const file = e.target.files && e.target.files[0];
  e.target.value = '';
  if (!file) return;
  try {
    const data = await shrinkImage(file, 480, 0.62);
    try { localStorage.setItem(WALLPAPER_PHOTO_KEY, data); } catch (err) {}
    applyWallpaper(0, data);
    if (inCall && iAmHost) socket.emit('door:wallpaper', { wallpaper: 0, photo: data });
    showToast('Fond de salon mis à jour.');
  } catch (err) { showToast("Impossible de lire cette image."); }
});

\$('chatBgInput').addEventListener('change', async (e) => {
  const file = e.target.files && e.target.files[0];
  e.target.value = '';
  if (!file) return;
  try {
    const data = await shrinkImage(file, 400, 0.6);
    try { localStorage.setItem(CHATBG_KEY, data); } catch (err) {}
    applyChatBackground();
    showToast('Fond du tchat mis à jour.');
  } catch (err) { showToast("Impossible de lire cette image."); }
});

\$('myStickerInput').addEventListener('change', async (e) => {
  const file = e.target.files && e.target.files[0];
  e.target.value = '';
  if (!file) return;
  try {
    const data = await shrinkImage(file, 160, 0.7);
    const list = myStickers();
    if (list.length >= 12) { showToast('12 stickers maximum — supprime-en un.'); return; }
    list.push(data);
    if (!saveMyStickers(list)) { showToast('Mémoire pleine, sticker non enregistré.'); return; }
    buildEmojiBar();
    showToast('Sticker ajouté.');
  } catch (err) { showToast("Impossible de lire cette image."); }
});

\$('clearImagesBtn').addEventListener('click', () => {
  if (!confirm('Remettre les fonds et supprimer tes stickers perso ?')) return;
  try {
    localStorage.removeItem(WALLPAPER_PHOTO_KEY);
    localStorage.removeItem(CHATBG_KEY);
    localStorage.removeItem(MYSTICKERS_KEY);
  } catch (e) {}
  applyWallpaper(wallpaperChoice(), '');
  applyChatBackground();
  buildEmojiBar();
  showToast('Tout est revenu par défaut.');
});

// ---- Historique ----
\$('settingsHistory').addEventListener('click', () => {
  renderHistory();
  \$('historyModal').classList.add('show');
});
\$('historyClose').addEventListener('click', () => \$('historyModal').classList.remove('show'));
\$('historyClear').addEventListener('click', () => {
  try { localStorage.removeItem(HISTORY_KEY); } catch (e) {}
  renderHistory();
});

// ---------------------------------------------------------------------------
// Icônes
//
// Des SVG dessinés au trait plutôt que des émojis : un émoji change de tête
// selon le téléphone (Apple, Android, Windows...) et fait toujours un peu
// dessin animé. Là, l'icône est identique partout et s'aligne au pixel.
// ---------------------------------------------------------------------------

const ICONS = {
  mic: '<path d="M12 2a3 3 0 0 0-3 3v6a3 3 0 0 0 6 0V5a3 3 0 0 0-3-3z"/><path d="M19 10v1a7 7 0 0 1-14 0v-1"/><path d="M12 18v4"/><path d="M8 22h8"/>',
  micOff: '<path d="M2 2l20 20"/><path d="M9 9v2a3 3 0 0 0 5 2"/><path d="M15 10V5a3 3 0 0 0-5.6-1.5"/><path d="M19 10v1a7 7 0 0 1-10.7 6"/><path d="M5 10v1a7 7 0 0 0 2 4.9"/><path d="M12 18v4"/><path d="M8 22h8"/>',
  camera: '<path d="M23 7l-7 5 7 5V7z"/><rect x="1" y="5" width="15" height="14" rx="2"/>',
  screen: '<rect x="2" y="3" width="20" height="13" rx="2"/><path d="M8 21h8"/><path d="M12 17v4"/>',
  chat: '<path d="M21 11.5a8.4 8.4 0 0 1-9 8.4 8.5 8.5 0 0 1-3.9-.9L3 21l1.9-5a8.4 8.4 0 0 1-.9-3.9 8.4 8.4 0 0 1 8.4-9 8.4 8.4 0 0 1 8.6 8.4z"/>',
  people: '<path d="M17 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2"/><circle cx="9.5" cy="7" r="4"/><path d="M22 21v-2a4 4 0 0 0-3-3.9"/><path d="M16.5 3.1a4 4 0 0 1 0 7.8"/>',
  image: '<rect x="3" y="3" width="18" height="18" rx="2"/><circle cx="8.5" cy="8.5" r="1.5"/><path d="M21 15l-5-5L5 21"/>',
  sparkle: '<path d="M12 3l1.9 5.1L19 10l-5.1 1.9L12 17l-1.9-5.1L5 10l5.1-1.9L12 3z"/><path d="M18.5 15l.8 2.2 2.2.8-2.2.8-.8 2.2-.8-2.2-2.2-.8 2.2-.8.8-2.2z"/>',
  hangup: '<path d="M22 16.9v3a2 2 0 0 1-2.2 2 19.8 19.8 0 0 1-8.6-3.1 19.5 19.5 0 0 1-6-6A19.8 19.8 0 0 1 2.1 4.2 2 2 0 0 1 4.1 2h3a2 2 0 0 1 2 1.7c.1.9.4 1.8.7 2.7a2 2 0 0 1-.5 2.1L8.1 9.9a16 16 0 0 0 6 6l1.4-1.2a2 2 0 0 1 2.1-.5c.9.3 1.8.6 2.7.7a2 2 0 0 1 1.7 2z" transform="rotate(135 12 12)"/>',
  gear: '<circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.7 1.7 0 0 0 .3 1.8l.1.1a2 2 0 1 1-2.8 2.8l-.1-.1a1.7 1.7 0 0 0-2.9 1.2v.2a2 2 0 1 1-4 0v-.1a1.7 1.7 0 0 0-3-1.2l-.1.1a2 2 0 1 1-2.8-2.8l.1-.1a1.7 1.7 0 0 0-1.2-2.9H3a2 2 0 1 1 0-4h.1a1.7 1.7 0 0 0 1.2-3l-.1-.1a2 2 0 1 1 2.8-2.8l.1.1a1.7 1.7 0 0 0 2.9-1.2V3a2 2 0 1 1 4 0v.1a1.7 1.7 0 0 0 3 1.2l.1-.1a2 2 0 1 1 2.8 2.8l-.1.1a1.7 1.7 0 0 0 1.2 2.9h.2a2 2 0 1 1 0 4h-.1a1.7 1.7 0 0 0-1.5 1z"/>',
  star: '<path d="M12 2.5l2.9 6 6.6.9-4.8 4.6 1.2 6.5-5.9-3.1-5.9 3.1 1.2-6.5L2.5 9.4l6.6-.9 2.9-6z"/>',
  heart: '<path d="M20.8 5.6a5.2 5.2 0 0 0-7.4 0L12 7l-1.4-1.4a5.2 5.2 0 1 0-7.4 7.4L12 21.4l8.8-8.4a5.2 5.2 0 0 0 0-7.4z"/>',
  pencil: '<path d="M17 3a2.8 2.8 0 0 1 4 4L7.5 20.5 2 22l1.5-5.5L17 3z"/>',
  block: '<circle cx="12" cy="12" r="9"/><path d="M5.6 5.6l12.8 12.8"/>',
  close: '<path d="M18 6L6 18"/><path d="M6 6l12 12"/>',
  plus: '<path d="M12 5v14"/><path d="M5 12h14"/>',
  bell: '<path d="M18 8a6 6 0 0 0-12 0c0 7-3 9-3 9h18s-3-2-3-9"/><path d="M13.7 21a2 2 0 0 1-3.4 0"/>',
  bellOff: '<path d="M2 2l20 20"/><path d="M18.6 13A18 18 0 0 1 18 8"/><path d="M6 8a6 6 0 0 1 9.3-5"/><path d="M6 8c0 7-3 9-3 9h13"/><path d="M13.7 21a2 2 0 0 1-3.4 0"/>',
  lock: '<rect x="4" y="10" width="16" height="11" rx="2"/><path d="M8 10V7a4 4 0 0 1 8 0v3"/>',
  unlock: '<rect x="4" y="10" width="16" height="11" rx="2"/><path d="M8 10V7a4 4 0 0 1 7.5-2"/>',
  logout: '<path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4"/><path d="M16 17l5-5-5-5"/><path d="M21 12H9"/>',
  eye: '<path d="M1.5 12S5 5.5 12 5.5 22.5 12 22.5 12 19 18.5 12 18.5 1.5 12 1.5 12z"/><circle cx="12" cy="12" r="3"/>',
  eyeOff: '<path d="M2 2l20 20"/><path d="M10.6 6a8 8 0 0 1 1.4-.1c7 0 10.5 6.1 10.5 6.1a17 17 0 0 1-3.3 4"/><path d="M6.3 7.9A16.6 16.6 0 0 0 1.5 12S5 18.5 12 18.5a10 10 0 0 0 4-.8"/><path d="M9.9 10a3 3 0 0 0 4.2 4.2"/>',
  sun: '<circle cx="12" cy="12" r="4"/><path d="M12 2v2"/><path d="M12 20v2"/><path d="M4.2 4.2l1.5 1.5"/><path d="M18.3 18.3l1.5 1.5"/><path d="M2 12h2"/><path d="M20 12h2"/><path d="M4.2 19.8l1.5-1.5"/><path d="M18.3 5.7l1.5-1.5"/>',
  moon: '<path d="M21 12.8A9 9 0 1 1 11.2 3a7 7 0 0 0 9.8 9.8z"/>',
  device: '<rect x="6" y="2" width="12" height="20" rx="3"/><path d="M11 18h2"/>',
  palette: '<path d="M12 3a9 9 0 1 0 0 18 2 2 0 0 0 1.6-3.2 2 2 0 0 1 1.6-3.2H18a3 3 0 0 0 3-3A9 9 0 0 0 12 3z"/><circle cx="7.5" cy="11" r="1.2"/><circle cx="10.5" cy="7" r="1.2"/><circle cx="15" cy="8" r="1.2"/>',
  refresh: '<path d="M3 12a9 9 0 0 1 15.3-6.4L21 8"/><path d="M21 3v5h-5"/><path d="M21 12a9 9 0 0 1-15.3 6.4L3 16"/><path d="M3 21v-5h5"/>',
  text: '<path d="M5 5h14"/><path d="M5 12h14"/><path d="M5 19h9"/>',
  video: '<path d="M23 7l-7 5 7 5V7z"/><rect x="1" y="5" width="15" height="14" rx="2"/>',
  shuffle: '<path d="M16 3h5v5"/><path d="M4 20L21 3"/><path d="M21 16v5h-5"/><path d="M15 15l6 6"/><path d="M4 4l5 5"/>',
  expand: '<path d="M15 3h6v6"/><path d="M9 21H3v-6"/><path d="M21 3l-7 7"/><path d="M3 21l7-7"/>',
  shrink: '<path d="M4 14h6v6"/><path d="M20 10h-6V4"/><path d="M14 10l7-7"/><path d="M3 21l7-7"/>',
};

function icon(name, size) {
  const px = size || 22;
  return '<svg viewBox="0 0 24 24" width="' + px + '" height="' + px + '" fill="none" '
    + 'stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">'
    + (ICONS[name] || '') + '</svg>';
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
// Les photos des contacts peuvent finir par remplir l'espace de stockage du
// navigateur (~5 Mo). Si l'enregistrement échoue, on réessaie sans les photos
// plutôt que de perdre tout le carnet en silence.
function saveContacts(list) {
  const trimmed = list.slice(0, 300);
  try {
    localStorage.setItem(CONTACTS_KEY, JSON.stringify(trimmed));
    return true;
  } catch (e) {
    try {
      const light = trimmed.map((c) => {
        const copy = Object.assign({}, c);
        delete copy.avatarPhoto;
        return copy;
      });
      localStorage.setItem(CONTACTS_KEY, JSON.stringify(light));
      return true;
    } catch (e2) { return false; }
  }
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
// Les "amis proches" sont ceux autorisés à frapper quand la porte est privée.
function isClose(phone) {
  const card = findContact(phone);
  return !!(card && card.close);
}
function closePhones() {
  return loadContacts().filter((c) => c.close).map((c) => c.phone);
}
function blockedPhones() {
  return loadContacts().filter((c) => c.blocked).map((c) => c.phone);
}

function toggleClose(phone) {
  if (!isPremium()) { showToast('Les amis proches font partie de LiveDoors Plus.'); return; }
  const now = !isClose(phone);
  updateContact(phone, { close: now });
  sendRegister(); // le serveur applique la porte privée
  render();
  showToast(now ? 'Ajouté à tes amis proches.' : 'Retiré de tes amis proches.');
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
  showToast(now ? 'Ajouté aux favoris.' : 'Retiré des favoris.');
}

function toggleBlocked(phone) {
  const card = findContact(phone);
  const now = !(card && card.blocked);
  if (now && !confirm('Bloquer ce contact ? Vous ne verrez plus vos portes respectives.')) return;
  updateContact(phone, { blocked: now, favorite: now ? false : (card && card.favorite) });
  sendRegister(); // le serveur applique le blocage des deux côtés
  render();
  showToast(now ? 'Contact bloqué.' : 'Contact débloqué.');
}
// Dès qu'un contact est vu en ligne, on met à jour sa fiche locale.
function refreshContactCards(list) {
  const saved = loadContacts();
  let changed = false;
  list.forEach((u) => {
    if (!u.phone) return;
    const key = normalizePhoneLocal(u.phone);
    const found = saved.find((c) => normalizePhoneLocal(c.phone) === key);
    if (!found) return;
    if (found.pseudo !== u.pseudo
      || found.avatarPhoto !== (u.avatarPhoto || '')
      || found.premium !== !!u.premium) {
      found.pseudo = u.pseudo;
      found.avatarPhoto = u.avatarPhoto || ''; // sinon la photo disparaît hors ligne
      found.premium = !!u.premium;             // pour garder le badge hors ligne
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
// Toutes les icônes sont posées au démarrage, en un seul endroit.
function paintIcons() {
  \$('muteIc').innerHTML = icon('mic');
  \$('camIc').innerHTML = icon('camera');
  \$('screenIc').innerHTML = icon('screen');
  \$('chatIc').innerHTML = icon('chat');
  \$('peopleIc').innerHTML = icon('people');
  \$('wallIc').innerHTML = icon('image');
  \$('fxIc').innerHTML = icon('sparkle');
  \$('leaveIc').innerHTML = icon('hangup');
  \$('settingsBtn').innerHTML = icon('gear', 18);
  \$('chatCloseBtn').innerHTML = icon('close', 16);
  \$('wallCloseBtn').innerHTML = icon('close', 16);
  \$('fxCloseBtn').innerHTML = icon('close', 16);
  \$('peopleCloseBtn').innerHTML = icon('close', 16);
  \$('emojiToggle').innerHTML = icon('sparkle', 18);

  // Paramètres
  \$('icHistory').innerHTML = icon('bell', 16);
  \$('icLock').innerHTML = icon('lock', 16);
  \$('icForget').innerHTML = icon('logout', 16);
  \$('icWallPhoto').innerHTML = icon('image', 16);
  \$('icChatBg').innerHTML = icon('chat', 16);
  \$('icSticker').innerHTML = icon('palette', 16);
  \$('icReset').innerHTML = icon('refresh', 16);
  \$('icSun').innerHTML = icon('sun', 14);
  \$('icMoon').innerHTML = icon('moon', 14);
  \$('icDevice').innerHTML = icon('device', 14);
  \$('icPhoto1').innerHTML = icon('camera', 15);
  \$('icPhoto2').innerHTML = icon('camera', 15);

  // Étiquettes des avantages
  const chips = {
    chipVideo: 'video', chipScreen: 'screen', chipPrivate: 'lock', chipBells: 'bell',
    chipStickers: 'palette', chipEmoji: 'sparkle', chipStatus: 'text',
  };
  Object.keys(chips).forEach((id) => {
    \$(id).insertAdjacentHTML('afterbegin', '<span class="chip-ic">' + icon(chips[id], 12) + '</span>');
  });
}

function paintAvatarFor(el, user) {
  if (user && user.avatarPhoto && isSafePhoto(user.avatarPhoto)) {
    el.style.background = 'transparent';
    el.innerHTML = '';
    const img = document.createElement('img');
    img.className = 'avatar-photo';
    img.src = user.avatarPhoto;
    img.alt = '';
    el.appendChild(img);
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

// Aperçu de l'avatar : la photo si elle existe, sinon les initiales du pseudo
// sur un fond coloré tiré du pseudo lui-même.
function refreshSignupPreview() {
  const pseudo = \$('pseudoInput').value.trim();
  if (signupPhoto) {
    \$('avatarPreview').innerHTML = '<img class="avatar-photo" alt="" src="' + signupPhoto + '">';
  } else {
    paintAvatarFor(\$('avatarPreview'), {
      avatarInitials: pseudo ? initialsFor(pseudo) : '--',
      avatarColor: colorForPseudo(pseudo || 'a'),
    });
  }
}

function refreshEditPreview() {
  const pseudo = \$('editPseudo').value.trim();
  if (editPhoto) {
    \$('editAvatarPreview').innerHTML = '<img class="avatar-photo" alt="" src="' + editPhoto + '">';
  } else {
    paintAvatarFor(\$('editAvatarPreview'), {
      avatarInitials: pseudo ? initialsFor(pseudo) : '--',
      avatarColor: (profile && profile.avatarColor) || colorForPseudo(pseudo || 'a'),
    });
  }
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
      showToast('Photo ajoutée.');
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
  if (!d) refreshSignupPreview();
});
wirePhotoPicker('editPhotoBtn', 'editPhotoInput', 'editPhotoClearBtn', 'editAvatarPreview', (d) => {
  editPhoto = d;
  if (!d) refreshEditPreview();
});

// -- Création du profil ------------------------------------------------------
\$('pseudoInput').addEventListener('input', refreshSignupPreview);
\$('editPseudo').addEventListener('input', refreshEditPreview);

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
  refreshEditPreview();
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
  showToast('Profil mis à jour.');
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
    username: onlineProfile.username || '',
    premium: isPremium(),
    vipOnly: vipOnly(),
    discreet: discreetMode(),
    vip: closePhones(),
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
  \$('myName').innerHTML = escapeHtml(user.pseudo)
    + (user.premium ? '<span class="premium-badge big">PLUS</span>' : '');
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
  refreshSignupPreview();
  buildEmojiBar();
  buildWallPicker();
  applyChatBackground();
  paintIcons();
  refreshPremiumUI(); // sinon la couleur du statut n'arrive qu'après un tour dans les réglages
  setPanelTab('emoji');
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
      avatarPhoto: profile.avatarPhoto || '',
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
  const close = card && card.close;
  const blocked = card && card.blocked;
  const p = escapeAttr(phone);
  return \`<div class="contact-actions">
      <button class="contact-btn\${fav ? ' on' : ''}" onclick="toggleFavorite('\${p}')" title="Favori">\${icon('star', 14)}</button>
      \${isPremium() ? \`<button class="contact-btn\${close ? ' close-on' : ''}" onclick="toggleClose('\${p}')" title="\${close ? 'Retirer des amis proches' : 'Ajouter aux amis proches'}">\${icon('heart', 14)}</button>\` : ''}
      <button class="contact-btn" onclick="renameContact('\${p}')" title="Renommer">\${icon('pencil', 14)}</button>
      <button class="contact-btn\${blocked ? ' danger' : ''}" onclick="toggleBlocked('\${p}')" title="\${blocked ? 'Débloquer' : 'Bloquer'}">\${icon('block', 14)}</button>
      <button class="contact-btn" onclick="forgetContact('\${p}')" title="Retirer">\${icon('close', 14)}</button>
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
        <div class="friend-name">\${f.phone && isFavorite(f.phone) ? '<span class="fav-star">' + icon('star', 12) + '</span>' : ''}\${escapeHtml(displayName(f))}\${f.premium ? '<span class="premium-badge">PLUS</span>' : ''}</div>
        <div class="friend-phone">\${f.username ? '@' + escapeHtml(f.username) : escapeHtml(f.phone || '')}</div>
        <div class="friend-meta live-meta">\${friendMeta(f)}</div>
        \${f.doorMessage ? \`<div class="friend-status-msg\${f.premium ? ' is-premium' : ''}">\${escapeHtml(f.doorMessage)}</div>\` : ''}
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
        <div class="friend-name">\${f.phone && isFavorite(f.phone) ? '<span class="fav-star">' + icon('star', 12) + '</span>' : ''}\${escapeHtml(displayName(f))}\${f.premium ? '<span class="premium-badge">PLUS</span>' : ''}</div>
        <div class="friend-phone">\${f.username ? '@' + escapeHtml(f.username) : escapeHtml(f.phone || '')}</div>
        \${f.doorMessage ? \`<div class="friend-status-msg\${f.premium ? ' is-premium' : ''}">\${escapeHtml(f.doorMessage)}</div>\` : ''}
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
    <div class="friend-row is-closed is-offline">
      <div class="avatar-wrap">
        <div class="avatar">\${avatarMarkup({ avatarPhoto: c.avatarPhoto || '', avatarColor: c.avatarColor || '#ff8a00', avatarInitials: (c.alias || c.pseudo || '?').slice(0, 2).toUpperCase() })}</div>
      </div>
      <div class="friend-info">
        <div class="friend-name">\${c.favorite ? '<span class="fav-star">' + icon('star', 12) + '</span>' : ''}\${escapeHtml(c.alias || c.pseudo || 'Contact')}\${c.premium ? '<span class="premium-badge">PLUS</span>' : ''}</div>
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
      localStream = await navigator.mediaDevices.getUserMedia({ audio: { echoCancellation: true, noiseSuppression: true, autoGainControl: true } });
    } catch (err) {
      showToast("Micro refusé — active-le pour ouvrir ta porte.");
      return;
    }
    const message = \$('doorMessageInput').value.trim();
    saveStatus(message);
    socket.emit('door:open', { message, wallpaper: isPremium() ? wallpaperChoice() : 0 });
    if (isPremium() && wallpaperPhoto()) {
      setTimeout(() => socket.emit('door:wallpaper', { wallpaper: 0, photo: wallpaperPhoto() }), 250);
    }
    startCallUI({ id: me.id, pseudo: 'En attente...', avatarInitials: me.avatarInitials, avatarColor: me.avatarColor, avatarPhoto: me.avatarPhoto }, true);
  } else {
    hostLeaves(); // propose de passer la main s'il y a du monde
  }
});

\$('doorMessageBtn').addEventListener('click', () => {
  const message = \$('doorMessageInput').value.trim();
  saveStatus(message);
  socket.emit('door:message', { message });
  showToast(message ? 'Statut gardé 24 h.' : 'Statut effacé.');
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
    localStream = await navigator.mediaDevices.getUserMedia({ audio: { echoCancellation: true, noiseSuppression: true, autoGainControl: true } });
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

    // Quand la personne coupe sa caméra ou arrête son partage, la piste
    // s'arrête de son côté. Sans ces écouteurs, l'image restait figée à
    // l'écran des autres pour toujours.
    track.addEventListener('ended', () => removeVideoTile(peerId));
    track.addEventListener('mute', () => removeVideoTile(peerId));
    if (stream) {
      stream.addEventListener('removetrack', (e) => {
        if (e.track && e.track.kind === 'video') removeVideoTile(peerId);
      });
    }
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
    if (peerId === 'me') video.muted = true; // pas de retour de son sur soi

    const label = document.createElement('div');
    label.className = 'video-tile-label';
    label.textContent = peerId === 'me' ? 'Toi' : (peerNames.get(peerId) || 'Participant');

    // Bouton d'agrandissement : l'image occupe tout l'écran d'appel.
    const zoom = document.createElement('button');
    zoom.className = 'video-zoom';
    zoom.type = 'button';
    zoom.innerHTML = icon('expand', 15);
    zoom.addEventListener('click', (e) => { e.stopPropagation(); toggleFullVideo(tile); });

    tile.appendChild(video);
    tile.appendChild(label);
    tile.appendChild(zoom);
    tile.addEventListener('click', () => toggleFullVideo(tile));
    \$('videoGrid').appendChild(tile);
  }
  tile.querySelector('video').srcObject = stream;
}

// En grand, la vidéo est affichée en entier (pas rognée) sur fond noir.
function toggleFullVideo(tile) {
  const already = tile.classList.contains('is-full');
  document.querySelectorAll('.video-tile.is-full').forEach((t) => {
    t.classList.remove('is-full');
    const b = t.querySelector('.video-zoom');
    if (b) b.innerHTML = icon('expand', 15);
  });
  \$('videoGrid').classList.remove('has-full');

  if (!already) {
    tile.classList.add('is-full');
    \$('videoGrid').classList.add('has-full');
    const b = tile.querySelector('.video-zoom');
    if (b) b.innerHTML = icon('shrink', 15);
  }
}

function removeVideoTile(peerId) {
  const tile = document.getElementById(\`videotile-\${peerId}\`);
  if (tile && tile.classList.contains('is-full')) \$('videoGrid').classList.remove('has-full');
  tile?.remove();
  if (!\$('videoGrid').children.length) \$('videoGrid').style.display = 'none';
}

function removePeer(peerId) {
  const pc = peers.get(peerId);
  if (pc) { pc.close(); peers.delete(peerId); }
  document.getElementById(\`audio-\${peerId}\`)?.remove();
  removeVideoTile(peerId);
}

// -- Signalisation entrante --

// On retient le nom de chaque participant : l'événement de départ ne donne
// qu'un identifiant, sans ça on ne pourrait pas dire QUI est parti.
const peerNames = new Map();
const peerCards = new Map();  // avatar / photo, pour la liste des présents
const peerStates = new Map(); // micro coupé, caméra, partage d'écran

// Mon propre état, envoyé au salon dès qu'il change.
let myCallState = { muted: false, cam: false, screen: false };

function sendMyCallState() {
  if (!inCall) return;
  socket.emit('call:state', myCallState);
  renderPeople();
}

function renderPeople() {
  const rows = [];

  if (me) {
    rows.push({ id: me.id, name: 'Toi', card: me, state: myCallState });
  }
  peerNames.forEach((name, id) => {
    rows.push({
      id,
      name,
      card: peerCards.get(id) || {},
      state: peerStates.get(id) || { muted: false, cam: false, screen: false },
    });
  });

  \$('peopleCount').textContent = String(rows.length);
  \$('peopleCount').classList.toggle('show', rows.length > 1);

  \$('peopleList').innerHTML = rows.map((r) => \`
    <div class="person-row">
      <div class="person-avatar">\${avatarMarkup(r.card)}</div>
      <div class="person-name">\${escapeHtml(r.name)}</div>
      <div class="person-icons">
        <span class="\${r.state.muted ? 'off' : 'live'}" title="\${r.state.muted ? 'Micro coupé' : 'Micro ouvert'}">\${icon(r.state.muted ? 'micOff' : 'mic', 16)}</span>
        <span class="\${r.state.cam ? 'live' : 'off'}" title="\${r.state.cam ? 'Caméra active' : 'Caméra éteinte'}">\${icon('camera', 16)}</span>
        <span class="\${r.state.screen ? 'live' : 'off'}" title="\${r.state.screen ? "Partage d'écran" : "Pas de partage d'écran"}">\${icon('screen', 16)}</span>
      </div>
    </div>
  \`).join('');
}

\$('peopleBtn').addEventListener('click', () => {
  \$('wallPanel').classList.remove('show');
  \$('fxPanel').classList.remove('show');
  renderPeople();
  \$('peoplePanel').classList.toggle('show');
  closeChat();
});
\$('peopleCloseBtn').addEventListener('click', () => \$('peoplePanel').classList.remove('show'));

socket.on('call:state', ({ id, muted, cam, screen }) => {
  peerStates.set(id, { muted: !!muted, cam: !!cam, screen: !!screen });
  // Ceinture et bretelles : si la personne annonce qu'elle n'a plus ni
  // caméra ni partage, on retire sa vignette même si le navigateur n'a pas
  // signalé la fin de la piste.
  if (!cam && !screen) removeVideoTile(id);
  renderPeople();
});

socket.on('call:room-state', async ({ members }) => {
  for (const member of members) {
    peerNames.set(member.id, displayName(member));
    peerCards.set(member.id, member);
    if (member.callState) peerStates.set(member.id, member.callState);
    createPeerConnection(member.id); // onnegotiationneeded envoie l'offre
  }
  renderPeople();
  sendMyCallState(); // les autres apprennent mon état à mon arrivée
});

socket.on('call:peer-joined', (peer) => {
  peerNames.set(peer.id, displayName(peer));
  peerCards.set(peer.id, peer);
  showToast(\`\${displayName(peer)} a rejoint l'appel\`);
  addSystemMessage(\`\${displayName(peer)} a rejoint l'appel 👋\`);
  renderPeople();
  sendMyCallState(); // pour que le nouveau voie tout de suite mon micro
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
  if (\$('historyModal').classList.contains('show')) renderHistory();
  showToast(found ? 'Contact ajouté !' : "Contact ajouté, il apparaîtra dès qu'il sera connecté.");
});

socket.on('contact:error', ({ message }) => showToast(message));

// ---------------------------------------------------------------------------
// Demandes d'appel entrantes (côté hôte : accepter / refuser)
// ---------------------------------------------------------------------------

let incomingRequestFromId = null;
let incomingRequestInfo = null;

socket.on('call:incoming-request', (from) => {
  incomingRequestFromId = from.id;
  incomingRequestInfo = {
    pseudo: displayName(from),
    phone: from.phone || '',
    username: from.username || '',
    message: from.message || '',
    at: Date.now(),
  };

  playBell();
  addHistory({ ...incomingRequestInfo, answer: 'none' });

  paintAvatarFor(\$('incomingRequestAvatar'), from);
  \$('incomingRequestName').textContent = displayName(from);
  if (from.message) {
    \$('incomingRequestMsg').textContent = \`"\${from.message}"\`;
    \$('incomingRequestMsg').style.display = 'block';
  } else {
    \$('incomingRequestMsg').style.display = 'none';
  }
  \$('incomingRequest').style.display = 'block';
});

// On remplace la dernière ligne de l'historique par la réponse donnée.
function answerHistory(answer) {
  const list = loadHistory();
  if (list.length && list[0].answer === 'none') {
    list[0].answer = answer;
    try { localStorage.setItem(HISTORY_KEY, JSON.stringify(list)); } catch (e) {}
  }
}

\$('acceptRequestBtn').addEventListener('click', () => {
  if (!incomingRequestFromId) return;
  socket.emit('call:accept', { fromId: incomingRequestFromId });
  answerHistory('ok');
  \$('incomingRequest').style.display = 'none';
  incomingRequestFromId = null;
});

\$('declineRequestBtn').addEventListener('click', () => {
  if (!incomingRequestFromId) return;
  socket.emit('call:decline', { fromId: incomingRequestFromId });
  answerHistory('no');
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
  const who = peerNames.get(id) || 'Quelqu\\'un';
  peerNames.delete(id);
  peerCards.delete(id);
  peerStates.delete(id);
  removePeer(id);
  renderPeople();
  showToast(\`\${who} a quitté l'appel\`);
  addSystemMessage(\`\${who} a quitté l'appel 👋\`);
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
  iAmHost = !!isHosting;
  callSeconds = 0;
  refreshWallButton();
  if (isHosting && isPremium()) applyWallpaper(wallpaperChoice(), wallpaperPhoto());
  else applyWallpaper(0, '');
  paintAvatarFor(\$('callAvatar'), target);
  \$('callName').textContent = isHosting ? 'Ta porte est ouverte' : target.pseudo;
  \$('callStatusLabel').textContent = isHosting ? 'En attente' : 'Connexion...';
  \$('callTimer').textContent = '00:00';
  \$('callOverlay').classList.add('active');
  \$('muteBtn').classList.remove('is-muted');
  \$('muteIc').innerHTML = icon('mic');
  \$('camBtn').classList.remove('is-on');
  \$('screenBtn').classList.remove('is-on');
  myCallState.screen = false;
  sendMyCallState();
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
  \$('muteIc').innerHTML = icon(track.enabled ? 'mic' : 'micOff');
  myCallState.muted = !track.enabled;
  sendMyCallState();
});

// -- Caméra : ajoute/retire une piste vidéo locale, renégociée automatiquement --
\$('camBtn').addEventListener('click', async () => {
  if (!inCall) return;
  if (!camOn && !isPremium()) { showToast('La vidéo est réservée à LiveDoors Plus.'); return; }
  if (!camOn) {
    try {
      const camStream = await navigator.mediaDevices.getUserMedia({ video: true });
      const videoTrack = camStream.getVideoTracks()[0];
      localStream.addTrack(videoTrack);
      ensureVideoTile('me', new MediaStream([videoTrack]));
      peers.forEach((pc) => pc.addTrack(videoTrack, localStream));
      camOn = true;
      \$('camBtn').classList.add('is-on');
      myCallState.cam = true;
      sendMyCallState();
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
    myCallState.cam = false;
    sendMyCallState();
  }
});

// -- Partage d'écran : remplace la piste vidéo envoyée par le flux d'écran --
\$('screenBtn').addEventListener('click', async () => {
  if (!inCall) return;
  if (!screenOn && !isPremium()) { showToast("Le partage d'écran est réservé à LiveDoors Plus."); return; }
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
      myCallState.screen = true;
      sendMyCallState();

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
  myCallState.screen = false;
  sendMyCallState();
}

// Quand l'hôte s'en va alors qu'il reste du monde, on lui demande quoi faire
// plutôt que de mettre tout le monde dehors sans prévenir.
function hostLeaves() {
  if (peerNames.size === 0) {
    socket.emit('door:close');
    endCall('local-close');
    return;
  }

  const rows = [];
  peerNames.forEach((name, id) => {
    rows.push({ id, name, card: peerCards.get(id) || {} });
  });

  \$('heirList').innerHTML = rows.map((r) => \`
    <button class="heir-btn" type="button" data-heir="\${escapeAttr(r.id)}">
      <span class="person-avatar">\${avatarMarkup(r.card)}</span>
      <span>\${escapeHtml(r.name)}</span>
    </button>
  \`).join('');

  Array.from(\$('heirList').children).forEach((b) => {
    b.addEventListener('click', () => {
      socket.emit('door:close', { transferTo: b.getAttribute('data-heir') });
      \$('closeModal').classList.remove('show');
      endCall('handover');
    });
  });

  \$('closeModal').classList.add('show');
}

\$('closeCancel').addEventListener('click', () => \$('closeModal').classList.remove('show'));

\$('closeEveryone').addEventListener('click', () => {
  socket.emit('door:close'); // sans destinataire : le serveur ferme la pièce
  \$('closeModal').classList.remove('show');
  endCall('local-close');
});

socket.on('door:transferred', ({ pseudo }) => {
  showToast(\`\${pseudo} reprend l'appel — ta porte est fermée.\`);
});

\$('leaveBtn').addEventListener('click', () => {
  if (iAmHost) { hostLeaves(); return; }
  socket.emit('call:leave');
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
  iAmHost = false;
  document.querySelectorAll('.video-tile.is-full').forEach((t) => t.classList.remove('is-full'));
  \$('videoGrid').classList.remove('has-full');
  peerNames.clear();
  peerCards.clear();
  peerStates.clear();
  myCallState = { muted: false, cam: false, screen: false };
  \$('peoplePanel').classList.remove('show');
  applyWallpaper(0, '');
  \$('wallPanel').classList.remove('show');
  \$('fxPanel').classList.remove('show');
  render();
}

// ---------------------------------------------------------------------------
// Fonds de salon
//
// Seul l'hôte abonné choisit le fond, et tout le monde dans l'appel le voit.
// Ce ne sont pas des images mais des dégradés CSS : rien à télécharger, et
// seul un numéro circule sur le réseau.
// ---------------------------------------------------------------------------

const WALLPAPERS = [
  { name: 'Aucun',    css: 'none' },
  { name: 'Coucher',  css: 'linear-gradient(160deg,#ff8a00,#ff3d77)' },
  { name: 'Océan',    css: 'linear-gradient(160deg,#2193b0,#6dd5ed)' },
  { name: 'Néon',     css: 'linear-gradient(160deg,#8e2de2,#4a00e0)' },
  { name: 'Forêt',    css: 'linear-gradient(160deg,#11998e,#38ef7d)' },
  { name: 'Nuit',     css: 'linear-gradient(160deg,#0f2027,#203a43,#2c5364)' },
  { name: 'Bonbon',   css: 'linear-gradient(160deg,#f797d2,#fbd786,#c6ffdd)' },
  { name: 'Braise',   css: 'linear-gradient(160deg,#f12711,#f5af19)' },
  { name: 'Damier',   css: 'repeating-linear-gradient(45deg,#2b2b3a 0 18px,#1d1d28 18px 36px)' },
  { name: 'Rayons',   css: 'repeating-conic-gradient(from 0deg,#3a1c71 0deg 18deg,#d76d77 18deg 36deg)' },
];

const WALLPAPER_KEY = 'livedoors-wallpaper';
const WALLPAPER_PHOTO_KEY = 'livedoors-wallphoto';
const CHATBG_KEY = 'livedoors-chatbg';
const MYSTICKERS_KEY = 'livedoors-mystickers';
const DISCREET_KEY = 'livedoors-discreet';
let iAmHost = false;

function wallpaperChoice() {
  try {
    const n = parseInt(localStorage.getItem(WALLPAPER_KEY) || '0', 10);
    return (!isNaN(n) && n >= 0 && n < WALLPAPERS.length) ? n : 0;
  } catch (e) { return 0; }
}
// Tout ce qui suit est conditionné à l'abonnement : si l'essai s'arrête, ces
// réglages cessent simplement de s'appliquer. On n'efface PAS les images ni
// les stickers de l'appareil — ils reviennent tels quels en cas de
// réabonnement, ce serait pénible de tout refaire.
function wallpaperPhoto() {
  if (!isPremium()) return '';
  try { return localStorage.getItem(WALLPAPER_PHOTO_KEY) || ''; } catch (e) { return ''; }
}
function chatBackground() {
  if (!isPremium()) return '';
  try { return localStorage.getItem(CHATBG_KEY) || ''; } catch (e) { return ''; }
}
function discreetMode() {
  try { return isPremium() && localStorage.getItem(DISCREET_KEY) === '1'; } catch (e) { return false; }
}
function myStickers() {
  if (!isPremium()) return [];
  try {
    const list = JSON.parse(localStorage.getItem(MYSTICKERS_KEY) || '[]');
    return Array.isArray(list) ? list.filter(isSafePhoto) : [];
  } catch (e) { return []; }
}
function saveMyStickers(list) {
  try { localStorage.setItem(MYSTICKERS_KEY, JSON.stringify(list.slice(0, 12))); return true; }
  catch (e) { return false; }
}

// Un voile sombre est posé par-dessus le fond, sinon le texte blanc de
// l'appel deviendrait illisible sur les fonds clairs (ou sur une photo).
function applyWallpaper(index, photo) {
  const overlay = \$('callOverlay');
  // Voile léger sur une photo (on veut la voir !), un peu plus marqué sur les
  // dégradés qui peuvent être très clairs.
  const scrimPhoto = 'linear-gradient(rgba(20,23,26,0.24), rgba(20,23,26,0.40))';
  const scrimGradient = 'linear-gradient(rgba(20,23,26,0.50), rgba(20,23,26,0.64))';

  if (photo && isSafePhoto(photo)) {
    overlay.style.backgroundImage = scrimPhoto + ', url("' + photo + '")';
    overlay.style.backgroundSize = 'cover';
    overlay.style.backgroundPosition = 'center';
  } else {
    const wall = WALLPAPERS[index] || WALLPAPERS[0];
    if (!index || wall.css === 'none') {
      overlay.style.backgroundImage = '';
    } else {
      overlay.style.backgroundImage = scrimGradient + ', ' + wall.css;
      overlay.style.backgroundSize = 'cover';
    }
  }

  Array.from(\$('wallGrid').children).forEach((b, i) => {
    b.classList.toggle('selected', !photo && i === index);
  });
}

// Le fond du tchat, lui, ne se voit que chez toi : rien n'est envoyé.
function applyChatBackground() {
  const bg = chatBackground();
  const panel = \$('chatPanel');
  if (bg && isSafePhoto(bg)) {
    panel.style.backgroundImage =
      'linear-gradient(rgba(20,23,26,0.86), rgba(20,23,26,0.90)), url("' + bg + '")';
    panel.style.backgroundSize = 'cover';
    panel.style.backgroundPosition = 'center';
  } else {
    panel.style.backgroundImage = '';
  }
}

function buildWallPicker() {
  const grid = \$('wallGrid');
  grid.innerHTML = '';
  WALLPAPERS.forEach((wall, i) => {
    const b = document.createElement('button');
    b.type = 'button';
    b.className = 'wall-choice';
    b.style.background = wall.css === 'none' ? '#14171a' : wall.css;
    b.innerHTML = '<span>' + wall.name + '</span>';
    b.addEventListener('click', () => {
      try {
        localStorage.setItem(WALLPAPER_KEY, String(i));
        localStorage.removeItem(WALLPAPER_PHOTO_KEY); // un dégradé remplace la photo
      } catch (e) {}
      applyWallpaper(i, '');
      socket.emit('door:wallpaper', { wallpaper: i, photo: '' });
    });
    grid.appendChild(b);
  });
}

// Le bouton fond n'apparaît que si tu es l'hôte ET abonné ; les effets sont
// accessibles à tout abonné présent dans l'appel.
function refreshWallButton() {
  const canWall = iAmHost && isPremium();
  \$('wallBtn').style.display = canWall ? 'block' : 'none';
  \$('fxBtn').style.display = (isPremium() && inCall) ? 'block' : 'none';
  if (!canWall) \$('wallPanel').classList.remove('show');
  if (!isPremium()) \$('fxPanel').classList.remove('show');
}

\$('wallBtn').addEventListener('click', () => {
  \$('fxPanel').classList.remove('show');
  \$('fxBtn').classList.remove('is-on');
  \$('peoplePanel').classList.remove('show');
  const open = \$('wallPanel').classList.toggle('show');
  \$('wallBtn').classList.toggle('is-on', open);
  closeChat();
});
\$('wallCloseBtn').addEventListener('click', () => {
  \$('wallPanel').classList.remove('show');
  \$('wallBtn').classList.remove('is-on');
});

\$('fxBtn').addEventListener('click', () => {
  \$('wallPanel').classList.remove('show');
  \$('wallBtn').classList.remove('is-on');
  \$('peoplePanel').classList.remove('show');
  const open = \$('fxPanel').classList.toggle('show');
  \$('fxBtn').classList.toggle('is-on', open);
  closeChat();
});
\$('fxCloseBtn').addEventListener('click', () => {
  \$('fxPanel').classList.remove('show');
  \$('fxBtn').classList.remove('is-on');
});

Array.from(document.querySelectorAll('.fx-choice')).forEach((b) => {
  b.addEventListener('click', () => {
    socket.emit('call:effect', { effect: b.getAttribute('data-fx') });
    \$('fxPanel').classList.remove('show');
  });
});

// ---- Effets de fête : uniquement un mot-clé sur le réseau, l'animation est
// fabriquée par chaque appareil. Confettis et pluie sont de vraies formes
// dessinées, pas des émojis : ça tombe comme il faut. ----
const CONFETTI_COLORS = ['#ff3d77','#ffe600','#26de81','#45aaf2','#a55eea','#ff8a00'];

function makeConfetti() {
  const bit = document.createElement('div');
  bit.className = 'fx-bit fx-confetti';
  bit.style.background = CONFETTI_COLORS[Math.floor(Math.random() * CONFETTI_COLORS.length)];
  bit.style.width = (5 + Math.random() * 5) + 'px';
  bit.style.height = (9 + Math.random() * 7) + 'px';
  if (Math.random() < 0.3) bit.style.borderRadius = '50%'; // quelques ronds
  return bit;
}

function makeDrop() {
  const bit = document.createElement('div');
  bit.className = 'fx-bit fx-drop';
  bit.style.height = (10 + Math.random() * 14) + 'px';
  bit.style.opacity = String(0.35 + Math.random() * 0.5);
  return bit;
}

function makeGlyph(chars) {
  const bit = document.createElement('div');
  bit.className = 'fx-bit';
  bit.textContent = chars[Math.floor(Math.random() * chars.length)];
  bit.style.fontSize = (14 + Math.random() * 18) + 'px';
  return bit;
}

const FX_STYLES = {
  confetti:  { make: makeConfetti, anim: 'fxTumble', dur: 2.8, count: 46 },
  rain:      { make: makeDrop, anim: 'fxRain', dur: 1.1, count: 60 },
  hearts:    { make: () => makeGlyph(['💖','💗','❤️','💜','💛']), anim: 'fxRise', dur: 3.0, count: 30 },
  fireworks: { make: () => makeGlyph(['✨','⭐','💫','🌟']), anim: 'fxRise', dur: 2.4, count: 30 },
};

function playEffect(kind) {
  const fx = FX_STYLES[kind] || FX_STYLES.confetti;
  const zone = \$('reactionZone');

  for (let i = 0; i < fx.count; i++) {
    const bit = fx.make();
    bit.style.left = Math.random() * 96 + '%';
    if (fx.anim === 'fxRise') { bit.style.top = 'auto'; bit.style.bottom = '40px'; }
    const spread = fx.anim === 'fxRain' ? 1.2 : 0.9;
    bit.style.animation = fx.anim + ' ' + (fx.dur + Math.random() * 0.8) + 's '
      + (fx.anim === 'fxRain' ? 'linear ' : 'ease-in ')
      + (Math.random() * spread) + 's forwards';
    zone.appendChild(bit);
    setTimeout(() => bit.remove(), (fx.dur + spread + 1.6) * 1000);
  }
}

socket.on('call:effect', ({ effect, from }) => {
  playEffect(effect);
  addSystemMessage((from || 'Quelqu\\'un') + ' a envoyé un effet 🎉');
});

socket.on('call:wallpaper', ({ wallpaper, photo }) => applyWallpaper(wallpaper || 0, photo || ''));

socket.on('call:host-changed', ({ hostId }) => {
  iAmHost = !!(me && hostId === me.id);
  refreshWallButton();
  if (iAmHost) {
    showToast("Tu es maintenant l'hôte de l'appel.");
    addSystemMessage("Tu es maintenant l'hôte de l'appel 🔑");
  }
});

socket.on('call:room-state', ({ wallpaper, wallpaperPhoto }) => {
  applyWallpaper(wallpaper || 0, wallpaperPhoto || '');
});

// ---------------------------------------------------------------------------
// Tchat écrit pendant l'appel (+ émojis)
//
// Les messages ne circulent qu'entre les personnes présentes dans l'appel, et
// rien n'est conservé : quand l'appel se termine, le tchat est vidé.
// ---------------------------------------------------------------------------

const CHAT_EMOJIS = [
  '😀','😃','😄','😁','😆','😅','🤣','😂',
  '🙂','🙃','😉','😊','😇','🥰','😍','🤩',
  '😘','😗','😚','😙','😋','😛','😜','🤪',
  '🤨','🧐','🤓','😎','🥸','🤯','😳','🥵',
  '😴','🤤','😪','😵','🤐','🥴','🤢','🤮',
  '😢','😭','😤','😠','😡','🤬','😱','😨',
  '👍','👎','👊','✊','🤛','🤜','👏','🙌',
  '🙏','🤝','💪','🖐️','✌️','🤞','🤙','👋',
  '❤️','🧡','💛','💚','💙','💜','🖤','💔',
  '🔥','💯','⭐','🌟','✨','⚡','🌈','☀️',
  '🎉','🎊','🎁','🎂','🍕','🍔','🍟','🍩',
  '⚽','🏀','🎮','🎧','🎸','🚀','👾','💀',
];

// Les stickers voyagent comme du texte : "::st:7::". Chaque appareil le
// redessine en grand de son côté, donc rien de lourd ne transite.
const STICKERS = [
  { emoji: '👋', bg: '#ffd166', label: 'Coucou' },
  { emoji: '😂', bg: '#8ecae6', label: 'Mdr' },
  { emoji: '❤️', bg: '#ffadad', label: 'Coeur' },
  { emoji: '👍', bg: '#a7e8a0', label: 'OK' },
  { emoji: '🔥', bg: '#ff8a00', label: 'Feu' },
  { emoji: '🎉', bg: '#bdb2ff', label: 'Fête' },
  { emoji: '😴', bg: '#c8d5b9', label: 'Dodo' },
  { emoji: '🤔', bg: '#ffc6ff', label: 'Hmm' },
  { emoji: '😭', bg: '#9bf6ff', label: 'Snif' },
  { emoji: '💀', bg: '#e0e0e0', label: 'Mort' },
  { emoji: '🚀', bg: '#ffb4a2', label: 'Go' },
  { emoji: '🍕', bg: '#ffe66d', label: 'Faim' },
  { emoji: '🎮', bg: '#b5ead7', label: 'Jeu' },
  { emoji: '👾', bg: '#c7ceea', label: 'Bug' },
  { emoji: '💯', bg: '#ffdac1', label: 'Top' },
  { emoji: '🙏', bg: '#e2f0cb', label: 'Merci' },
];

const STICKER_PREFIX = '::st:';

function stickerIndex(text) {
  const t = String(text || '');
  if (t.indexOf(STICKER_PREFIX) !== 0 || t.slice(-2) !== '::') return -1;
  const n = parseInt(t.slice(STICKER_PREFIX.length, -2), 10);
  return (!isNaN(n) && n >= 0 && n < STICKERS.length) ? n : -1;
}

function stickerMarkup(index) {
  const st = STICKERS[index];
  return '<div class="sticker" style="background:' + st.bg + '">' + st.emoji + '</div>';
}

let chatOpen = false;
let emojiPanelOpen = false;
let panelTab = 'emoji';
let unreadCount = 0;

function buildEmojiBar() {
  const grid = \$('emojiGrid');
  grid.innerHTML = '';
  CHAT_EMOJIS.forEach((emo) => {
    const b = document.createElement('button');
    b.type = 'button';
    b.textContent = emo;
    // Petit point jaune : cet émoji a un mouvement à lui (clin d'œil, cœur
    // qui bat...). Sans repère, impossible de savoir lesquels bougent.
    if (ANIMATED_SET[emo]) {
      b.classList.add('has-move');
      b.title = 'Cet émoji est animé';
    }
    b.addEventListener('click', () => {
      const input = \$('chatInput');
      input.value = (input.value + emo).slice(0, 200);
      input.focus();
    });
    grid.appendChild(b);
  });

  const sticks = \$('stickerGrid');
  sticks.innerHTML = '';

  // Les stickers perso passent en premier.
  myStickers().forEach((data, i) => {
    const b = document.createElement('button');
    b.type = 'button';
    b.className = 'sticker-choice custom';
    b.style.backgroundImage = 'url("' + data + '")';
    b.title = 'Sticker perso (appui long pour supprimer)';
    b.addEventListener('click', () => {
      if (!inCall) { showToast("Le tchat ne marche que pendant un appel."); return; }
      socket.emit('chat:message', { text: '::mine::', sticker: data });
    });
    let timer = null;
    const startHold = () => { timer = setTimeout(() => removeMySticker(i), 700); };
    const cancelHold = () => { if (timer) clearTimeout(timer); };
    b.addEventListener('pointerdown', startHold);
    b.addEventListener('pointerup', cancelHold);
    b.addEventListener('pointerleave', cancelHold);
    sticks.appendChild(b);
  });

  if (isPremium()) {
    const add = document.createElement('button');
    add.type = 'button';
    add.className = 'sticker-add';
    add.textContent = '＋';
    add.title = 'Ajouter un sticker depuis mes photos';
    add.addEventListener('click', () => \$('myStickerInput').click());
    sticks.appendChild(add);
  }

  STICKERS.forEach((st, i) => {
    const b = document.createElement('button');
    b.type = 'button';
    b.className = 'sticker-choice';
    b.style.background = st.bg;
    b.textContent = st.emoji;
    b.title = st.label;
    b.addEventListener('click', () => {
      if (!inCall) { showToast("Le tchat ne marche que pendant un appel."); return; }
      socket.emit('chat:message', { text: STICKER_PREFIX + i + '::' });
    });
    sticks.appendChild(b);
  });
}

function setPanelTab(tab) {
  panelTab = tab;
  \$('emojiGrid').style.display = tab === 'emoji' ? 'grid' : 'none';
  \$('stickerGrid').style.display = tab === 'sticker' ? 'grid' : 'none';
  \$('tabEmoji').classList.toggle('active', tab === 'emoji');
  \$('tabSticker').classList.toggle('active', tab === 'sticker');
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
  \$('wallPanel').classList.remove('show');
  \$('fxPanel').classList.remove('show');
  \$('peoplePanel').classList.remove('show');
  \$('wallBtn').classList.remove('is-on');
  \$('fxBtn').classList.remove('is-on');
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
\$('tabEmoji').addEventListener('click', () => setPanelTab('emoji'));
\$('tabSticker').addEventListener('click', () => setPanelTab('sticker'));
\$('chatCloseBtn').addEventListener('click', closeChat);
\$('chatSendBtn').addEventListener('click', sendChat);
\$('chatInput').addEventListener('keydown', (e) => {
  if (e.key === 'Enter') { e.preventDefault(); sendChat(); }
});

socket.on('chat:message', (msg) => {
  addChatMessage(msg);
  const stick = stickerIndex(msg.text);
  if (stick >= 0) floatEmoji(STICKERS[stick].emoji);
  else if (isEmojiOnly(msg.text)) floatEmoji(msg.text);

  const fromMe = me && msg.fromId === me.id;
  if (!chatOpen && !fromMe) {
    unreadCount++;
    updateChatBadge();
    const idx = stickerIndex(msg.text);
    showToast(msg.pseudo + ' : ' + (idx >= 0 ? STICKERS[idx].emoji : msg.text));
  }
});

// Ligne grise au milieu du tchat : arrivées, départs, changement d'hôte.
function addSystemMessage(text) {
  const box = \$('chatMessages');
  const empty = box.querySelector('.chat-empty');
  if (empty) empty.remove();

  const line = document.createElement('div');
  line.className = 'chat-system-line';
  line.textContent = text;
  box.appendChild(line);
  box.scrollTop = box.scrollHeight;
}

// À chaque émoji son geste : le clin d'œil cligne, le cœur bat, le rire
// se secoue. On regarde le premier émoji du message pour choisir.
const EMOJI_MOVES = {
  '😉': 'fx-wink', '😜': 'fx-wink', '😝': 'fx-wink', '🥴': 'fx-wink',
  '❤️': 'fx-beat', '💖': 'fx-beat', '💗': 'fx-beat', '🧡': 'fx-beat',
  '💛': 'fx-beat', '💚': 'fx-beat', '💙': 'fx-beat', '💜': 'fx-beat', '💔': 'fx-beat',
  '😂': 'fx-laugh', '🤣': 'fx-laugh', '😆': 'fx-laugh', '😅': 'fx-laugh', '😄': 'fx-laugh',
  '😴': 'fx-sleep', '😪': 'fx-sleep', '🤤': 'fx-sleep',
  '🔥': 'fx-flame', '⚡': 'fx-flame', '💥': 'fx-flame',
  '🎉': 'fx-spin', '🎊': 'fx-spin', '🤯': 'fx-spin', '💫': 'fx-spin', '🌟': 'fx-spin',
  '👍': 'fx-jump', '👏': 'fx-jump', '🙌': 'fx-jump', '💪': 'fx-jump', '🥳': 'fx-jump',
  '😡': 'fx-shake', '😠': 'fx-shake', '🤬': 'fx-shake', '😱': 'fx-shake', '😨': 'fx-shake',
  '👋': 'fx-shake', '😢': 'fx-sleep', '😭': 'fx-shake',
};

function emojiMove(text) {
  const t = String(text).trim();
  for (const key of Object.keys(EMOJI_MOVES)) {
    if (t.indexOf(key) === 0) return EMOJI_MOVES[key];
  }
  return 'fx-pop'; // pas d'animation dédiée : juste un rebond à l'arrivée
}

// ---------------------------------------------------------------------------
// Émojis vraiment animés
//
// Un émoji écrit en texte est UNE image figée : on ne peut pas animer sa
// paupière toute seule. Pour qu'un clin d'œil cligne vraiment, il faut une
// petite animation dessinée. On utilise celles de Google (Noto Emoji
// Animations, libres d'utilisation), chargées à la demande.
//
// Toutes n'existent pas, et il faut du réseau : si l'image ne charge pas, on
// remet l'émoji normal avec le mouvement CSS. Rien ne casse.
// ---------------------------------------------------------------------------

const NOTO_ANIM = 'https://fonts.gstatic.com/s/e/notoemoji/latest/';

// Les émojis pour lesquels une animation existe.
const ANIMATED_EMOJIS = [
  '😀','😃','😄','😁','😆','😅','🤣','😂','🙂','🙃','😉','😊','😇',
  '🥰','😍','🤩','😘','😗','😚','😙','😋','😛','😜','🤪','😝',
  '🤨','🧐','🤓','😎','🥸','🤯','😳','🥵','😴','🤤','😪','🤐','🥴',
  '🤢','🤮','😢','😭','😤','😠','😡','🤬','😱','😨','😰','😥',
  '👍','👎','👏','🙌','🙏','🤝','💪','👋','✌️','🤞','🤙',
  '❤️','🧡','💛','💚','💙','💜','🖤','💔','💯','🔥','⚡','✨','🌈',
  '🎉','🎊','🎁','🎂','🚀','👾','💀','⭐','🌟','💫','☀️','🍕',
];

const ANIMATED_SET = {};
ANIMATED_EMOJIS.forEach((e) => { ANIMATED_SET[e] = true; });

// L'adresse de l'animation se construit à partir du code du caractère :
// 😉 devient 1f609, ❤️ devient 2764_fe0f.
function notoCode(emoji) {
  const parts = [];
  for (const ch of emoji) parts.push(ch.codePointAt(0).toString(16));
  return parts.join('_');
}

// Découpe "😉❤️" en ['😉', '❤️'] : un émoji peut occuper plusieurs
// caractères (couleur de peau, sélecteur de variante, liaison).
function splitEmoji(text) {
  const out = [];
  let cur = '';
  for (const ch of String(text).trim()) {
    const c = ch.codePointAt(0);
    const attaches = (c === 0xFE0F) || (c === 0x200D) || (c === 0x20E3)
      || (c >= 0x1F3FB && c <= 0x1F3FF);
    if (!cur) { cur = ch; continue; }
    if (attaches || cur.charCodeAt(cur.length - 1) === 0x200D) cur += ch;
    else { out.push(cur); cur = ch; }
  }
  if (cur) out.push(cur);
  return out;
}

// Construit l'émoji animé, avec repli automatique si l'image ne vient pas.
function animatedEmojiNode(text) {
  const pieces = splitEmoji(text).slice(0, 3);
  const holder = document.createElement('div');
  holder.className = 'emoji-anim-row';

  pieces.forEach((piece) => {
    if (!ANIMATED_SET[piece]) {
      const span = document.createElement('span');
      span.className = 'emoji-anim-fallback ' + emojiMove(piece);
      span.textContent = piece;
      holder.appendChild(span);
      return;
    }
    const img = document.createElement('img');
    img.className = 'emoji-anim';
    img.alt = piece;
    img.src = NOTO_ANIM + notoCode(piece) + '/512.gif';
    img.addEventListener('error', () => {
      const span = document.createElement('span');
      span.className = 'emoji-anim-fallback ' + emojiMove(piece);
      span.textContent = piece;
      if (img.parentNode) img.parentNode.replaceChild(span, img);
    });
    holder.appendChild(img);
  });

  return holder;
}

function addChatMessage(msg) {
  const box = \$('chatMessages');
  const empty = box.querySelector('.chat-empty');
  if (empty) empty.remove();

  const mine = me && msg.fromId === me.id;
  const sticker = stickerIndex(msg.text);
  const custom = msg.sticker && isSafePhoto(msg.sticker) ? msg.sticker : '';

  const wrap = document.createElement('div');
  wrap.className = mine ? 'chat-msg mine' : 'chat-msg';

  const author = document.createElement('div');
  author.className = 'chat-msg-author';
  author.textContent = mine ? 'Toi' : msg.pseudo;
  wrap.appendChild(author);

  if (custom) {
    const img = document.createElement('img');
    img.className = 'sticker-img';
    img.src = custom;
    img.alt = 'sticker';
    wrap.appendChild(img);
  } else if (sticker >= 0) {
    const holder = document.createElement('div');
    holder.innerHTML = stickerMarkup(sticker);
    wrap.appendChild(holder.firstChild);
  } else {
    const emojiOnly = isEmojiOnly(msg.text);

    // Abonné + message tout en émojis : on affiche les vraies animations.
    if (emojiOnly && msg.premium) {
      const holder = animatedEmojiNode(msg.text);
      wrap.appendChild(holder);
    } else {
      const bubble = document.createElement('div');
      bubble.className = emojiOnly ? 'chat-bubble big-emoji' : 'chat-bubble';
      if (emojiOnly) bubble.classList.add('fx-pop');
      bubble.textContent = msg.text; // textContent : impossible d'injecter du HTML
      wrap.appendChild(bubble);
    }
  }

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
