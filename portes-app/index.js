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
const fs = require('fs');
const path = require('path');
const { randomUUID, randomBytes, scryptSync, timingSafeEqual } = require('crypto');
const { Server } = require('socket.io');

// ---------------------------------------------------------------------------
// Base de données des comptes
//
// Deux modes, choisis tout seuls :
//   • si la variable DATABASE_URL existe -> PostgreSQL (la vraie base)
//   • sinon -> un simple fichier JSON à côté du programme
//
// Le fichier JSON suffit pour essayer sur son ordinateur. En ligne sur Render,
// il faut PostgreSQL : le disque y est effacé à chaque mise à jour du code,
// donc les comptes payants seraient perdus à chaque déploiement.
// ---------------------------------------------------------------------------

const DATA_FILE = process.env.DATA_FILE || path.join(__dirname, 'livedoors-data.json');
const DATABASE_URL = process.env.DATABASE_URL || '';

const db = DATABASE_URL ? createPostgresStore(DATABASE_URL) : createFileStore(DATA_FILE);

function createFileStore(file) {
  let data = { accounts: {}, messages: [] };
  try {
    if (fs.existsSync(file)) data = JSON.parse(fs.readFileSync(file, 'utf8')) || data;
  } catch (e) {
    console.warn('Fichier de données illisible, on repart à vide :', e.message);
  }
  if (!data.accounts) data.accounts = {};
  if (!data.messages) data.messages = [];

  let writing = false;
  function persist() {
    if (writing) return;
    writing = true;
    setTimeout(() => {
      writing = false;
      try {
        fs.writeFileSync(file, JSON.stringify(data, null, 2));
      } catch (e) {
        console.warn('Écriture impossible :', e.message);
      }
    }, 50); // on groupe les écritures rapprochées
  }

  return {
    kind: 'fichier',
    async init() {
      console.log('Base : fichier local ->', file);
    },
    async getAccount(phoneKey) {
      return data.accounts[phoneKey] || null;
    },
    async findByUsername(username) {
      const key = Object.keys(data.accounts)
        .find((k) => data.accounts[k].username === username);
      return key ? data.accounts[key] : null;
    },
    async findByCustomer(customerId) {
      const key = Object.keys(data.accounts)
        .find((k) => data.accounts[k].stripeCustomerId === customerId);
      return key ? data.accounts[key] : null;
    },
    async addMessage(msg) {
      data.messages.push(msg);
      // On garde les 5000 derniers : au-delà, le fichier deviendrait énorme.
      if (data.messages.length > 5000) data.messages = data.messages.slice(-5000);
      persist();
      return msg;
    },
    async getMessages(a, b, limit) {
      return data.messages
        .filter((m) => (m.from === a && m.to === b) || (m.from === b && m.to === a))
        .slice(-limit);
    },
    async markRead(me, other) {
      let n = 0;
      data.messages.forEach((m) => {
        if (m.to === me && m.from === other && !m.read) {
          m.read = true;
          m.readAt = Date.now(); // point de départ des 24 h
          n++;
        }
      });
      if (n) persist();
      return n;
    },
    async getStreak(paire) {
      if (!data.streaks) data.streaks = {};
      return data.streaks[paire] || null;
    },
    async saveStreak(paire, valeur) {
      if (!data.streaks) data.streaks = {};
      data.streaks[paire] = valeur;
      persist();
      return valeur;
    },
    async purgeMessages(apresLecture, siNonLu) {
      const avant = data.messages.length;
      const t = Date.now();
      data.messages = data.messages.filter((m) => (
        (m.read && m.readAt) ? (t - m.readAt <= apresLecture) : (t - m.at <= siNonLu)
      ));
      if (data.messages.length !== avant) persist();
      return avant - data.messages.length;
    },
    async unreadFor(me) {
      const counts = {};
      data.messages.forEach((m) => {
        if (m.to === me && !m.read) counts[m.from] = (counts[m.from] || 0) + 1;
      });
      return counts;
    },
    async saveAccount(acc) {
      data.accounts[acc.phoneKey] = acc;
      persist();
      return acc;
    },
  };
}

function createPostgresStore(url) {
  // `pg` n'est chargé que dans ce mode : pas besoin de l'installer pour
  // essayer l'appli sur son ordinateur.
  const { Pool } = require('pg');
  const pool = new Pool({
    connectionString: url,
    ssl: url.includes('localhost') ? false : { rejectUnauthorized: false },
  });

  function fromRow(row) {
    if (!row) return null;
    return {
      phoneKey: row.phone_key,
      phone: row.phone,
      username: row.username || '',
      passHash: row.pass_hash,
      premium: row.premium,
      premiumUntil: row.premium_until ? new Date(row.premium_until).getTime() : null,
      stripeCustomerId: row.stripe_customer_id || null,
      callSeconds: row.call_seconds || 0,
      bonusPoints: row.bonus_points || 0,
      lastBonusAt: row.last_bonus_at ? new Date(row.last_bonus_at).getTime() : null,
      hostedCalls: row.hosted_calls || 0,
      nightCalls: row.night_calls || 0,
      longestCall: row.longest_call || 0,
      bestStreak: row.best_streak || 0,
      usernameChangedAt: row.username_changed_at ? new Date(row.username_changed_at).getTime() : null,
      createdAt: new Date(row.created_at).getTime(),
    };
  }

  return {
    kind: 'postgres',
    async init() {
      await pool.query(`
        CREATE TABLE IF NOT EXISTS accounts (
          phone_key     TEXT PRIMARY KEY,
          phone         TEXT,
          username      TEXT,
          pass_hash     TEXT NOT NULL,
          premium       BOOLEAN NOT NULL DEFAULT FALSE,
          premium_until TIMESTAMPTZ,
          stripe_customer_id TEXT,
          created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
      `);
      // Pour les bases créées avant l'ajout du paiement.
      await pool.query('ALTER TABLE accounts ADD COLUMN IF NOT EXISTS stripe_customer_id TEXT');
      await pool.query('ALTER TABLE accounts ADD COLUMN IF NOT EXISTS call_seconds INTEGER NOT NULL DEFAULT 0');
      await pool.query('ALTER TABLE accounts ADD COLUMN IF NOT EXISTS bonus_points INTEGER NOT NULL DEFAULT 0');
      await pool.query('ALTER TABLE accounts ADD COLUMN IF NOT EXISTS last_bonus_at TIMESTAMPTZ');
      await pool.query('ALTER TABLE accounts ADD COLUMN IF NOT EXISTS hosted_calls INTEGER NOT NULL DEFAULT 0');
      await pool.query('ALTER TABLE accounts ADD COLUMN IF NOT EXISTS night_calls INTEGER NOT NULL DEFAULT 0');
      await pool.query('ALTER TABLE accounts ADD COLUMN IF NOT EXISTS longest_call INTEGER NOT NULL DEFAULT 0');
      await pool.query('ALTER TABLE accounts ADD COLUMN IF NOT EXISTS best_streak INTEGER NOT NULL DEFAULT 0');
      await pool.query('ALTER TABLE accounts ADD COLUMN IF NOT EXISTS username_changed_at TIMESTAMPTZ');
      await pool.query(`
        CREATE TABLE IF NOT EXISTS messages (
          id         TEXT PRIMARY KEY,
          from_key   TEXT NOT NULL,
          to_key     TEXT NOT NULL,
          body       TEXT NOT NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          image      TEXT,
          read       BOOLEAN NOT NULL DEFAULT FALSE
        )
      `);
      await pool.query('ALTER TABLE messages ADD COLUMN IF NOT EXISTS image TEXT');
      await pool.query('ALTER TABLE messages ADD COLUMN IF NOT EXISTS read_at TIMESTAMPTZ');
      await pool.query('ALTER TABLE messages ADD COLUMN IF NOT EXISTS delivered BOOLEAN NOT NULL DEFAULT FALSE');
      await pool.query('ALTER TABLE messages ADD COLUMN IF NOT EXISTS audio TEXT');
      await pool.query(`
        CREATE TABLE IF NOT EXISTS streaks (
          pair_key      TEXT PRIMARY KEY,
          days          INTEGER NOT NULL DEFAULT 0,
          last_day      TEXT,
          seconds_today INTEGER NOT NULL DEFAULT 0
        )
      `);
      await pool.query('CREATE INDEX IF NOT EXISTS messages_pair ON messages (from_key, to_key, created_at)');
      await pool.query('CREATE UNIQUE INDEX IF NOT EXISTS accounts_username ON accounts (username) WHERE username <> \'\'');
      console.log('Base : PostgreSQL');
    },
    async getAccount(phoneKey) {
      const r = await pool.query('SELECT * FROM accounts WHERE phone_key = $1', [phoneKey]);
      return fromRow(r.rows[0]);
    },
    async findByUsername(username) {
      const r = await pool.query('SELECT * FROM accounts WHERE username = $1', [username]);
      return fromRow(r.rows[0]);
    },
    async findByCustomer(customerId) {
      const r = await pool.query('SELECT * FROM accounts WHERE stripe_customer_id = $1', [customerId]);
      return fromRow(r.rows[0]);
    },
    async addMessage(msg) {
      await pool.query(
        'INSERT INTO messages (id, from_key, to_key, body, image, audio, created_at, read, delivered) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)',
        [msg.id, msg.from, msg.to, msg.text, msg.image || null, msg.audio || null, new Date(msg.at), false, !!msg.delivered],
      );
      return msg;
    },
    async getMessages(a, b, limit) {
      const r = await pool.query(`
        SELECT * FROM messages
        WHERE (from_key = $1 AND to_key = $2) OR (from_key = $2 AND to_key = $1)
        ORDER BY created_at DESC LIMIT $3
      `, [a, b, limit]);
      return r.rows.reverse().map((row) => ({
        id: row.id,
        from: row.from_key,
        to: row.to_key,
        text: row.body,
        image: row.image || '',
        audio: row.audio || '',
        delivered: !!row.delivered,
        at: new Date(row.created_at).getTime(),
        read: row.read,
      }));
    },
    async markRead(me, other) {
      const r = await pool.query(
        'UPDATE messages SET read = TRUE, read_at = NOW() WHERE to_key = $1 AND from_key = $2 AND read = FALSE',
        [me, other],
      );
      return r.rowCount;
    },
    async getStreak(paire) {
      const r = await pool.query('SELECT * FROM streaks WHERE pair_key = $1', [paire]);
      const row = r.rows[0];
      return row ? { days: row.days, lastDay: row.last_day, secondsToday: row.seconds_today } : null;
    },
    async saveStreak(paire, v) {
      await pool.query(`
        INSERT INTO streaks (pair_key, days, last_day, seconds_today)
        VALUES ($1,$2,$3,$4)
        ON CONFLICT (pair_key) DO UPDATE SET
          days = EXCLUDED.days, last_day = EXCLUDED.last_day, seconds_today = EXCLUDED.seconds_today
      `, [paire, v.days, v.lastDay, v.secondsToday]);
      return v;
    },
    async purgeMessages(apresLecture, siNonLu) {
      const r = await pool.query(`
        DELETE FROM messages
        WHERE (read = TRUE AND read_at IS NOT NULL AND read_at < NOW() - ($1::bigint * INTERVAL '1 millisecond'))
           OR (read = FALSE AND created_at < NOW() - ($2::bigint * INTERVAL '1 millisecond'))
      `, [apresLecture, siNonLu]);
      return r.rowCount;
    },
    async unreadFor(me) {
      const r = await pool.query(
        'SELECT from_key, COUNT(*)::int AS n FROM messages WHERE to_key = $1 AND read = FALSE GROUP BY from_key',
        [me],
      );
      const counts = {};
      r.rows.forEach((row) => { counts[row.from_key] = row.n; });
      return counts;
    },
    async saveAccount(acc) {
      await pool.query(`
        INSERT INTO accounts (phone_key, phone, username, pass_hash, premium, premium_until, stripe_customer_id, call_seconds, bonus_points, last_bonus_at, hosted_calls, night_calls, longest_call, best_streak, username_changed_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
        ON CONFLICT (phone_key) DO UPDATE SET
          phone = EXCLUDED.phone,
          username = EXCLUDED.username,
          pass_hash = EXCLUDED.pass_hash,
          premium = EXCLUDED.premium,
          premium_until = EXCLUDED.premium_until,
          stripe_customer_id = EXCLUDED.stripe_customer_id,
          call_seconds = EXCLUDED.call_seconds,
          bonus_points = EXCLUDED.bonus_points,
          last_bonus_at = EXCLUDED.last_bonus_at,
          hosted_calls = EXCLUDED.hosted_calls,
          night_calls = EXCLUDED.night_calls,
          longest_call = EXCLUDED.longest_call,
          best_streak = EXCLUDED.best_streak,
          username_changed_at = EXCLUDED.username_changed_at
      `, [
        acc.phoneKey, acc.phone, acc.username || '', acc.passHash,
        !!acc.premium, acc.premiumUntil ? new Date(acc.premiumUntil) : null,
        acc.stripeCustomerId || null, acc.callSeconds || 0,
        acc.bonusPoints || 0, acc.lastBonusAt ? new Date(acc.lastBonusAt) : null,
        acc.hostedCalls || 0, acc.nightCalls || 0, acc.longestCall || 0, acc.bestStreak || 0,
        acc.usernameChangedAt ? new Date(acc.usernameChangedAt) : null,
      ]);
      return acc;
    },
  };
}

// ---------------------------------------------------------------------------
// Mot de passe
//
// On n'enregistre JAMAIS le code lui-même, seulement une empreinte calculée
// avec scrypt (lent volontairement : cela rend les essais en masse inutiles)
// et un "sel" différent pour chaque compte.
// ---------------------------------------------------------------------------

function hashSecret(secret) {
  const salt = randomBytes(16).toString('hex');
  const hash = scryptSync(String(secret), salt, 32).toString('hex');
  return salt + ':' + hash;
}

function checkSecret(secret, stored) {
  try {
    const [salt, hash] = String(stored).split(':');
    if (!salt || !hash) return false;
    const test = scryptSync(String(secret), salt, 32);
    const ref = Buffer.from(hash, 'hex');
    return test.length === ref.length && timingSafeEqual(test, ref);
  } catch (e) {
    return false;
  }
}

// Un abonnement expiré ne compte plus, même s'il est encore marqué actif.
function accountIsPremium(acc) {
  if (!acc || !acc.premium) return false;
  if (acc.premiumUntil && Date.now() > acc.premiumUntil) return false;
  return true;
}

// ---------------------------------------------------------------------------
// Paiement (Stripe)
//
// Rien ne s'active tant que les clés ne sont pas là : sans elles, l'appli
// fonctionne comme avant avec le bouton d'essai. Il faut renseigner, dans les
// variables d'environnement de l'hébergeur :
//
//   STRIPE_SECRET_KEY      la clé secrète du compte      (sk_...)
//   STRIPE_PRICE_ID        le tarif créé dans Stripe     (price_...)
//   STRIPE_WEBHOOK_SECRET  la clé de l'écouteur          (whsec_...)
//   PUBLIC_URL             l'adresse publique de l'appli
//
// ⚠️ L'abonnement n'est JAMAIS accordé par le navigateur ni au retour de la
// page de paiement : uniquement quand Stripe prévient le serveur directement
// (le "webhook"). Sinon il suffirait de recopier l'adresse de retour pour
// s'abonner gratuitement.
// ---------------------------------------------------------------------------

const STRIPE_SECRET_KEY = process.env.STRIPE_SECRET_KEY || '';
const STRIPE_PRICE_ID = process.env.STRIPE_PRICE_ID || '';
const STRIPE_WEBHOOK_SECRET = process.env.STRIPE_WEBHOOK_SECRET || '';
const PUBLIC_URL = process.env.PUBLIC_URL || '';
const PRICE_LABEL = process.env.STRIPE_PRICE_LABEL || '2,99 €/mois';

const paymentsOn = !!(STRIPE_SECRET_KEY && STRIPE_PRICE_ID && STRIPE_WEBHOOK_SECRET && PUBLIC_URL);
const stripe = paymentsOn ? require('stripe')(STRIPE_SECRET_KEY) : null;

// Met à jour un compte d'après ce que dit Stripe. Volontairement séparé du
// reste : cette fonction ne connaît pas Stripe, elle applique juste un état,
// ce qui la rend simple à vérifier.
async function findAccountByCustomer(customerId) {
  if (!customerId) return null;
  return db.findByCustomer ? db.findByCustomer(customerId) : null;
}

async function applySubscription({ phoneKey, active, until, customerId }) {
  if (!phoneKey) return null;
  const account = await db.getAccount(phoneKey);
  if (!account) return null;

  account.premium = !!active;
  account.premiumUntil = active ? (until || null) : null;
  if (customerId) account.stripeCustomerId = customerId;
  grantMonthlyBonus(account); // 200 points offerts à l'abonnement
  await db.saveAccount(account);

  // Si la personne est connectée, son écran se met à jour tout de suite.
  for (const user of users.values()) {
    if (user.phoneKey === phoneKey) {
      user.premium = accountIsPremium(account);
      if (!user.premium) { user.discreet = false; user.vipOnly = false; }
      user.points = pointsOf(account);
      io.to(user.id).emit('premium:update', {
        premium: user.premium,
        until: account.premiumUntil,
        points: user.points,
        badge: badgeLevel(user.points),
      });
    }
  }
  broadcastFriends();
  return account;
}

// ---------------------------------------------------------------------------
// Durée de vie des messages
//
// Comme sur Snapchat, les conversations ne s'accumulent pas indéfiniment :
//   • un message lu disparaît 24 h après sa lecture
//   • un message jamais lu est gardé 7 jours, puis abandonné
// Le ménage tourne toutes les heures, et à chaque ouverture d'une conversation.
// ---------------------------------------------------------------------------

const KEEP_AFTER_READ = 24 * 3600000;   // 24 heures
const KEEP_IF_UNREAD = 7 * 86400000;    // 7 jours

function messageExpired(m) {
  const maintenant = Date.now();
  if (m.read && m.readAt) return maintenant - m.readAt > KEEP_AFTER_READ;
  return maintenant - m.at > KEEP_IF_UNREAD;
}

async function sweepMessages() {
  try {
    if (db.purgeMessages) await db.purgeMessages(KEEP_AFTER_READ, KEEP_IF_UNREAD);
  } catch (e) {}
}
setInterval(sweepMessages, 3600000);

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
    points: u.points || 0,
    badge: badgeLevel(u.points || 0),
    title: (u.titles || []).indexOf(u.title) !== -1 ? u.title : '', // jamais un titre non débloqué
    skin: u.skin || 0,
    friendCount: u.showFriends ? u.contacts.size : null,
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

// Un nom d'utilisateur ne se change qu'une fois par mois — sauf abonnement.
// Sans cette limite, quelqu'un pourrait changer d'identité en boucle et
// devenir impossible à suivre pour ses contacts.
const USERNAME_DELAY = 30 * 86400000;

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
const DM_IMAGE_MAX = 120000;       // photo envoyée dans une conversation
const DM_AUDIO_MAX = 300000;       // mot vocal (environ 20 secondes)

// Skins de porte, débloqués avec les points.
const SKINS = [
  { id: 0, nom: 'Bois classique', cout: 0 },
  { id: 1, nom: 'Chêne foncé',    cout: 10 },
  { id: 2, nom: 'Néon',           cout: 50 },
  { id: 3, nom: 'Blindée',        cout: 100 },
  { id: 4, nom: 'Futuriste',      cout: 250 },
  { id: 5, nom: 'Or massif',      cout: 1000 },
];
function skinAllowed(skin, points) {
  const s = SKINS.find((x) => x.id === Number(skin));
  return !!(s && points >= s.cout);
}

function cleanAudio(value) {
  const raw = String(value || '');
  if (!raw || raw.length > DM_AUDIO_MAX) return '';
  const ok = raw.indexOf('data:audio/') === 0 && raw.indexOf(';base64,') > 0;
  return ok ? raw : '';
}

// Un point par tranche de 10 minutes d'appel.
const SECONDS_PER_POINT = 600;
const BADGE_STEPS = [10, 25, 50, 100, 200, 500, 1000, 2500, 5000, 10000];

const PREMIUM_BONUS = 200;         // points offerts chaque mois aux abonnés
const BONUS_PERIOD = 30 * 86400000;

function pointsOf(account) {
  if (!account) return 0;
  const gagnes = Math.floor((account.callSeconds || 0) / SECONDS_PER_POINT);
  return gagnes + (account.bonusPoints || 0);
}

// Verse les 200 points mensuels si le compte est abonné et que le dernier
// versement date de plus de 30 jours. Renvoie true si des points ont été
// ajoutés (le compte doit alors être enregistré).
function grantMonthlyBonus(account) {
  if (!accountIsPremium(account)) return false;
  const dernier = account.lastBonusAt || 0;
  if (Date.now() - dernier < BONUS_PERIOD) return false;
  account.bonusPoints = (account.bonusPoints || 0) + PREMIUM_BONUS;
  account.lastBonusAt = Date.now();
  return true;
}
// Titres débloqués par les statistiques du compte.
const TITRES = [
  { id: 'nuit',      nom: 'Oiseau de nuit',   test: (a) => (a.nightCalls || 0) >= 10 },
  { id: 'hote',      nom: 'Hôte parfait',     test: (a) => (a.hostedCalls || 0) >= 25 },
  { id: 'marathon',  nom: 'Marathonien vocal',test: (a) => (a.longestCall || 0) >= 3600 },
  { id: 'fidele',    nom: 'Fidèle',           test: (a) => (a.bestStreak || 0) >= 7 },
  { id: 'or',        nom: "Clé d'or",         test: (a) => (a.bestStreak || 0) >= 30 },
  { id: 'pilier',    nom: 'Pilier du salon',  test: (a) => (a.callSeconds || 0) >= 36000 },
];

function titlesOf(account) {
  if (!account) return [];
  return TITRES.filter((t) => t.test(account)).map((t) => t.id);
}

function badgeLevel(points) {
  let n = 0;
  BADGE_STEPS.forEach((seuil) => { if (points >= seuil) n++; });
  return n; // 0 = aucun badge, 10 = badge maximum
}
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

  socket.on('register', async ({ pseudo, username, avatarInitials, avatarColor, avatarPhoto, phone, pass, contacts, blocked, vipOnly, vip, discreet, showFriends, keys, title, skin }) => {
    // Se réenregistrer sert aussi à modifier son profil : on referme d'abord
    // proprement porte et appel en cours, sinon une room fantôme resterait
    // ouverte côté serveur avec des participants coincés dedans.
    if (users.has(socket.id)) {
      closeDoorAndRoom(socket.id);
      leaveCurrentRoom(socket.id);
    }

    const phoneKey = phone ? normalizePhone(phone) : null;
    const wanted = cleanUsername(username);

    if (!phoneKey) {
      socket.emit('auth:error', { message: 'Numéro de téléphone invalide.' });
      return;
    }
    if (!pass) {
      socket.emit('auth:error', { message: 'Code secret manquant.' });
      return;
    }

    let account;
    try {
      account = await db.getAccount(phoneKey);

      // Le nom d'utilisateur doit être unique — y compris à la création du
      // compte, sinon deux personnes pouvaient prendre le même.
      if (wanted && (!account || wanted !== account.username)) {
        const taken = await db.findByUsername(wanted);
        if (taken && taken.phoneKey !== phoneKey) {
          socket.emit('auth:error', { message: 'Ce nom d\'utilisateur est déjà pris.' });
          return;
        }
      }

      if (!account) {
        // Premier passage : on crée le compte avec ce code.
        account = {
          phoneKey,
          phone: String(phone).slice(0, 32),
          username: wanted,
          usernameChangedAt: wanted ? Date.now() : null,
          passHash: hashSecret(pass),
          premium: false,
          premiumUntil: null,
          createdAt: Date.now(),
        };
        await db.saveAccount(account);
      } else if (!checkSecret(pass, account.passHash)) {
        // Ce numéro appartient déjà à quelqu'un, et le code ne correspond pas.
        socket.emit('auth:error', {
          message: 'Ce numéro est déjà utilisé avec un autre code secret.',
        });
        return;
      } else if (wanted && wanted !== account.username) {
        // Changement de nom d'utilisateur : une fois par mois, sauf abonné.
        const dernier = account.usernameChangedAt || 0;
        const reste = USERNAME_DELAY - (Date.now() - dernier);

        if (!accountIsPremium(account) && dernier && reste > 0) {
          const jours = Math.ceil(reste / 86400000);
          socket.emit('profile:error', {
            champ: 'username',
            actuel: account.username,
            message: "Tu ne peux changer de nom d'utilisateur qu'une fois par mois. "
              + 'Encore ' + jours + ' jour' + (jours > 1 ? 's' : '') + ' à attendre '
              + '(ou passe à LiveDoors Plus).',
          });
          // On garde l'ancien nom et on continue : la connexion n'est pas bloquée.
        } else {
          account.username = wanted;
          account.usernameChangedAt = Date.now();
          await db.saveAccount(account);
        }
      }
    } catch (e) {
      console.error('Base de données indisponible :', e.message);
      socket.emit('auth:error', { message: 'Service momentanément indisponible.' });
      return;
    }

    // L'abonnement vient de la base, JAMAIS du navigateur : c'est tout
    // l'intérêt de cette étape.
    const premium = accountIsPremium(account);

    // Les 200 points mensuels de l'abonnement, versés au plus une fois par mois.
    if (grantMonthlyBonus(account)) {
      try { await db.saveAccount(account); } catch (e) {}
    }

    const user = {
      id: socket.id,
      pseudo: String(pseudo || 'Anonyme').slice(0, 24),
      username: account.username || '',
      avatarInitials: String(avatarInitials || pseudo || '??').slice(0, 4),
      avatarColor: avatarColor || '#ff8a00',
      avatarPhoto: cleanPhoto(avatarPhoto),
      phone: String(phone).slice(0, 32),
      phoneKey,
      doorOpen: false,
      doorMessage: '',
      roomId: null,
      premium,
      points: pointsOf(account),
      callSeconds: account.callSeconds || 0,
      titles: titlesOf(account),
      usernameChangedAt: account.usernameChangedAt || null,
      title: cleanUsername(title).slice(0, 12),
      // Un skin non débloqué est simplement ignoré : le serveur décide.
      skin: skinAllowed(skin, pointsOf(account)) ? Number(skin) : 0,
      showFriends: showFriends !== false, // visible par défaut
      vipOnly: !!vipOnly,
      discreet: premium && !!discreet, // le mode discret fait partie de l'abonnement
      contacts: new Set(),
      blocked: new Set(),
      vip: new Set(),
      keys: new Set(), // les amis à qui j'ai donné un double des clés
    };

    if (Array.isArray(keys)) {
      keys.slice(0, 300).forEach((p) => {
        const k = normalizePhone(p);
        if (k) user.keys.add(k);
      });
    }

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
    user.titles = titlesOf(account);
    socket.emit('registered', {
      ...publicUser(user),
      titles: user.titles,
      stats: {
        nightCalls: account.nightCalls || 0,
        hostedCalls: account.hostedCalls || 0,
        longestCall: account.longestCall || 0,
        bestStreak: account.bestStreak || 0,
        callSeconds: account.callSeconds || 0,
      },
      paiement: paymentsOn,
      prix: PRICE_LABEL,
      premiumUntil: account.premiumUntil || null,
    });
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
    user.callStartedAt = Date.now(); // pour compter les points
    user.wasHost = true;
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

  // Ouvrir la page de paiement Stripe. Le serveur y attache la clé du compte
  // (client_reference_id) : c'est ce qui permettra au webhook de savoir qui
  // vient de payer.
  socket.on('premium:checkout', async () => {
    const user = users.get(socket.id);
    if (!user || !user.phoneKey) return;
    if (!paymentsOn) {
      socket.emit('call:error', { message: 'Le paiement n\'est pas encore activé.' });
      return;
    }

    try {
      const account = await db.getAccount(user.phoneKey);
      const session = await stripe.checkout.sessions.create({
        mode: 'subscription',
        line_items: [{ price: STRIPE_PRICE_ID, quantity: 1 }],
        client_reference_id: user.phoneKey,
        customer: account && account.stripeCustomerId ? account.stripeCustomerId : undefined,
        success_url: PUBLIC_URL + '/?paiement=ok',
        cancel_url: PUBLIC_URL + '/?paiement=annule',
        allow_promotion_codes: true,
      });
      socket.emit('premium:checkout-url', { url: session.url });
    } catch (e) {
      console.error('Stripe checkout :', e.message);
      socket.emit('call:error', { message: 'Impossible d\'ouvrir le paiement.' });
    }
  });

  // Page Stripe pour changer de carte ou résilier soi-même.
  socket.on('premium:manage', async () => {
    const user = users.get(socket.id);
    if (!user || !user.phoneKey || !paymentsOn) return;

    try {
      const account = await db.getAccount(user.phoneKey);
      if (!account || !account.stripeCustomerId) {
        socket.emit('call:error', { message: 'Aucun abonnement payant sur ce compte.' });
        return;
      }
      const portal = await stripe.billingPortal.sessions.create({
        customer: account.stripeCustomerId,
        return_url: PUBLIC_URL,
      });
      socket.emit('premium:checkout-url', { url: portal.url });
    } catch (e) {
      socket.emit('call:error', { message: 'Espace de gestion indisponible.' });
    }
  });

  // Activer / arrêter l'abonnement.
  //
  // ⚠️ PROVISOIRE : aujourd'hui n'importe qui peut appeler ça gratuitement.
  // Le jour où le paiement sera branché, cette fonction disparaîtra : seul le
  // prestataire de paiement (via son webhook) aura le droit de rendre un
  // compte abonné. Le reste du code n'aura pas à changer, puisqu'il lit déjà
  // l'abonnement dans la base.
  socket.on('premium:trial', async ({ on }) => {
    const user = users.get(socket.id);
    if (!user || !user.phoneKey) return;
    if (paymentsOn) {
      // Dès que le paiement est branché, l'essai gratuit à volonté disparaît.
      socket.emit('call:error', { message: 'Passe par la page d\'abonnement.' });
      return;
    }

    try {
      const account = await db.getAccount(user.phoneKey);
      if (!account) return;

      account.premium = !!on;
      // Un essai dure 30 jours ; ensuite il faudra un vrai paiement.
      account.premiumUntil = on ? Date.now() + 30 * 86400000 : null;
      await db.saveAccount(account);

      user.premium = accountIsPremium(account);
      if (!user.premium) { user.discreet = false; user.vipOnly = false; }

      // Points offerts dès l'activation.
      let offerts = false;
      if (grantMonthlyBonus(account)) { await db.saveAccount(account); offerts = true; }
      user.points = pointsOf(account);

      socket.emit('premium:update', {
        premium: user.premium,
        until: account.premiumUntil,
        points: user.points,
        badge: badgeLevel(user.points),
        bonus: offerts ? PREMIUM_BONUS : 0,
      });
      broadcastFriends();
    } catch (e) {
      socket.emit('call:error', { message: 'Impossible de modifier l\'abonnement.' });
    }
  });

  // ---- Messages privés, hors appel ----
  // Ils sont enregistrés en base : la personne les recevra même si elle
  // n'était pas connectée au moment de l'envoi.
  socket.on('dm:send', async ({ toPhone, text, image, audio }) => {
    const me = users.get(socket.id);
    if (!me || !me.phoneKey) return;

    const toKey = normalizePhone(toPhone);
    const clean = String(text || '').trim().slice(0, 800);
    const photo = cleanImage(image, DM_IMAGE_MAX);
    const voix = cleanAudio(audio);
    if (!toKey || toKey === me.phoneKey) return;
    if (!clean && !photo && !voix) return;

    // On respecte les blocages, dans les deux sens.
    const cible = Array.from(users.values()).find((u) => u.phoneKey === toKey);
    if (me.blocked.has(toKey) || (cible && cible.blocked.has(me.phoneKey))) {
      socket.emit('call:error', { message: 'Message impossible.' });
      return;
    }

    const msg = {
      id: randomUUID(),
      from: me.phoneKey,
      to: toKey,
      text: clean,
      image: photo,
      audio: voix,
      at: Date.now(),
      delivered: false,
      read: false,
    };

    try {
      await db.addMessage(msg);
    } catch (e) {
      socket.emit('call:error', { message: 'Message non enregistré.' });
      return;
    }

    // Remis tout de suite si la personne est connectée.
    if (cible) msg.delivered = true;

    socket.emit('dm:new', { ...msg, withKey: toKey, mine: true });
    if (cible) {
      io.to(cible.id).emit('dm:new', {
        ...msg,
        withKey: me.phoneKey,
        mine: false,
        pseudo: me.pseudo,
        phone: me.phone,
      });
    }
  });

  socket.on('dm:history', async ({ withPhone }) => {
    const me = users.get(socket.id);
    if (!me || !me.phoneKey) return;
    const other = normalizePhone(withPhone);
    if (!other) return;
    try {
      await sweepMessages(); // on enlève d'abord ce qui a expiré
      const list = await db.getMessages(me.phoneKey, other, 100);
      const lus = await db.markRead(me.phoneKey, other);

      // L'expéditeur voit ses messages passer en « lu ».
      if (lus > 0) {
        const expediteur = Array.from(users.values()).find((u) => u.phoneKey === other);
        if (expediteur) io.to(expediteur.id).emit('dm:read', { withKey: me.phoneKey });
      }
      socket.emit('dm:history', { withKey: other, messages: list });
      socket.emit('dm:unread', await db.unreadFor(me.phoneKey));
    } catch (e) {}
  });

  // Rendre visible ou non le nombre d'amis sur son profil.
  socket.on('profile:showFriends', ({ on }) => {
    const user = users.get(socket.id);
    if (!user) return;
    user.showFriends = !!on;
    broadcastFriends();
  });

  socket.on('dm:unread', async () => {
    const me = users.get(socket.id);
    if (!me || !me.phoneKey) return;
    try { socket.emit('dm:unread', await db.unreadFor(me.phoneKey)); } catch (e) {}
  });

  // Inviter quelqu'un pendant l'appel. On ne donne pas l'identifiant du
  // salon à n'importe qui : l'invitation part du serveur, et seul un contact
  // de la personne invitée peut l'atteindre.
  socket.on('call:invite', ({ toId }) => {
    const me = users.get(socket.id);
    const target = users.get(toId);
    if (!me || !target || !me.roomId) return;

    const room = rooms.get(me.roomId);
    if (!room) return;
    if (room.memberIds.has(target.id)) {
      socket.emit('call:error', { message: 'Cette personne est déjà dans l\'appel.' });
      return;
    }
    if (room.memberIds.size >= roomLimitFor()) {
      socket.emit('call:error', { message: 'Le salon est complet.' });
      return;
    }
    // Blocage : dans un sens comme dans l'autre, on ne dérange pas.
    if ((me.phoneKey && target.blocked.has(me.phoneKey))
      || (target.phoneKey && me.blocked.has(target.phoneKey))) {
      socket.emit('call:error', { message: 'Invitation impossible.' });
      return;
    }

    const host = users.get(room.hostId);
    io.to(target.id).emit('call:invitation', {
      hostId: room.hostId,
      hostPseudo: host ? host.pseudo : 'Quelqu\'un',
      from: publicUser(me),
      people: room.memberIds.size,
    });
    socket.emit('call:invite-sent', { pseudo: target.pseudo });
  });

  // ---- Sondages en direct ----
  // Le sondage vit dans la pièce : il se ferme tout seul quand tout le monde
  // a voté, ou au bout d'une minute trente si quelqu'un ne répond pas.
  socket.on('poll:start', ({ question, options }) => {
    const user = users.get(socket.id);
    if (!user || !user.roomId) return;
    const room = rooms.get(user.roomId);
    if (!room) return;
    if (room.poll) { socket.emit('call:error', { message: 'Un sondage est déjà en cours.' }); return; }

    const q = String(question || '').trim().slice(0, 80);
    const opts = (Array.isArray(options) ? options : [])
      .map((o) => String(o || '').trim().slice(0, 40))
      .filter(Boolean)
      .slice(0, POLL_MAX_OPTIONS);
    if (!q || opts.length < 2) return;

    room.poll = {
      question: q,
      options: opts,
      votes: {},
      auteur: socket.id,
      auteurPseudo: user.pseudo,
      fin: Date.now() + POLL_DURATION,
    };
    room.poll.timer = setTimeout(() => closePoll(user.roomId), POLL_DURATION);

    io.to(user.roomId).emit('poll:show', {
      question: q,
      options: opts,
      par: user.pseudo,
      auteur: socket.id === user.id,
      resultats: opts.map(() => 0),
      fin: room.poll.fin,
    });
  });

  socket.on('poll:vote', ({ index }) => {
    const user = users.get(socket.id);
    if (!user || !user.roomId) return;
    const room = rooms.get(user.roomId);
    if (!room || !room.poll) return;

    const i = Number(index);
    if (!(i >= 0 && i < room.poll.options.length)) return;

    room.poll.votes[socket.id] = i; // un seul vote par personne, modifiable

    const resultats = room.poll.options.map(() => 0);
    Object.values(room.poll.votes).forEach((v) => { resultats[v]++; });
    io.to(user.roomId).emit('poll:results', {
      resultats,
      votants: Object.keys(room.poll.votes).length,
      total: room.memberIds.size,
    });

    // Tout le monde a voté : inutile d'attendre la fin du temps.
    if (Object.keys(room.poll.votes).length >= room.memberIds.size) {
      closePoll(user.roomId);
    }
  });

  socket.on('poll:close', () => {
    const user = users.get(socket.id);
    if (!user || !user.roomId) return;
    const room = rooms.get(user.roomId);
    if (!room || !room.poll) return;
    if (room.poll.auteur !== socket.id) {
      socket.emit('call:error', { message: 'Seul celui qui a lancé le sondage peut le terminer.' });
      return;
    }
    closePoll(user.roomId);
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
    // Double des clés : la personne entre sans avoir à toquer.
    if (me.phoneKey && host.keys.has(me.phoneKey)) {
      socket.emit('call:accepted', { hostId, avecCle: true });
      io.to(hostId).emit('call:key-used', { ...publicUser(me) });
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
    me.callStartedAt = Date.now();
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
  creditCallTime(user);

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
      dropPoll(roomId);
    rooms.delete(roomId);
    }
  }

  user.doorOpen = false;
  user.roomId = null;
}

// ---------------------------------------------------------------------------
// Séries de portes (les « flammes »)
//
// Deux amis qui se parlent au moins 5 minutes dans la même journée font
// avancer leur série d'un cran. Un jour sauté et la série repart de zéro.
// ---------------------------------------------------------------------------

const STREAK_MIN_SECONDS = 300; // 5 minutes

// Plus la série est longue, plus l'appel rapporte : +10 % par jour de série,
// plafonné à deux fois plus de points (série de 10 jours ou davantage).
function streakMultiplier(jours) {
  return 1 + Math.min(jours || 0, 10) * 0.1;
}

function pairKey(a, b) {
  return [a, b].sort().join('|');
}
function today() {
  return new Date().toISOString().slice(0, 10); // AAAA-MM-JJ
}
function hier() {
  return new Date(Date.now() - 86400000).toISOString().slice(0, 10);
}

async function addSharedTime(keyA, keyB, secondes) {
  if (!keyA || !keyB || keyA === keyB || secondes < 1) return;
  const paire = pairKey(keyA, keyB);

  try {
    const actuel = (await db.getStreak(paire)) || { days: 0, lastDay: null, secondsToday: 0 };
    const jour = today();

    // Le compteur du jour repart à zéro à chaque nouvelle journée.
    if (actuel.countedDay !== jour) actuel.secondsToday = 0;
    actuel.countedDay = jour;
    actuel.secondsToday += secondes;

    let progresse = false;
    if (actuel.secondsToday >= STREAK_MIN_SECONDS && actuel.lastDay !== jour) {
      actuel.days = (actuel.lastDay === hier()) ? actuel.days + 1 : 1;
      actuel.lastDay = jour;
      progresse = true;
    }

    await db.saveStreak(paire, actuel);

    if (progresse) {
      for (const u of users.values()) {
        if (u.phoneKey === keyA || u.phoneKey === keyB) {
          io.to(u.id).emit('streak:update', {
            withKey: u.phoneKey === keyA ? keyB : keyA,
            days: actuel.days,
          });
        }
      }
      // On note le record personnel, utile pour les titres.
      for (const cle of [keyA, keyB]) {
        const compte = await db.getAccount(cle);
        if (compte && (compte.bestStreak || 0) < actuel.days) {
          compte.bestStreak = actuel.days;
          await db.saveAccount(compte);
        }
      }
    }
  } catch (e) {}
}

// Ajoute le temps passé en appel au compte, et prévient si un badge est
// débloqué. Un point par tranche de 10 minutes.
async function creditCallTime(user) {
  if (!user || !user.callStartedAt || !user.phoneKey) return;
  const secondes = Math.floor((Date.now() - user.callStartedAt) / 1000);
  const depart = user.callStartedAt;
  user.callStartedAt = null;
  if (secondes < 5) return; // on ignore les appels ratés

  // Temps réellement partagé avec chacun des autres participants : c'est ce
  // qui fait avancer les séries. On retient au passage la plus longue série,
  // qui donnera le bonus de points.
  const room = user.roomId ? rooms.get(user.roomId) : null;
  let meilleureSerie = 0;
  if (room) {
    for (const id of room.memberIds) {
      const autre = users.get(id);
      if (!autre || autre.id === user.id || !autre.phoneKey) continue;
      const communDepuis = Math.max(depart, autre.callStartedAt || depart);
      const ensemble = Math.floor((Date.now() - communDepuis) / 1000);
      if (ensemble > 0) addSharedTime(user.phoneKey, autre.phoneKey, ensemble);
      try {
        const serie = await db.getStreak(pairKey(user.phoneKey, autre.phoneKey));
        if (serie && serie.days > meilleureSerie) meilleureSerie = serie.days;
      } catch (e) {}
    }
  }

  try {
    const account = await db.getAccount(user.phoneKey);
    if (!account) return;

    const avant = badgeLevel(pointsOf(account));
    account.callSeconds = (account.callSeconds || 0) + secondes;

    // Bonus de série : les points en plus sont ajoutés à part, pour que le
    // temps d'appel réel reste exact dans les statistiques.
    const mult = streakMultiplier(meilleureSerie);
    const enPlus = Math.floor((secondes / SECONDS_PER_POINT) * (mult - 1));
    if (enPlus > 0) account.bonusPoints = (account.bonusPoints || 0) + enPlus;
    await db.saveAccount(account);

    // Statistiques servant aux titres
    const heure = new Date().getHours();
    if (heure >= 22 || heure < 6) account.nightCalls = (account.nightCalls || 0) + 1;
    if (user.wasHost) account.hostedCalls = (account.hostedCalls || 0) + 1;
    if (secondes > (account.longestCall || 0)) account.longestCall = secondes;
    await db.saveAccount(account);

    const points = pointsOf(account);
    const apres = badgeLevel(points);
    user.points = points;
    user.callSeconds = account.callSeconds;

    io.to(user.id).emit('points:update', {
      points,
      badge: apres,
      seconds: account.callSeconds,
      nouveau: apres > avant,
      serie: meilleureSerie,
      multiplicateur: mult,
      bonus: enPlus,
    });
    broadcastFriends();
  } catch (e) {}
}

// ---------------------------------------------------------------------------
// Sondages : fermeture et annonce du résultat
// ---------------------------------------------------------------------------

const POLL_MAX_OPTIONS = 10;
const POLL_DURATION = 90000; // 1 min 30

// Annule le minuteur d'un sondage quand la pièce disparaît.
function dropPoll(roomId) {
  const room = rooms.get(roomId);
  if (room && room.poll && room.poll.timer) clearTimeout(room.poll.timer);
}

function closePoll(roomId) {
  const room = rooms.get(roomId);
  if (!room || !room.poll) return;

  const poll = room.poll;
  if (poll.timer) clearTimeout(poll.timer);
  room.poll = null;

  const resultats = poll.options.map(() => 0);
  Object.values(poll.votes).forEach((v) => { resultats[v]++; });

  // Le maximum peut être atteint par plusieurs réponses : on les garde toutes.
  const max = Math.max(...resultats);
  const gagnants = max > 0
    ? poll.options.filter((o, i) => resultats[i] === max)
    : [];

  io.to(roomId).emit('poll:end', {
    question: poll.question,
    options: poll.options,
    resultats,
    gagnants,
    max,
    votants: Object.keys(poll.votes).length,
  });

  // Le détail nominatif ne part qu'à celui qui a lancé le sondage.
  const detail = Object.keys(poll.votes).map((id) => {
    const u = users.get(id);
    return { pseudo: u ? u.pseudo : 'Parti', reponse: poll.options[poll.votes[id]] };
  });
  io.to(poll.auteur).emit('poll:detail', { question: poll.question, detail });
}

function closeDoorAndRoom(socketId) {
  { const u = users.get(socketId); if (u) creditCallTime(u); }
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
    dropPoll(user.roomId);
  rooms.delete(user.roomId);
  }

  // On ferme la porte mais on GARDE le petit mot : il doit rester visible
  // pour les contacts tant que la personne est connectée.
  user.doorOpen = false;
  user.roomId = null;
}

function leaveCurrentRoom(socketId) {
  { const u = users.get(socketId); if (u) creditCallTime(u); }
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
    if (room.memberIds.size === 0) { dropPoll(roomId); rooms.delete(roomId); }
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
/* Pastille dorée sur l'avatar : plus discret et plus « vraie appli »
   qu'une étoile collée devant le nom. */
.fav-dot{
  position:absolute; right:-2px; bottom:-2px; z-index:2;
  width:17px; height:17px; border-radius:50%;
  background:#f2b705; color:#fff; border:2px solid var(--bg);
  display:flex; align-items:center; justify-content:center;
}
.fav-dot svg{ fill:currentColor; width:9px; height:9px; }
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

.audio-sink{
  position:absolute; width:1px; height:1px; overflow:hidden;
  opacity:0; pointer-events:none; left:0; bottom:0;
}
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
/* ---------- Salle d'attente ---------- */
#waitingRoom{
  display:none; position:absolute; inset:0; z-index:25;
  background:rgba(20,23,26,0.94); backdrop-filter:blur(8px);
  flex-direction:column; align-items:center; justify-content:center; gap:14px; padding:28px;
}
#waitingRoom.show{ display:flex; }
.wait-door{
  width:96px; height:132px; border-radius:8px 8px 4px 4px; position:relative;
  background:linear-gradient(160deg,#c98b3a,#8a5a20);
  box-shadow:0 12px 30px -12px rgba(0,0,0,0.8);
  animation:doorWobble 2.4s ease-in-out infinite;
}
.wait-panel{
  position:absolute; inset:12px 12px 30px; border-radius:4px;
  border:2px solid rgba(0,0,0,0.22);
}
.wait-knock{
  position:absolute; right:14px; top:66px; width:11px; height:11px;
  border-radius:50%; background:#ffe600; box-shadow:0 0 12px #ffe600;
  animation:knockPulse 1.2s ease-in-out infinite;
}
@keyframes doorWobble{
  0%,100%{ transform:rotate(-1.5deg); }
  50%{ transform:rotate(1.5deg); }
}
@keyframes knockPulse{
  0%,100%{ transform:scale(1); opacity:0.55; }
  50%{ transform:scale(1.5); opacity:1; }
}
.wait-text{
  font-family:'Baloo 2', sans-serif; font-weight:700; font-size:16px; color:#fff;
  text-align:center;
}
.wait-sub{ font-size:12px; color:rgba(255,255,255,0.6); font-weight:700; }
#waitCancel{ max-width:180px; }

/* ---------- Flammes et titres ---------- */
.streak-chip{
  display:inline-flex; align-items:center; gap:3px; margin-left:5px;
  font-size:11px; font-weight:800; color:#ff8a00;
  font-family:'Baloo 2', sans-serif; vertical-align:middle;
}
.title-chip{
  display:inline-block; margin-left:5px; padding:2px 7px; border-radius:999px;
  font-size:9.5px; font-weight:800; font-family:'Baloo 2', sans-serif;
  background:var(--bg-soft); color:var(--ink-soft); border:1px solid var(--border);
  vertical-align:middle;
}
.title-list{ display:flex; flex-wrap:wrap; gap:6px; margin-top:4px; }
.title-opt{
  cursor:pointer; border-radius:999px; padding:7px 12px;
  border:1px solid var(--border); background:transparent; color:var(--ink);
  font-family:'Baloo 2', sans-serif; font-weight:700; font-size:11.5px;
}
.title-opt.on{ background:var(--yellow); border-color:transparent; color:#14171a; }
.title-opt.locked{ opacity:0.45; cursor:not-allowed; }

#callInvite{
  display:none; z-index:3; width:100%; max-width:320px; margin-top:14px;
  background:rgba(255,255,255,0.10); border:1px solid rgba(255,255,255,0.14);
  border-radius:20px; padding:14px; backdrop-filter:blur(10px);
  animation:reqIn 0.3s cubic-bezier(.2,1.3,.4,1);
}
.invite-btn{
  margin-left:auto; flex:none; cursor:pointer; border:none; border-radius:10px;
  padding:7px 12px; background:var(--yellow); color:#14171a;
  font-family:'Baloo 2', sans-serif; font-weight:700; font-size:11.5px;
}
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
.header-avatar-wrap{ position:relative; display:inline-flex; }
/* Cadenas violet : rappelle en permanence que la porte est en mode privé. */
.private-badge{
  display:none; position:absolute; right:-3px; bottom:-3px;
  width:19px; height:19px; border-radius:50%;
  background:#9b51e0; color:#fff; border:2px solid var(--yellow);
  align-items:center; justify-content:center;
  box-shadow:0 2px 6px -2px rgba(0,0,0,0.5);
}
.private-badge.show{ display:flex; }
.private-badge svg{ width:11px; height:11px; }
.me-avatar, .avatar, .call-avatar, .lock-avatar{ overflow:hidden; }

/* ---------- Ligne de contact : bouton principal + menu ---------- */
.contact-actions{ display:flex; gap:6px; margin-top:7px; align-items:center; }
.row-btn{
  display:flex; align-items:center; gap:6px; cursor:pointer; position:relative;
  border:1px solid var(--border); background:var(--bg); color:var(--ink);
  border-radius:11px; padding:7px 12px;
  font-family:'Baloo 2', sans-serif; font-weight:700; font-size:12px;
}
.row-btn:hover{ background:var(--bg-soft); }
.row-badge{
  min-width:17px; height:17px; border-radius:9px; background:#ff3d77; color:#fff;
  font-size:10px; line-height:17px; padding:0 5px; text-align:center;
}
.row-more{
  width:32px; height:32px; border-radius:10px; cursor:pointer; padding:0;
  border:1px solid var(--border); background:transparent; color:var(--ink-faint);
  display:flex; align-items:center; justify-content:center;
}
.row-more:hover{ background:var(--bg-soft); color:var(--ink); }

/* ---------- Menu d'un contact (feuille du bas) ---------- */
.sheet-name{
  font-family:'Baloo 2', sans-serif; font-weight:700; font-size:16px;
  color:var(--ink); margin-bottom:2px;
}
.sheet-sub{ font-size:12px; color:var(--ink-faint); margin-bottom:14px; }
.sheet-item{
  width:100%; display:flex; align-items:center; gap:11px; cursor:pointer;
  border:none; background:transparent; color:var(--ink); text-align:left;
  border-radius:12px; padding:13px 12px;
  font-family:'Baloo 2', sans-serif; font-weight:700; font-size:13.5px;
}
.sheet-item:hover{ background:var(--bg-soft); }
.sheet-item .ic{ display:flex; color:var(--ink-soft); }
.sheet-item.on .ic{ color:#e0a800; }
.sheet-item.on .ic svg{ fill:currentColor; }
.sheet-item.key-on .ic{ color:#d4a017; }
.sheet-item.close-on .ic{ color:#e6398b; }
.sheet-item.close-on .ic svg{ fill:currentColor; }
.sheet-item.danger{ color:#c0143c; }
.sheet-item.danger .ic{ color:#c0143c; }

/* ---------- Conversation ---------- */
.dm-screen{
  position:absolute; inset:0; z-index:35; background:var(--bg);
  display:none; flex-direction:column;
}
.dm-screen.show{ display:flex; }
.dm-head{
  display:flex; align-items:center; gap:11px; padding:14px 16px;
  background:var(--yellow); flex-shrink:0;
}
.dm-back{
  width:34px; height:34px; border-radius:50%; border:none; cursor:pointer;
  background:rgba(0,0,0,0.08); color:#14171a;
  display:flex; align-items:center; justify-content:center; flex-shrink:0;
}
.dm-avatar{ width:38px; height:38px; border-radius:50%; overflow:hidden; flex:none;
  display:flex; align-items:center; justify-content:center;
  font-family:'Baloo 2', sans-serif; font-weight:700; color:#fff; font-size:14px; }
.dm-title{ font-family:'Baloo 2', sans-serif; font-weight:700; font-size:15px; color:#14171a; }
.dm-sub{ font-size:11.5px; font-weight:700; color:rgba(0,0,0,0.55); }
.dm-note{
  text-align:center; font-size:10.5px; font-weight:700; color:var(--ink-faint);
  padding:8px 14px 0; flex-shrink:0;
}
.dm-list{
  flex:1; overflow-y:auto; padding:16px; display:flex; flex-direction:column; gap:8px;
}
.dm-msg{ max-width:78%; display:flex; flex-direction:column; gap:2px; }
.dm-msg.mine{ align-self:flex-end; align-items:flex-end; }
.dm-bubble{
  background:var(--bg-soft); color:var(--ink); font-size:14px; font-weight:600;
  padding:10px 13px; border-radius:16px; border-bottom-left-radius:5px; word-break:break-word;
}
.dm-msg.mine .dm-bubble{
  background:var(--yellow); color:#14171a;
  border-bottom-left-radius:16px; border-bottom-right-radius:5px;
}
.dm-time{
  font-size:10px; color:var(--ink-faint); padding:0 5px;
  display:flex; align-items:center; gap:4px;
}
.dm-tick{ display:inline-flex; color:var(--ink-faint); }
.dm-tick.lu{ color:#d4a017; }
.dm-input-row{
  display:flex; gap:8px; padding:12px 14px;
  padding-bottom:calc(12px + env(safe-area-inset-bottom));
  border-top:1px solid var(--border); flex-shrink:0;
}
.dm-input{
  /* min-width:0 : sans lui, un champ de saisie refuse de descendre sous sa
     largeur naturelle et pousse le bouton d'envoi hors de l'écran. */
  flex:1; min-width:0; padding:12px 14px; border-radius:14px; border:1px solid var(--border);
  background:var(--bg); color:var(--ink);
  font-family:'Nunito', sans-serif; font-size:14px; font-weight:600;
}
.dm-input:focus{ outline:2px solid var(--yellow); }
.dm-send{
  /* flex:none indispensable : sans lui le bouton se faisait écraser par le
     champ de texte et finissait coupé sur les petits écrans. */
  flex:none; width:48px; height:48px; border-radius:50%; border:none; cursor:pointer;
  background:var(--yellow); color:#14171a;
  display:flex; align-items:center; justify-content:center;
}
.dm-send:active{ transform:scale(0.94); }
.dm-extra{ height:44px; }

/* ---------- Barre d'enregistrement vocal ---------- */
.dm-rec-row{
  display:none; align-items:center; gap:10px;
  padding:12px 14px; padding-bottom:calc(12px + env(safe-area-inset-bottom));
  border-top:1px solid var(--border);
}
.dm-rec-row.show{ display:flex; }
.dm-rec-info{
  flex:1; display:flex; align-items:center; gap:8px;
  font-family:'JetBrains Mono', monospace; font-size:15px; font-weight:600; color:var(--ink);
}
.dm-rec-hint{ font-family:'Nunito', sans-serif; font-size:12px; color:var(--ink-faint); font-weight:700; }
.dm-rec-dot{
  width:11px; height:11px; border-radius:50%; background:#e63946;
  animation:recPulse 1s ease-in-out infinite; flex:none;
}
.dm-rec-btn{
  flex:none; width:48px; height:48px; border-radius:50%; border:none; cursor:pointer;
  display:flex; align-items:center; justify-content:center;
}
.dm-rec-btn.cancel{ background:var(--bg-soft); color:#c0143c; }
.dm-rec-btn.send{ background:#e63946; color:#fff; }
.dm-rec-btn:active{ transform:scale(0.94); }

.dm-extra{
  width:42px; border-radius:14px; cursor:pointer; flex:none;
  border:1px solid var(--border); background:var(--bg); color:var(--ink-soft);
  display:flex; align-items:center; justify-content:center;
}
.dm-extra:hover{ background:var(--bg-soft); color:var(--ink); }
.dm-extra.is-on{ background:var(--yellow); color:#14171a; border-color:transparent; }
.dm-stickers{
  display:none; grid-template-columns:repeat(6, 1fr); gap:6px;
  padding:10px 14px; border-top:1px solid var(--border);
  max-height:170px; overflow-y:auto;
}
.dm-stickers.show{ display:grid; }
.dm-stickers button{
  aspect-ratio:1; border:none; border-radius:12px; cursor:pointer;
  font-size:24px; line-height:1; padding:0; background-size:cover; background-position:center;
}
.dm-image{
  max-width:220px; border-radius:16px; display:block; cursor:pointer;
  -webkit-touch-callout:none;
}
.dm-sticker{
  width:96px; height:96px; border-radius:20px; display:flex;
  align-items:center; justify-content:center; font-size:52px;
}
.dm-sticker-img{ width:96px; height:96px; border-radius:20px; object-fit:cover; display:block; }

/* ---------- Mot vocal ---------- */
.dm-extra.recording{ background:#e63946; color:#fff; border-color:transparent; animation:recPulse 1s ease-in-out infinite; }
@keyframes recPulse{ 0%,100%{ opacity:1; } 50%{ opacity:0.55; } }
.dm-audio{ display:flex; align-items:center; gap:8px; }
.dm-audio audio{ height:36px; max-width:200px; }

/* ---------- Sondage ---------- */
.poll-add{
  width:100%; margin-top:6px; cursor:pointer; border-radius:11px; padding:9px;
  border:1px dashed rgba(255,255,255,0.3); background:transparent; color:rgba(255,255,255,0.75);
  font-family:'Baloo 2', sans-serif; font-weight:700; font-size:12px;
}
.poll-add:disabled{ opacity:0.4; cursor:not-allowed; }
.poll-note{ font-size:10.5px; color:rgba(255,255,255,0.5); font-weight:700; margin-top:8px; text-align:center; }
.poll-row{ display:flex; gap:6px; margin-top:6px; }
.poll-row .chat-input{ flex:1; min-width:0; }
.poll-del{
  flex:none; width:38px; border-radius:12px; cursor:pointer;
  border:1px solid rgba(255,255,255,0.18); background:transparent; color:rgba(255,255,255,0.6);
}

/* Carte de sondage affichée dans le tchat de l'appel */
.poll-card{
  align-self:stretch; background:rgba(255,255,255,0.09);
  border:1px solid rgba(255,255,255,0.16); border-radius:16px; padding:12px; margin:4px 0;
}
.poll-head{ display:flex; justify-content:space-between; align-items:baseline; margin-bottom:8px; gap:8px; }
.poll-timer{ font-family:'JetBrains Mono', monospace; font-size:11px; color:var(--yellow); flex:none; }
.poll-count{ font-size:10.5px; color:rgba(255,255,255,0.55); font-weight:700; margin-top:6px; }
.poll-win{
  margin-top:8px; padding:8px 10px; border-radius:12px;
  background:rgba(255,252,0,0.16); color:#fff; font-size:12.5px; font-weight:700;
  font-family:'Baloo 2', sans-serif;
}
.poll-detail{
  margin-top:8px; font-size:11px; color:rgba(255,255,255,0.7); line-height:1.5;
}
.poll-q{
  font-family:'Baloo 2', sans-serif; font-weight:700; font-size:14px; color:#fff;
  margin-bottom:10px;
}
.poll-opt{
  position:relative; width:100%; margin-bottom:6px; cursor:pointer; text-align:left;
  border:1px solid rgba(255,255,255,0.18); background:rgba(255,255,255,0.08);
  color:#fff; border-radius:12px; padding:11px 13px; overflow:hidden;
  font-family:'Baloo 2', sans-serif; font-weight:700; font-size:13px;
}
.poll-fill{
  position:absolute; left:0; top:0; bottom:0; background:rgba(255,252,0,0.28);
  transition:width .35s ease; z-index:0;
}
.poll-label{ position:relative; z-index:1; display:flex; justify-content:space-between; }
.poll-opt.mine{ border-color:var(--yellow); }

/* ---------- Skins de porte ---------- */
.skin-grid{ display:grid; grid-template-columns:repeat(3, 1fr); gap:8px; }
.skin-opt{
  aspect-ratio:3/4; border-radius:12px; cursor:pointer; padding:0; position:relative;
  border:2px solid transparent; overflow:hidden;
}
.skin-opt.on{ border-color:var(--ink); }
.skin-opt.locked{ opacity:0.5; cursor:not-allowed; }
.skin-name{
  position:absolute; left:0; right:0; bottom:0; padding:3px;
  background:rgba(0,0,0,0.5); color:#fff; font-size:8.5px; font-weight:700;
  font-family:'Baloo 2', sans-serif;
}

/* ---------- Badges ---------- */
.badge-chip{
  display:inline-flex; align-items:center; gap:4px; margin-left:5px;
  padding:2px 7px; border-radius:999px; font-size:9.5px; font-weight:800;
  font-family:'Baloo 2', sans-serif; vertical-align:middle;
  background:linear-gradient(135deg,#ffd166,#ff8a00); color:#3d2600;
}
.badge-chip.lvl4{ background:linear-gradient(135deg,#a8e6cf,#26de81); color:#0c3a24; }
.badge-chip.lvl7{ background:linear-gradient(135deg,#9bd0ff,#45aaf2); color:#0b2c47; }
.badge-chip.lvl9{ background:linear-gradient(135deg,#d5b3ff,#a55eea); color:#2b0f4a; }
.badge-chip.lvl10{
  background:linear-gradient(90deg,#ff3d77,#ff8a00,#ffe600,#26de81,#45aaf2,#a55eea,#ff3d77);
  background-size:400% 100%; color:#fff; animation:rgbSlide 4s linear infinite;
}
.points-box{
  border:1px solid var(--border); border-radius:14px; padding:12px; margin-top:4px;
}
.points-line{ display:flex; align-items:baseline; gap:8px; margin-bottom:8px; }
.points-nb{ font-family:'Baloo 2', sans-serif; font-weight:700; font-size:26px; color:var(--ink); }
.points-lb{ font-size:12px; color:var(--ink-faint); font-weight:700; }
.points-bar{ height:8px; border-radius:4px; background:var(--bg-soft); overflow:hidden; }
.points-fill{ height:100%; background:linear-gradient(90deg,#ffd166,#ff8a00); }
.points-next{ font-size:11.5px; color:var(--ink-faint); margin-top:6px; font-weight:700; }
.friend-count{ font-size:11.5px; color:var(--ink-faint); font-weight:700; }

/* ---------- QR code ---------- */
.qr-btn{
  flex:none; width:44px; border-radius:12px; cursor:pointer;
  border:1px solid var(--border); background:var(--bg); color:var(--ink);
  display:flex; align-items:center; justify-content:center;
}
.qr-btn:hover{ background:var(--bg-soft); }
.scan-box{
  position:relative; width:100%; aspect-ratio:1; border-radius:16px;
  overflow:hidden; background:#000;
}
.scan-box video{ width:100%; height:100%; object-fit:cover; display:block; }
.scan-frame{
  position:absolute; inset:16%; border-radius:14px;
  border:3px solid rgba(255,252,0,0.9);
  box-shadow:0 0 0 2000px rgba(0,0,0,0.35);
}
.qr-holder{
  background:#fff; border-radius:16px; padding:12px; display:flex;
  align-items:center; justify-content:center;
}
.qr-holder svg{ width:100%; height:auto; max-width:240px; display:block; }
.qr-name{
  text-align:center; margin-top:10px; font-family:'Baloo 2', sans-serif;
  font-weight:700; font-size:15px; color:var(--ink);
}

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
.fx-prev-heart{
  background:#ff3d77;
  -webkit-mask:url('data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"><path d="M20.8 5.6a5.2 5.2 0 0 0-7.4 0L12 7l-1.4-1.4a5.2 5.2 0 1 0-7.4 7.4L12 21.4l8.8-8.4a5.2 5.2 0 0 0 0-7.4z"/></svg>') center/contain no-repeat;
  mask:url('data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"><path d="M20.8 5.6a5.2 5.2 0 0 0-7.4 0L12 7l-1.4-1.4a5.2 5.2 0 1 0-7.4 7.4L12 21.4l8.8-8.4a5.2 5.2 0 0 0 0-7.4z"/></svg>') center/contain no-repeat;
}
.fx-prev-spark i, .fx-prev-spark{ position:relative; }
.fx-prev-spark::before, .fx-prev-spark::after{
  content:''; position:absolute; background:#ffe600;
  clip-path:polygon(50% 0%, 60% 40%, 100% 50%, 60% 60%, 50% 100%, 40% 60%, 0% 50%, 40% 40%);
  filter:drop-shadow(0 0 3px #ffe600);
}
.fx-prev-spark::before{ width:14px; height:14px; left:1px; top:1px; }
.fx-prev-spark::after{ width:9px; height:9px; right:0; bottom:1px; background:#fff8c4; }
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
/* Étincelle : une vraie étoile à quatre branches, découpée dans un carré. */
.fx-spark{
  clip-path:polygon(50% 0%, 60% 40%, 100% 50%, 60% 60%, 50% 100%, 40% 60%, 0% 50%, 40% 40%);
  filter:drop-shadow(0 0 5px currentColor);
}
/* Elle monte en scintillant et en tournant lentement. */
@keyframes fxSparkle{
  0%{ transform:translateY(0) scale(0.2) rotate(0); opacity:0; }
  15%{ transform:translateY(-70px) scale(1.15) rotate(60deg); opacity:1; }
  40%{ transform:translateY(-200px) scale(0.55) rotate(160deg); opacity:0.75; }
  65%{ transform:translateY(-330px) scale(1.05) rotate(250deg); opacity:1; }
  100%{ transform:translateY(-520px) scale(0.3) rotate(400deg); opacity:0; }
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
.fx-heart{ filter:drop-shadow(0 0 6px rgba(255,61,119,0.55)); }
/* Le cœur monte en zigzaguant doucement et en battant. */
@keyframes fxHeart{
  0%{ transform:translate(0,0) scale(0.3); opacity:0; }
  12%{ transform:translate(14px,-70px) scale(1.15); opacity:1; }
  35%{ transform:translate(-16px,-190px) scale(0.9); opacity:1; }
  60%{ transform:translate(16px,-320px) scale(1.1); opacity:0.9; }
  100%{ transform:translate(-10px,-540px) scale(0.75); opacity:0; }
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
        <div class="header-avatar-wrap">
          <div class="header-avatar" id="headerAvatar">--</div>
          <span class="private-badge" id="privateBadge" title="Porte privée : amis proches seulement"></span>
        </div>
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
        <button class="qr-btn" id="qrBtn" title="Mon QR code"></button>
        <button class="qr-btn" id="scanBtn" title="Scanner un QR code"></button>
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

    <!-- ---- Feuille : actions sur un contact ---- -->
    <div class="modal-backdrop" id="contactSheet">
      <div class="modal-card">
        <div class="sheet-name" id="sheetName"></div>
        <div class="sheet-sub" id="sheetSub"></div>
        <button class="sheet-item" type="button" id="sheetFav"></button>
        <button class="sheet-item" type="button" id="sheetClose"></button>
        <button class="sheet-item" type="button" id="sheetKey"></button>
        <button class="sheet-item" type="button" id="sheetRename"></button>
        <button class="sheet-item" type="button" id="sheetBlock"></button>
        <button class="sheet-item danger" type="button" id="sheetForget"></button>
        <div class="modal-actions">
          <button class="toggle-btn" id="sheetDismiss">Fermer</button>
        </div>
      </div>
    </div>

    <!-- ---- Écran de conversation ---- -->
    <div class="dm-screen" id="dmScreen">
      <div class="dm-head">
        <button class="dm-back" id="dmBack"></button>
        <div class="dm-avatar" id="dmAvatar"></div>
        <div>
          <div class="dm-title" id="dmTitle"></div>
          <div class="dm-sub" id="dmSub"></div>
        </div>
      </div>
      <div class="dm-note">Les messages disparaissent 24 h après avoir été lus.</div>
      <div class="dm-list" id="dmList"></div>
      <div class="dm-stickers" id="dmStickers"></div>
      <div class="dm-input-row" id="dmInputRow">
        <button class="dm-extra" id="dmPhotoBtn" title="Envoyer une photo"></button>
        <button class="dm-extra" id="dmStickerBtn" title="Envoyer un sticker"></button>
        <button class="dm-extra" id="dmVoiceBtn" title="Maintenir pour enregistrer"></button>
        <input class="dm-input" id="dmInput" type="text" maxlength="800" placeholder="Ton message…" autocomplete="off">
        <button class="dm-send" id="dmSend"></button>
      </div>
      <div class="dm-rec-row" id="dmRecRow">
        <button class="dm-rec-btn cancel" id="dmRecCancel" title="Annuler"></button>
        <div class="dm-rec-info">
          <span class="dm-rec-dot"></span>
          <span id="dmRecTime">0:00</span>
          <span class="dm-rec-hint">Enregistrement…</span>
        </div>
        <button class="dm-rec-btn send" id="dmRecSend" title="Envoyer"></button>
      </div>
      <input type="file" id="dmPhotoInput" accept="image/*" style="display:none">
    </div>

    <!-- ---- Modale : mon QR code ---- -->
    <div class="modal-backdrop" id="qrModal">
      <div class="modal-card">
        <div class="modal-title">Mon QR code</div>
        <div class="field-hint" style="margin-bottom:12px;">Fais-le scanner avec l'appareil photo d'un téléphone : la personne t'ajoutera automatiquement.</div>
        <div class="qr-holder" id="qrHolder"></div>
        <div class="qr-name" id="qrName"></div>
        <div class="modal-actions">
          <button class="modal-cancel-btn" id="qrCopy">Copier le lien</button>
          <button class="toggle-btn" id="qrClose">Fermer</button>
        </div>
      </div>
    </div>

    <!-- ---- Modale : scanner un QR code ---- -->
    <div class="modal-backdrop" id="scanModal">
      <div class="modal-card">
        <div class="modal-title">Scanner un QR code</div>
        <div class="scan-box" id="scanBox">
          <video id="scanVideo" playsinline muted></video>
          <div class="scan-frame"></div>
        </div>
        <div class="field-hint" id="scanHint" style="margin-top:10px;">Vise le QR code de ton ami.</div>
        <button class="settings-action" type="button" id="scanFileBtn"><span class="btn-ic" id="icScanFile"></span>Choisir une photo à la place</button>
        <input type="file" id="scanFileInput" accept="image/*" style="display:none">
        <div class="modal-actions">
          <button class="toggle-btn" id="scanClose">Fermer</button>
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

        <label class="field-label">Mes badges</label>
        <div class="points-box">
          <div class="points-line">
            <span class="points-nb" id="pointsNb">0</span>
            <span class="points-lb">points</span>
            <span id="pointsBadge"></span>
          </div>
          <div class="points-bar"><div class="points-fill" id="pointsFill" style="width:0%"></div></div>
          <div class="points-next" id="pointsNext"></div>
          <div class="field-hint">1 point par tranche de 10 minutes d'appel. Une série en cours augmente le gain jusqu'à deux fois.</div>
        </div>

        <label class="field-label">Ma porte</label>
        <div class="skin-grid" id="skinGrid"></div>
        <div class="field-hint">Les skins se débloquent avec tes points d'appel.</div>

        <label class="field-label">Mon titre</label>
        <div class="title-list" id="titleList"></div>
        <div class="field-hint">Les titres grisés ne sont pas encore débloqués.</div>

        <label class="field-label">Quand je rejoins un salon</label>
        <button class="settings-action" type="button" id="quietToggle"></button>
        <div class="field-hint">La porte entrebâillée : tu entres micro coupé, tu écoutes, et tu parles quand tu veux.</div>

        <label class="field-label">Mon profil</label>
        <button class="settings-action" type="button" id="friendsToggle"></button>
        <div class="field-hint">Choisis si tes contacts voient combien d'amis tu as.</div>

        <label class="field-label">Mon compte</label>
        <button class="settings-action" type="button" id="settingsHistory"><span class="btn-ic" id="icHistory"></span>Historique des toc-toc</button>
        <button class="settings-action" type="button" id="settingsLock"><span class="btn-ic" id="icLock"></span>Verrouiller maintenant</button>
        <button class="settings-action danger" type="button" id="settingsForget"><span class="btn-ic" id="icForget"></span>Changer de compte</button>

        <label class="field-label">LiveDoors Plus <span class="premium-badge">PLUS</span></label>
        <div class="premium-box">
          <div class="premium-list">
            <span class="premium-chip" id="chipPrivate">Porte privée</span>
            <span class="premium-chip" id="chipBells">8 sonneries</span>
            <span class="premium-chip" id="chipStickers">Stickers</span>
            <span class="premium-chip" id="chipEmoji">Émojis animés</span>
            <span class="premium-chip" id="chipStatus">Statut long</span>
          </div>
          <button class="settings-action" type="button" id="premiumToggle">Activer l'essai</button>
          <button class="settings-action" type="button" id="premiumBuy" style="display:none;">S'abonner</button>
          <button class="settings-action" type="button" id="premiumManage" style="display:none;">Gérer mon abonnement</button>
          <div class="field-hint" id="premiumHint">Maquette : aucun paiement n'est branché pour l'instant.</div>
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
        <div class="field-hint" id="usernameHint"></div>

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
        <button class="call-btn" id="pollBtn"><span class="call-ic" id="pollIc"></span><span class="call-lb">Sondage</span></button>
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
        <div class="chat-title" style="margin-top:8px;">Inviter quelqu'un</div>
        <div id="inviteList"></div>
      </div>

      <div class="wall-panel" id="pollPanel">
        <div class="chat-head">
          <div class="chat-title">Sondage</div>
          <button class="chat-close" id="pollCloseBtn"></button>
        </div>
        <div id="pollCreate">
          <input class="chat-input" id="pollQuestion" maxlength="80" placeholder="Ta question…">
          <div id="pollInputs"></div>
          <button class="poll-add" type="button" id="pollAdd">+ Ajouter une réponse</button>
          <button class="chat-send" id="pollSend" style="width:100%; margin-top:8px; padding:11px;">Lancer le sondage</button>
          <div class="poll-note">Le sondage se termine quand tout le monde a voté, ou au bout d'1 min 30.</div>
        </div>
      </div>

      <div class="wall-panel" id="fxPanel">
        <div class="chat-head">
          <div class="chat-title">Envoyer un effet</div>
          <button class="chat-close" id="fxCloseBtn">✕</button>
        </div>
        <div class="fx-grid">
          <button class="fx-choice" type="button" data-fx="confetti"><i class="fx-prev fx-prev-confetti"></i><span>Confettis</span></button>
          <button class="fx-choice" type="button" data-fx="hearts"><i class="fx-prev fx-prev-heart"></i><span>Cœurs</span></button>
          <button class="fx-choice" type="button" data-fx="fireworks"><i class="fx-prev fx-prev-spark"></i><span>Étincelles</span></button>
          <button class="fx-choice" type="button" data-fx="rain"><i class="fx-prev fx-prev-rain"></i><span>Pluie</span></button>
        </div>
      </div>

      <div class="reaction-zone" id="reactionZone"></div>

      <div id="waitingRoom">
        <div class="wait-door">
          <div class="wait-panel"></div>
          <div class="wait-knock"></div>
        </div>
        <div class="wait-text" id="waitText"></div>
        <div class="wait-sub">Tu entreras dès qu'on t'ouvre.</div>
        <button class="req-btn req-no" id="waitCancel">Annuler</button>
      </div>

      <div id="callInvite">
        <div class="req-head">
          <div class="req-avatar" id="inviteAvatar"></div>
          <div class="req-texts">
            <div class="req-name" id="inviteName"></div>
            <div class="req-sub" id="inviteSub"></div>
          </div>
        </div>
        <div class="req-actions">
          <button class="req-btn req-no" id="inviteIgnore">Ignorer</button>
          <button class="req-btn req-yes" id="inviteJoin">Rejoindre</button>
        </div>
      </div>

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
    <!-- Invisible mais PAS display:none : certains téléphones refusent de
         jouer le son d'un élément situé dans une zone masquée. -->
    <div id="remoteAudioContainer" class="audio-sink"></div>

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

const BELL_KEY = 'livedoors-bell';
const VIP_KEY = 'livedoors-viponly';
const HISTORY_KEY = 'livedoors-history';

// L'abonnement n'est plus une case cochée dans le navigateur : c'est le
// serveur qui le dit, d'après la base de données. Impossible de tricher en
// modifiant son propre téléphone.
let premiumUntil = null;

function isPremium() {
  return !!(me && me.premium);
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

// Un SEUL contexte audio pour toute l'appli. En créer un à chaque sonnerie
// pouvait couper le son de l'appel sur iPhone.
let bellCtx = null;

function playBell() {
  const bell = BELLS[bellChoice()] || BELLS[0];
  if (!bell.notes.length) return;
  try {
    const Ctx = window.AudioContext || window.webkitAudioContext;
    if (!Ctx) return;
    if (!bellCtx) bellCtx = new Ctx();
    if (bellCtx.state === 'suspended') bellCtx.resume();
    const ctx = bellCtx;

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
  } catch (e) {}
}

// Filet de sécurité : au moindre appui, on relance les sons en pause. Les
// navigateurs bloquent parfois la lecture tant qu'il n'y a pas eu de geste.
function reviveAudio() {
  document.querySelectorAll('#remoteAudioContainer audio').forEach((el) => {
    if (el.paused) {
      const p = el.play();
      if (p && p.catch) p.catch(() => {});
    }
  });
  if (bellCtx && bellCtx.state === 'suspended') bellCtx.resume();
}
document.addEventListener('click', reviveAudio);
document.addEventListener('touchend', reviveAudio);

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
  const payant = !!(me && me.paiement);

  // Tant que le paiement n'est pas branché : bouton d'essai.
  // Dès qu'il l'est : vrai bouton d'abonnement, et l'essai disparaît.
  \$('premiumToggle').style.display = payant ? 'none' : 'block';
  \$('premiumBuy').style.display = (payant && !on) ? 'block' : 'none';
  \$('premiumManage').style.display = (payant && on) ? 'block' : 'none';

  \$('premiumBuy').innerHTML = '<span class="btn-ic">' + icon('star', 16) + '</span>'
    + "S'abonner — " + ((me && me.prix) || '');
  \$('premiumManage').innerHTML = '<span class="btn-ic">' + icon('gear', 16) + '</span>'
    + 'Gérer mon abonnement';

  \$('premiumHint').textContent = payant
    ? (on && premiumUntil
        ? "Actif jusqu'au " + new Date(premiumUntil).toLocaleDateString('fr-FR') + '.'
        : 'Résiliable à tout moment depuis cette page.')
    : "Maquette : aucun paiement n'est branché pour l'instant.";

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
  \$('privateBadge').classList.toggle('show', vip);
  \$('vipToggle').innerHTML = '<span class="btn-ic">' + icon(vip ? 'lock' : 'unlock', 16) + '</span>'
    + (vip ? 'Privée : amis proches seulement' : 'Ouverte à tous mes contacts');
  \$('vipToggle').classList.toggle('on', vip);

  const discreet = discreetMode();
  \$('discreetToggle').innerHTML = '<span class="btn-ic">' + icon(discreet ? 'eyeOff' : 'eye', 16) + '</span>'
    + (discreet ? 'Discret : amis proches seulement' : 'Tout le monde voit ma porte');
  \$('discreetToggle').classList.toggle('on', discreet);

}

\$('premiumToggle').addEventListener('click', () => {
  // On demande au serveur : c'est lui qui décide et qui enregistre.
  socket.emit('premium:trial', { on: !isPremium() });
  \$('premiumToggle').textContent = 'Un instant…';
});

\$('premiumBuy').addEventListener('click', () => {
  socket.emit('premium:checkout');
  showToast('Ouverture de la page de paiement…');
});

\$('premiumManage').addEventListener('click', () => {
  socket.emit('premium:manage');
});

// Stripe nous renvoie une adresse : on y emmène la personne.
socket.on('premium:checkout-url', ({ url }) => {
  window.location.href = url;
});

// Réponse du serveur : on remet à plat tout ce qui dépend de l'abonnement.
socket.on('premium:update', ({ premium, until, points, badge, bonus }) => {
  if (typeof points === 'number') { myPoints = points; myBadge = badge || 0; refreshPoints(); }
  if (bonus) showToast('+' + bonus + " points offerts avec l'abonnement !");
  if (me) me.premium = !!premium;
  premiumUntil = until || null;

  refreshPremiumUI();
  refreshWallButton();
  buildEmojiBar();
  applyChatBackground();
  if (inCall) applyWallpaper(iAmHost && premium ? wallpaperChoice() : 0, wallpaperPhoto());
  render();
  sendRegister(); // pour que le serveur reçoive les réglages liés à l'abonnement

  showToast(premium ? 'LiveDoors Plus activé.' : 'Retour à la version gratuite.');
});

// Le compte n'a pas pu être ouvert : mauvais code, numéro déjà pris...
socket.on('auth:error', ({ message }) => {
  showToast(message);
  \$('pinHint').classList.add('pin-error');
  \$('pinHint').textContent = message;
  clearSession();
  \$('homeScreen').style.display = 'none';
  if (profile && profile.pinHash) {
    \$('loginScreen').style.display = 'none';
    \$('lockScreen').style.display = 'flex';
  } else {
    \$('loginScreen').style.display = 'flex';
  }
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
// Générateur de QR code
//
// Écrit à la main plutôt que d'utiliser un service extérieur : ton nom
// d'utilisateur n'a pas à transiter par un site tiers, et le code fonctionne
// même sans réseau.
//
// Limité au niveau de correction M et aux versions 1 à 6 (jusqu'à ~100
// caractères) : largement assez pour un lien d'invitation, et cela évite
// toute la complexité des grandes versions.
// ---------------------------------------------------------------------------

// -- Arithmétique dans le corps de Galois GF(256), utilisée par Reed-Solomon --
const GF_EXP = new Array(512);
const GF_LOG = new Array(256);
(function initGF() {
  let x = 1;
  for (let i = 0; i < 255; i++) {
    GF_EXP[i] = x;
    GF_LOG[x] = i;
    x <<= 1;
    if (x & 0x100) x ^= 0x11d;
  }
  for (let i = 255; i < 512; i++) GF_EXP[i] = GF_EXP[i - 255];
})();

function gfMul(a, b) {
  if (a === 0 || b === 0) return 0;
  return GF_EXP[GF_LOG[a] + GF_LOG[b]];
}

// Polynôme générateur de degré n
function rsGenPoly(n) {
  let poly = [1];
  for (let i = 0; i < n; i++) {
    const next = new Array(poly.length + 1).fill(0);
    for (let j = 0; j < poly.length; j++) {
      next[j] ^= poly[j];
      next[j + 1] ^= gfMul(poly[j], GF_EXP[i]);
    }
    poly = next;
  }
  return poly;
}

// Codes de correction d'erreur d'un bloc de données
function rsEncode(data, ecLen) {
  const gen = rsGenPoly(ecLen);
  const res = data.slice().concat(new Array(ecLen).fill(0));
  for (let i = 0; i < data.length; i++) {
    const coef = res[i];
    if (coef === 0) continue;
    for (let j = 0; j < gen.length; j++) res[i + j] ^= gfMul(gen[j], coef);
  }
  return res.slice(data.length);
}

// -- Tables officielles, niveau M, versions 1 à 6 ---------------------------
// [codets de correction par bloc, [nb blocs, codets de données par bloc], ...]
const QR_SPEC_M = {
  1: { ec: 10, blocks: [[1, 16]] },
  2: { ec: 16, blocks: [[1, 28]] },
  3: { ec: 26, blocks: [[1, 44]] },
  4: { ec: 18, blocks: [[2, 32]] },
  5: { ec: 24, blocks: [[2, 43]] },
  6: { ec: 16, blocks: [[4, 27]] },
};
const QR_ALIGN = { 1: [], 2: [6, 18], 3: [6, 22], 4: [6, 26], 5: [6, 30], 6: [6, 34] };

function qrDataCapacity(version) {
  return QR_SPEC_M[version].blocks.reduce((t, [n, d]) => t + n * d, 0);
}

function qrPickVersion(byteLength) {
  for (let v = 1; v <= 6; v++) {
    // 4 bits de mode + 8 bits de longueur = 12 bits d'en-tête
    if (byteLength + 2 <= qrDataCapacity(v)) return v;
  }
  return null;
}

// -- Construction du flux de données ----------------------------------------
function qrBuildData(text, version) {
  const bytes = Array.from(new TextEncoder().encode(text));
  const bits = [];
  const push = (value, len) => {
    for (let i = len - 1; i >= 0; i--) bits.push((value >> i) & 1);
  };

  push(0b0100, 4);        // mode octet
  push(bytes.length, 8);  // longueur (8 bits pour les versions 1 à 9)
  bytes.forEach((b) => push(b, 8));

  const capacityBits = qrDataCapacity(version) * 8;
  for (let i = 0; i < 4 && bits.length < capacityBits; i++) bits.push(0); // terminateur
  while (bits.length % 8 !== 0) bits.push(0);

  const codewords = [];
  for (let i = 0; i < bits.length; i += 8) {
    let b = 0;
    for (let j = 0; j < 8; j++) b = (b << 1) | bits[i + j];
    codewords.push(b);
  }
  const pads = [0xec, 0x11];
  let k = 0;
  while (codewords.length < qrDataCapacity(version)) codewords.push(pads[k++ % 2]);

  return codewords;
}

// Découpage en blocs, calcul des corrections, puis entrelacement
function qrFinalMessage(codewords, version) {
  const spec = QR_SPEC_M[version];
  const dataBlocks = [];
  const ecBlocks = [];
  let pos = 0;

  spec.blocks.forEach(([count, size]) => {
    for (let i = 0; i < count; i++) {
      const block = codewords.slice(pos, pos + size);
      pos += size;
      dataBlocks.push(block);
      ecBlocks.push(rsEncode(block, spec.ec));
    }
  });

  const out = [];
  const maxData = Math.max(...dataBlocks.map((b) => b.length));
  for (let i = 0; i < maxData; i++) {
    dataBlocks.forEach((b) => { if (i < b.length) out.push(b[i]); });
  }
  for (let i = 0; i < spec.ec; i++) {
    ecBlocks.forEach((b) => out.push(b[i]));
  }
  return out;
}

// -- Dessin de la matrice ----------------------------------------------------
function qrMakeMatrix(version) {
  const size = version * 4 + 17;
  const m = [];
  const reserved = [];
  for (let i = 0; i < size; i++) {
    m.push(new Array(size).fill(0));
    reserved.push(new Array(size).fill(false));
  }

  function setFinder(r, c) {
    for (let dr = -1; dr <= 7; dr++) {
      for (let dc = -1; dc <= 7; dc++) {
        const rr = r + dr;
        const cc = c + dc;
        if (rr < 0 || cc < 0 || rr >= size || cc >= size) continue;
        const dedans = dr >= 0 && dr <= 6 && dc >= 0 && dc <= 6;
        const noir = dedans && (dr === 0 || dr === 6 || dc === 0 || dc === 6
          || (dr >= 2 && dr <= 4 && dc >= 2 && dc <= 4));
        m[rr][cc] = noir ? 1 : 0;
        reserved[rr][cc] = true;
      }
    }
  }

  setFinder(0, 0);
  setFinder(0, size - 7);
  setFinder(size - 7, 0);

  // Motifs de synchronisation
  for (let i = 8; i < size - 8; i++) {
    const v = i % 2 === 0 ? 1 : 0;
    m[6][i] = v; reserved[6][i] = true;
    m[i][6] = v; reserved[i][6] = true;
  }

  // Motifs d'alignement
  const centers = QR_ALIGN[version];
  centers.forEach((r) => {
    centers.forEach((c) => {
      const coinFinder = (r <= 8 && c <= 8) || (r <= 8 && c >= size - 9) || (r >= size - 9 && c <= 8);
      if (coinFinder) return;
      for (let dr = -2; dr <= 2; dr++) {
        for (let dc = -2; dc <= 2; dc++) {
          const noir = Math.max(Math.abs(dr), Math.abs(dc)) !== 1;
          m[r + dr][c + dc] = noir ? 1 : 0;
          reserved[r + dr][c + dc] = true;
        }
      }
    });
  });

  // Module toujours noir
  m[size - 8][8] = 1;
  reserved[size - 8][8] = true;

  // Emplacements réservés à l'information de format
  for (let i = 0; i <= 8; i++) {
    if (!reserved[8][i]) reserved[8][i] = true;
    if (!reserved[i][8]) reserved[i][8] = true;
  }
  for (let i = 0; i < 8; i++) {
    reserved[8][size - 1 - i] = true;
    reserved[size - 1 - i][8] = true;
  }

  return { m, reserved, size };
}

// Remplissage en zigzag depuis le coin bas droit
function qrPlaceData(grid, bytes) {
  const { m, reserved, size } = grid;
  const bits = [];
  bytes.forEach((b) => { for (let i = 7; i >= 0; i--) bits.push((b >> i) & 1); });

  let idx = 0;
  let up = true;
  for (let col = size - 1; col > 0; col -= 2) {
    if (col === 6) col--; // on saute la colonne de synchronisation
    for (let n = 0; n < size; n++) {
      const row = up ? size - 1 - n : n;
      for (let c = 0; c < 2; c++) {
        const cc = col - c;
        if (reserved[row][cc]) continue;
        m[row][cc] = idx < bits.length ? bits[idx] : 0;
        idx++;
      }
    }
    up = !up;
  }
}

function qrMaskBit(mask, r, c) {
  switch (mask) {
    case 0: return (r + c) % 2 === 0;
    case 1: return r % 2 === 0;
    case 2: return c % 3 === 0;
    case 3: return (r + c) % 3 === 0;
    case 4: return (Math.floor(r / 2) + Math.floor(c / 3)) % 2 === 0;
    case 5: return ((r * c) % 2) + ((r * c) % 3) === 0;
    case 6: return (((r * c) % 2) + ((r * c) % 3)) % 2 === 0;
    default: return (((r + c) % 2) + ((r * c) % 3)) % 2 === 0;
  }
}

// Information de format : 5 bits utiles + BCH(15,5), puis masque fixe
function qrFormatBits(mask) {
  const data = (0b00 << 3) | mask; // 00 = niveau M
  let value = data << 10;
  for (let i = 4; i >= 0; i--) {
    if ((value >> (i + 10)) & 1) value ^= 0b10100110111 << i;
  }
  return ((data << 10) | value) ^ 0b101010000010010;
}

function qrApplyFormat(grid, mask) {
  const { m, size } = grid;
  const bits = qrFormatBits(mask);
  const bit = (i) => (bits >> i) & 1;

  // Bande verticale : colonne 8, de haut en bas puis en bas a gauche.
  for (let i = 0; i < 15; i++) {
    const v = bit(i);
    if (i < 6) m[i][8] = v;
    else if (i < 8) m[i + 1][8] = v;
    else m[size - 15 + i][8] = v;
  }

  // Bande horizontale : ligne 8, de droite a gauche.
  for (let i = 0; i < 15; i++) {
    const v = bit(i);
    if (i < 8) m[8][size - 1 - i] = v;
    else if (i === 8) m[8][7] = v;
    else m[8][14 - i] = v;
  }

  // Module toujours noir, redessine apres coup.
  m[size - 8][8] = 1;
}

// Score de pénalité : sert à choisir le masque le plus lisible
function qrPenalty(m, size) {
  let score = 0;

  // Règle 1 : suites de 5 modules identiques ou plus
  for (let i = 0; i < size; i++) {
    for (const ligne of [true, false]) {
      let prev = -1;
      let run = 0;
      for (let j = 0; j < size; j++) {
        const v = ligne ? m[i][j] : m[j][i];
        if (v === prev) { run++; } else { prev = v; run = 1; }
        if (run === 5) score += 3;
        else if (run > 5) score += 1;
      }
    }
  }

  // Règle 2 : carrés 2x2 de même couleur
  for (let r = 0; r < size - 1; r++) {
    for (let c = 0; c < size - 1; c++) {
      const v = m[r][c];
      if (v === m[r][c + 1] && v === m[r + 1][c] && v === m[r + 1][c + 1]) score += 3;
    }
  }

  // Règle 3 : motifs ressemblant aux repères de position
  const p1 = [1, 0, 1, 1, 1, 0, 1, 0, 0, 0, 0];
  const p2 = [0, 0, 0, 0, 1, 0, 1, 1, 1, 0, 1];
  for (let i = 0; i < size; i++) {
    for (let j = 0; j <= size - 11; j++) {
      const ligne = [];
      const colonne = [];
      for (let k = 0; k < 11; k++) { ligne.push(m[i][j + k]); colonne.push(m[j + k][i]); }
      const eq = (a, b) => a.every((v, x) => v === b[x]);
      if (eq(ligne, p1) || eq(ligne, p2)) score += 40;
      if (eq(colonne, p1) || eq(colonne, p2)) score += 40;
    }
  }

  // Règle 4 : déséquilibre entre noir et blanc
  let noirs = 0;
  for (let r = 0; r < size; r++) for (let c = 0; c < size; c++) noirs += m[r][c];
  const pourcent = (noirs * 100) / (size * size);
  score += Math.floor(Math.abs(pourcent - 50) / 5) * 10;

  return score;
}

// Fabrique la matrice finale : renvoie un tableau de 0 et 1
function qrMatrix(text) {
  const bytes = new TextEncoder().encode(text);
  const version = qrPickVersion(bytes.length);
  if (!version) return null; // texte trop long

  const codewords = qrBuildData(text, version);
  const finalBytes = qrFinalMessage(codewords, version);

  let meilleur = null;
  let meilleurScore = Infinity;

  for (let mask = 0; mask < 8; mask++) {
    const grid = qrMakeMatrix(version);
    qrPlaceData(grid, finalBytes);
    for (let r = 0; r < grid.size; r++) {
      for (let c = 0; c < grid.size; c++) {
        if (!grid.reserved[r][c] && qrMaskBit(mask, r, c)) grid.m[r][c] ^= 1;
      }
    }
    qrApplyFormat(grid, mask);
    const score = qrPenalty(grid.m, grid.size);
    if (score < meilleurScore) { meilleurScore = score; meilleur = grid.m; }
  }

  return meilleur;
}

// Rendu en SVG, avec la marge blanche obligatoire de 4 modules
function qrSvg(text, pixels) {
  const m = qrMatrix(text);
  if (!m) return '';
  const size = m.length;
  const quiet = 4;
  const total = size + quiet * 2;

  let rects = '';
  for (let r = 0; r < size; r++) {
    for (let c = 0; c < size; c++) {
      if (m[r][c]) rects += '<rect x="' + (c + quiet) + '" y="' + (r + quiet) + '" width="1" height="1"/>';
    }
  }

  return '<svg viewBox="0 0 ' + total + ' ' + total + '" width="' + (pixels || 220)
    + '" height="' + (pixels || 220) + '" xmlns="http://www.w3.org/2000/svg" shape-rendering="crispEdges">'
    + '<rect width="' + total + '" height="' + total + '" fill="#fff"/>'
    + '<g fill="#000">' + rects + '</g></svg>';
}

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
  back: '<path d="M19 12H5"/><path d="M12 19l-7-7 7-7"/>',
  more: '<circle cx="12" cy="5" r="1.6"/><circle cx="12" cy="12" r="1.6"/><circle cx="12" cy="19" r="1.6"/>',
  check: '<path d="M20 6L9 17l-5-5"/>',
  trash: '<path d="M3 6h18"/><path d="M8 6V4h8v2"/><path d="M19 6l-1 14H6L5 6"/><path d="M10 11v6"/><path d="M14 11v6"/>',
  poll: '<path d="M4 20V10"/><path d="M12 20V4"/><path d="M20 20v-6"/>',
  checks: '<path d="M1 13l4 4L15 7"/><path d="M9 17L20 6"/>',
  send: '<path d="M22 2L11 13"/><path d="M22 2l-7 20-4-9-9-4 20-7z"/>',
  key: '<circle cx="7.5" cy="15.5" r="4.5"/><path d="M10.7 12.3L21 2"/><path d="M17 6l3 3"/><path d="M14 9l3 3"/>',
  scan: '<path d="M3 8V5a2 2 0 0 1 2-2h3"/><path d="M16 3h3a2 2 0 0 1 2 2v3"/><path d="M21 16v3a2 2 0 0 1-2 2h-3"/><path d="M8 21H5a2 2 0 0 1-2-2v-3"/><path d="M3 12h18"/>',
  qr: '<rect x="3" y="3" width="7" height="7" rx="1"/><rect x="14" y="3" width="7" height="7" rx="1"/><rect x="3" y="14" width="7" height="7" rx="1"/><path d="M14 14h3v3h-3z"/><path d="M20 14v3"/><path d="M14 20h3"/><path d="M20 20h1"/>',
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
// Photo de profil
//
// La photo choisie est redessinée dans un carré de 128 px avant d'être
// enregistrée : une photo de téléphone fait plusieurs Mo, ce qui serait
// impossible à envoyer à tous les contacts à chaque changement. Après
// réduction, elle pèse quelques dizaines de Ko.
// ---------------------------------------------------------------------------

const PHOTO_SIZE = 128;

// Réduit n'importe quelle image à un carré de la taille demandée.
function shrinkImage(file, size, quality) {
  return new Promise((resolve, reject) => {
    if (!file || file.type.indexOf('image/') !== 0) { reject(new Error('pas une image')); return; }

    const reader = new FileReader();
    reader.onerror = () => reject(new Error('lecture impossible'));
    reader.onload = () => {
      const img = new Image();
      img.onerror = () => reject(new Error('image illisible'));
      img.onload = () => {
        const canvas = document.createElement('canvas');
        canvas.width = size;
        canvas.height = size;
        const ctx = canvas.getContext('2d');

        // On recadre au centre pour garder un carré sans déformer l'image.
        const side = Math.min(img.width, img.height);
        const sx = (img.width - side) / 2;
        const sy = (img.height - side) / 2;
        ctx.drawImage(img, sx, sy, side, side, 0, 0, size, size);

        resolve(canvas.toDataURL('image/jpeg', quality));
      };
      img.src = reader.result;
    };
    reader.readAsDataURL(file);
  });
}

function shrinkPhoto(file) {
  return shrinkImage(file, PHOTO_SIZE, 0.72);
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
// EXACTEMENT la même règle que le serveur : on garde tous les chiffres.
// Une règle différente des deux côtés faisait rater les correspondances
// (séries, messages non lus) sans que rien ne plante visiblement.
function normalizePhoneLocal(phone) {
  let out = '';
  for (const ch of String(phone || '')) {
    if (ch >= '0' && ch <= '9') out += ch;
  }
  return out;
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

// Le trousseau : les amis autorisés à entrer sans toquer.
function hasKey(phone) {
  const card = findContact(phone);
  return !!(card && card.hasKey);
}
function keyPhones() {
  return loadContacts().filter((c) => c.hasKey).map((c) => c.phone);
}
function toggleKey(phone) {
  const now = !hasKey(phone);
  if (now && !confirm('Donner un double des clés ? Cette personne pourra entrer dans ton salon sans toquer.')) return;
  updateContact(phone, { hasKey: now });
  sendRegister(); // le serveur applique l'entrée directe
  render();
  showToast(now ? 'Double des clés donné.' : 'Clés reprises.');
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
      || found.premium !== !!u.premium
      || found.doorMessage !== (u.doorMessage || '')
      || found.username !== (u.username || '')) {
      found.pseudo = u.pseudo;
      found.avatarPhoto = u.avatarPhoto || ''; // sinon la photo disparaît hors ligne
      found.premium = !!u.premium;             // pour garder le badge hors ligne
      found.username = u.username || '';
      found.doorMessage = u.doorMessage || ''; // dernier petit mot connu
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
  \$('privateBadge').innerHTML = icon('lock', 11);
  \$('qrBtn').innerHTML = icon('qr', 18);
  \$('scanBtn').innerHTML = icon('scan', 18);
  \$('icScanFile').innerHTML = icon('image', 16);
  \$('dmBack').innerHTML = icon('back', 16);
  \$('dmSend').innerHTML = icon('send', 18);
  \$('dmPhotoBtn').innerHTML = icon('image', 18);
  \$('dmStickerBtn').innerHTML = icon('palette', 18);
  \$('dmVoiceBtn').innerHTML = icon('mic', 18);
  \$('pollIc').innerHTML = icon('poll', 22);
  \$('pollCloseBtn').innerHTML = icon('close', 16);
  \$('dmRecCancel').innerHTML = icon('trash', 20);
  \$('dmRecSend').innerHTML = icon('send', 20);
  refreshFriendsToggle();
  refreshQuietToggle();
  \$('icSun').innerHTML = icon('sun', 14);
  \$('icMoon').innerHTML = icon('moon', 14);
  \$('icDevice').innerHTML = icon('device', 14);
  \$('icPhoto1').innerHTML = icon('camera', 15);
  \$('icPhoto2').innerHTML = icon('camera', 15);

  // Étiquettes des avantages
  const chips = {
    chipPrivate: 'lock', chipBells: 'bell',
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

  // On explique clairement quand le prochain changement sera possible.
  const change = (me && me.usernameChangedAt) || 0;
  const reste = (30 * 86400000) - (Date.now() - change);
  if (isPremium()) {
    \$('usernameHint').textContent = 'Avec LiveDoors Plus, tu peux le changer autant que tu veux.';
  } else if (!change) {
    \$('usernameHint').textContent = 'Choisis-le bien : il ne se change qu\\'une fois par mois.';
  } else if (reste > 0) {
    const j = Math.ceil(reste / 86400000);
    \$('usernameHint').textContent = 'Prochain changement possible dans ' + j + ' jour' + (j > 1 ? 's' : '') + '.';
  } else {
    \$('usernameHint').textContent = 'Tu peux le changer maintenant (une fois par mois).';
  }
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
    // Preuve d'identité : l'empreinte du code secret, jamais le code lui-même.
    pass: onlineProfile.pinHash,
    vipOnly: vipOnly(),
    discreet: discreetMode(),
    showFriends: showFriendsOn(),
    title: myTitle,
    skin: mySkin,
    vip: closePhones(),
    keys: keyPhones(),
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
  premiumUntil = user.premiumUntil || null;
  \$('lockScreen').style.display = 'none';
  \$('loginScreen').style.display = 'none';
  \$('homeScreen').style.display = 'flex';

  paintAvatarFor(\$('headerAvatar'), user);
  paintAvatarFor(\$('myAvatar'), user);
  \$('myName').innerHTML = escapeHtml(user.username ? '@' + user.username : user.pseudo)
    + (user.premium ? '<span class="premium-badge big">PLUS</span>' : '');
  \$('myPhone').textContent = user.pseudo + (user.phone ? ' · ' + user.phone : '');
  myPoints = user.points || 0;
  myBadge = user.badge || 0;
  \$('myName').innerHTML += badgeChip(myBadge);
  refreshPoints();
  \$('connectionState').textContent = 'Connecté';

  socket.emit('dm:unread'); // pastilles de messages non lus

  // L'interface se cale sur l'abonnement annoncé par le serveur.
  refreshPremiumUI();
  buildEmojiBar();
  applyChatBackground();

  // Petit mot encore valable : on le remet en place tout seul.
  const keptStatus = loadStatus();
  if (keptStatus) {
    \$('doorMessageInput').value = keptStatus;
    if (!me.doorMessage) socket.emit('door:message', { message: keptStatus });
  }
});

// -- Démarrage de l'appli ----------------------------------------------------
// Retour depuis la page de paiement. On ne débloque RIEN ici : c'est Stripe
// qui préviendra le serveur. On explique juste ce qui se passe.
function checkPaymentReturn() {
  const params = new URLSearchParams(window.location.search);
  const etat = params.get('paiement');
  if (!etat) return;

  history.replaceState({}, '', window.location.pathname);
  if (etat === 'ok') {
    showToast('Paiement reçu — ton abonnement arrive dans quelques secondes.');
  } else if (etat === 'annule') {
    showToast('Paiement annulé.');
  }
}

// Ajout d'un contact via un lien (?add=nom) : utile pour le partage de lien.
function checkAddLink() {
  const params = new URLSearchParams(window.location.search);
  const qui = params.get('add');
  if (!qui) return;
  history.replaceState({}, '', window.location.pathname);
  setTimeout(() => {
    if (looksLikePhone(qui)) socket.emit('contact:add', { phone: qui });
    else socket.emit('contact:addByUsername', { username: qui });
  }, 1500);
}

function boot() {
  refreshSignupPreview();
  buildEmojiBar();
  buildWallPicker();
  applyChatBackground();
  paintIcons();
  refreshPremiumUI(); // sinon la couleur du statut n'arrive qu'après un tour dans les réglages
  checkPaymentReturn();
  checkAddLink();
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
  const p = escapeAttr(phone);
  const n = unreadCounts[normalizePhoneLocal(phone)] || 0;
  return \`<div class="contact-actions">
      <button class="row-btn" onclick="openChatWith('\${p}')">
        \${icon('chat', 14)}<span>Message</span>
        \${n ? \`<span class="row-badge">\${n > 9 ? '9+' : n}</span>\` : ''}
      </button>
      <button class="row-more" onclick="openContactSheet('\${p}')" title="Plus">\${icon('more', 16)}</button>
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
        <div class="avatar">\${avatarMarkup(f)}</div>\${f.phone && isFavorite(f.phone) ? '<span class="fav-dot">' + icon('star', 10) + '</span>' : ''}
      </div>
      <div class="friend-info">
        <div class="friend-name">\${escapeHtml(f.username ? '@' + f.username : displayName(f))}\${f.premium ? '<span class="premium-badge">PLUS</span>' : ''}\${badgeChip(f.badge)}\${titleChip(f.title)}\${streakChip(f.phone)}</div>
        <div class="friend-phone">\${escapeHtml(displayName(f))}\${f.phone ? ' · ' + escapeHtml(f.phone) : ''}\${f.friendCount !== null && f.friendCount !== undefined ? ' · ' + f.friendCount + ' ami' + (f.friendCount > 1 ? 's' : '') : ''}</div>
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
        <div class="avatar">\${avatarMarkup(f)}</div>\${f.phone && isFavorite(f.phone) ? '<span class="fav-dot">' + icon('star', 10) + '</span>' : ''}
      </div>
      <div class="friend-info">
        <div class="friend-name">\${escapeHtml(f.username ? '@' + f.username : displayName(f))}\${f.premium ? '<span class="premium-badge">PLUS</span>' : ''}\${badgeChip(f.badge)}\${titleChip(f.title)}\${streakChip(f.phone)}</div>
        <div class="friend-phone">\${escapeHtml(displayName(f))}\${f.phone ? ' · ' + escapeHtml(f.phone) : ''}\${f.friendCount !== null && f.friendCount !== undefined ? ' · ' + f.friendCount + ' ami' + (f.friendCount > 1 ? 's' : '') : ''}</div>
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
        \${c.favorite ? '<span class="fav-dot">' + icon('star', 10) + '</span>' : ''}
        <div class="avatar">\${avatarMarkup({ avatarPhoto: c.avatarPhoto || '', avatarColor: c.avatarColor || '#ff8a00', avatarInitials: (c.alias || c.pseudo || '?').slice(0, 2).toUpperCase() })}</div>
      </div>
      <div class="friend-info">
        <div class="friend-name">\${escapeHtml(c.username ? '@' + c.username : (c.alias || c.pseudo || 'Contact'))}\${c.premium ? '<span class="premium-badge">PLUS</span>' : ''}</div>
        <div class="friend-phone">\${escapeHtml(c.alias || c.pseudo || 'Contact')}\${c.phone ? ' · ' + escapeHtml(c.phone) : ''}</div>
        \${c.doorMessage ? \`<div class="friend-status-msg\${c.premium ? ' is-premium' : ''}">\${escapeHtml(c.doorMessage)}</div>\` : ''}
        <div class="friend-meta">\${c.blocked ? 'Bloqué' : 'Pas connecté'}</div>
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
    askNotifyPermission(); // le navigateur exige un clic pour le demander
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
  hideWaiting();
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

  // Porte entrebâillée : on entre micro coupé si le réglage est actif.
  if (quietJoin()) {
    const piste = localStream.getAudioTracks()[0];
    if (piste) piste.enabled = false;
  }

  startCallUI(host, false);

  if (quietJoin()) {
    \$('muteBtn').classList.add('is-muted');
    \$('muteIc').innerHTML = icon('micOff');
    myCallState.muted = true;
    showToast('Tu es entré micro coupé.');
  }

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
      audioEl.playsInline = true; // sans ça, iOS peut refuser de jouer
      audioEl.setAttribute('playsinline', '');
      \$('remoteAudioContainer').appendChild(audioEl);
    }
    audioEl.srcObject = stream;
    // autoplay est parfois bloqué : on redemande explicitement.
    const p = audioEl.play();
    if (p && p.catch) p.catch(() => {});
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
    // TOUJOURS muet : le son passe par l'élément audio dédié. Sinon la voix
    // sortait deux fois, avec un décalage qui donnait un effet d'écho.
    video.muted = true;
    video.setAttribute('playsinline', '');

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

  // Contacts connectés qui ne sont pas encore dans l'appel : on propose de
  // les inviter sans avoir à quitter l'écran.
  const dedans = new Set(Array.from(peerNames.keys()));
  const invitables = friends.filter((f) => !dedans.has(f.id));

  \$('inviteList').innerHTML = invitables.length ? invitables.map((f) => \`
    <div class="person-row">
      <div class="person-avatar">\${avatarMarkup(f)}</div>
      <div class="person-name">\${escapeHtml(displayName(f))}</div>
      <button class="invite-btn" onclick="inviteToCall('\${f.id}')">Inviter</button>
    </div>
  \`).join('') : '<div class="chat-empty" style="margin:8px 0;">Aucun contact connecté à inviter.</div>';
}

function inviteToCall(id) {
  socket.emit('call:invite', { toId: id });
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
  const avant = peerStates.get(id);
  const apres = { muted: !!muted, cam: !!cam, screen: !!screen };
  const moi = me && id === me.id;

  // On n'annonce que les CHANGEMENTS, et jamais le tout premier état reçu :
  // sinon chaque arrivée déclencherait trois messages inutiles.
  if (avant && !moi) {
    const qui = peerNames.get(id) || 'Quelqu\\'un';
    const dire = (texte) => { showToast(texte); addSystemMessage(texte); };

    if (avant.muted !== apres.muted) {
      dire(qui + (apres.muted ? ' a coupé son micro' : ' a rallumé son micro'));
    }
    if (avant.cam !== apres.cam) {
      dire(qui + (apres.cam ? ' a allumé sa caméra' : ' a éteint sa caméra'));
    }
    if (avant.screen !== apres.screen) {
      dire(qui + (apres.screen ? ' partage son écran' : ' a arrêté le partage'));
    }
  }

  peerStates.set(id, apres);
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
    // On enregistre l'état de départ SANS rien annoncer : les changements
    // seront signalés ensuite, par comparaison.
    peerStates.set(member.id, member.callState || { muted: false, cam: false, screen: false });
    createPeerConnection(member.id); // onnegotiationneeded envoie l'offre
  }
  renderPeople();
  sendMyCallState(); // les autres apprennent mon état à mon arrivée
});

socket.on('call:key-used', (qui) => {
  showToast(displayName(qui) + ' entre avec ses clés.');
});

socket.on('call:peer-joined', (peer) => {
  peerNames.set(peer.id, displayName(peer));
  peerCards.set(peer.id, peer);
  // État de départ enregistré en silence : sinon son premier envoi d'état
  // serait annoncé comme un changement.
  if (!peerStates.has(peer.id)) {
    peerStates.set(peer.id, { muted: false, cam: false, screen: false });
  }
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
  systemNotify('LiveDoors', displayName(from) + ' veut rejoindre ton appel');
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
  pendingRequestHostId = null;
  hideWaiting();
  render();
});

// ---------------------------------------------------------------------------
// UI de l'écran d'appel
// ---------------------------------------------------------------------------

function startCallUI(target, isHosting) {
  hideWaiting();
  inCall = true;
  iAmHost = !!isHosting;
  callSeconds = 0;
  \$('callInvite').style.display = 'none';
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
    \$('fxBtn').classList.remove('is-on'); // sinon le bouton restait allumé
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

function makeSpark() {
  const couleurs = ['#fff8c4', '#ffe600', '#ffd166', '#ffffff', '#9bf6ff'];
  const bit = document.createElement('div');
  bit.className = 'fx-bit fx-spark';
  const taille = 8 + Math.random() * 12;
  bit.style.width = taille + 'px';
  bit.style.height = taille + 'px';
  bit.style.background = couleurs[Math.floor(Math.random() * couleurs.length)];
  return bit;
}

function makeHeart() {
  const couleurs = ['#ff3d77', '#ff6b9d', '#e6398b', '#ff8fab', '#c9184a'];
  const c = couleurs[Math.floor(Math.random() * couleurs.length)];
  const taille = 16 + Math.random() * 16;
  const bit = document.createElement('div');
  bit.className = 'fx-bit fx-heart';
  bit.style.width = taille + 'px';
  bit.style.height = taille + 'px';
  bit.style.color = c;
  bit.innerHTML = '<svg viewBox="0 0 24 24" width="100%" height="100%">'
    + '<path fill="currentColor" d="M20.8 5.6a5.2 5.2 0 0 0-7.4 0L12 7l-1.4-1.4a5.2 5.2 0 1 0-7.4 7.4L12 21.4l8.8-8.4a5.2 5.2 0 0 0 0-7.4z"/>'
    + '</svg>';
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
  hearts:    { make: makeHeart, anim: 'fxHeart', dur: 3.0, count: 34 },
  fireworks: { make: makeSpark, anim: 'fxSparkle', dur: 2.2, count: 44 },
};

function playEffect(kind) {
  const fx = FX_STYLES[kind] || FX_STYLES.confetti;
  const zone = \$('reactionZone');

  for (let i = 0; i < fx.count; i++) {
    const bit = fx.make();
    bit.style.left = Math.random() * 96 + '%';
    // Les effets qui montent doivent partir du BAS de l'écran.
    if (fx.anim === 'fxRise' || fx.anim === 'fxSparkle' || fx.anim === 'fxHeart') {
      bit.style.top = 'auto';
      bit.style.bottom = '40px';
    }
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
// Mon QR code
//
// Il contient un simple lien. La personne le scanne avec l'appareil photo de
// son téléphone (aucune application à installer), le lien ouvre LiveDoors, et
// l'app ajoute le contact toute seule grâce au ?add= .
// ---------------------------------------------------------------------------

function myInviteLink() {
  const base = window.location.origin + window.location.pathname;
  const qui = (me && me.username) ? me.username : ((me && me.phone) || '');
  return base.replace(/\\/$/, '') + '/?add=' + encodeURIComponent(qui);
}

\$('qrBtn').addEventListener('click', () => {
  if (!me) return;
  const lien = myInviteLink();
  const svg = qrSvg(lien, 240);

  if (!svg) { showToast('Lien trop long pour un QR code.'); return; }
  \$('qrHolder').innerHTML = svg;
  \$('qrName').textContent = me.username ? '@' + me.username : (me.phone || '');
  \$('qrModal').classList.add('show');
});

\$('qrClose').addEventListener('click', () => \$('qrModal').classList.remove('show'));

\$('qrCopy').addEventListener('click', async () => {
  try {
    await navigator.clipboard.writeText(myInviteLink());
    showToast('Lien copié.');
  } catch (e) {
    showToast(myInviteLink());
  }
});

// ---------------------------------------------------------------------------
// Menu d'un contact
// ---------------------------------------------------------------------------

let sheetPhone = null;

function openContactSheet(phone) {
  sheetPhone = phone;
  const card = findContact(phone) || {};
  const ami = friends.find((f) => normalizePhoneLocal(f.phone) === normalizePhoneLocal(phone));

  \$('sheetName').textContent = (ami && ami.username) ? '@' + ami.username : (card.alias || card.pseudo || 'Contact');
  \$('sheetSub').textContent = (card.alias || card.pseudo || '') + (phone ? ' · ' + phone : '');

  const ligne = (id, ic, texte, actif) => {
    const b = \$(id);
    b.innerHTML = '<span class="ic">' + icon(ic, 17) + '</span>' + texte;
    b.classList.toggle('on', id === 'sheetFav' && !!actif);
    b.classList.toggle('close-on', id === 'sheetClose' && !!actif);
  };

  ligne('sheetFav', 'star', card.favorite ? 'Retirer des favoris' : 'Mettre en favori', card.favorite);
  \$('sheetClose').style.display = isPremium() ? 'flex' : 'none';
  ligne('sheetClose', 'heart', card.close ? 'Retirer des amis proches' : 'Ajouter aux amis proches', card.close);
  ligne('sheetKey', 'key', card.hasKey ? 'Reprendre mes clés' : 'Donner un double des clés', card.hasKey);
  \$('sheetKey').classList.toggle('key-on', !!card.hasKey);
  ligne('sheetRename', 'pencil', 'Renommer');
  ligne('sheetBlock', 'block', card.blocked ? 'Débloquer' : 'Bloquer');
  ligne('sheetForget', 'logout', 'Retirer de mes contacts');

  \$('contactSheet').classList.add('show');
}

function fermerSheet() {
  \$('contactSheet').classList.remove('show');
}

// Nombre d'amis visible ou non sur mon profil
const FRIENDS_KEY = 'livedoors-showfriends';
function showFriendsOn() {
  try { return localStorage.getItem(FRIENDS_KEY) !== '0'; } catch (e) { return true; }
}
const QUIET_KEY = 'livedoors-quietjoin';
function quietJoin() {
  try { return localStorage.getItem(QUIET_KEY) === '1'; } catch (e) { return false; }
}
function refreshQuietToggle() {
  const on = quietJoin();
  \$('quietToggle').innerHTML = '<span class="btn-ic">' + icon(on ? 'micOff' : 'mic', 16) + '</span>'
    + (on ? "J'entre micro coupé" : "J'entre micro ouvert");
  \$('quietToggle').classList.toggle('on', on);
}
\$('quietToggle').addEventListener('click', () => {
  const suivant = quietJoin() ? '0' : '1';
  try { localStorage.setItem(QUIET_KEY, suivant); } catch (e) {}
  refreshQuietToggle();
  showToast(suivant === '1' ? 'Tu entreras micro coupé.' : 'Tu entreras micro ouvert.');
});

function refreshFriendsToggle() {
  const on = showFriendsOn();
  \$('friendsToggle').innerHTML = '<span class="btn-ic">' + icon(on ? 'eye' : 'eyeOff', 16) + '</span>'
    + (on ? 'Mon nombre d\\'amis est visible' : 'Mon nombre d\\'amis est caché');
  \$('friendsToggle').classList.toggle('on', on);
}
\$('friendsToggle').addEventListener('click', () => {
  const suivant = showFriendsOn() ? '0' : '1';
  try { localStorage.setItem(FRIENDS_KEY, suivant); } catch (e) {}
  refreshFriendsToggle();
  socket.emit('profile:showFriends', { on: suivant === '1' });
  showToast(suivant === '1' ? 'Nombre d\\'amis visible.' : 'Nombre d\\'amis caché.');
});

\$('sheetDismiss').addEventListener('click', fermerSheet);
\$('sheetFav').addEventListener('click', () => { toggleFavorite(sheetPhone); fermerSheet(); });
\$('sheetClose').addEventListener('click', () => { toggleClose(sheetPhone); fermerSheet(); });
\$('sheetKey').addEventListener('click', () => { toggleKey(sheetPhone); fermerSheet(); });
\$('sheetRename').addEventListener('click', () => { fermerSheet(); renameContact(sheetPhone); });
\$('sheetBlock').addEventListener('click', () => { toggleBlocked(sheetPhone); fermerSheet(); });
\$('sheetForget').addEventListener('click', () => { forgetContact(sheetPhone); fermerSheet(); });

// ---------------------------------------------------------------------------
// Messages privés
//
// Ils passent par le serveur et sont enregistrés en base : contrairement au
// tchat d'appel, ils arrivent même si la personne est déconnectée.
// ---------------------------------------------------------------------------

let dmWith = null;          // numéro de la conversation ouverte
let unreadCounts = {};      // messages non lus, par contact

function openChatWith(phone) {
  const key = normalizePhoneLocal(phone);
  dmWith = phone;

  const card = findContact(phone) || {};
  const ami = friends.find((f) => normalizePhoneLocal(f.phone) === key);
  const nom = (card.alias || (ami && ami.pseudo) || card.pseudo || 'Contact');

  \$('dmTitle').textContent = nom;
  \$('dmSub').textContent = ami
    ? (ami.doorOpen ? 'Porte ouverte' : 'En ligne')
    : 'Pas connecté';
  paintAvatarFor(\$('dmAvatar'), ami || {
    avatarPhoto: card.avatarPhoto || '',
    avatarInitials: nom.slice(0, 2).toUpperCase(),
    avatarColor: colorForPseudo(nom),
  });

  \$('dmList').innerHTML = '<div class="empty-note">Chargement…</div>';
  \$('dmScreen').classList.add('show');
  socket.emit('dm:history', { withPhone: phone });
  setTimeout(() => \$('dmInput').focus(), 150);
}

\$('dmBack').addEventListener('click', () => {
  if (recorder) finirEnregistrement(true);
  toggleDmStickers(false);
  \$('dmScreen').classList.remove('show');
  dmWith = null;
  render();
});

function heure(at) {
  const d = new Date(at);
  return String(d.getHours()).padStart(2, '0') + ':' + String(d.getMinutes()).padStart(2, '0');
}

// Un message peut être du texte, une photo, ou un sticker.
function dmContent(m) {
  if (m.audio && String(m.audio).indexOf('data:audio/') === 0) {
    return '<div class="dm-bubble dm-audio"><audio controls preload="none" src="'
      + m.audio + '"></audio></div>';
  }
  if (m.image && isSafePhoto(m.image)) {
    return '<img class="dm-image" alt="photo" src="' + m.image + '">';
  }
  const st = stickerIndex(m.text);
  if (st >= 0) {
    return '<div class="dm-sticker" style="background:' + STICKERS[st].bg + '">' + STICKERS[st].emoji + '</div>';
  }
  return '<div class="dm-bubble">' + escapeHtml(m.text) + '</div>';
}

// Coches : une = envoyé, deux grises = reçu, deux jaunes = lu.
function dmTicks(m) {
  const moi = me ? normalizePhoneLocal(me.phone) : '';
  if (m.from !== moi) return '';
  if (m.read) return '<span class="dm-tick lu">' + icon('checks', 13) + '</span>';
  if (m.delivered) return '<span class="dm-tick">' + icon('checks', 13) + '</span>';
  return '<span class="dm-tick">' + icon('check', 13) + '</span>';
}

let dmMessages = [];

function dmRender(messages) {
  dmMessages = messages.slice();
  const box = \$('dmList');
  if (!messages.length) {
    box.innerHTML = '<div class="empty-note">Aucun message. Écris le premier !</div>';
    return;
  }
  const moi = me ? normalizePhoneLocal(me.phone) : '';
  box.innerHTML = messages.map((m) => \`
    <div class="dm-msg\${m.from === moi ? ' mine' : ''}">
      \${dmContent(m)}
      <div class="dm-time">\${heure(m.at)}\${dmTicks(m)}</div>
    </div>
  \`).join('');
  box.scrollTop = box.scrollHeight;
}

function dmAppend(m) {
  const box = \$('dmList');
  const vide = box.querySelector('.empty-note');
  if (vide) { box.innerHTML = ''; }

  const moi = me ? normalizePhoneLocal(me.phone) : '';
  const wrap = document.createElement('div');
  wrap.className = (m.from === moi) ? 'dm-msg mine' : 'dm-msg';

  const holder = document.createElement('div');
  holder.innerHTML = dmContent(m);

  const t = document.createElement('div');
  t.className = 'dm-time';
  t.innerHTML = escapeHtml(heure(m.at)) + dmTicks(m);

  wrap.appendChild(holder.firstElementChild);
  wrap.appendChild(t);
  dmMessages.push(m);
  box.appendChild(wrap);
  box.scrollTop = box.scrollHeight;
}

function dmSend() {
  const input = \$('dmInput');
  const texte = input.value.trim();
  if (!texte || !dmWith) return;
  socket.emit('dm:send', { toPhone: dmWith, text: texte });
  input.value = '';
  input.focus();
}

\$('dmSend').addEventListener('click', dmSend);
\$('dmInput').addEventListener('keydown', (e) => {
  if (e.key === 'Enter') { e.preventDefault(); dmSend(); }
});

socket.on('dm:history', ({ withKey, messages }) => {
  if (!dmWith || normalizePhoneLocal(dmWith) !== withKey) return;
  dmRender(messages);
});

socket.on('dm:new', (m) => {
  const ouverte = dmWith && normalizePhoneLocal(dmWith) === m.withKey;

  if (ouverte) {
    dmAppend(m);
    if (!m.mine) socket.emit('dm:history', { withPhone: dmWith }); // marque comme lu
    return;
  }

  if (!m.mine) {
    unreadCounts[m.withKey] = (unreadCounts[m.withKey] || 0) + 1;
    const nom = m.pseudo || 'Message';
    showToast(nom + ' : ' + m.text.slice(0, 40));
    systemNotify(nom, m.text.slice(0, 80));
    playBell();
    render();
  }
});

socket.on('dm:read', ({ withKey }) => {
  if (!dmWith || normalizePhoneLocal(dmWith) !== withKey) return;
  const moi = me ? normalizePhoneLocal(me.phone) : '';
  dmMessages.forEach((m) => { if (m.from === moi) { m.read = true; m.delivered = true; } });
  dmRender(dmMessages);
});

socket.on('dm:unread', (counts) => {
  unreadCounts = counts || {};
  render();
});

// -- Photos et stickers dans une conversation --------------------------------
let dmStickersOpen = false;

function buildDmStickers() {
  const box = \$('dmStickers');
  box.innerHTML = '';

  myStickers().forEach((data) => {
    const b = document.createElement('button');
    b.type = 'button';
    b.style.backgroundImage = 'url("' + data + '")';
    b.addEventListener('click', () => {
      socket.emit('dm:send', { toPhone: dmWith, image: data });
      toggleDmStickers(false);
    });
    box.appendChild(b);
  });

  STICKERS.forEach((st, i) => {
    const b = document.createElement('button');
    b.type = 'button';
    b.style.background = st.bg;
    b.textContent = st.emoji;
    b.title = st.label;
    b.addEventListener('click', () => {
      socket.emit('dm:send', { toPhone: dmWith, text: STICKER_PREFIX + i + '::' });
      toggleDmStickers(false);
    });
    box.appendChild(b);
  });
}

function toggleDmStickers(force) {
  dmStickersOpen = (typeof force === 'boolean') ? force : !dmStickersOpen;
  \$('dmStickers').classList.toggle('show', dmStickersOpen);
  \$('dmStickerBtn').classList.toggle('is-on', dmStickersOpen);
}

\$('dmStickerBtn').addEventListener('click', () => {
  buildDmStickers();
  toggleDmStickers();
});

\$('dmPhotoBtn').addEventListener('click', () => \$('dmPhotoInput').click());

\$('dmPhotoInput').addEventListener('change', async (e) => {
  const file = e.target.files && e.target.files[0];
  e.target.value = '';
  if (!file || !dmWith) return;
  try {
    // 640 px : assez net pour être regardé, assez léger pour partir vite.
    const data = await shrinkImage(file, 640, 0.6);
    socket.emit('dm:send', { toPhone: dmWith, image: data });
  } catch (err) {
    showToast("Impossible de lire cette image.");
  }
});

// Transformer une photo reçue en sticker perso : on la redessine en 160 px
// carrés, comme les stickers créés depuis la galerie.
function stickerFromDataUrl(dataUrl) {
  return new Promise((resolve, reject) => {
    const img = new Image();
    img.onerror = () => reject(new Error('image illisible'));
    img.onload = () => {
      const canvas = document.createElement('canvas');
      canvas.width = 160;
      canvas.height = 160;
      const ctx = canvas.getContext('2d');
      const cote = Math.min(img.width, img.height);
      ctx.drawImage(img, (img.width - cote) / 2, (img.height - cote) / 2, cote, cote, 0, 0, 160, 160);
      resolve(canvas.toDataURL('image/jpeg', 0.7));
    };
    img.src = dataUrl;
  });
}

async function photoVersSticker(dataUrl) {
  if (!isPremium()) { showToast('Les stickers perso font partie de LiveDoors Plus.'); return; }
  const liste = myStickers();
  if (liste.length >= 12) { showToast('12 stickers maximum — supprime-en un.'); return; }
  try {
    const petit = await stickerFromDataUrl(dataUrl);
    liste.push(petit);
    if (!saveMyStickers(liste)) { showToast('Mémoire pleine, sticker non enregistré.'); return; }
    buildEmojiBar();
    showToast('Photo ajoutée à tes stickers.');
  } catch (e) {
    showToast("Impossible de transformer cette photo.");
  }
}

// Appui long sur une photo de la conversation -> proposition de sticker.
\$('dmList').addEventListener('pointerdown', (e) => {
  const img = e.target.closest && e.target.closest('img.dm-image');
  if (!img) return;
  const minuteur = setTimeout(() => {
    if (confirm('Transformer cette photo en sticker ?')) photoVersSticker(img.src);
  }, 600);
  const annuler = () => clearTimeout(minuteur);
  img.addEventListener('pointerup', annuler, { once: true });
  img.addEventListener('pointerleave', annuler, { once: true });
});

// ---------------------------------------------------------------------------
// Points et badges
//
// Un point par tranche de 10 minutes d'appel. Les badges se débloquent à
// 10, 25, 50, 100, 200, 500, 1000, 2500, 5000 et 10 000 points.
// ---------------------------------------------------------------------------

const BADGE_STEPS = [10, 25, 50, 100, 200, 500, 1000, 2500, 5000, 10000];
const BADGE_NAMES = ['Débutant', 'Habitué', 'Bavard', 'Pilier', 'Vétéran',
  'Expert', 'Maître', 'Légende', 'Mythique', 'Ultime'];

let myPoints = 0;
let myBadge = 0;

function badgeClass(niveau) {
  if (niveau >= 10) return 'lvl10';
  if (niveau >= 9) return 'lvl9';
  if (niveau >= 7) return 'lvl7';
  if (niveau >= 4) return 'lvl4';
  return '';
}

function badgeChip(niveau) {
  if (!niveau) return '';
  return '<span class="badge-chip ' + badgeClass(niveau) + '">'
    + escapeHtml(BADGE_NAMES[niveau - 1]) + '</span>';
}

function refreshPoints() {
  if (document.getElementById('skinGrid')) refreshSkins();
  \$('pointsNb').textContent = myPoints;
  \$('pointsBadge').innerHTML = myBadge
    ? badgeChip(myBadge)
    : '<span class="points-lb">Aucun badge pour l\\'instant</span>';

  const suivant = BADGE_STEPS.find((s) => s > myPoints);
  if (suivant) {
    const precedent = myBadge ? BADGE_STEPS[myBadge - 1] : 0;
    const pct = Math.max(0, Math.min(100,
      ((myPoints - precedent) / (suivant - precedent)) * 100));
    \$('pointsFill').style.width = pct + '%';
    \$('pointsNext').textContent = 'Encore ' + (suivant - myPoints)
      + ' point' + (suivant - myPoints > 1 ? 's' : '') + ' pour « ' + BADGE_NAMES[myBadge] + ' »';
  } else {
    \$('pointsFill').style.width = '100%';
    \$('pointsNext').textContent = 'Tous les badges sont débloqués.';
  }
}

socket.on('points:update', ({ points, badge, nouveau, serie, multiplicateur, bonus }) => {
  myPoints = points;
  myBadge = badge;
  refreshPoints();
  if (bonus > 0) {
    showToast('Série de ' + serie + ' jours : x' + multiplicateur.toFixed(1)
      + ' — ' + bonus + ' point' + (bonus > 1 ? 's' : '') + ' en plus.');
  }
  if (nouveau) showToast('Nouveau badge débloqué : ' + BADGE_NAMES[badge - 1] + ' !');
});

// ---------------------------------------------------------------------------
// Scanner le QR code de quelqu'un
//
// On utilise le lecteur intégré au navigateur (BarcodeDetector). Il existe sur
// Android/Chrome mais PAS sur iPhone : dans ce cas on explique quoi faire, car
// l'appareil photo du téléphone sait déjà ouvrir le lien tout seul.
// ---------------------------------------------------------------------------

let scanStream = null;
let scanTimer = null;

function scanSupported() {
  return typeof window.BarcodeDetector !== 'undefined';
}

// Extrait le contact d'un texte scanné : lien complet, ou simple nom.
function contactFromScan(texte) {
  const t = String(texte || '').trim();
  if (!t) return '';
  const pos = t.indexOf('add=');
  if (pos !== -1) {
    let v = t.slice(pos + 4);
    const fin = v.search(/[&#]/);
    if (fin !== -1) v = v.slice(0, fin);
    try { return decodeURIComponent(v); } catch (e) { return v; }
  }
  // Pas un lien : peut-être directement un numéro ou un nom d'utilisateur.
  if (t.indexOf('http') === 0) return '';
  return t.slice(0, 40);
}

function ajouterDepuisScan(valeur) {
  if (!valeur) { showToast('QR code non reconnu.'); return false; }
  if (looksLikePhone(valeur)) socket.emit('contact:add', { phone: valeur });
  else socket.emit('contact:addByUsername', { username: valeur });
  showToast('Contact trouvé : ' + valeur);
  return true;
}

async function stopScan() {
  if (scanTimer) { clearInterval(scanTimer); scanTimer = null; }
  if (scanStream) {
    scanStream.getTracks().forEach((t) => t.stop());
    scanStream = null;
  }
  \$('scanVideo').srcObject = null;
}

\$('scanBtn').addEventListener('click', async () => {
  \$('scanModal').classList.add('show');

  if (!scanSupported()) {
    \$('scanBox').style.display = 'none';
    \$('scanHint').textContent = "Ton navigateur ne sait pas lire les QR codes. "
      + "Ouvre simplement l'appareil photo de ton téléphone et vise le QR : "
      + "il ouvrira LiveDoors et ajoutera le contact tout seul.";
    return;
  }

  \$('scanBox').style.display = 'block';
  \$('scanHint').textContent = 'Vise le QR code de ton ami.';

  try {
    scanStream = await navigator.mediaDevices.getUserMedia({
      video: { facingMode: 'environment' },
    });
    const video = \$('scanVideo');
    video.srcObject = scanStream;
    await video.play();

    const detecteur = new window.BarcodeDetector({ formats: ['qr_code'] });
    scanTimer = setInterval(async () => {
      try {
        const codes = await detecteur.detect(video);
        if (!codes.length) return;
        const valeur = contactFromScan(codes[0].rawValue);
        if (ajouterDepuisScan(valeur)) {
          await stopScan();
          \$('scanModal').classList.remove('show');
        }
      } catch (e) {}
    }, 350);
  } catch (e) {
    \$('scanBox').style.display = 'none';
    \$('scanHint').textContent = "Accès à la caméra refusé. Tu peux choisir une photo du QR à la place.";
  }
});

\$('scanClose').addEventListener('click', async () => {
  await stopScan();
  \$('scanModal').classList.remove('show');
});

// Lire un QR depuis une photo de la galerie
\$('scanFileBtn').addEventListener('click', () => \$('scanFileInput').click());

\$('scanFileInput').addEventListener('change', async (e) => {
  const file = e.target.files && e.target.files[0];
  e.target.value = '';
  if (!file) return;

  if (!scanSupported()) {
    showToast("Ton navigateur ne sait pas lire les QR codes.");
    return;
  }
  try {
    const img = await createImageBitmap(file);
    const detecteur = new window.BarcodeDetector({ formats: ['qr_code'] });
    const codes = await detecteur.detect(img);
    if (!codes.length) { showToast('Aucun QR code sur cette photo.'); return; }
    if (ajouterDepuisScan(contactFromScan(codes[0].rawValue))) {
      await stopScan();
      \$('scanModal').classList.remove('show');
    }
  } catch (err) {
    showToast("Impossible de lire cette image.");
  }
});

// ---------------------------------------------------------------------------
// Séries, titres et salle d'attente
// ---------------------------------------------------------------------------

let streaks = {};        // séries par contact, en mémoire
let myTitles = [];       // titres débloqués
let myTitle = '';        // titre affiché

const TITRE_NOMS = {
  nuit: 'Oiseau de nuit',
  hote: 'Hôte parfait',
  marathon: 'Marathonien vocal',
  fidele: 'Fidèle',
  or: "Clé d'or",
  pilier: 'Pilier du salon',
};
const TITRE_AIDE = {
  nuit: '10 appels après 22 h',
  hote: '25 appels chez toi',
  marathon: "un appel d'une heure",
  fidele: 'une série de 7 jours',
  or: 'une série de 30 jours',
  pilier: "10 heures d'appel au total",
};

function streakChip(phone) {
  const n = streaks[normalizePhoneLocal(phone)] || 0;
  if (!n) return '';
  return '<span class="streak-chip">🔥 ' + n + '</span>';
}

function titleChip(id) {
  if (!id || !TITRE_NOMS[id]) return '';
  return '<span class="title-chip">' + escapeHtml(TITRE_NOMS[id]) + '</span>';
}

socket.on('streak:update', ({ withKey, days }) => {
  streaks[withKey] = days;
  showToast('Série de ' + days + ' jour' + (days > 1 ? 's' : '') + ' 🔥');
  render();
});

// Choix du titre affiché sur le profil
function refreshTitles() {
  const box = \$('titleList');
  box.innerHTML = Object.keys(TITRE_NOMS).map((id) => {
    const debloque = myTitles.indexOf(id) !== -1;
    return '<button class="title-opt' + (myTitle === id ? ' on' : '')
      + (debloque ? '' : ' locked') + '" type="button" data-title="' + id + '"'
      + ' title="' + escapeAttr(debloque ? 'Débloqué' : TITRE_AIDE[id]) + '">'
      + escapeHtml(TITRE_NOMS[id]) + '</button>';
  }).join('');

  Array.from(box.children).forEach((b) => {
    b.addEventListener('click', () => {
      const id = b.getAttribute('data-title');
      if (myTitles.indexOf(id) === -1) {
        showToast('Pas encore débloqué : ' + TITRE_AIDE[id] + '.');
        return;
      }
      myTitle = (myTitle === id) ? '' : id;
      try { localStorage.setItem('livedoors-title', myTitle); } catch (e) {}
      refreshTitles();
      try { mySkin = parseInt(localStorage.getItem('livedoors-skin') || '0', 10) || 0; } catch (e) { mySkin = 0; }
      refreshSkins();
      applySkin();
      sendRegister();
      showToast(myTitle ? 'Titre affiché : ' + TITRE_NOMS[myTitle] : 'Titre retiré.');
    });
  });
}

// -- Salle d'attente : ce qu'on voit après avoir toqué --
function showWaiting(nom) {
  \$('waitText').textContent = 'Tu as toqué chez ' + nom + '…';
  \$('waitingRoom').classList.add('show');
}
function hideWaiting() {
  \$('waitingRoom').classList.remove('show');
}

\$('waitCancel').addEventListener('click', () => {
  pendingRequestHostId = null;
  hideWaiting();
  render();
  showToast('Demande annulée.');
});

// ---------------------------------------------------------------------------
// Mot vocal
//
// On maintient le bouton micro appuyé pour enregistrer, on relâche pour
// envoyer. Le son est compressé par le navigateur (format opus), ce qui rend
// une vingtaine de secondes très légère.
// ---------------------------------------------------------------------------

let recorder = null;
let recChunks = [];
let recStream = null;
let recAnnule = false;
let recDebut = 0;
let recMinuteur = null;

function recAffiche(actif) {
  \$('dmRecRow').classList.toggle('show', actif);
  \$('dmInputRow').style.display = actif ? 'none' : 'flex';
}

function recTemps() {
  const sec = Math.floor((Date.now() - recDebut) / 1000);
  const m = Math.floor(sec / 60);
  \$('dmRecTime').textContent = m + ':' + String(sec % 60).padStart(2, '0');
  if (sec >= 60) finirEnregistrement(false); // une minute maximum
}

async function startRecording() {
  if (!navigator.mediaDevices || !window.MediaRecorder) {
    showToast("Ton navigateur ne sait pas enregistrer le son.");
    return;
  }
  try {
    recStream = await navigator.mediaDevices.getUserMedia({ audio: true });
    recChunks = [];
    recAnnule = false;
    recorder = new MediaRecorder(recStream);

    recorder.ondataavailable = (e) => { if (e.data.size) recChunks.push(e.data); };
    recorder.onstop = () => {
      if (recStream) { recStream.getTracks().forEach((t) => t.stop()); recStream = null; }
      if (recMinuteur) { clearInterval(recMinuteur); recMinuteur = null; }
      recAffiche(false);

      if (recAnnule) { showToast('Enregistrement annulé.'); return; }

      const blob = new Blob(recChunks, { type: recorder && recorder.mimeType ? recorder.mimeType : 'audio/webm' });
      if (blob.size < 800) { showToast('Trop court.'); return; }

      const lecteur = new FileReader();
      lecteur.onload = () => {
        const data = String(lecteur.result);
        if (data.length > 300000) { showToast('Message vocal trop long.'); return; }
        socket.emit('dm:send', { toPhone: dmWith, audio: data });
      };
      lecteur.readAsDataURL(blob);
    };

    recorder.start();
    recDebut = Date.now();
    \$('dmRecTime').textContent = '0:00';
    recAffiche(true);
    recMinuteur = setInterval(recTemps, 250);
  } catch (e) {
    showToast("Accès au micro refusé.");
  }
}

// annule = true -> on jette l'enregistrement
function finirEnregistrement(annule) {
  recAnnule = !!annule;
  if (recorder && recorder.state === 'recording') recorder.stop();
  else recAffiche(false);
}

// Un appui suffit pour lancer : c'est ce que font les applis de messagerie,
// et c'est bien plus fiable que « maintenir appuyé » sur un écran tactile.
\$('dmVoiceBtn').addEventListener('click', startRecording);
\$('dmRecSend').addEventListener('click', () => finirEnregistrement(false));
\$('dmRecCancel').addEventListener('click', () => finirEnregistrement(true));

// ---------------------------------------------------------------------------
// Sondages en direct
// ---------------------------------------------------------------------------

let pollOptions = [];
let monVote = -1;
let pollCarte = null;      // la carte affichée dans le tchat
let pollFin = 0;           // heure de fin, pour le compte à rebours
let pollMinuteur = null;
let pollAuteur = false;

// -- Création : de 2 à 10 réponses --------------------------------------------
const POLL_MAX = 10;
let pollNbChamps = 2;

function renderPollInputs() {
  const box = \$('pollInputs');
  const valeurs = Array.from(box.querySelectorAll('input')).map((i) => i.value);
  box.innerHTML = '';

  for (let i = 0; i < pollNbChamps; i++) {
    const ligne = document.createElement('div');
    ligne.className = 'poll-row';

    const champ = document.createElement('input');
    champ.className = 'chat-input';
    champ.maxLength = 40;
    champ.placeholder = 'Réponse ' + (i + 1);
    champ.value = valeurs[i] || '';
    ligne.appendChild(champ);

    if (pollNbChamps > 2) {
      const sup = document.createElement('button');
      sup.type = 'button';
      sup.className = 'poll-del';
      sup.innerHTML = icon('close', 14);
      sup.addEventListener('click', () => {
        const restants = Array.from(box.querySelectorAll('input')).map((x) => x.value);
        restants.splice(i, 1);
        pollNbChamps--;
        renderPollInputs();
        Array.from(box.querySelectorAll('input')).forEach((x, k) => { x.value = restants[k] || ''; });
      });
      ligne.appendChild(sup);
    }
    box.appendChild(ligne);
  }
  \$('pollAdd').disabled = pollNbChamps >= POLL_MAX;
  \$('pollAdd').textContent = pollNbChamps >= POLL_MAX
    ? '10 réponses maximum'
    : '+ Ajouter une réponse (' + pollNbChamps + '/10)';
}

\$('pollAdd').addEventListener('click', () => {
  if (pollNbChamps >= POLL_MAX) return;
  pollNbChamps++;
  renderPollInputs();
});

\$('pollBtn').addEventListener('click', () => {
  \$('wallPanel').classList.remove('show');
  \$('fxPanel').classList.remove('show');
  const ouvert = \$('pollPanel').classList.toggle('show');
  \$('pollBtn').classList.toggle('is-on', ouvert);
  if (ouvert && !\$('pollInputs').children.length) renderPollInputs();
  closeChat();
});
\$('pollCloseBtn').addEventListener('click', () => {
  \$('pollPanel').classList.remove('show');
  \$('pollBtn').classList.remove('is-on');
});

\$('pollSend').addEventListener('click', () => {
  const q = \$('pollQuestion').value.trim();
  const opts = Array.from(\$('pollInputs').querySelectorAll('input'))
    .map((i) => i.value.trim()).filter(Boolean);
  if (!q || opts.length < 2) { showToast('Il faut une question et au moins deux réponses.'); return; }
  socket.emit('poll:start', { question: q, options: opts });
  \$('pollPanel').classList.remove('show');
  \$('pollBtn').classList.remove('is-on');
});

// -- Affichage dans le tchat ---------------------------------------------------
function pollTimerText() {
  const reste = Math.max(0, Math.ceil((pollFin - Date.now()) / 1000));
  return Math.floor(reste / 60) + ':' + String(reste % 60).padStart(2, '0');
}

function renderPollCard(question, resultats, votants, total) {
  if (!pollCarte) return;
  const somme = resultats.reduce((a, b) => a + b, 0) || 1;

  pollCarte.innerHTML = '<div class="poll-head"><div class="poll-q">'
    + escapeHtml(question) + '</div><div class="poll-timer">' + pollTimerText() + '</div></div>'
    + pollOptions.map((o, i) => '<button class="poll-opt' + (monVote === i ? ' mine' : '')
      + '" type="button" data-i="' + i + '">'
      + '<span class="poll-fill" style="width:' + ((resultats[i] / somme) * 100) + '%"></span>'
      + '<span class="poll-label"><span>' + escapeHtml(o) + '</span><span>' + resultats[i] + '</span></span>'
      + '</button>').join('')
    + '<div class="poll-count">' + votants + ' vote' + (votants > 1 ? 's' : '')
    + (total ? ' sur ' + total : '') + '</div>';

  Array.from(pollCarte.querySelectorAll('.poll-opt')).forEach((b) => {
    b.addEventListener('click', () => {
      monVote = Number(b.getAttribute('data-i'));
      socket.emit('poll:vote', { index: monVote });
    });
  });
}

socket.on('poll:show', ({ question, options, par, resultats, fin, auteur }) => {
  pollOptions = options;
  monVote = -1;
  pollFin = fin;
  pollAuteur = !!auteur;

  const box = \$('chatMessages');
  const vide = box.querySelector('.chat-empty');
  if (vide) vide.remove();

  pollCarte = document.createElement('div');
  pollCarte.className = 'poll-card';
  box.appendChild(pollCarte);
  renderPollCard(question, resultats, 0, 0);
  box.scrollTop = box.scrollHeight;

  if (pollMinuteur) clearInterval(pollMinuteur);
  pollMinuteur = setInterval(() => {
    const t = pollCarte && pollCarte.querySelector('.poll-timer');
    if (t) t.textContent = pollTimerText();
  }, 1000);

  openChat();
  showToast(par + ' a lancé un sondage : ' + question);
  playBell();
});

socket.on('poll:results', ({ resultats, votants, total }) => {
  const q = pollCarte && pollCarte.querySelector('.poll-q');
  renderPollCard(q ? q.textContent : '', resultats, votants, total);
});

socket.on('poll:end', ({ question, options, resultats, gagnants, max, votants }) => {
  if (pollMinuteur) { clearInterval(pollMinuteur); pollMinuteur = null; }
  if (!pollCarte) return;

  const somme = resultats.reduce((a, b) => a + b, 0) || 1;
  const titre = gagnants.length === 0
    ? "Personne n'a voté."
    : (gagnants.length === 1
      ? 'Gagnant : ' + gagnants[0] + ' (' + max + ' voix)'
      : 'Égalité : ' + gagnants.join(', ') + ' (' + max + ' voix chacun)');

  pollCarte.innerHTML = '<div class="poll-q">' + escapeHtml(question) + '</div>'
    + options.map((o, i) => '<button class="poll-opt" type="button" disabled>'
      + '<span class="poll-fill" style="width:' + ((resultats[i] / somme) * 100) + '%"></span>'
      + '<span class="poll-label"><span>' + escapeHtml(o) + '</span><span>' + resultats[i] + '</span></span>'
      + '</button>').join('')
    + '<div class="poll-win">' + escapeHtml(titre) + '</div>'
    + '<div class="poll-count">' + votants + ' participant' + (votants > 1 ? 's' : '') + '</div>';

  pollCarte = null;
  showToast(titre);
  systemNotify('Sondage terminé', titre);
});

// Le détail nominatif n'arrive qu'à celui qui a lancé le sondage.
socket.on('poll:detail', ({ detail }) => {
  if (!detail || !detail.length) return;
  const lignes = detail.map((d) => escapeHtml(d.pseudo) + ' → ' + escapeHtml(d.reponse)).join('<br>');
  const bloc = document.createElement('div');
  bloc.className = 'poll-card';
  bloc.innerHTML = '<div class="poll-q">Qui a voté quoi</div><div class="poll-detail">' + lignes + '</div>';
  \$('chatMessages').appendChild(bloc);
  \$('chatMessages').scrollTop = \$('chatMessages').scrollHeight;
});

// ---------------------------------------------------------------------------
// Skins de porte
// ---------------------------------------------------------------------------

const SKINS = [
  { id: 0, nom: 'Bois classique', cout: 0,    css: 'linear-gradient(160deg,#c98b3a,#8a5a20)' },
  { id: 1, nom: 'Chêne foncé',    cout: 10,   css: 'linear-gradient(160deg,#6b4423,#3b2412)' },
  { id: 2, nom: 'Néon',           cout: 50,   css: 'linear-gradient(160deg,#ff3d77,#a55eea,#45aaf2)' },
  { id: 3, nom: 'Blindée',        cout: 100,  css: 'repeating-linear-gradient(45deg,#4a4f57 0 10px,#3a3f47 10px 20px)' },
  { id: 4, nom: 'Futuriste',      cout: 250,  css: 'linear-gradient(160deg,#0f2027,#2c5364,#26de81)' },
  { id: 5, nom: 'Or massif',      cout: 1000, css: 'linear-gradient(160deg,#fff1a8,#e0a800,#8a6400)' },
];

let mySkin = 0;

function skinCss(id) {
  const s = SKINS.find((x) => x.id === Number(id));
  return s ? s.css : SKINS[0].css;
}

function refreshSkins() {
  const box = \$('skinGrid');
  box.innerHTML = SKINS.map((s) => {
    const ouvert = myPoints >= s.cout;
    return '<button class="skin-opt' + (mySkin === s.id ? ' on' : '')
      + (ouvert ? '' : ' locked') + '" type="button" data-skin="' + s.id + '"'
      + ' style="background:' + s.css + '">'
      + '<span class="skin-name">' + escapeHtml(s.nom)
      + (ouvert ? '' : ' · ' + s.cout + ' pts') + '</span></button>';
  }).join('');

  Array.from(box.children).forEach((b) => {
    b.addEventListener('click', () => {
      const id = Number(b.getAttribute('data-skin'));
      const s = SKINS.find((x) => x.id === id);
      if (myPoints < s.cout) { showToast('Il te faut ' + s.cout + ' points pour ce skin.'); return; }
      mySkin = id;
      try { localStorage.setItem('livedoors-skin', String(id)); } catch (e) {}
      refreshSkins();
      applySkin();
      sendRegister();
      showToast('Porte : ' + s.nom);
    });
  });
}

// Le skin habille la porte de la salle d'attente et le bouton d'ouverture.
function applySkin() {
  const css = skinCss(mySkin);
  const porte = document.querySelector('.wait-door');
  if (porte) porte.style.background = css;
}

// Le serveur a refusé un changement de profil : on remet la valeur d'avant
// pour que l'écran ne mente pas sur ce qui est réellement enregistré.
socket.on('profile:error', ({ champ, actuel, message }) => {
  showToast(message);
  if (champ === 'username') {
    if (profile) { profile.username = actuel || ''; saveProfile(profile); }
    if (onlineProfile) onlineProfile.username = actuel || '';
    \$('editUsername').value = actuel || '';
  }
});

// ---------------------------------------------------------------------------
// Invitations pendant un appel
// ---------------------------------------------------------------------------

let inviteHostId = null;

// Notification du téléphone : elle n'apparaît que si la personne a donné son
// accord ET que l'appli n'est pas au premier plan (sinon la bannière suffit).
function askNotifyPermission() {
  try {
    if ('Notification' in window && Notification.permission === 'default') {
      Notification.requestPermission();
    }
  } catch (e) {}
}

function systemNotify(titre, texte) {
  try {
    if (!('Notification' in window)) return;
    if (Notification.permission !== 'granted') return;
    if (document.visibilityState === 'visible') return;
    new Notification(titre, { body: texte, tag: 'livedoors' });
  } catch (e) {}
}

socket.on('call:invite-sent', ({ pseudo }) => {
  showToast('Invitation envoyée à ' + pseudo + '.');
});

socket.on('call:invitation', ({ hostId, hostPseudo, from, people }) => {
  if (inCall) { showToast(displayName(from) + " t'invite, mais tu es déjà en appel."); return; }

  inviteHostId = hostId;
  paintAvatarFor(\$('inviteAvatar'), from);
  \$('inviteName').textContent = displayName(from) + " t'invite";
  \$('inviteSub').textContent = people > 1
    ? 'Appel chez ' + hostPseudo + ' · ' + people + ' personnes'
    : 'Appel chez ' + hostPseudo;
  \$('callInvite').style.display = 'block';

  playBell();
  showToast(displayName(from) + " t'invite à rejoindre un appel");
  systemNotify('LiveDoors', displayName(from) + " t'invite à rejoindre un appel");
});

\$('inviteIgnore').addEventListener('click', () => {
  \$('callInvite').style.display = 'none';
  inviteHostId = null;
});

\$('inviteJoin').addEventListener('click', () => {
  if (!inviteHostId) return;
  socket.emit('call:request', { hostId: inviteHostId, message: 'Invité à rejoindre' });
  pendingRequestHostId = inviteHostId;
  \$('callInvite').style.display = 'none';
  inviteHostId = null;
  showToast('Demande envoyée…');
  render();
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

// ---------------------------------------------------------------------------
// Webhook Stripe : c'est LA seule porte d'entrée qui rend un compte abonné.
// Stripe appelle cette adresse de serveur à serveur, avec une signature qu'on
// vérifie. Personne ne peut l'imiter depuis un navigateur.
// ---------------------------------------------------------------------------

app.post('/webhooks/stripe', express.raw({ type: 'application/json' }), async (req, res) => {
  if (!paymentsOn) return res.status(404).end();

  let event;
  try {
    event = stripe.webhooks.constructEvent(
      req.body,
      req.headers['stripe-signature'],
      STRIPE_WEBHOOK_SECRET,
    );
  } catch (e) {
    console.warn('Webhook refusé (signature invalide) :', e.message);
    return res.status(400).send('signature invalide');
  }

  try {
    const objet = event.data.object;

    if (event.type === 'checkout.session.completed') {
      // Le paiement est validé : on note le client Stripe et on ouvre l'accès.
      const sub = objet.subscription
        ? await stripe.subscriptions.retrieve(objet.subscription)
        : null;
      await applySubscription({
        phoneKey: objet.client_reference_id,
        active: true,
        until: sub ? sub.current_period_end * 1000 : Date.now() + 31 * 86400000,
        customerId: objet.customer,
      });
    }

    if (event.type === 'customer.subscription.updated'
      || event.type === 'customer.subscription.deleted') {
      // Renouvellement, résiliation, échec de paiement : on suit Stripe.
      const actif = ['active', 'trialing'].includes(objet.status);
      const compte = await findAccountByCustomer(objet.customer);
      if (compte) {
        await applySubscription({
          phoneKey: compte.phoneKey,
          active: actif,
          until: actif ? objet.current_period_end * 1000 : null,
          customerId: objet.customer,
        });
      }
    }

    res.json({ received: true });
  } catch (e) {
    console.error('Erreur de traitement du webhook :', e.message);
    res.status(500).end();
  }
});

// Adresse légère pour les vérifications d'état et les « pings » anti-veille.
// Elle ne renvoie que quelques octets, au lieu des ~250 Ko de la page.
app.get('/health', (req, res) => {
  res.type('text/plain').send('ok');
});

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
// On prépare la base AVANT d'accepter des connexions : sinon les premiers
// visiteurs tomberaient sur une table qui n'existe pas encore.
db.init()
  .then(() => {
    server.listen(PORT, () => {
      console.log(`LiveDoors — serveur tout-en-un sur http://localhost:${PORT}`);
    });
  })
  .catch((e) => {
    console.error('Impossible de préparer la base de données :', e.message);
    process.exit(1);
  });
