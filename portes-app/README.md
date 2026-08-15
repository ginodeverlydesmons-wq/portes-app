# Porte Ouverte — squelette temps réel

Ce dossier remplace la maquette statique par une vraie base technique :

- **Comptes réels** : écran de création de profil (pseudo + numéro de téléphone),
  avatar généré automatiquement. Le compte vit côté serveur pendant la connexion
  Socket.io (voir limites plus bas).
- **Présence en temps réel** : la liste "en direct / portes fermées" n'est plus
  simulée — elle reflète les comptes réellement connectés au serveur, via
  l'événement `friends:update`.
- **Appel audio réel** : "Ouvrir sa porte" capture ton micro (`getUserMedia`) et
  crée une session côté serveur. "Rejoindre" établit une vraie connexion
  **WebRTC peer-to-peer** entre les deux navigateurs ; le serveur Socket.io ne
  sert qu'à échanger les messages de signalisation (offer/answer/ICE), jamais
  à transporter l'audio lui-même.

## Lancer le projet — méthode simple (double-clic)

**Sur Windows** : double-clique sur `demarrer.bat`.
**Sur Mac/Linux** : double-clique sur `demarrer.sh` (ou lance `./demarrer.sh` dans un terminal).

Le script installe automatiquement ce qu'il faut la première fois, démarre
le serveur, et ouvre ton navigateur sur `http://localhost:3000`. Une fenêtre
noire ("Portes - Serveur") reste ouverte pendant que tu utilises l'appli —
c'est normal, c'est le serveur qui tourne : ne la ferme pas tant que tu veux
garder l'appli active.

⚠️ Il faut avoir **Node.js** installé sur l'ordinateur pour que ça
fonctionne (le script te prévient et te redirige vers
[nodejs.org](https://nodejs.org) si ce n'est pas le cas — prends la version
"LTS").

## Lancer le projet — méthode manuelle (ligne de commande)

```bash
cd porte-ouverte-realtime
npm install
npm start
```

Ouvre `http://localhost:3000` dans **deux onglets/navigateurs différents**
(ou deux appareils sur le même réseau via l'IP locale de ta machine), crée un
profil dans chacun, ouvre la porte dans l'un et rejoins-la depuis l'autre.

Le navigateur demandera l'autorisation d'utiliser le micro — accepte-la des
deux côtés pour que l'appel fonctionne.

## Ce qui est déjà en place

- Tout le projet (serveur Express/Socket.io + HTML/CSS/JS client) tient dans
  un seul `index.js`, présence des comptes, ouverture/fermeture de "porte"
  (= une room), et relais de signalisation WebRTC (`webrtc:offer` /
  `webrtc:answer` / `webrtc:ice-candidate`).
- Logique client embarquée : capture micro, création des
  `RTCPeerConnection` (topologie *mesh*, une connexion directe par paire de
  participants), rattachement de l'audio distant.
- Plus aucune donnée fictive : `simulateActivity` et les faux amis ont été
  supprimés, tout vient du serveur.
- **PWA installable** : manifest (`/manifest.json`), service worker
  (`/sw.js`) qui met en cache la coquille de l'appli (jamais Socket.io), et
  icônes générées (`/icons/icon-180.png`, `-192.png`, `-512.png`).

## Installer l'appli sur un téléphone (PWA)

Une PWA doit être servie en **HTTPS** pour s'installer (le service worker
refuse de s'enregistrer en HTTP simple) — `localhost` fait exception pour
les tests sur ton ordinateur, mais pas pour un vrai téléphone sur le réseau.
Le plus simple pour tester rapidement sur mobile : déployer sur un hébergeur
qui fournit du HTTPS automatiquement (Render, Railway, Fly.io...), ou passer
par un tunnel HTTPS temporaire (ex. `ngrok http 3000`) pointé vers ton
serveur local.

Une fois l'URL en HTTPS ouverte sur le téléphone :

- **Android (Chrome)** : un bandeau "Ajouter à l'écran d'accueil" apparaît
  automatiquement, ou via le menu ⋮ → "Installer l'application".
- **iOS (Safari)** : Safari n'installe pas automatiquement — il faut ouvrir
  le menu Partager (icône carrée avec une flèche) → "Sur l'écran d'accueil".
  C'est une limite d'iOS, pas de ton code.

Une fois installée, l'icône "porte" apparaît sur l'écran d'accueil et
l'appli s'ouvre en plein écran, sans barre d'adresse.

### Limites à connaître pour une PWA (par rapport à une vraie appli native)

- **Pas de vraies notifications push fiables sur iOS** avant iOS 16.4, et
  même après, ça reste plus limité que sur Android.
- **Pas d'appel audio en tâche de fond** : si l'utilisateur verrouille son
  téléphone ou change d'appli pendant un appel, la connexion WebRTC peut être
  coupée par l'OS (surtout sur iOS). Une vraie appli native (ou un wrapper
  Capacitor avec les bonnes permissions) gère mieux ce cas.
- **Pas de distribution sur l'App Store / Play Store** avec une PWA seule —
  si tu veux une présence sur les stores plus tard, la même base de code
  peut être enveloppée avec **Capacitor** sans tout réécrire.

## Ce qu'il reste à faire avant une vraie mise en production

1. **Comptes persistants** : remplacer la `Map` en mémoire par une vraie base
   (Postgres/Mongo) + une authentification (mot de passe, magic link, ou
   vérification du numéro par SMS/OTP — un simple champ texte non vérifié
   comme ici ne suffit pas pour prouver qu'un numéro appartient à la personne).
2. **Vrai carnet d'amis** : aujourd'hui, "amis" = tous les comptes connectés
   au serveur. Une vraie version aurait une relation d'amitié en base
   (demandes d'ami, blocage, appariement par numéro de téléphone haché).
3. **Serveur TURN** : le STUN public (`stun.l.google.com`) suffit pour les
   tests, mais échoue dès qu'un participant est derrière un réseau
   restrictif (4G, wifi d'entreprise). Il faut un serveur TURN en prod (ex.
   coturn auto-hébergé, ou un service géré comme Twilio/Xirsys).
4. **Hébergement** : Socket.io a besoin d'un serveur qui garde des connexions
   ouvertes en permanence — ça fonctionne sur Render, Railway, Fly.io ou un
   VPS classique, mais pas sur une plateforme purement serverless (type
   Vercel Functions) sans adaptation.
5. **Passage à l'échelle des groupes** : la topologie *mesh* actuelle (chaque
   participant se connecte directement à tous les autres) convient à de
   petits groupes d'amis. Au-delà de 4-5 personnes dans un même appel, il
   vaut mieux passer par un SFU (ex. mediasoup, LiveKit) plutôt que du mesh
   pur.
6. **Sécurité** : `cors: { origin: '*' }` est à restreindre à ton domaine, et
   il faudra réfléchir à qui peut voir le numéro de téléphone de qui.
