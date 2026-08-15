#!/bin/bash
cd "$(dirname "$0")"

if ! command -v node >/dev/null 2>&1; then
  echo ""
  echo "=========================================================="
  echo " Node.js n'est pas installé sur cet ordinateur."
  echo " Télécharge-le (version LTS) ici : https://nodejs.org"
  echo " Une fois installé, relance ce script (./demarrer.sh)."
  echo "=========================================================="
  echo ""
  read -p "Appuie sur Entrée pour fermer..."
  exit 1
fi

if [ ! -d "node_modules" ]; then
  echo "Première installation, patiente quelques secondes..."
  npm install
fi

echo "Démarrage du serveur..."
node index.js &
SERVER_PID=$!

sleep 2

if command -v open >/dev/null 2>&1; then
  open "http://localhost:3000"       # macOS
elif command -v xdg-open >/dev/null 2>&1; then
  xdg-open "http://localhost:3000"   # Linux
fi

echo ""
echo "L'appli tourne sur http://localhost:3000"
echo "Pour l'arrêter : ferme ce terminal ou fais Ctrl+C."
wait $SERVER_PID
