@echo off
title Portes
cd /d "%~dp0"

where node >nul 2>nul
if errorlevel 1 (
    echo.
    echo ==========================================================
    echo  Node.js n'est pas installe sur cet ordinateur.
    echo  Telecharge-le (version LTS) ici : https://nodejs.org
    echo  Une fois installe, relance ce fichier (demarrer.bat).
    echo ==========================================================
    echo.
    pause
    exit /b
)

if not exist node_modules (
    echo.
    echo Premiere installation, patiente quelques secondes...
    call npm install
    echo.
)

echo Demarrage du serveur...
start "Portes - Serveur (ne pas fermer cette fenetre)" cmd /k node index.js

timeout /t 3 /nobreak >nul
start "" http://localhost:3000

echo.
echo L'appli s'ouvre dans ton navigateur (http://localhost:3000).
echo Pour arreter l'appli, ferme la fenetre "Portes - Serveur".
echo.
timeout /t 5 >nul
