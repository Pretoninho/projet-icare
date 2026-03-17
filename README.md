# Projet iCare

## Description
Projet iCare est une application de réplication en direct des données publiques Deribit.
Le service se connecte en WebSocket, souscrit à des canaux de marché, conserve le dernier état en mémoire et persiste les événements en local.
Une interface web locale permet aussi d'exploiter le service sans passer directement par les endpoints JSON.

## Fonctionnalités
- Connexion temps réel à l'API WebSocket Deribit (JSON-RPC v2)
- Souscription à plusieurs canaux (ticker et index)
- Reconnexion automatique en cas de coupure
- Écriture continue des événements dans data/events.jsonl
- Snapshot régulier de l'état courant dans data/snapshot.json
- API locale REST pour exposer la réplication
- Flux WebSocket local pour diffuser les mises à jour en direct
- Endpoint analytics pour indicateurs temps réel
- Persistance PostgreSQL optionnelle pour historisation
- Mode TimescaleDB optionnel (hypertable, rétention, agrégats OHLC)
- Test d'intégration avec mock Deribit

## Installation
1. Cloner le dépôt puis se placer dans le dossier du projet
2. Installer les dépendances:

	npm install

3. Si vous activez PostgreSQL/TimescaleDB:

	npm install pg

## Démarrage
Lancer le service:

npm start

Ouvrir ensuite l'interface web locale:

http://localhost:3000/

Lancer les tests:

npm test

## Stack locale TimescaleDB (Docker)
Démarrer la base locale:

npm run db:up

Vérifier les logs:

npm run db:logs

Arrêter la stack:

npm run db:down

Déployer application + base (build inclus):

npm run deploy:up

Logs applicatifs:

npm run deploy:logs

Stop complet:

npm run deploy:down

Configuration recommandée:
1. Copier .env.example vers .env
2. Garder DATABASE_URL=postgres://icare:icare@localhost:5432/icare
3. Activer TIMESCALE_ENABLED=true

Démarrage complet en local:
1. Copier .env.example vers .env
2. npm install
3. npm install pg
4. npm run deploy:up
5. Ouvrir http://localhost:3000/health

## Variables d'environnement
- DERIBIT_WS_URL: URL WebSocket Deribit (défaut: wss://www.deribit.com/ws/api/v2)
- DERIBIT_CHANNELS: liste CSV des canaux à souscrire
- HEARTBEAT_INTERVAL_SECONDS: intervalle heartbeat Deribit (défaut: 10)
- RECONNECT_DELAY_MS: délai avant reconnexion (défaut: 3000)
- SNAPSHOT_INTERVAL_MS: fréquence d'écriture snapshot (défaut: 30000)
- MAX_EVENTS_IN_MEMORY: taille max du buffer mémoire (défaut: 2000)
- API_PORT: port HTTP de l'API locale (défaut: 3000)
- LOCAL_WS_PATH: chemin du flux WebSocket local (défaut: /stream)
- DATABASE_URL: URL PostgreSQL pour activer la persistance (optionnel)
- DB_SCHEMA: schéma SQL cible (défaut: public)
- DB_TABLE: table SQL cible (défaut: deribit_events)
- TIMESCALE_ENABLED: active la configuration TimescaleDB (true/false, défaut: false)
- TS_CHUNK_INTERVAL: taille des chunks hypertable (défaut: 1 day)
- TS_RETENTION_INTERVAL: rétention des données brutes (défaut: 30 days)
- TS_AGG_BUCKET: bucket d'agrégation OHLC (défaut: 1 minute)
- TS_AGG_POLICY_START_OFFSET: fenêtre de recalcul agrégat (défaut: 7 days)
- TS_AGG_POLICY_END_OFFSET: décalage fin pour policy agrégat (défaut: 1 minute)
- TS_AGG_POLICY_SCHEDULE: fréquence policy agrégat (défaut: 1 minute)
- ANALYTICS_DEFAULT_WINDOW_MS: fenêtre par défaut pour /analytics (défaut: 300000)

## Exemple de lancement personnalisé
DERIBIT_CHANNELS=ticker.BTC-PERPETUAL.raw,ticker.ETH-PERPETUAL.raw SNAPSHOT_INTERVAL_MS=10000 npm start

Exemple avec TimescaleDB local:

DATABASE_URL=postgres://icare:icare@localhost:5432/icare TIMESCALE_ENABLED=true npm start

## Sorties générées
- data/events.jsonl: flux append-only des mises à jour reçues
- data/snapshot.json: dernier état consolidé par canal

## API locale
- GET /health: état de connexion vers Deribit et métriques locales
- GET /snapshot: dernier état consolidé par canal
- GET /events?limit=200: derniers événements en mémoire
- GET /candles?channel=ticker.BTC-PERPETUAL.raw&limit=500: bougies OHLC (TimescaleDB requis)
- GET /analytics?channel=ticker.BTC-PERPETUAL.raw&windowMs=300000: stats prix et volatilité sur la fenêtre

## Interface web
- URL: http://localhost:3000/
- Vue statut de connexion Deribit + WebSocket local
- Sélection du canal observé et de la fenêtre analytics
- Courbe live basée sur les derniers ticks en mémoire
- Tableau snapshot par canal
- Flux des événements récents
- Tableau de bougies OHLC si TimescaleDB est disponible

## Flux WebSocket local
- URL: ws://localhost:3000/stream (selon API_PORT et LOCAL_WS_PATH)
- Message initial: snapshot complet
- Messages suivants: événements en direct

## Persistance PostgreSQL
Si DATABASE_URL est défini, l'application:
- crée le schéma/table si nécessaire
- insère chaque événement de canal dans une colonne JSONB

Pour activer PostgreSQL, installer d'abord le driver:

npm install pg

Structure de table:
- id BIGSERIAL PRIMARY KEY
- received_at TIMESTAMPTZ
- channel TEXT
- payload JSONB

## Mode TimescaleDB
Avec TIMESCALE_ENABLED=true et une base compatible TimescaleDB:
- la table brute est convertie en hypertable
- une policy de rétention est appliquée
- une vue continue OHLC est créée
- l'endpoint /candles expose les bougies agrégées

Exemple:

DATABASE_URL=postgres://user:pass@localhost:5432/icare TIMESCALE_ENABLED=true npm start