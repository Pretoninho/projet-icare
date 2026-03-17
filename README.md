# Projet iCare

## Description
Projet iCare est une application de replication en direct des donnees publiques Deribit.
Le service se connecte en WebSocket, souscrit a des canaux de marche, garde le dernier etat en memoire et persiste les evenements en local.

## Fonctionnalites
- Connexion temps reel a l'API WebSocket Deribit (JSON-RPC v2)
- Souscription a plusieurs canaux (ticker et index)
- Reconnexion automatique en cas de coupure
- Ecriture continue des evenements dans data/events.jsonl
- Snapshot regulier de l'etat courant dans data/snapshot.json
- API locale REST pour exposer la replication
- Flux WebSocket local pour diffuser les updates en direct
- Endpoint analytics pour indicateurs temps reel
- Persistance PostgreSQL optionnelle pour historisation
- Mode TimescaleDB optionnel (hypertable, retention, agregats OHLC)
- Test d'integration avec mock Deribit

## Installation
1. Cloner le depot puis se placer dans le dossier du projet
2. Installer les dependances:

	npm install

3. Si vous activez PostgreSQL/TimescaleDB:

	npm install pg

## Demarrage
Lancer le service:

npm start

Lancer les tests:

npm test

## Stack locale TimescaleDB (Docker)
Demarrer la base locale:

npm run db:up

Verifier les logs:

npm run db:logs

Arreter la stack:

npm run db:down

Deployer application + base (build inclus):

npm run deploy:up

Logs applicatifs:

npm run deploy:logs

Stop complet:

npm run deploy:down

Configuration recommandee:
1. Copier .env.example vers .env
2. Garder DATABASE_URL=postgres://icare:icare@localhost:5432/icare
3. Activer TIMESCALE_ENABLED=true

Demarrage complet en local:
1. Copier .env.example vers .env
2. npm install
3. npm install pg
4. npm run deploy:up
5. Ouvrir http://localhost:3000/health

## Variables d'environnement
- DERIBIT_WS_URL: URL WebSocket Deribit (defaut: wss://www.deribit.com/ws/api/v2)
- DERIBIT_CHANNELS: liste CSV des canaux a souscrire
- HEARTBEAT_INTERVAL_SECONDS: intervalle heartbeat Deribit (defaut: 10)
- RECONNECT_DELAY_MS: delai avant reconnexion (defaut: 3000)
- SNAPSHOT_INTERVAL_MS: frequence d'ecriture snapshot (defaut: 30000)
- MAX_EVENTS_IN_MEMORY: taille max du buffer memoire (defaut: 2000)
- API_PORT: port HTTP de l'API locale (defaut: 3000)
- LOCAL_WS_PATH: path du flux WebSocket local (defaut: /stream)
- DATABASE_URL: URL PostgreSQL pour activer la persistance (optionnel)
- DB_SCHEMA: schema SQL cible (defaut: public)
- DB_TABLE: table SQL cible (defaut: deribit_events)
- TIMESCALE_ENABLED: active la configuration TimescaleDB (true/false, defaut: false)
- TS_CHUNK_INTERVAL: taille des chunks hypertable (defaut: 1 day)
- TS_RETENTION_INTERVAL: retention des donnees brutes (defaut: 30 days)
- TS_AGG_BUCKET: bucket d'agregation OHLC (defaut: 1 minute)
- TS_AGG_POLICY_START_OFFSET: fenetre de recalcul agregat (defaut: 7 days)
- TS_AGG_POLICY_END_OFFSET: decalage fin pour policy agregat (defaut: 1 minute)
- TS_AGG_POLICY_SCHEDULE: frequence policy agregat (defaut: 1 minute)
- ANALYTICS_DEFAULT_WINDOW_MS: fenetre par defaut pour /analytics (defaut: 300000)

## Exemple de lancement personnalise
DERIBIT_CHANNELS=ticker.BTC-PERPETUAL.raw,ticker.ETH-PERPETUAL.raw SNAPSHOT_INTERVAL_MS=10000 npm start

Exemple avec TimescaleDB local:

DATABASE_URL=postgres://icare:icare@localhost:5432/icare TIMESCALE_ENABLED=true npm start

## Sorties generees
- data/events.jsonl: flux append-only des updates recus
- data/snapshot.json: dernier etat consolide par canal

## API locale
- GET /health: etat de connexion vers Deribit et metriques locales
- GET /snapshot: dernier etat consolide par canal
- GET /events?limit=200: derniers evenements en memoire
- GET /candles?channel=ticker.BTC-PERPETUAL.raw&limit=500: bougies OHLC (TimescaleDB requis)
- GET /analytics?channel=ticker.BTC-PERPETUAL.raw&windowMs=300000: stats prix et volatilite sur la fenetre

## Flux WebSocket local
- URL: ws://localhost:3000/stream (selon API_PORT et LOCAL_WS_PATH)
- Message initial: snapshot complet
- Messages suivants: events en direct

## Persistance PostgreSQL
Si DATABASE_URL est defini, l'application:
- cree le schema/table si necessaire
- insere chaque event de canal dans une colonne JSONB

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
- une policy de retention est appliquee
- une vue continue OHLC est creee
- l'endpoint /candles expose les bougies agregees

Exemple:

DATABASE_URL=postgres://user:pass@localhost:5432/icare TIMESCALE_ENABLED=true npm start