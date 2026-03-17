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
- SCORE_LONG_THRESHOLD: seuil score pour biais long (défaut: 65)
- SCORE_SHORT_THRESHOLD: seuil score pour biais short (défaut: 35)
- SCORE_WEIGHT_TREND: poids composante trend (défaut: 0.25)
- SCORE_WEIGHT_FLOW: poids composante flow (défaut: 0.25)
- SCORE_WEIGHT_DERIV: poids composante dérivés (défaut: 0.20)
- SCORE_WEIGHT_OPTIONS: poids composante options (défaut: 0.20)
- SCORE_WEIGHT_RISK: poids pénalité risque (défaut: 0.10)
- KILL_SWITCH_MAX_SPREAD_BPS: spread max avant neutralisation (défaut: 24)
- KILL_SWITCH_MAX_REALIZED_VOL: vol max avant neutralisation (défaut: 0.03)
- CALIBRATOR_METHOD: calibrateur probabiliste (`platt` ou `isotonic`, défaut: platt)
- CALIBRATION_LOOKBACK_LABELS: historique labels utilisé pour calibration (défaut: 800)
- CALIBRATION_MIN_SAMPLES: minimum labels décisionnels pour calibrer (défaut: 60)
- REGIME_TREND_THRESHOLD: seuil trend/range pour qualité par régime (défaut: 0.0035)
- REGIME_VOL_HIGH_THRESHOLD: seuil vol high/low pour qualité par régime (défaut: 0.01)
- WEB_PUSH_ENABLED: active les notifications Web Push serveur (true/false, défaut: false)
- VAPID_SUBJECT: contact VAPID (défaut: mailto:alerts@icare.local)
- VAPID_PUBLIC_KEY: clé publique VAPID pour abonnement navigateur
- VAPID_PRIVATE_KEY: clé privée VAPID pour envoi push
- SERVER_NOTIFICATION_COOLDOWN_MS: anti-spam serveur par type de notification (défaut: 300000)
- SERVER_NOTIFICATION_MIN_PROBABILITY: seuil minimal serveur pour alerte signal (défaut: 0.62)

## Exemple de lancement personnalisé
DERIBIT_CHANNELS=ticker.BTC-PERPETUAL.100ms,ticker.ETH-PERPETUAL.100ms,ticker.SOL-PERPETUAL.100ms,deribit_price_index.btc_usd,deribit_price_index.eth_usd,deribit_price_index.sol_usd SNAPSHOT_INTERVAL_MS=10000 npm start

Exemple multi-instruments (6 canaux):

DERIBIT_CHANNELS=ticker.BTC-PERPETUAL.100ms,ticker.ETH-PERPETUAL.100ms,ticker.SOL-PERPETUAL.100ms,deribit_price_index.btc_usd,deribit_price_index.eth_usd,deribit_price_index.sol_usd npm start

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
- Pipeline de features/labels dérivés des ticks Deribit
- Endpoint signal (direction, probabilité, SL/TP)
## Flux WebSocket local
- URL: ws://localhost:3000/stream (selon API_PORT et LOCAL_WS_PATH)
- FEATURE_WINDOW_FAST: fenêtre courte (ticks) pour features momentum (défaut: 5)
- FEATURE_WINDOW_SLOW: fenêtre longue (ticks) pour features tendance/volatilité (défaut: 20)
- SIGNAL_HORIZON_POINTS: horizon (ticks) pour génération des labels (défaut: 15)
- SIGNAL_MIN_PROBABILITY: seuil minimum de décision long/short (défaut: 0.55)
- DERIBIT_SERIES_PATH: fichier JSONL série marché dérivée (défaut: series_deribit.jsonl)
- FEATURES_SERIES_PATH: fichier JSONL série features (défaut: series_features.jsonl)
- LABELS_SERIES_PATH: fichier JSONL série labels (défaut: series_labels.jsonl)
Avec TIMESCALE_ENABLED=true et une base compatible TimescaleDB:
- GET /signal?channel=ticker.BTC-PERPETUAL.raw: signal directionnel + probabilité + niveaux SL/TP
- GET /features?channel=ticker.BTC-PERPETUAL.raw&limit=200: derniers points de features calculés
- GET /labels?channel=ticker.BTC-PERPETUAL.raw&limit=200: labels ex post (tp/sl/timeout)
- GET /series/schema: schéma logique des séries market/features/labels
- GET /backtest/rolling?channel=ticker.BTC-PERPETUAL.raw&labels=200&windows=10: accuracy rolling, score de fiabilité et historique pour sparkline
- GET /quality/regime?channel=ticker.BTC-PERPETUAL.raw&labels=300: qualité du modèle par régime (trend/range x vol)
- GET /notifications?limit=200: historique serveur des alertes (JSONL)
- POST /notifications: journalise une alerte côté serveur pour audit persistant
- GET /push/status: état du module Web Push et nombre d'abonnements
- GET /push/public-key: clé publique VAPID pour abonnement client
- POST /push/subscribe: enregistre un abonnement Push API
- POST /push/unsubscribe: retire un abonnement Push API
Exemple:
- data/series_deribit.jsonl: série normalisée Deribit (prix, mark, bid/ask, open interest)
- data/series_features.jsonl: features calculées (retours, z-score, vol réalisée, spread)
- data/series_labels.jsonl: labels horizon (succès long/short vs SL/TP)
- data/notifications.jsonl: audit append-only des alertes
- data/push_subscriptions.json: abonnements push persistés

## Pipeline features/labels
1. Normalisation tick Deribit en point de marché (prix et microstructure)
2. Calcul feature point par canal sur fenêtres fast/slow
3. Génération du signal courant (long/short/neutral, probas, SL/TP)
4. Labellisation différée après horizon de ticks (outcome tp/sl/timeout)
5. Persistance JSONL des trois séries pour backtest et calibration