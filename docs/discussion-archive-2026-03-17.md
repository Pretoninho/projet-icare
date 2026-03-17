# Archive de discussion - 2026-03-17

## Objectif du projet
Mettre en place iCare comme application de réplication en direct des données Deribit, avec exposition locale des données et capacité de déploiement.

## Ce qui a été construit
- Service Node.js de réplication WebSocket Deribit.
- Reconnexion automatique et heartbeat.
- Stockage local des événements en JSONL.
- Snapshot local consolidé.
- API REST locale:
  - GET /health
  - GET /snapshot
  - GET /events
  - GET /candles (si TimescaleDB actif)
  - GET /analytics
- Flux WebSocket local pour la diffusion des événements.
- Persistance PostgreSQL optionnelle.
- Mode TimescaleDB optionnel:
  - hypertable
  - retention
  - vue continue OHLC
- Test d’intégration avec mock Deribit.
- Déploiement Docker:
  - Dockerfile applicatif
  - docker-compose avec app + timescaledb
  - scripts npm db:up/db:down/db:logs/deploy:up/deploy:down/deploy:logs

## Fichiers principaux ajoutés ou modifiés
- README.md
- package.json
- package-lock.json
- src/index.js
- test/integration.test.js
- docker-compose.yml
- Dockerfile
- .env.example
- .env

## État Git lors de la session
Les changements ont été appliqués et indexés, puis la tentative de commit/push via le terminal agent a été bloquée par une erreur d’environnement ENOPRO.

## Commandes à exécuter sur ton poste
1. git status --short
2. git commit -m "feat: build deribit realtime replication service with local api and docker deployment"
3. git push origin main

## Démarrage rapide local
1. npm install
2. npm install pg
3. npm run deploy:up
4. Ouvrir http://localhost:3000/health

## Notes
Le blocage ENOPRO venait de l’exécution terminal dans cet environnement agent, pas du code du projet.
