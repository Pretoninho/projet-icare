# Schéma de stockage et pipeline signal

## Séries persistées

### 1) Série marché Deribit
- Fichier: `data/series_deribit.jsonl`
- Granularité: 1 enregistrement par tick Deribit exploitable
- Clé logique: `(ts, channel)`
- Colonnes JSON:
  - `ts` (ISO-8601)
  - `channel` (string)
  - `instrument` (string)
  - `lastPrice` (number)
  - `markPrice` (number|null)
  - `indexPrice` (number|null)
  - `bestBid` (number|null)
  - `bestAsk` (number|null)
  - `openInterest` (number|null)
  - `volume24h` (number|null)

### 2) Série features
- Fichier: `data/series_features.jsonl`
- Granularité: 1 enregistrement par tick après warmup minimal
- Clé logique: `(ts, channel)`
- Colonnes JSON:
  - `tickReturn`
  - `returnFast`
  - `returnSlow`
  - `zScore`
  - `realizedVol`
  - `spreadBps`
  - `oiDelta`
  - `windows.fast`, `windows.slow`

### 3) Série labels
- Fichier: `data/series_labels.jsonl`
- Granularité: 1 label par feature lorsque l'horizon est atteint
- Clé logique: `(featureTs, channel)`
- Colonnes JSON:
  - `horizonPoints`
  - `entryPrice`
  - `closePrice`
  - `realizedReturn`
  - `long.outcome` in `{tp, sl, timeout}`
  - `short.outcome` in `{tp, sl, timeout}`
  - `riskModel.slPct`, `riskModel.tpPct`

## Pipeline de calcul
1. Ingestion tick Deribit (WebSocket).
2. Normalisation en point marché.
3. Mise à jour fenêtres roulantes par canal.
4. Calcul feature point (momentum, tendance, vol, z-score, spread, OI delta).
5. Calcul signal courant:
   - `direction`: long, short, neutral
   - `score.long`, `score.short` in `[0,1]`
   - SL/TP dynamiques selon volatilité
6. Labellisation différée à horizon fixe:
   - simulation hit TP/SL dans la trajectoire future
   - fallback `timeout` si aucun niveau touché
7. Persistance append-only des trois séries JSONL.

## Endpoints API
- `GET /series/schema`: schéma logique de stockage
- `GET /features?channel=...&limit=...`: features récentes
- `GET /labels?channel=...&limit=...`: labels récents
- `GET /signal?channel=...`: signal actuel + probabilité + SL/TP
- `GET /backtest/rolling?channel=...&labels=...`: accuracy rolling, coverage, reliability score

## Notes d'exploitation
- Le signal est heuristique (`model: heuristic-v1`) et doit être calibré avec historique plus long.
- Les labels permettent ensuite un apprentissage supervisé (calibration des probabilités).
