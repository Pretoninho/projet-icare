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
  - `fundingRate` (number|null)
  - `funding8h` (number|null)
  - `basisBps` (number|null)
  - `markIv` (number|null)
  - `bidIv` (number|null)
  - `askIv` (number|null)
  - `ivSkew` (number|null)

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
  - `fundingRate`
  - `fundingZScore`
  - `basisBps`
  - `basisDislocation`
  - `ivMark`
  - `ivRank`
  - `ivSkew`
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
  - `regime.trend` in `{trend, range}`
  - `regime.volatility` in `{high, low}`
  - `prediction.success` (bool) pour calibration

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
- `GET /quality/regime?channel=...&labels=...`: qualité par régime trend/range et vol low/high
- `GET /notifications?limit=...`: audit des notifications persisté côté serveur
- `POST /notifications`: append d'une notification dans `data/notifications.jsonl`

## Notes d'exploitation
- Le signal est heuristique (`model: heuristic-v1`) et doit être calibré avec historique plus long.
- Les labels permettent ensuite un apprentissage supervisé (calibration des probabilités).
- Calibration probabiliste active via `CALIBRATOR_METHOD=platt|isotonic`.

## Cadre d'intégration étendu (Deribit + features)

### 1) Données minimales indispensables
- OHLCV multi-timeframes (1m, 5m, 15m, 1h, 4h) sur perp/futures
- Order book L2: profondeur bid/ask, spread, imbalance
- Trades tick-by-tick: agressions acheteur/vendeur, volume delta
- Open Interest + variation OI
- Funding rate (actuel + historique)
- Basis futures vs spot/index
- Index price + mark price + last price
- Volatilité implicite options (ATM IV, skew, term structure)
- Liquidations (si flux disponible)
- Calendrier macro/événements en binaire (CPI/FOMC)

### 2) Données edge Deribit
- Surface IV complète (strikes + maturités)
- Skew 25-delta (RR/BF)
- OI options par strike (zones d'attraction)
- Put/Call ratio volume + OI
- Greeks agrégés (gamma proxy)
- Flux block trades options
- Term structure perp/futures

### 3) Features cibles
- Momentum: returns, RSI, MACD, breakout
- Microstructure: imbalance, queue pressure, spread regime
- Flow: CVD, buy/sell aggressor ratio
- Regime: vol réalisée, trend/range classifier
- Derivatives: funding z-score, OI acceleration, basis dislocation
- Options: IV rank, skew regime, smile steepness
- Risk: drawdown récent, proxy VaR intraday

### 4) Labels ML
- Horizon strict: 15m, 1h, 4h (selon stratégie)
- Direction: sign(return_{t+h}) ou triple barrier
- Triple barrier:
  - TP avant SL: succès
  - SL avant TP: échec
  - timeout: neutre
- Probabilité: calibration de P(TP avant SL | features_t)

### 5) SL/TP data-driven
- SL: k * ATR ou quantile de mouvement adverse
- TP: distribution historique des mouvements favorables
- R:R dynamique selon régime (trend/range)
- Sizing fixe (0.5%-1% capital)
- Kill-switch: spread, latence, news

### 6) Scoring agrégé (implémenté v1)
Formule actuelle:

$$
Score = 0.25\,S_{trend} + 0.25\,S_{flow} + 0.20\,S_{deriv} + 0.20\,S_{options} - 0.10\,S_{risk}
$$

Mapping:
- Score > 65: biais Long
- Score < 35: biais Short
- Sinon: Neutral

Sortie `/signal` inclut:
- direction, probabilité calibrée, entrée, SL, TP1/TP2
- confiance (low/medium/high)
- 3 raisons clés
- état kill-switch

### 7) Historique requis
- Minimum viable: 3-6 mois 1m
- Correct: 12-18 mois
- Idéal: 24-36 mois
- Alignement temporel funding/OI/options obligatoire

### 8) Validation
- Walk-forward (pas de split aléatoire)
- Coûts réels: fees + slippage + latence
- Analyse par régime de volatilité
- Calibration probabiliste: Brier + reliability curve
- Paper trading live avant réel

### 9) Conclusion UI par instrument
- Signal: Long/Short/Neutral
- Probabilité (%)
- Entrée
- SL (+ distance %)
- TP1/TP2 (+ distances %)
- Confiance
- 3 raisons clés
