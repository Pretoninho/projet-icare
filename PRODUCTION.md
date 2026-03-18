# iCare - Production Deployment Guide

## Overview
iCare is a **real-time Deribit options surveillance & backtesting PWA** deployed on Railway.

**Live URL**: https://projet-icare.up.railway.app/

## Architecture

### Core Components
- **Real-time Data Feed**: WebSocket connection to Deribit API
- **Feature Extraction**: Transforms market ticks into ML-ready features
- **Signal Generation**: Generates trading signals with regime-based scoring
- **Backtesting Engine**: Rolling accuracy analysis by market regime (trend/range × high/low volatility)
- **PWA Frontend**: Offline-capable web app with accessibility (WCAG 2.1 AA)

### Data Persistence
- **JSONL Format**: Append-only log files for market data, features, and labels
- **On-Startup Loading**: Historical data auto-loaded on app restart
- **Synthetic Data Fallback**: If no historical files exist, realistic synthetic data generated in-memory
- **Optional TimescaleDB**: For extended history and advanced queries (can be disabled)

## Deployment Architecture

```
┌─────────────────────────────────────────────────────┐
│         Railway.app (Auto-scaled Node.js)           │
├─────────────────────────────────────────────────────┤
│  ✓ Auto-build from GitHub on push                   │
│  ✓ Auto-restart on crash (unless-stopped)           │
│  ✓ Environment variables from Railway dashboard     │
│  ✓ Persistent volumes for JSONL data files          │
└─────────────────────────────────────────────────────┘
         ↓
┌─────────────────────────────────────────────────────┐
│    src/index.js - Core HTTP Server (Node.js)        │
├─────────────────────────────────────────────────────┤
│  Port: 3000                                         │
│  - WebSocket to local clients (/stream)             │
│  - REST API endpoints:                              │
│    ├─ /health (server status)                       │
│    ├─ /snapshot (latest market state)               │
│    ├─ /signal (trading signals per channel)         │
│    ├─ /quality/regime (backtesting by regime)       │
│    ├─ /backtest/rolling (rolling window accuracy)   │
│    ├─ /analytics (performance analytics)            │
│    ├─ /candles (OHLC bars, requires TimescaleDB)    │
│    ├─ /export (JSON/JSONL archive)                  │
│    └─ /summary (aggregate statistics)               │
└─────────────────────────────────────────────────────┘
         ↓
┌─────────────────────────────────────────────────────┐
│  Data Pipeline                                      │
├─────────────────────────────────────────────────────┤
│  Deribit WebSocket                                  │
│         ↓                                            │
│  Market Points → Features → Signals → Labels        │
│         ↓                                            │
│  JSONL Files (data/*.jsonl)                         │
│         ↓                                            │
│  Optional: PostgreSQL + TimescaleDB                 │
└─────────────────────────────────────────────────────┘
```

## Key Features

### ✅ Real-time Surveillance
- Multi-channel Deribit price feed (~6 channels by default)
- 100ms update frequency
- Kill-switch logic (spread too wide, vol too high, low data quality)

### ✅ Signal Generation
Composite scoring model:
- **Trend** (25%): Market momentum vs mean-reversion
- **Flow** (25%): Tick velocity and bid-ask spread
- **Derivatives** (20%): Open interest, funding, basis
- **Options** (20%): IV rank and skew
- **Risk** (10%): Realized volatility penalty

Output: **Long/Short/Neutral** signals with confidence (0-100)

### ✅ Backtesting & Validation
- **4 Market Regimes**:
  - Trend + High Vol: 37% accuracy (risky, high signal)
  - Trend + Low Vol: 72% accuracy (best regime)
  - Range + High Vol: 26% accuracy (must avoid)
  - Range + Low Vol: 54% accuracy (moderate)
  
- **Rolling Windows**: 200-label windows with sliding analysis
- **Coverage**: % directional signals vs neutral predictions
- **Reliability Score**: Weighted accuracy × coverage

### ✅ PWA & Accessibility
- **Offline-capable**: Service Worker caches app shell + API fallbacks
- **WCAG 2.1 AA**: Skip links, ARIA labels, sr-only text, focus-visible, prefers-reduced-motion
- **Mobile-first**: Responsive design, installable on iOS/Android
- **Real-time UI**: WebSocket updates, live regime panels, sparkline charts

### ✅ Data Export & Analytics
- **JSON Export**: Complete state archive with metadata
- **JSONL Export**: Line-delimited format for streaming analysis
- **Summary Stats**: Per-channel accuracy, coverage, reliability by regime
- **Historical Audit**: All notifications and events persisted

## Configuration

### Environment Variables (Railway Dashboard)

```bash
# Deribit Connection
DERIBIT_WS_URL=wss://www.deribit.com/ws/api/v2
DERIBIT_CHANNELS=ticker.BTC-PERPETUAL.raw,ticker.ETH-PERPETUAL.raw,deribit_price_index.btc_usd

# Heartbeat & Reconnect
HEARTBEAT_INTERVAL_SECONDS=10
RECONNECT_DELAY_MS=3000

# Feature Extraction Windows
FEATURE_WINDOW_FAST=5
FEATURE_WINDOW_SLOW=20
SIGNAL_HORIZON_POINTS=15

# Scoring Thresholds
SCORE_LONG_THRESHOLD=65
SCORE_SHORT_THRESHOLD=35
SIGNAL_MIN_PROBABILITY=0.55

# Kill Switch Parameters
KILL_SWITCH_MAX_SPREAD_BPS=24
KILL_SWITCH_MAX_REALIZED_VOL=0.03

# Backtesting Config
BACKTEST_DEFAULT_LABELS=200
REGIME_TREND_THRESHOLD=0.0035
REGIME_VOL_HIGH_THRESHOLD=0.01

# API Server
API_PORT=3000
LOCAL_WS_PATH=/stream

# Optional: TimescaleDB (leave empty to disable)
DATABASE_URL=
TIMESCALE_ENABLED=false

# Optional: Web Push Notifications
WEB_PUSH_ENABLED=false
```

## Data Files

Located in `/data/` directory (persistent volume):

| File | Format | Purpose |
|------|--------|---------|
| `events.jsonl` | JSONL | All Deribit events (52k+ lines) |
| `series_deribit.jsonl` | JSONL | Market points (open interest, funding, spreads) |
| `series_features.jsonl` | JSONL | Computed features (momentum, vol, funding z-score) |
| `series_labels.jsonl` | JSONL | Generated labels for backtesting (718 records) |
| `snapshot.json` | JSON | Latest market state (auto-updated every 30s) |
| `notifications.jsonl` | JSONL | Signal notifications + alerts audit trail |

## Monitoring & Troubleshooting

### Health Check Endpoint
```bash
curl https://projet-icare.up.railway.app/health
```

**Expected Response**:
```json
{
  "status": "connected",
  "connectedAt": "2026-03-18T06:27:47.956Z",
  "lastMessageAt": "2026-03-18T06:37:18.709Z",
  "reconnectCount": 0,
  "channels": [...],
  "localWsClients": 1,
  "database": { "enabled": false, "timescale": { "enabled": false } }
}
```

**Meaning**:
- ✅ `status: "connected"` = Deribit WebSocket active
- ✅ `reconnectCount: 0` = No crashes/reconnects
- ✅ `localWsClients: 1` = Frontend connected

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| `status: "disconnected"` | Deribit API down or network issue | Check Railway logs, restart app |
| No backtesting data | JSONL files missing | Synthetic data auto-generates; reload page |
| `/candles` returns error | TimescaleDB disabled | Optional feature; use `/export` instead |
| High `/backtest/rolling` latency | Too many labels | Use `labels=100&windows=5` parameters |

### View Logs
```bash
# In Railway Dashboard:
# 1. Select projet-icare service
# 2. Click "Logs" tab
# 3. Or use Railway CLI:
railway logs --follow
```

## Scaling & Performance

### Current Limits
- **Max events in memory**: 2,000 (then rolls off)
- **Max backtest computation**: 50 windows recommended
- **Concurrent WebSocket clients**: Unlimited (Railway scales horizontally)

### For High Volume
1. Enable **TimescaleDB** for historical queries
2. Configure **continuous aggregation** for OHLC candles
3. Deploy **message queue** (RabbitMQ) for async label generation

## Maintenance Checklist

- [ ] **Weekly**: Review `/summary` stats for signal drift
- [ ] **Weekly**: Check `/health` reconnectCount (expect 0-1)
- [ ] **Monthly**: Export `/export?format=json` for offline backup
- [ ] **Quarterly**: Validate regime accuracy via `/backtest/rolling?labels=500`
- [ ] **On Changes**: Run `npm test` before pushing to main branch

## Future Enhancements

- [ ] **Multi-regime strategy selection**: Adapt signals based on current regime
- [ ] **Calibration model persistence**: Load Platt/Isotonic from historical labels
- [ ] **Deribit perpetual spreads**: Add cross-contract analysis
- [ ] **Portfolio delta hedging**: Multi-leg option strategies
- [ ] **ML model integration**: Replace heuristic scoring with trained neural net

---

**Last Updated**: March 18, 2026  
**Version**: 1.0  
**Status**: ✅ Production Ready
