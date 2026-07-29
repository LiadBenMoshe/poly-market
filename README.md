# Polymarket Bot

Production-oriented FastAPI trading bot for Polymarket with a mobile-first HTML dashboard.

## Features

- Async FastAPI backend with Polymarket market, position, PnL, order, and bot-control endpoints
- Polymarket integration using `py-clob-client`, `httpx`, and `eth-account`
- Mean-reversion strategy with Kelly-lite sizing and configurable risk limits
- BTC arbitrage strategy using Bybit public market data versus Polymarket BTC 5-minute markets
- Whale scanner service and standalone `polymarket_screener.py` runner for high-conviction trade flow
- APScheduler trading loop with dry-run mode enabled by default
- Single-file, mobile-optimized dashboard with offline-first caching via `localStorage`
- `/health` endpoint for monitoring and CORS enabled for local development

## Setup

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

Update `.env` with your wallet key and Polymarket API credentials before live trading.
Use `.env.example` as the template for new environments.

## BTC Arbitrage Setup

The BTC arbitrage module uses Bybit's public WebSocket feed. No authenticated Bybit key is required for market data.

Key environment values:

```env
BYBIT_API_KEY=
BYBIT_API_SECRET=
MIN_EDGE=0.08
MAX_TRADE_USDC=50
MIN_TRADE_USDC=5
MAX_OPEN_POSITIONS=3
MIN_SIGNAL_SCORE=10
TRADE_COOLDOWN_SEC=60
ARBOT_ENABLED=false
ARB_SCHEDULER_INTERVAL_SECONDS=10
DAILY_LOSS_LIMIT=100
ARB_STOP_LOSS_FRACTION=0.35
ARB_STOP_LOSS_CUTOFF_SECONDS=45
```

Start the arb bot from the dashboard `ARB` tab or:

```powershell
Invoke-RestMethod -Method POST http://localhost:8000/api/arb/start
```

Useful arb endpoints:

- `GET /api/arb/signal`
- `GET /api/arb/btc-price`
- `GET /api/arb/markets`
- `GET /api/arb/trades`
- `GET /api/arb/performance`
- `POST /api/arb/start`
- `POST /api/arb/stop`
- `GET /api/whales`

## Whale Scanner

Run the standalone screener with:

```powershell
python polymarket_screener.py
```

What it does:

- scans up to `WHALE_MARKET_LIMIT` active Polymarket markets every `WHALE_SCAN_INTERVAL_SECONDS`
- pulls recent public trade data and uses a dynamic whale threshold of `WHALE_THRESHOLD_MULTIPLIER x market median trade size`
- scores each detected whale trade from `0-100` using relative size, wallet reputation, clustering, persistence, absolute size, category keywords, momentum, crypto correlation, and external oracle context
- assigns tier-based position sizing of `30% / 20% / 10% / 5%`
- shares its latest signals with the regular bot via the `whale_following` strategy and with the BTC arb bot as an additional bias input

Oracle feeds used on a best-effort basis:

- Binance BTC price change
- Fear and Greed Index
- Open-Meteo current weather
- FRED macro series
- ESPN scoreboard
- FiveThirtyEight polling JSON

If one oracle endpoint is unavailable, the scanner keeps running and simply scores without that component.

## BTC Arbitrage Strategy

The strategy compares short-term BTC momentum from Bybit with Polymarket's `Bitcoin Up or Down - 5 Minutes` markets.

Signal inputs:

- 30-second price velocity
- 90-second price velocity
- RSI(14) on 1-minute candles
- top-10 orderbook imbalance

The signal is converted into a score from `-100` to `+100`. When the score is strong enough, the bot scans Polymarket for BTC 5-minute markets closing in the next 1-6 minutes, computes a fair value for `YES` / `NO`, and only trades when the estimated edge is above `MIN_EDGE`.

Risk controls:

- max concurrent BTC arb positions
- per-market cooldown
- daily loss stop
- percentage-based stop-loss per arb position
- no stop-loss liquidation in the final 45 seconds before market close
- no trading in the last 30 seconds before close
- slippage warning if fill deviates by more than 3 cents

## Backtesting And Paper Mode

When `DRY_RUN=true`, the bot uses the local paper engine. BTC arbitrage trades are simulated and persisted under `data/`.

Paper mode assumptions:

- fills happen immediately at the intended limit price
- no queue priority modeling
- no partial fills
- no external transaction failures

This makes paper mode good for logic verification and workflow testing, but not a full execution simulator.

## API Keys

If you do not already have Polymarket CLOB API credentials, the official SDK can derive them from your wallet signature when `POLYMARKET_PRIVATE_KEY` is configured and the explicit API key fields are left blank.

Official references:

- https://github.com/Polymarket/py-clob-client
- https://docs.polymarket.com/

## Safety Defaults

- `DRY_RUN=true` by default
- Max trade size: 5% of bankroll
- Max total exposure: 30% of bankroll
- Per-market exposure: 10% of bankroll
- Stop-loss threshold: 20%
