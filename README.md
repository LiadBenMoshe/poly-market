# Polymarket Bot

Production-oriented FastAPI trading bot for Polymarket with a mobile-first HTML dashboard.

## Features

- Async FastAPI backend with Polymarket market, position, PnL, order, and bot-control endpoints
- Polymarket integration using `py-clob-client`, `httpx`, and `eth-account`
- Mean-reversion strategy with Kelly-lite sizing and configurable risk limits
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
