# PolymarketBot: late-favourite strategy

One strategy only: in the last minutes of Polymarket's **BTC Up or Down 5m / 15m** markets,
buy the side that has a **≥ 90% chance of winning**, but only when the market sells it
**cheaper than that chance after fees**. Hold to resolution.

## Why "90% chance" alone is not enough
A share that wins 90% of the time and costs 90¢ loses money once fees are counted:

| | per share |
|---|---|
| price | 0.9000 |
| taker fee `0.07 × p × (1−p)` | 0.0063 |
| **break-even win rate** | **90.63%** |

So the bot computes its **own** probability and requires
`p_model − avg_fill_price − fee_per_share ≥ MIN_NET_EDGE` (default 2¢/share).

## How the probability is computed
- **Resolution source.** The markets settle on the Chainlink BTC/USD **TWAP-60s** stream.
  - The "price to beat" is its value at the window open. The bot reads it from Polymarket's `crypto-price` endpoint.
  - Settlement is the stream's value at the window close.
- **Inputs.** The bot streams that same Chainlink feed from Polymarket's RTDS websocket, plus Bybit BTCUSDT trades for the unsmoothed price.
  - A basis term maps Bybit onto Chainlink: `TWAP_now − Bybit 60s average`.
- **The model** (`model.py`): Brownian motion for the remaining seconds, with the TWAP averaging handled exactly.
  - Inside the last 60s, the already-observed part of the average is locked in.
  - Volatility is the larger of the 15-minute tick volatility and the 60-minute 1m-candle volatility, times `VOL_MULTIPLIER`.
  - Then `MODEL_HAIRCUT` is subtracted.
- **Entry gates** (`strategy.py`):
  - time left within the entry zone
  - `p_model ≥ 0.90`
  - expected settle at least `MIN_DISTANCE_USD` from the strike
  - best ask between 0.85 and 0.97
  - net edge after fees, at the real depth-weighted fill price
- **Size.** ¼ Kelly, capped by `MAX_TRADE_USDC` and `MAX_OPEN_EXPOSURE_USDC`.
- **Orders.** One fill-or-kill entry per market. Taker fees are calculated with the fee schedule each market publishes.

## Files
| File | What it does |
|---|---|
| `run.py` | Main loop: discover the market, estimate, decide, buy, settle |
| `strategy.py` | Entry gates and sizing |
| `model.py` | Win probability with TWAP settlement |
| `fees.py` | Fee formula, depth-weighted fills, break-even and edge |
| `price_feed.py` | Chainlink (RTDS) and Bybit websockets |
| `pm_client.py` | Gamma, CLOB book, price to beat, FOK orders |
| `ledger.py` | Trades in `data/trades.json`; PnL net of fees |
| `report.py` | Win rate vs break-even, fees, net PnL, calibration |
| `dashboard.py` + `dashboard.html` | Local web dashboard: total money, net PnL, wins/losses, PnL curve, trade history |
| `backtest.py` | Checks that 90% really means 90%, against official historical results |

## Usage
```bash
pip install -r requirements.txt
cp .env.example .env              # DRY_RUN=true by default (paper trading on the real order book)
python -m pytest -q               # unit tests
python backtest.py --days 7       # calibration on official Polymarket results
python run.py                     # paper trade; decisions logged to data/decisions.jsonl
                                  # dashboard at http://127.0.0.1:8050 (or run: python dashboard.py)
python report.py                  # net-of-fees results
```

## Wallet setup (live trading)
Polymarket only accepts API orders from a **Deposit Wallet** (signature type 3). Older email/Google
"proxy" wallets are rejected with `maker address not allowed, please use the deposit wallet flow`.
The bot trades through the official SDK (`polymarket-client`), acting for the Deposit Wallet owned by
`POLYMARKET_PRIVATE_KEY`; a builder API key in `.env` pays for its gasless wallet transactions.

## Run as a service (Ubuntu, systemd)
Needs Python 3.11+ (Ubuntu 24.04 ships 3.12; on 22.04 install `python3.11` from the deadsnakes PPA).
```bash
sudo apt install -y python3-venv git
git clone <your repo> ~/PolymarketBot && cd ~/PolymarketBot
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
cp .env.example .env && nano .env                     # keep DRY_RUN=true at first
sed "s/YOUR_USER/$USER/g" deploy/polymarket-bot.service | sudo tee /etc/systemd/system/polymarket-bot.service
sudo systemctl daemon-reload
sudo systemctl enable --now polymarket-bot            # start now and on every boot
journalctl -u polymarket-bot -f                       # live logs
```
Restart after changing code or `.env`: `sudo systemctl restart polymarket-bot`. Stop: `sudo systemctl stop polymarket-bot`.

The dashboard listens on 127.0.0.1 only. From your PC, open it through SSH:
`ssh -L 8050:127.0.0.1:8050 you@server`, then browse to http://127.0.0.1:8050.

Go live (`DRY_RUN=false`, with credentials from `python derive_creds.py`) only when **both** of these hold:
- the backtest's realized win rate is at or above the predicted rate in every bucket;
- paper trading shows positive net PnL, with a win rate above the break-even rate.

## Risks
- **One loss wipes out about 10 wins.** At 0.90, a win earns about +$0.09 per share and a loss costs about −$0.91.
- **Competition.** Other bots compete for the same late-window mispricings, so fills can be rare.
- **Paper fills are not real fills.** They walk the real order book but ignore latency and queue position. Live fills will be worse.
- **Redemption is manual.** Winning shares may need to be redeemed on Polymarket; this bot does not do it.
