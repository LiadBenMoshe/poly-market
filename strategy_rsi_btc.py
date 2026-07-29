"""
strategy_rsi_btc.py
────────────────────────────────────────────────────────────────────────────
Strategy : RSI vs RSI-Base-MA  (5-min BTC candles)
Signal   : RSI(5m) > RSI_base_MA  →  BUY UP
           RSI(5m) ≤ RSI_base_MA  →  BUY DOWN

Drop this file in the same directory as your bot.
The bot should call  get_signal()  once per 5-min candle close.
────────────────────────────────────────────────────────────────────────────
"""

from collections import deque


# ──────────────────────────────────────────────
# Tuneable parameters
# ──────────────────────────────────────────────
RSI_PERIOD     = 14       # RSI look-back (candles)
BASE_MA_PERIOD = 14       # SMA of the RSI values  (the "RSI base MA")
CANDLE_TF      = "5m"     # informational – matches your data feed


# ──────────────────────────────────────────────
# Internal state  (module-level, reset on import)
# ──────────────────────────────────────────────
_closes:   deque = deque(maxlen=RSI_PERIOD + BASE_MA_PERIOD + 10)
_rsi_vals: deque = deque(maxlen=BASE_MA_PERIOD)


def _ensure_state() -> None:
    """Rebuild rolling buffers if periods were changed after import."""
    global _closes, _rsi_vals

    closes_maxlen = RSI_PERIOD + BASE_MA_PERIOD + 10
    rsi_maxlen = BASE_MA_PERIOD
    if _closes.maxlen != closes_maxlen:
        _closes = deque(_closes, maxlen=closes_maxlen)
    if _rsi_vals.maxlen != rsi_maxlen:
        _rsi_vals = deque(_rsi_vals, maxlen=rsi_maxlen)


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────

def _calc_rsi(closes: list[float], period: int = RSI_PERIOD) -> float | None:
    """
    Wilder / classic RSI.
    Returns None when there isn't enough data yet.
    """
    if len(closes) < period + 1:
        return None

    gains, losses = [], []
    for i in range(-period, 0):
        diff = closes[i] - closes[i - 1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))

    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period

    if avg_loss == 0:
        return 100.0

    rs  = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return round(rsi, 4)


def _calc_sma(values: list[float], period: int) -> float | None:
    """Simple moving average of the last `period` values."""
    if len(values) < period:
        return None
    return sum(values[-period:]) / period


# ──────────────────────────────────────────────
# Public interface
# ──────────────────────────────────────────────

def feed_close(price: float) -> None:
    """
    Call this with every new 5-min candle CLOSE price.
    The strategy keeps its own rolling state.

    Parameters
    ----------
    price : float
        BTC/USD close price for the finished 5-min candle.
    """
    _ensure_state()
    _closes.append(float(price))

    rsi = _calc_rsi(list(_closes))
    if rsi is not None:
        _rsi_vals.append(rsi)


def get_signal() -> dict:
    """
    Returns the current trading signal.

    Returns
    -------
    dict with keys:
        signal      : "UP" | "DOWN" | "WAIT"
        rsi         : float | None
        rsi_base_ma : float | None
        reason      : str   (human-readable explanation)

    Call this AFTER  feed_close()  on the same candle.
    """
    _ensure_state()
    rsi         = _rsi_vals[-1]  if _rsi_vals                       else None
    rsi_base_ma = _calc_sma(list(_rsi_vals), BASE_MA_PERIOD)

    # Not enough data yet
    if rsi is None or rsi_base_ma is None:
        candles_needed = RSI_PERIOD + BASE_MA_PERIOD
        candles_have   = len(_closes)
        return {
            "signal":      "WAIT",
            "rsi":          rsi,
            "rsi_base_ma":  rsi_base_ma,
            "reason": (
                f"Warming up - need {candles_needed} candles, "
                f"have {candles_have}."
            ),
        }

    # ── Core logic ──────────────────────────────
    if rsi > rsi_base_ma:
        signal = "UP"
        reason = (
            f"RSI ({rsi:.2f}) > RSI-base-MA ({rsi_base_ma:.2f}) "
            f"-> bullish momentum, BUY UP."
        )
    else:
        signal = "DOWN"
        reason = (
            f"RSI ({rsi:.2f}) <= RSI-base-MA ({rsi_base_ma:.2f}) "
            f"-> bearish momentum, BUY DOWN."
        )

    return {
        "signal":      signal,
        "rsi":          rsi,
        "rsi_base_ma":  rsi_base_ma,
        "reason":       reason,
    }


def reset() -> None:
    """Clear all internal state (useful for backtesting multiple runs)."""
    _closes.clear()
    _rsi_vals.clear()


# ──────────────────────────────────────────────
# Quick smoke-test  (run: python strategy_rsi_btc.py)
# ──────────────────────────────────────────────
if __name__ == "__main__":
    import random
    random.seed(42)

    print("=== RSI-vs-RSI-base-MA strategy – smoke test ===\n")

    # Simulate 60 candles of fake BTC price around 65 000
    price = 65_000.0
    for i in range(60):
        price += random.uniform(-300, 300)
        feed_close(price)
        result = get_signal()

        if result["signal"] != "WAIT":
            print(
                f"Candle {i+1:02d} | Close: {price:,.0f} | "
                f"RSI: {result['rsi']:.2f} | "
                f"MA:  {result['rsi_base_ma']:.2f} | "
                f"→ {result['signal']}"
            )
        else:
            print(f"Candle {i+1:02d} | {result['reason']}")
