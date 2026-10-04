from price_feed import BybitSpotFeed


def msg(kind, bids, asks, ts=1_000_000):
    return {"topic": "orderbook.1.BTCUSDT", "type": kind, "ts": ts, "data": {"b": bids, "a": asks}}


def test_delta_with_deleted_level_first_uses_new_level():
    feed = BybitSpotFeed("wss://unused")
    feed.handle(msg("snapshot", [["100.0", "1"]], [["100.2", "1"]]))
    # top bid moves up: the deleted old level is listed first
    feed.handle(msg("delta", [["100.0", "0"], ["100.1", "2"]], [], ts=1_001_000))
    assert feed.series.latest() == (1001, (100.1 + 100.2) / 2)


def test_snapshot_resets_book_and_crossed_book_is_ignored():
    feed = BybitSpotFeed("wss://unused")
    feed.handle(msg("snapshot", [["100.0", "1"]], [["100.2", "1"]]))
    feed.handle(msg("snapshot", [["90.0", "1"]], [["90.2", "1"]], ts=1_002_000))
    assert feed.series.latest() == (1002, 90.1)
    feed.handle(msg("delta", [["95.0", "1"]], [], ts=1_003_000))   # bid 95 > ask 90.2: crossed, skip
    assert feed.series.latest() == (1002, 90.1)
