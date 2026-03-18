"""
Simple Replay - Paper trading / streaming backtest harness.
Streams live L2 + trade data from Hyperliquid via WebSocket → Redis channels,
processes through the orderflow engine and absorption strategy,
and logs ticks / features / signals to PostgreSQL.

Usage:
    # Stream for 5 minutes, log everything to Postgres
    python -m brain_agent.backtesting.simple_replay --coin BTC --duration-seconds 300 --db-log

    # Historical replay (no Postgres)
    python -m brain_agent.backtesting.simple_replay --coin BTC --limit 2000
"""
import asyncio
import argparse
import logging
import uuid
from collections import deque
from datetime import datetime, timezone
from typing import Optional

from brain_agent.config.settings import get_settings
from brain_agent.core.data_pipeline import DataPipeline
from brain_agent.core.orderflow_engine import OrderflowEngine
from brain_agent.core.models import TradeTick, L2Update, Event, EventType
from brain_agent.strategies.absorption import AbsorptionStrategy

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Number of trades to wait before evaluating a signal's outcome
WIN_RATE_WINDOW = 50


def _now_utc() -> datetime:
    return datetime.now(tz=timezone.utc)


def _raise_no_data_error(coin: str, limit: int) -> None:
    raise RuntimeError(
        f"No real data from Hyperliquid for {coin} (requested {limit} trades). "
        f"Check: 1) Hyperliquid API is accessible, 2) Coin '{coin}' is available."
    )


# ---------------------------------------------------------------------------
# Streaming backtest (paper trading via live WebSocket → Redis)
# ---------------------------------------------------------------------------

async def run_backtest_streaming(
    coin: str,
    duration_seconds: int = 300,
    print_every: int = 100,
    db=None,
    session_id: str = "",
) -> dict:
    """
    Subscribe to live Hyperliquid data via Redis channels and run the
    absorption strategy in real time.

    Args:
        coin: e.g. "BTC"
        duration_seconds: stop after N seconds (0 = run until Ctrl-C)
        print_every: console progress interval
        db: Database instance (None → no DB logging)
        session_id: UUID string to group this run's rows in Postgres

    Returns:
        Summary dict with events, signals, win_rate, signal_log.
    """
    logger.info(f"Streaming backtest | coin={coin} duration={duration_seconds}s session={session_id[:8]}")

    pipeline = DataPipeline()
    engine = OrderflowEngine(window=500, absorption_threshold=3.0)
    strategy = AbsorptionStrategy(min_score=4.0, min_trade_count=10)

    await pipeline.connect()

    if not pipeline._redis:
        await pipeline.disconnect()
        raise RuntimeError(
            "Redis not available. Check that Redis is running and credentials in .env are correct."
        )

    # Counters
    events_processed = 0
    trades_processed = 0
    l2_processed = 0
    signals_generated = 0
    signal_log = []

    # Win-rate tracking: deque of (signal_id, direction, entry_price, eval_at_trade_count)
    pending_signals: deque = deque()
    wins = 0
    losses = 0
    last_price = 0.0

    # Duration timeout
    stop_flag = asyncio.Event()
    timeout_handle = None
    if duration_seconds > 0:
        loop = asyncio.get_event_loop()
        timeout_handle = loop.call_later(duration_seconds, stop_flag.set)

    try:
        async for event in pipeline.stream_events(coin):
            if stop_flag.is_set():
                break

            events_processed += 1

            try:
                if event.event_type == EventType.TRADE:
                    trades_processed += 1
                    tick = event.data
                    last_price = tick.price

                    tick_dict = {
                        "timestamp": tick.timestamp,
                        "coin": tick.coin,
                        "price": tick.price,
                        "size": tick.size,
                        "side": tick.side,
                        "hash": getattr(tick, "hash", None),
                    }

                    # Log raw tick
                    if db:
                        await db.log_trade_tick(tick_dict, session_id)

                    features = engine.process_tick(tick_dict)
                    signal = strategy.generate_signal(features, tick.price)

                    # Evaluate any pending signals whose window has elapsed
                    while pending_signals and pending_signals[0][3] <= trades_processed:
                        sig_id, direction, entry_price, _ = pending_signals.popleft()
                        if db:
                            outcome, pnl_pct = await db.update_signal_outcome(
                                sig_id, last_price, entry_price, direction
                            )
                        else:
                            pnl_pct = (
                                (last_price - entry_price) / entry_price * 100
                                if direction == "LONG"
                                else (entry_price - last_price) / entry_price * 100
                            )
                            outcome = "WIN" if pnl_pct > 0 else "LOSS"
                        if outcome == "WIN":
                            wins += 1
                        else:
                            losses += 1

                    if signal.direction != "FLAT":
                        signals_generated += 1
                        sig_ts = _now_utc()
                        entry = f"[{trades_processed}] {signal.direction} conf={signal.confidence:.2f} | {signal.reason}"
                        signal_log.append(entry)
                        logger.info(entry)

                        sig_id = None
                        if db:
                            sig_id = await db.log_signal(
                                direction=signal.direction,
                                confidence=signal.confidence,
                                reason=signal.reason,
                                entry_price=tick.price,
                                session_id=session_id,
                                coin=coin,
                                ts=sig_ts,
                            )
                        pending_signals.append(
                            (sig_id, signal.direction, tick.price, trades_processed + WIN_RATE_WINDOW)
                        )

                    # Log features every print_every trades
                    if trades_processed % print_every == 0:
                        snapshot = engine.get_snapshot()
                        if db:
                            await db.log_engine_features(snapshot, session_id, coin, _now_utc())
                        logger.info(
                            f"[T={trades_processed} E={events_processed}] "
                            f"CVD={snapshot['cvd']:+.2f} Abs={snapshot['absorption_score']:.2f} "
                            f"Imb={snapshot['bid_ask_imbalance']:.2f}"
                        )

                elif event.event_type == EventType.L2_UPDATE:
                    l2_processed += 1
                    engine.process_l2({"bids": event.data.bids, "asks": event.data.asks})

            except Exception as e:
                logger.error(f"Error processing event {events_processed}: {e}")

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    finally:
        if timeout_handle:
            timeout_handle.cancel()
        
        # Evaluate any remaining pending signals at session end
        logger.info(f"Evaluating {len(pending_signals)} pending signals at session end...")
        while pending_signals:
            sig_id, direction, entry_price, _ = pending_signals.popleft()
            if db and sig_id:
                outcome, pnl_pct = await db.update_signal_outcome(
                    sig_id, last_price, entry_price, direction
                )
            else:
                pnl_pct = (
                    (last_price - entry_price) / entry_price * 100
                    if direction == "LONG"
                    else (entry_price - last_price) / entry_price * 100
                )
                outcome = "WIN" if pnl_pct > 0 else "LOSS"
            if outcome == "WIN":
                wins += 1
            else:
                losses += 1
            logger.info(f"  Signal {sig_id}: {outcome} (PnL: {pnl_pct:+.2f}%)")
        
        await pipeline.disconnect()

    total_evaluated = wins + losses
    win_rate = wins / total_evaluated if total_evaluated else 0.0
    final = engine.get_snapshot()

    return {
        "session_id": session_id,
        "events": events_processed,
        "trades": trades_processed,
        "l2_updates": l2_processed,
        "signals": signals_generated,
        "wins": wins,
        "losses": losses,
        "win_rate": win_rate,
        "final_cvd": final["cvd"],
        "final_absorption": final["absorption_score"],
        "signal_log": signal_log[-20:] if len(signal_log) > 20 else signal_log,
    }


# ---------------------------------------------------------------------------
# Historical replay (REST API fetch → local processing)
# ---------------------------------------------------------------------------

async def run_backtest(
    coin: str,
    limit: int,
    print_every: int = 100,
    db=None,
    session_id: str = "",
) -> dict:
    """
    Fetch historical trades from Hyperliquid REST API and replay them
    through the engine + strategy.

    No historical L2 data is available from Hyperliquid REST — only the
    initial L2 snapshot (at session start) is used for the first 10 trade
    cycles. Streaming mode gives proper L2 replay.
    """
    logger.info(f"Historical replay | coin={coin} limit={limit} session={session_id[:8]}")

    pipeline = DataPipeline()
    engine = OrderflowEngine(window=500, absorption_threshold=3.0)
    strategy = AbsorptionStrategy(min_score=4.0, min_trade_count=10)

    await pipeline.connect()

    trades = []
    initial_l2 = None

    try:
        trade_objects = await pipeline.get_historical_trades(coin, limit)
        for t in trade_objects:
            trades.append(TradeTick(
                timestamp=t.timestamp,
                coin=t.coin,
                price=t.price,
                size=t.size,
                side=t.side,
                hash=t.hash,
            ))
        try:
            initial_l2 = await pipeline.get_l2_snapshot(coin)
            if initial_l2:
                logger.info("Got initial L2 snapshot")
        except Exception as e:
            logger.warning(f"Could not get initial L2 snapshot: {e}")
    except Exception as e:
        logger.warning(f"Failed to fetch data: {e}")

    if not trades:
        await pipeline.disconnect()
        _raise_no_data_error(coin, limit)

    trades = trades[:limit]
    logger.info(f"Processing {len(trades)} trades...")

    signals_generated = 0
    signal_log = []

    # Win-rate tracking
    pending_signals: deque = deque()
    wins = 0
    losses = 0
    last_price = 0.0
    l2_counter = 0

    for i, trade in enumerate(trades):
        try:
            last_price = trade.price
            tick_dict = {
                "timestamp": trade.timestamp,
                "coin": trade.coin,
                "price": trade.price,
                "size": trade.size,
                "side": trade.side,
                "hash": trade.hash,
            }

            if db:
                await db.log_trade_tick(tick_dict, session_id)

            features = engine.process_tick(tick_dict)
            signal = strategy.generate_signal(features, trade.price)

            # Evaluate matured pending signals
            while pending_signals and pending_signals[0][3] <= i:
                sig_id, direction, entry_price, _ = pending_signals.popleft()
                if db:
                    outcome, _ = await db.update_signal_outcome(
                        sig_id, last_price, entry_price, direction
                    )
                else:
                    pnl = (
                        (last_price - entry_price) / entry_price * 100
                        if direction == "LONG"
                        else (entry_price - last_price) / entry_price * 100
                    )
                    outcome = "WIN" if pnl > 0 else "LOSS"
                if outcome == "WIN":
                    wins += 1
                else:
                    losses += 1

            if signal.direction != "FLAT":
                signals_generated += 1
                entry = f"[{i}] {signal.direction} conf={signal.confidence:.2f} | {signal.reason}"
                signal_log.append(entry)
                logger.info(entry)

                sig_id = None
                if db:
                    sig_id = await db.log_signal(
                        direction=signal.direction,
                        confidence=signal.confidence,
                        reason=signal.reason,
                        entry_price=trade.price,
                        session_id=session_id,
                        coin=coin,
                        ts=_now_utc(),
                    )
                pending_signals.append(
                    (sig_id, signal.direction, trade.price, i + WIN_RATE_WINDOW)
                )

            # L2 injection every 5 trades (real snapshot for first 10 cycles only)
            l2_counter += 1
            if l2_counter >= 5:
                l2_counter = 0
                if initial_l2 and i < 10:
                    engine.process_l2({"bids": initial_l2.bids[:5], "asks": initial_l2.asks[:5]})

            if (i + 1) % print_every == 0:
                snapshot = engine.get_snapshot()
                if db:
                    await db.log_engine_features(snapshot, session_id, coin, _now_utc())
                logger.info(
                    f"[{i+1}] CVD={snapshot['cvd']:+.2f} "
                    f"Abs={snapshot['absorption_score']:.2f} "
                    f"Imb={snapshot['bid_ask_imbalance']:.2f}"
                )

        except Exception as e:
            logger.error(f"Error at trade {i}: {e}")

    # Evaluate any remaining pending signals at session end
    logger.info(f"Evaluating {len(pending_signals)} pending signals at session end...")
    while pending_signals:
        sig_id, direction, entry_price, _ = pending_signals.popleft()
        if db and sig_id:
            outcome, pnl_pct = await db.update_signal_outcome(
                sig_id, last_price, entry_price, direction
            )
        else:
            pnl_pct = (
                (last_price - entry_price) / entry_price * 100
                if direction == "LONG"
                else (entry_price - last_price) / entry_price * 100
            )
            outcome = "WIN" if pnl_pct > 0 else "LOSS"
        if outcome == "WIN":
            wins += 1
        else:
            losses += 1
        logger.info(f"  Signal {sig_id}: {outcome} (PnL: {pnl_pct:+.2f}%)")

    await pipeline.disconnect()

    total_evaluated = wins + losses
    win_rate = wins / total_evaluated if total_evaluated else 0.0
    final = engine.get_snapshot()

    return {
        "session_id": session_id,
        "events": len(trades),
        "signals": signals_generated,
        "wins": wins,
        "losses": losses,
        "win_rate": win_rate,
        "final_cvd": final["cvd"],
        "final_absorption": final["absorption_score"],
        "signal_log": signal_log[-20:] if len(signal_log) > 20 else signal_log,
    }


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def print_summary(results: dict, coin: str, limit: int):
    total_evaluated = results["wins"] + results["losses"]
    print("\n" + "=" * 62)
    print(f"  BACKTEST SUMMARY  |  {coin}  |  session={results.get('session_id','')[:8]}")
    print("=" * 62)

    if results.get("signal_log"):
        print("\nSignals:")
        for s in results["signal_log"]:
            print(f"  {s}")

    print("\n" + "-" * 62)
    trades_line = f"Trades={results.get('trades', results['events'])}"
    if "l2_updates" in results:
        trades_line += f"  L2={results['l2_updates']}"
    print(f"  Events={results['events']}  {trades_line}  Signals={results['signals']}")
    print(f"  CVD={results['final_cvd']:+.4f}  Absorption={results['final_absorption']:.2f}")

    if total_evaluated:
        print(f"\n  Win Rate : {results['win_rate']*100:.1f}%  "
              f"({results['wins']}W / {results['losses']}L of {total_evaluated} evaluated)")
    else:
        print(f"\n  Win Rate : N/A (need {WIN_RATE_WINDOW}+ trades after a signal to evaluate)")

    print("=" * 62)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Brain Agent — streaming backtest / historical replay",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Stream 5 minutes of live data, log to Postgres
  python -m brain_agent.backtesting.simple_replay --coin BTC --duration-seconds 300 --db-log

  # Historical replay of 2000 trades (no Postgres)
  python -m brain_agent.backtesting.simple_replay --coin BTC --limit 2000
        """,
    )
    parser.add_argument("--coin", type=str, default="BTC")
    parser.add_argument("--limit", type=int, default=2000,
                        help="Trades to replay in historical mode (default: 2000)")
    parser.add_argument("--duration-seconds", type=int, default=300,
                        help="Stream for N seconds (0 = infinite streaming)")
    parser.add_argument("--print-every", type=int, default=100)
    parser.add_argument("--db-log", action="store_true", default=False,
                        help="Enable PostgreSQL logging (requires POSTGRES_* env vars)")
    parser.add_argument("--log-level", type=str, default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return parser.parse_args()


async def main():
    args = parse_args()
    logging.getLogger().setLevel(getattr(logging, args.log_level))

    session_id = str(uuid.uuid4())
    coin = args.coin.upper()

    # Optionally connect to Postgres
    db = None
    if args.db_log:
        from brain_agent.core.database import Database
        settings = get_settings()
        db = Database(settings.postgres.dsn)
        try:
            await db.connect()
            logger.info(f"DB logging enabled | session={session_id[:8]}")
        except Exception as e:
            logger.error(f"Could not connect to Postgres ({e}) — continuing without DB logging")
            db = None

    try:
        if args.duration_seconds >= 0:
            results = await run_backtest_streaming(
                coin=coin,
                duration_seconds=args.duration_seconds,
                print_every=args.print_every,
                db=db,
                session_id=session_id,
            )
        else:
            results = await run_backtest(
                coin=coin,
                limit=args.limit,
                print_every=args.print_every,
                db=db,
                session_id=session_id,
            )
    finally:
        if db:
            await db.disconnect()

    print_summary(results, coin, args.limit)


if __name__ == "__main__":
    asyncio.run(main())
