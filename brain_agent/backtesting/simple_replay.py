"""
Simple Replay - Backtesting harness for historical data replay
Uses real Hyperliquid data when available, falls back to synthetic data

Usage:
    python -m brain_agent.backtesting.simple_replay --coin BTC --limit 2000
"""
import asyncio
import argparse
import logging
import random
from datetime import datetime
from typing import List, Optional

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


def _generate_synthetic_trades(coin: str, limit: int) -> List[TradeTick]:
    """
    Generate synthetic trade data for testing when real data is unavailable.
    """
    base_price = 50000 if coin == "BTC" else 3000
    trades = []
    current_time = int(datetime.now().timestamp() * 1000)
    
    for i in range(limit):
        # Random walk price
        base_price += random.uniform(-50, 50)
        
        trade = TradeTick(
            timestamp=current_time - (limit - i) * 1000,
            coin=coin,
            price=base_price,
            size=random.uniform(0.1, 5.0),
            side=random.choice(["B", "A"])
        )
        trades.append(trade)
    
    return trades


def _simulate_l2_update(
    coin: str, 
    last_price: float, 
    include_imbalance: bool = False,
    force_imbalance_ratio: float = None
) -> L2Update:
    """
    Simulate L2 orderbook update with improved realism.
    
    Features:
    - Creates bids/asks clustered around current price (±5-10 ticks)
    - Size decays with distance from best level
    - Occasionally creates strong imbalance (ratio > 3.0) or absorption
    
    Args:
        coin: Trading symbol
        last_price: Current price to center orderbook around
        include_imbalance: Whether to force an imbalance
        force_imbalance_ratio: If set, force this specific imbalance ratio
    """
    import random
    
    timestamp = int(datetime.now().timestamp() * 1000)
    
    # Determine if we should create an imbalance
    create_imbalance = include_imbalance or (force_imbalance_ratio is not None) or random.random() < 0.2
    
    # Calculate imbalance ratio
    if force_imbalance_ratio:
        imbalance_ratio = force_imbalance_ratio
    elif create_imbalance:
        # Create strong imbalance (ratio > 3.0 or < 0.33)
        imbalance_ratio = random.choice([random.uniform(3.0, 6.0), random.uniform(0.15, 0.33)])
    else:
        imbalance_ratio = 1.0
    
    # Base tick size
    tick_size = 0.5 if coin == "BTC" else 0.05
    
    # Generate bids (below last price)
    bids = []
    bid_side_volume = 0
    for i in range(5):
        px = last_price - (i + 1) * tick_size * random.uniform(0.8, 1.2)
        # Size decays with distance
        base_sz = 10.0 * (0.7 ** i)
        sz = base_sz * random.uniform(0.8, 1.2)
        
        # Occasionally create large size at best level (absorption)
        if i == 0 and random.random() < 0.15:
            sz *= random.uniform(3, 6)  # Large order at best bid
        
        bid_side_volume += sz
        bids.append({"px": str(int(px * 1e6)), "sz": str(int(sz * 1e6))})
    
    # Generate asks (above last price) - adjusted for imbalance
    asks = []
    target_ask_volume = bid_side_volume / imbalance_ratio if imbalance_ratio != 0 else bid_side_volume
    
    for i in range(5):
        px = last_price + (i + 1) * tick_size * random.uniform(0.8, 1.2)
        # Size decays with distance
        base_sz = 10.0 * (0.7 ** i)
        sz = base_sz * random.uniform(0.8, 1.2)
        
        # Scale to achieve target imbalance
        if imbalance_ratio != 1.0:
            sz = sz * (target_ask_volume / (bid_side_volume + 0.001)) * random.uniform(0.8, 1.2)
        
        # Occasionally create large size at best level (absorption)
        if i == 0 and random.random() < 0.15:
            sz *= random.uniform(3, 6)  # Large order at best ask
        
        asks.append({"px": str(int(px * 1e6)), "sz": str(int(sz * 1e6))})
    
    return L2Update(
        timestamp=timestamp,
        coin=coin,
        bids=bids,
        asks=asks,
        top_n=5
    )


async def run_backtest(
    coin: str, 
    limit: int, 
    print_every: int = 100,
    use_synthetic: bool = False,
    publish: bool = False
) -> dict:
    """
    Run backtest on historical data.
    
    Args:
        coin: Trading symbol (e.g., "BTC", "ETH")
        limit: Number of trades to process
        print_every: Print progress every N trades
        use_synthetic: Force use of synthetic data
        publish: Whether to publish events to Redis
        
    Returns:
        Dictionary with backtest results
    """
    logger.info(f"Starting backtest: coin={coin}, limit={limit}, publish={publish}")
    
    # Initialize components
    pipeline = DataPipeline()
    engine = OrderflowEngine(
        window=500,
        absorption_threshold=3.0
    )
    strategy = AbsorptionStrategy(
        min_absorption_score=4.0,
        min_volume=10.0
    )
    
    # Connect to data source
    await pipeline.connect()
    
    # Try to get real data first
    trades = []
    initial_l2 = None
    
    if not use_synthetic:
        try:
            trade_objects = await pipeline.get_historical_trades(coin, limit)
            # Convert Trade objects to TradeTick
            for t in trade_objects:
                trades.append(TradeTick(
                    timestamp=t.timestamp,
                    coin=t.coin,
                    price=t.price,
                    size=t.size,
                    side=t.side,
                    hash=t.hash
                ))
            
            # TODO: full historical L2 replay with Tardis later
            # Get real L2 snapshot at start for more realistic replay
            try:
                initial_l2 = await pipeline.get_l2_snapshot(coin)
                if initial_l2:
                    logger.info(f"Using real L2 snapshot at start")
            except Exception as e:
                logger.warning(f"Could not get initial L2 snapshot: {e}")
                
        except Exception as e:
            logger.warning(f"Failed to fetch real data: {e}")
    
    # Fallback to synthetic if no real data
    if not trades:
        logger.warning("⚠️ Generating synthetic data (no real data available)")
        trades = _generate_synthetic_trades(coin, limit)
    
    # Limit to requested number
    trades = trades[:limit]
    
    logger.info(f"✅ Processing {len(trades)} trades...")
    
    # Process trades
    signals_generated = 0
    signal_log = []
    published_count = 0
    
    # Use initial L2 if available, otherwise start with None
    l2_update_counter = 0
    
    for i, trade in enumerate(trades):
        try:
            # Create trade event
            event = Event(
                event_type=EventType.TRADE,
                data=trade
            )
            
            # Publish to Redis if requested (only first 5 events)
            if publish and published_count < 5:
                await pipeline.publish_event(event)
                published_count += 1
                if published_count == 1:
                    logger.info(f"📡 Published to market:data (first event)")
            
            # Process through engine
            if isinstance(event.data, TradeTick):
                tick_dict = {
                    "timestamp": event.data.timestamp,
                    "coin": event.data.coin,
                    "price": event.data.price,
                    "size": event.data.size,
                    "side": event.data.side
                }
                features = engine.process_tick(tick_dict)
                
                # Generate signal
                signal = strategy.generate_signal(features, trade.price)
                
                if signal.signal.value != "FLAT":
                    signals_generated += 1
                    signal_entry = f"[{i}] {signal.signal.value} | conf {signal.confidence:.2f} | {signal.reason}"
                    signal_log.append(signal_entry)
                    logger.info(signal_entry)
            
            # Simulate L2 updates periodically (every 5-10 trades)
            l2_update_counter += 1
            if l2_update_counter >= 5 and trade.price > 0:
                l2_update_counter = 0
                # Use initial L2 snapshot for first update, then simulate
                if initial_l2 and i < 10:
                    # Use real L2 data
                    l2_dict = {
                        "bids": initial_l2.bids[:5],
                        "asks": initial_l2.asks[:5]
                    }
                else:
                    # Simulate with occasional imbalance
                    include_imbalance = (i % 20 == 0)
                    l2 = _simulate_l2_update(coin, trade.price, include_imbalance=include_imbalance)
                    l2_dict = {
                        "bids": [(float(b.get("px", "0")) / 1e6, float(b.get("sz", "0"))) for b in l2.bids],
                        "asks": [(float(a.get("px", "0")) / 1e6, float(a.get("sz", "0"))) for a in l2.asks]
                    }
                engine.process_l2(l2_dict)
            
            # Print progress
            if (i + 1) % print_every == 0:
                snapshot = engine.get_snapshot()
                logger.info(
                    f"[{i+1}] CVD: {snapshot['cvd']:>8.2f} | "
                    f"Abs: {snapshot['absorption_score']:>5.2f} | "
                    f"Imb: {snapshot['bid_ask_imbalance']:>6.2f}"
                )
                
        except Exception as e:
            logger.error(f"Error processing trade {i}: {e}")
            continue
    
    # Cleanup
    await pipeline.disconnect()
    
    # Summary
    final_snapshot = engine.get_snapshot()
    results = {
        "events": len(trades),
        "signals": signals_generated,
        "final_cvd": final_snapshot["cvd"],
        "final_absorption": final_snapshot["absorption_score"],
        "signal_log": signal_log[-20:] if len(signal_log) > 20 else signal_log  # Last 20 signals
    }
    
    return results


def print_summary(results: dict, coin: str, limit: int):
    """Print backtest summary"""
    print("\n" + "=" * 60)
    print(f"BACKTEST: {coin} | Limit: {limit}")
    print("=" * 60)
    
    if results.get("signal_log"):
        print("\n📊 Signals generated:")
        for sig in results["signal_log"]:
            print(f"  {sig}")
    
    print("\n" + "-" * 60)
    print(f"SUMMARY: Events={results['events']} | Signals={results['signals']} | "
          f"Final CVD={results['final_cvd']:+.2f}")
    print("=" * 60)


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Simple replay backtesting harness",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m brain_agent.backtesting.simple_replay --coin BTC --limit 2000
  python -m brain_agent.backtesting.simple_replay --coin ETH --limit 1000 --print-every 50
  python -m brain_agent.backtesting.simple_replay --coin SOL --limit 500 --synthetic
  python -m brain_agent.backtesting.simple_replay --coin BTC --limit 1000 --publish
        """
    )
    
    parser.add_argument(
        "--coin",
        type=str,
        default="BTC",
        help="Trading symbol (default: BTC)"
    )
    
    parser.add_argument(
        "--limit",
        type=int,
        default=2000,
        help="Number of trades to process (default: 2000)"
    )
    
    parser.add_argument(
        "--print-every",
        type=int,
        default=100,
        help="Print progress every N trades (default: 100)"
    )
    
    parser.add_argument(
        "--synthetic",
        action="store_true",
        help="Force use of synthetic data"
    )
    
    parser.add_argument(
        "--publish",
        action="store_true",
        default=False,
        help="Publish events to Redis 'market:data' channel"
    )
    
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)"
    )
    
    return parser.parse_args()


async def main():
    """Main entry point"""
    args = parse_args()
    
    # Set logging level
    logging.getLogger().setLevel(getattr(logging, args.log_level))
    
    # Run backtest
    results = await run_backtest(
        coin=args.coin.upper(),
        limit=args.limit,
        print_every=args.print_every,
        use_synthetic=args.synthetic,
        publish=args.publish
    )
    
    # Print summary
    print_summary(results, args.coin.upper(), args.limit)


if __name__ == "__main__":
    asyncio.run(main())
