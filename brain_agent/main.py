"""
Main entry point for Brain Agent
Run in backtest, paper, or live mode with structured logging
"""
import asyncio
import argparse
import sys
import logging
from pathlib import Path
from typing import Optional
from datetime import datetime

import structlog

# Configure structlog
structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.dev.ConsoleRenderer()
    ],
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
    cache_logger_on_first_use=False,
)

logger = structlog.get_logger()

# Add project root to path
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from brain_agent.config.settings import get_settings, Settings
from brain_agent.core.data_pipeline import DataPipeline, create_data_pipeline
from brain_agent.core.orderflow_engine import OrderflowEngine
from brain_agent.core.models import OrderflowFeatures
from brain_agent.strategies.absorption import AbsorptionStrategy


class BrainAgent:
    """
    Main Brain Agent orchestrator.
    Connects data pipeline -> orderflow engine -> strategies -> execution
    """
    
    def __init__(self, settings: Optional[Settings] = None):
        self.settings = settings or get_settings()
        self.pipeline: Optional[DataPipeline] = None
        self.engine: Optional[OrderflowEngine] = None
        self.strategies = []
        self._running = False
    
    async def initialize(self):
        """Initialize agent components"""
        logger.info(
            "brain_agent_starting",
            data_source=self.settings.data_source,
            mode=self.settings.execution.mode,
            symbols=self.settings.symbols
        )
        
        # Initialize data pipeline
        self.pipeline = create_data_pipeline(self.settings.data_source)
        
        # Initialize orderflow engine with settings
        self.engine = OrderflowEngine(
            window=500,
            absorption_threshold=3.0
        )
        
        # Initialize strategies
        self.strategies = [
            AbsorptionStrategy(
                min_absorption_score=4.0,
                min_volume=10.0
            )
        ]
        
        logger.info(
            "agent_initialized",
            strategies=[s.name for s in self.strategies],
            data_source=self.settings.data_source
        )
    
    async def run_backtest(self, symbol: str = "BTC", limit: int = 1000):
        """Run backtest on historical data"""
        logger.info("backtest_starting", symbol=symbol, limit=limit)
        
        # Connect to pipeline
        await self.pipeline.connect()
        
        # Get historical data
        trades = await self.pipeline.get_historical_trades(symbol, limit=limit)
        
        if not trades:
            logger.warning("no_historical_data", symbol=symbol)
            # Generate synthetic data for testing
            trades = self._generate_synthetic_trades(symbol, limit)
        
        logger.info("processing_trades", count=len(trades))
        
        # Process each trade
        signals_generated = 0
        
        for trade in trades:
            # Convert trade to tick format
            tick = {
                "timestamp": trade.timestamp,
                "coin": symbol,
                "price": trade.price,
                "size": trade.size,
                "side": trade.side
            }
            
            # Process through engine
            features = self.engine.process_tick(tick)
            
            # Generate signals from all strategies
            for strategy in self.strategies:
                signal = strategy.generate_signal(
                    features,
                    tick["price"]
                )
                
                if signal.signal.value != "FLAT":
                    signals_generated += 1
                    logger.info(
                        "signal_generated",
                        direction=signal.signal.value,
                        confidence=signal.confidence,
                        reason=signal.reason
                    )
        
        logger.info(
            "backtest_complete",
            total_trades=len(trades),
            signals=signals_generated
        )
        
        # Print summary
        self._print_backtest_summary(trades, signals_generated)
        
        return signals_generated
    
    def _generate_synthetic_trades(self, symbol: str, count: int):
        """Generate synthetic data for testing"""
        import random
        
        base_price = 50000 if symbol == "BTC" else 3000
        trades = []
        current_time = int(datetime.now().timestamp() * 1000)
        
        for i in range(count):
            # Random walk price
            base_price += random.uniform(-50, 50)
            
            from brain_agent.core.models import Trade
            trade = Trade(
                timestamp=current_time - (count - i) * 1000,
                coin=symbol,
                price=base_price,
                size=random.uniform(0.1, 5.0),
                side=random.choice(["B", "S"])
            )
            trades.append(trade)
        
        return trades
    
    def _print_backtest_summary(self, trades, signals_generated):
        """Print backtest summary"""
        print("\n" + "=" * 50)
        print("BACKTEST SUMMARY")
        print("=" * 50)
        print(f"Total Trades Processed: {len(trades)}")
        print(f"Signals Generated: {signals_generated}")
        print(f"Signal Rate: {signals_generated/len(trades)*100:.2f}%" if trades else "N/A")
        print("=" * 50)
    
    async def run_paper(self, symbol: str = "BTC"):
        """Run paper trading (simulated)"""
        logger.info("paper_trading_starting", symbol=symbol)
        
        await self.pipeline.connect()
        self._running = True
        
        try:
            async for tick in self.pipeline.stream_ticks(symbol):
                if not self._running:
                    break
                
                # Process tick
                features = self.engine.process_tick(tick.model_dump())
                
                # Generate signals
                for strategy in self.strategies:
                    signal = strategy.generate_signal(features, tick.price)
                    
                    if signal.signal.value != "FLAT":
                        logger.info(
                            "live_signal",
                            direction=signal.signal.value,
                            confidence=signal.confidence,
                            reason=signal.reason
                        )
                        
        except KeyboardInterrupt:
            logger.info("shutdown_requested")
        finally:
            await self.shutdown()
    
    async def run_live(self, symbol: str = "BTC"):
        """Run live trading"""
        logger.warning("live_trading_not_implemented")
        print("⚠️ WARNING: Live trading is not yet implemented")
        print("   Please use paper mode for now.")
    
    async def shutdown(self):
        """Cleanup resources"""
        self._running = False
        if self.pipeline:
            await self.pipeline.disconnect()
        logger.info("brain_agent_shutdown_complete")


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Brain Agent - ML Trading System",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py backtest BTC
  python main.py --mode paper --symbol ETH
  python main.py --mode live --symbol SOL --log-level DEBUG
        """
    )
    
    parser.add_argument(
        "--mode",
        type=str,
        default="backtest",
        choices=["backtest", "paper", "live"],
        help="Execution mode (default: backtest)"
    )
    
    parser.add_argument(
        "--symbol",
        type=str,
        default="BTC",
        help="Trading symbol (default: BTC)"
    )
    
    parser.add_argument(
        "--limit",
        type=int,
        default=1000,
        help="Number of trades for backtest (default: 1000)"
    )
    
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level (default: INFO)"
    )
    
    parser.add_argument(
        "--data-source",
        type=str,
        default=None,
        choices=["hyperliquid", "sierra_dtc"],
        help="Data source (default: from settings)"
    )
    
    return parser.parse_args()


async def main():
    """Main entry point"""
    args = parse_args()
    
    # Get settings and update from args
    settings = get_settings()
    
    # Override settings from command line
    if args.log_level:
        settings.log_level = args.log_level.upper()
    if args.data_source:
        settings.data_source = args.data_source
    settings.execution.mode = args.mode
    settings.trading.default_symbol = args.symbol.upper()
    
    # Create and initialize agent
    agent = BrainAgent(settings)
    await agent.initialize()
    
    # Print startup banner
    print("\n" + "=" * 50)
    print("🧠 BRAIN AGENT - ML TRADING SYSTEM")
    print("=" * 50)
    print(f"Mode: {args.mode}")
    print(f"Symbol: {args.symbol}")
    print(f"Data Source: {settings.data_source}")
    print(f"Log Level: {args.log_level}")
    print("=" * 50 + "\n")
    
    try:
        if args.mode == "backtest":
            await agent.run_backtest(args.symbol, args.limit)
        elif args.mode == "paper":
            await agent.run_paper(args.symbol)
        elif args.mode == "live":
            await agent.run_live(args.symbol)
    finally:
        await agent.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
