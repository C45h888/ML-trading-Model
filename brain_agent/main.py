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
                min_score=4.0,
                min_trade_count=10
            )
        ]
        
        logger.info(
            "agent_initialized",
            strategies=[s.name for s in self.strategies],
            data_source=self.settings.data_source
        )
    
    async def run_backtest(self, symbol: str = "BTC", limit: int = 1000, duration_seconds: int = 0):
        """
        Run backtest on live streaming data.
        Processes real-time data through strategy without execution.
        
        Args:
            symbol: Trading symbol (e.g., BTC, ETH)
            limit: Maximum trades to process (for backward compatibility)
            duration_seconds: Stop after X seconds (0 = indefinite)
        """
        import asyncio
        
        logger.info("backtest_starting_realtime", symbol=symbol, duration=duration_seconds)
        
        # Connect to pipeline (starts WebSocket subscriptions)
        await self.pipeline.connect()
        
        # Process live data streams
        signals_generated = 0
        trades_processed = 0
        l2_processed = 0
        
        # Set up duration timeout if specified
        timeout_task = None
        if duration_seconds > 0:
            loop = asyncio.get_event_loop()
            timeout_task = loop.call_later(duration_seconds, lambda: self._set_stop())
        
        try:
            # Create async tasks for both trades and L2 streams
            async for tick in self.pipeline.stream_ticks(symbol):
                trades_processed += 1
                
                # Process trade tick through engine
                tick_dict = {
                    "timestamp": tick.timestamp,
                    "coin": symbol,
                    "price": tick.price,
                    "size": tick.size,
                    "side": tick.side
                }
                
                features = self.engine.process_tick(tick_dict)
                
                # Generate signals from all strategies
                for strategy in self.strategies:
                    signal = strategy.generate_signal(features, tick.price)
                    
                    if signal.direction != "FLAT":
                        signals_generated += 1
                        logger.info(
                            "signal_generated",
                            direction=signal.direction,
                            confidence=signal.confidence,
                            reason=signal.reason
                        )
                
                # Log progress every 100 trades
                if trades_processed % 100 == 0:
                    logger.info(
                        "backtest_progress",
                        trades=trades_processed,
                        l2_updates=l2_processed,
                        signals=signals_generated
                    )
                    
        except KeyboardInterrupt:
            logger.info("backtest_interrupted")
        except Exception as e:
            logger.error(f"Backtest error: {e}")
        finally:
            self._running = False
            if timeout_task:
                timeout_task.cancel()
            logger.info(
                "backtest_complete",
                total_trades=trades_processed,
                l2_updates=l2_processed,
                signals=signals_generated
            )
        
        # Print summary
        self._print_backtest_summary(trades_processed, signals_generated)
        
        return signals_generated
    
    def _print_backtest_summary(self, trades, signals_generated):
        """Print backtest summary"""
        print("\n" + "=" * 50)
        print("BACKTEST SUMMARY")
        print("=" * 50)
        print(f"Total Trades Processed: {trades}")
        print(f"Signals Generated: {signals_generated}")
        print(f"Signal Rate: {signals_generated/trades*100:.2f}%" if trades else "N/A")
        print("=" * 50)
    
    async def run_paper(self, symbol: str = "BTC"):
        """
        Run paper trading with live data.
        Processes real-time trades and L2 data through strategy without execution.
        """
        logger.info("paper_trading_starting", symbol=symbol)
        
        await self.pipeline.connect()
        self._running = True
        
        trades_processed = 0
        l2_processed = 0
        signals_generated = 0
        
        try:
            # Process trades stream
            async for tick in self.pipeline.stream_ticks(symbol):
                if not self._running:
                    break
                
                trades_processed += 1
                
                # Process tick
                tick_dict = {
                    "timestamp": tick.timestamp,
                    "coin": symbol,
                    "price": tick.price,
                    "size": tick.size,
                    "side": tick.side
                }
                features = self.engine.process_tick(tick_dict)
                
                # Generate signals
                for strategy in self.strategies:
                    signal = strategy.generate_signal(features, tick.price)
                    
                    if signal.direction != "FLAT":
                        signals_generated += 1
                        logger.info(
                            "live_signal",
                            direction=signal.direction,
                            confidence=signal.confidence,
                            reason=signal.reason
                        )
                
                # Log progress
                if trades_processed % 50 == 0:
                    logger.info(
                        "paper_progress",
                        trades=trades_processed,
                        l2=l2_processed,
                        signals=signals_generated
                    )
            
            # Also process L2 updates in parallel
            async for l2_data in self.pipeline.stream_l2(symbol):
                if not self._running:
                    break
                
                l2_processed += 1
                
                # Process L2 through engine
                l2_dict = {
                    "bids": l2_data.bids,
                    "asks": l2_data.asks
                }
                features = self.engine.process_l2(l2_dict)
                
                # Log L2 processing
                if l2_processed % 100 == 0:
                    logger.info(
                        "l2_processed",
                        count=l2_processed,
                        spread=l2_data.spread
                    )
                    
        except KeyboardInterrupt:
            logger.info("shutdown_requested")
        finally:
            await self.shutdown()
            logger.info(
                "paper_complete",
                trades=trades_processed,
                l2_updates=l2_processed,
                signals=signals_generated
            )
    
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
    
    def _set_stop(self):
        """Set stop flag for duration-based shutdown"""
        self._running = False
        logger.info("duration_timeout_reached")


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
        "--duration-seconds",
        type=int,
        default=0,
        help="Stop after X seconds (0 = indefinite, default: 0)"
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
    if args.duration_seconds > 0:
        print(f"Duration: {args.duration_seconds} seconds")
    print("=" * 50 + "\n")
    
    try:
        if args.mode == "backtest":
            await agent.run_backtest(args.symbol, args.limit, args.duration_seconds)
        elif args.mode == "paper":
            await agent.run_paper(args.symbol)
        elif args.mode == "live":
            await agent.run_live(args.symbol)
    finally:
        await agent.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
