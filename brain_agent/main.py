"""
Main entry point for Brain Agent
Run in backtest, paper, or live mode
"""
import asyncio
import os
import sys
from datetime import datetime
from typing import Optional

import yaml
from dotenv import load_dotenv

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from brain_agent.core.data_pipeline import DataPipeline, create_data_pipeline
from brain_agent.core.orderflow_engine import OrderflowEngine
from brain_agent.strategies.absorption import AbsorptionStrategy


class BrainAgent:
    """
    Main Brain Agent orchestrator.
    Connects data pipeline -> orderflow engine -> strategies -> execution
    """
    
    def __init__(self, config_path: Optional[str] = None):
        self.config = self._load_config(config_path)
        self.pipeline: Optional[DataPipeline] = None
        self.engine: Optional[OrderflowEngine] = None
        self.strategies = []
        self._running = False
    
    def _load_config(self, config_path: Optional[str] = None) -> dict:
        """Load configuration from YAML"""
        if config_path is None:
            config_path = os.path.join(
                os.path.dirname(__file__),
                "config",
                "data_sources.yaml"
            )
        
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    async def initialize(self):
        """Initialize agent components"""
        print("🧠 Initializing Brain Agent...")
        
        # Load environment variables
        load_dotenv()
        
        # Initialize data pipeline
        source = self.config.get("default_source", "hyperliquid")
        self.pipeline = create_data_pipeline(source)
        
        # Initialize orderflow engine
        self.engine = OrderflowEngine(window=500)
        
        # Initialize strategies
        self.strategies = [
            AbsorptionStrategy(
                min_absorption_score=4.0,
                min_volume=10.0
            )
        ]
        
        print(f"   📊 Data source: {source}")
        print(f"   📈 Strategies loaded: {[s.name for s in self.strategies]}")
    
    async def run_backtest(self, symbol: str = "BTC", limit: int = 1000):
        """Run backtest on historical data"""
        print(f"\n🔄 Running backtest for {symbol}...")
        
        # Get historical data
        trades = await self.pipeline.get_historical_trades(symbol, limit=limit)
        
        if not trades:
            print("⚠️ No historical data available")
            # Generate synthetic data for testing
            trades = self._generate_synthetic_data(symbol, limit)
        
        print(f"   📊 Processing {len(trades)} trades...")
        
        # Process each trade
        signals_generated = 0
        for trade in trades:
            # Convert trade to tick format
            tick = {
                "timestamp": trade.get("timestamp", 0),
                "coin": symbol,
                "price": float(trade.get("price", 0)),
                "size": float(trade.get("size", 0)),
                "side": trade.get("side", "B")
            }
            
            # Process through engine
            features = self.engine.process_tick(tick)
            
            # Generate signals from all strategies
            for strategy in self.strategies:
                signal = strategy.generate_signal(
                    features.__dict__ if hasattr(features, '__dict__') else features,
                    tick["price"]
                )
                
                if signal.signal.value != "FLAT":
                    signals_generated += 1
                    print(f"   🚀 {signal.signal.value} | "
                          f"Conf: {signal.confidence:.1%} | "
                          f"{signal.reason}")
        
        print(f"\n✅ Backtest complete | Signals: {signals_generated}")
        return signals_generated
    
    def _generate_synthetic_data(self, symbol: str, count: int):
        """Generate synthetic data for testing"""
        import random
        import time
        
        base_price = 50000 if symbol == "BTC" else 3000
        trades = []
        
        for i in range(count):
            # Random walk price
            base_price += random.uniform(-50, 50)
            
            trades.append({
                "timestamp": int(time.time() * 1000) - (count - i) * 1000,
                "coin": symbol,
                "price": base_price,
                "size": random.uniform(0.1, 5.0),
                "side": random.choice(["B", "S"])
            })
        
        return trades
    
    async def run_paper(self, symbol: str = "BTC"):
        """Run paper trading (simulated)"""
        print(f"\n📝 Starting paper trading for {symbol}...")
        
        await self.pipeline.connect()
        self._running = True
        
        # In real implementation, this would stream live data
        # For now, just keep the agent running
        try:
            while self._running:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            print("\n🛑 Stopping paper trading...")
            self._running = False
    
    async def run_live(self, symbol: str = "BTC"):
        """Run live trading"""
        print(f"\n🔴 Starting live trading for {symbol}...")
        print("⚠️ WARNING: Live trading is not yet implemented")
        print("   Please use paper mode for now.")
    
    async def shutdown(self):
        """Cleanup resources"""
        self._running = False
        if self.pipeline:
            await self.pipeline.disconnect()
        print("👋 Brain Agent shutdown complete")


async def main():
    """Main entry point"""
    # Parse command line arguments
    mode = sys.argv[1] if len(sys.argv) > 1 else "backtest"
    symbol = sys.argv[2] if len(sys.argv) > 2 else "BTC"
    
    # Create and initialize agent
    agent = BrainAgent()
    await agent.initialize()
    
    try:
        if mode == "backtest":
            await agent.run_backtest(symbol)
        elif mode == "paper":
            await agent.run_paper(symbol)
        elif mode == "live":
            await agent.run_live(symbol)
        else:
            print(f"Unknown mode: {mode}")
            print("Usage: python main.py [backtest|paper|live] [symbol]")
    finally:
        await agent.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
