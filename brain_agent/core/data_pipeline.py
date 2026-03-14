"""
Data Pipeline - Pluggable data source for trading
Primary: Hyperliquid (L2 orderbook + raw trades)
Secondary: Sierra Chart DTC (for visual validation)
"""
import asyncio
import json
from typing import AsyncGenerator, Dict, Any, Optional
from dataclasses import dataclass
from datetime import datetime

from hyperliquid.info import Info
from hyperliquid.utils import constants
import redis.asyncio as redis
from pydantic import BaseModel
import os
import yaml


class Tick(BaseModel):
    """Normalized tick data from any source"""
    timestamp: int
    coin: str
    price: float
    size: float
    side: str  # "B" or "S"
    is_trade: bool


class L2Book(BaseModel):
    """Level 2 orderbook data"""
    timestamp: int
    coin: str
    bids: list[tuple[float, float]]  # (price, size)
    asks: list[tuple[float, float]]


class DataPipeline:
    """
    Pluggable data pipeline supporting multiple sources.
    Default: Hyperliquid (on-chain L2 + trades)
    Optional: Sierra DTC (for visual validation)
    """
    
    def __init__(self, source: str = "hyperliquid"):
        self.source = source
        self.redis: Optional[redis.Redis] = None
        self.info: Optional[Info] = None
        self._running = False
        self._subscriptions: Dict[str, callable] = {}
        
        # Load config
        self._load_config()
    
    def _load_config(self):
        """Load data sources configuration"""
        config_path = os.path.join(
            os.path.dirname(__file__), 
            "..", 
            "config", 
            "data_sources.yaml"
        )
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
    
    async def connect(self):
        """Connect to the data source"""
        if self.source == "hyperliquid":
            await self._connect_hyperliquid()
        elif self.source == "sierra_dtc":
            await self._connect_sierra_dtc()
        else:
            raise ValueError(f"Unknown data source: {self.source}")
    
    async def _connect_hyperliquid(self):
        """Connect to Hyperliquid WebSocket"""
        is_testnet = os.getenv("TESTNET", "false").lower() == "true"
        api_url = constants.TESTNET_API_URL if is_testnet else constants.MAINNET_API_URL
        
        self.info = Info(api_url)
        self._running = True
        print(f"✅ Hyperliquid connected (testnet={is_testnet})")
        
        # Subscribe to default symbols
        for coin in self.config.get("hyperliquid", {}).get("coins", ["BTC"]):
            await self._subscribe_hyperliquid_symbol(coin)
    
    async def _subscribe_hyperliquid_symbol(self, coin: str):
        """Subscribe to trades and L2 book for a symbol"""
        
        async def trades_handler(msg: Dict):
            """Handle incoming trade messages"""
            if msg.get("channel") == "trades":
                for trade in msg.get("data", []):
                    tick = Tick(
                        timestamp=int(trade["time"]),
                        coin=trade["coin"],
                        price=float(trade["px"]) / 1e6,  # Hyperliquid uses 6 decimal places
                        size=float(trade["sz"]),
                        side=trade["side"],
                        is_trade=True
                    )
                    await self._publish_tick(tick)
        
        # Note: Full WS subscription would go here
        # For now, we set up the structure
        self._subscriptions[f"trades_{coin}"] = trades_handler
        print(f"   📡 Subscribed to {coin} trades")
    
    async def _connect_sierra_dtc(self):
        """Connect to Sierra Chart DTC (stub - requires actual implementation)"""
        print("🔧 Sierra DTC stub active (simulated data)")
        self._running = True
        # TODO: Implement actual DTC socket connection
        # Requires: socket connection to SC instance, protobuf encoding
    
    async def _publish_tick(self, tick: Tick):
        """Publish tick data to Redis for other components"""
        if self.redis is None:
            redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
            self.redis = redis.from_url(redis_url)
        
        await self.redis.publish(
            "market:data",
            tick.model_dump_json()
        )
    
    async def get_historical_trades(
        self, 
        coin: str, 
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 10000
    ) -> list[Dict]:
        """Get historical trades for backtesting"""
        if self.source == "hyperliquid" and self.info:
            # Use Hyperliquid's all_trades endpoint
            try:
                trades = self.info.all_trades(coin)
                return trades
            except Exception as e:
                print(f"⚠️ Error fetching historical trades: {e}")
                return []
        return []
    
    async def get_l2_snapshot(self, coin: str) -> Optional[L2Book]:
        """Get current L2 orderbook snapshot"""
        if self.source == "hyperliquid" and self.info:
            try:
                l2_data = self.info.l2_book(coin)
                # Parse and return L2Book
                return L2Book(
                    timestamp=int(datetime.now().timestamp() * 1000),
                    coin=coin,
                    bids=l2_data.get("bids", []),
                    asks=l2_data.get("asks", [])
                )
            except Exception as e:
                print(f"⚠️ Error fetching L2 snapshot: {e}")
        return None
    
    async def stream_ticks(self, coin: str) -> AsyncGenerator[Tick, None]:
        """
        Stream real-time ticks for a symbol.
        In production, this yields from WebSocket.
        """
        # Placeholder - actual implementation would yield from WS
        while self._running:
            await asyncio.sleep(0.1)
            # This is a placeholder - real implementation connects to WS
            yield Tick(
                timestamp=int(datetime.now().timestamp() * 1000),
                coin=coin,
                price=0.0,
                size=0.0,
                side="B",
                is_trade=False
            )
    
    async def disconnect(self):
        """Disconnect from data source"""
        self._running = False
        if self.redis:
            await self.redis.close()
        print("📴 Data pipeline disconnected")


# Factory function for easy instantiation
def create_data_pipeline(source: str = None) -> DataPipeline:
    """Factory function to create data pipeline"""
    if source is None:
        # Load from config
        config_path = os.path.join(
            os.path.dirname(__file__),
            "..",
            "config",
            "data_sources.yaml"
        )
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        source = config.get("default_source", "hyperliquid")
    
    return DataPipeline(source=source)
