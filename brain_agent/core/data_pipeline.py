"""
Data Pipeline - Pluggable data source for trading
Split architecture: HyperliquidSource, SierraDTCSource, DataPipeline

Primary: Hyperliquid (L2 orderbook + raw trades)
Secondary: Sierra Chart DTC (for visual validation)
"""
import asyncio
import logging
from typing import AsyncGenerator, Dict, Any, Optional, Callable
from abc import ABC, abstractmethod

import redis.asyncio as redis
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type
)

from brain_agent.config.settings import get_settings
from brain_agent.core.models import Tick, OrderbookSnapshot, Trade


class BaseDataSource(ABC):
    """Abstract base class for data sources"""
    
    @abstractmethod
    async def connect(self) -> None:
        """Connect to the data source"""
        pass
    
    @abstractmethod
    async def disconnect(self) -> None:
        """Disconnect from the data source"""
        pass
    
    @abstractmethod
    async def subscribe(self, coin: str, callback: Callable) -> None:
        """Subscribe to data for a coin"""
        pass
    
    @abstractmethod
    async def get_historical_trades(
        self, 
        coin: str, 
        limit: int = 1000
    ) -> list[Trade]:
        """Get historical trades"""
        pass
    
    @abstractmethod
    async def get_l2_snapshot(self, coin: str) -> Optional[OrderbookSnapshot]:
        """Get current L2 orderbook snapshot"""
        pass
    
    @property
    @abstractmethod
    def name(self) -> str:
        """Data source name"""
        pass


class HyperliquidSource(BaseDataSource):
    """
    Hyperliquid data source with WebSocket connection.
    Includes automatic reconnection with tenacity.
    """
    
    def __init__(self):
        self.settings = get_settings()
        self._info = None
        self._ws = None
        self._running = False
        self._subscriptions: Dict[str, Callable] = {}
    
    @property
    def name(self) -> str:
        return "hyperliquid"
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(Exception)
    )
    async def connect(self) -> None:
        """Connect to Hyperliquid"""
        from hyperliquid.info import Info
        from hyperliquid.utils import constants
        
        is_testnet = self.settings.hyperliquid.testnet
        api_url = self.settings.hyperliquid.api_url or (
            constants.TESTNET_API_URL if is_testnet else constants.MAINNET_API_URL
        )
        
        self._info = Info(api_url)
        self._running = True
        
        print(f"✅ Hyperliquid connected (testnet={is_testnet})")
        
        # Subscribe to default symbols
        for coin in self.settings.hyperliquid.coins:
            await self.subscribe(coin, lambda m: m)
    
    async def disconnect(self) -> None:
        """Disconnect from Hyperliquid"""
        self._running = False
        self._info = None
        print("📴 Hyperliquid disconnected")
    
    async def subscribe(self, coin: str, callback: Callable) -> None:
        """Subscribe to trades for a coin"""
        self._subscriptions[coin] = callback
        print(f"📡 Subscribed to {coin} trades")
    
    async def get_historical_trades(
        self, 
        coin: str, 
        limit: int = 1000
    ) -> list[Trade]:
        """Get historical trades from Hyperliquid"""
        if not self._info:
            print("Hyperliquid not connected")
            return []
        
        try:
            trades_data = self._info.all_trades(coin)
            trades = []
            
            for t in trades_data[:limit]:
                trade = Trade(
                    timestamp=int(t.get("time", 0)),
                    coin=coin,
                    price=float(t.get("px", 0)) / 1e6,  # Hyperliquid uses 6 decimal places
                    size=float(t.get("sz", 0)),
                    side=t.get("side", "B"),
                    hash=t.get("hash")
                )
                trades.append(trade)
            
            print(f"📊 Fetched {len(trades)} historical trades for {coin}")
            return trades
            
        except Exception as e:
            print(f"Error fetching historical trades: {e}")
            return []
    
    async def get_l2_snapshot(self, coin: str) -> Optional[OrderbookSnapshot]:
        """Get current L2 orderbook snapshot"""
        if not self._info:
            return None
        
        try:
            l2_data = self._info.l2_book(coin)
            
            return OrderbookSnapshot(
                timestamp=int(asyncio.get_event_loop().time() * 1000),
                coin=coin,
                bids=l2_data.get("bids", []),
                asks=l2_data.get("asks", [])
            )
        except Exception as e:
            print(f"Error fetching L2 snapshot: {e}")
            return None
    
    async def stream_ticks(self, coin: str) -> AsyncGenerator[Tick, None]:
        """
        Stream real-time ticks (placeholder - actual WS would yield here).
        For now, returns historical replay for backtesting.
        """
        trades = await self.get_historical_trades(coin)
        
        for trade in trades:
            yield trade.to_tick()
            await asyncio.sleep(0.001)  # Small delay to simulate streaming


class SierraDTCSource(BaseDataSource):
    """
    Sierra Chart DTC data source (stub implementation).
    Full implementation would require socket connection to Sierra Chart.
    """
    
    def __init__(self):
        self.settings = get_settings()
        self._running = False
    
    @property
    def name(self) -> str:
        return "sierra_dtc"
    
    async def connect(self) -> None:
        """Connect to Sierra DTC (stub)"""
        print("⚠️ Sierra DTC is a stub - no actual connection")
        self._running = True
    
    async def disconnect(self) -> None:
        """Disconnect from Sierra DTC"""
        self._running = False
        print("📴 Sierra DTC disconnected")
    
    async def subscribe(self, coin: str, callback: Callable) -> None:
        """Subscribe to data (stub)"""
        print(f"📡 Sierra DTC subscribe to {coin} (stub)")
    
    async def get_historical_trades(
        self, 
        coin: str, 
        limit: int = 1000
    ) -> list[Trade]:
        """Get historical trades (stub)"""
        print("⚠️ Sierra DTC historical trades not implemented")
        return []
    
    async def get_l2_snapshot(self, coin: str) -> Optional[OrderbookSnapshot]:
        """Get L2 snapshot (stub)"""
        print("⚠️ Sierra DTC L2 snapshot not implemented")
        return None


class DataPipeline:
    """
    Main data pipeline orchestrator.
    Manages data sources and provides unified async interface.
    """
    
    def __init__(self, source: Optional[str] = None):
        self.settings = get_settings()
        
        # Determine source
        self.source_name = source or self.settings.data_source
        
        # Initialize source
        if self.source_name == "hyperliquid":
            self._source: BaseDataSource = HyperliquidSource()
        elif self.source_name == "sierra_dtc":
            self._source: BaseDataSource = SierraDTCSource()
        else:
            raise ValueError(f"Unknown data source: {self.source_name}")
        
        # Redis for publishing
        self._redis: Optional[redis.Redis] = None
        
        print(f"📡 DataPipeline initialized with source: {self.source_name}")
    
    async def connect(self) -> None:
        """Connect to the data source"""
        await self._source.connect()
        
        # Initialize Redis connection
        try:
            self._redis = redis.from_url(self.settings.redis.url)
            await self._redis.ping()
            print("📡 Redis connected")
        except Exception as e:
            print(f"⚠️ Redis connection failed: {e}")
            self._redis = None
    
    async def disconnect(self) -> None:
        """Disconnect from all sources"""
        await self._source.disconnect()
        
        if self._redis:
            await self._redis.close()
            print("📴 Redis disconnected")
    
    async def publish_tick(self, tick: Tick) -> None:
        """Publish tick to Redis"""
        if self._redis:
            try:
                await self._redis.publish(
                    "market:data",
                    tick.model_dump_json()
                )
            except Exception as e:
                print(f"Error publishing tick: {e}")
    
    async def get_historical_trades(
        self, 
        coin: str, 
        limit: int = 1000
    ) -> list[Trade]:
        """Get historical trades"""
        return await self._source.get_historical_trades(coin, limit)
    
    async def get_l2_snapshot(self, coin: str) -> Optional[OrderbookSnapshot]:
        """Get L2 snapshot"""
        return await self._source.get_l2_snapshot(coin)
    
    async def stream_ticks(self, coin: str) -> AsyncGenerator[Tick, None]:
        """Stream ticks from the source"""
        async for tick in self._source.stream_ticks(coin):
            yield tick
    
    @property
    def source(self) -> BaseDataSource:
        """Get the underlying source"""
        return self._source


# Factory function
def create_data_pipeline(source: Optional[str] = None) -> DataPipeline:
    """Factory function to create data pipeline"""
    return DataPipeline(source=source)
