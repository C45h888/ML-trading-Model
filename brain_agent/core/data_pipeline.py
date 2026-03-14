"""
Data Pipeline - Pluggable data source for trading
Split architecture: HyperliquidSource, SierraDTCSource, DataPipeline

Primary: Hyperliquid (L2 orderbook + raw trades)
Secondary: Sierra Chart DTC (for visual validation)

Features:
- Automatic reconnection with tenacity (10x retry with exponential backoff)
- Normalized data (TradeTick, L2Update)
- Async event streaming
- Redis publishing to "market:data" channel
"""
import asyncio
import logging
import json
from typing import AsyncGenerator, Dict, Any, Optional, Callable, Union
from abc import ABC, abstractmethod

import redis.asyncio as redis
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type
)

from brain_agent.config.settings import get_settings
from brain_agent.core.models import (
    Tick, 
    OrderbookSnapshot, 
    Trade, 
    TradeTick, 
    L2Update, 
    Event, 
    EventType
)

logger = logging.getLogger(__name__)


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
    Includes automatic reconnection with tenacity (10x retry with exponential backoff).
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
        stop=stop_after_attempt(10),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        retry=retry_if_exception_type(Exception)
    )
    async def _connect_with_retry(self) -> None:
        """Connect to Hyperliquid with retry logic"""
        from hyperliquid.info import Info
        from hyperliquid.utils import constants
        
        is_testnet = self.settings.hyperliquid.testnet
        api_url = self.settings.hyperliquid.api_url or (
            constants.TESTNET_API_URL if is_testnet else constants.MAINNET_API_URL
        )
        
        self._info = Info(api_url)
        self._running = True
        
        logger.info(f"✅ Hyperliquid connected (testnet={is_testnet})")
    
    async def connect(self) -> None:
        """Connect to Hyperliquid"""
        try:
            await self._connect_with_retry()
        except Exception as e:
            logger.error(f"Failed to connect to Hyperliquid after 10 attempts: {e}")
            raise
        
        # Subscribe to default symbols
        for coin in self.settings.hyperliquid.coins:
            await self.subscribe(coin, lambda m: m)
    
    async def disconnect(self) -> None:
        """Disconnect from Hyperliquid"""
        self._running = False
        self._info = None
        logger.info("📴 Hyperliquid disconnected")
    
    async def subscribe(self, coin: str, callback: Callable) -> None:
        """Subscribe to trades for a coin"""
        self._subscriptions[coin] = callback
        logger.info(f"📡 Subscribed to {coin} trades")
    
    def _normalize_trade(self, t: Dict, coin: str) -> TradeTick:
        """
        Normalize Hyperliquid trade to TradeTick.
        Hyperliquid returns prices as strings with 6 decimal places.
        """
        return TradeTick(
            timestamp=int(t.get("time", 0)),
            coin=coin,
            price=float(t.get("px", "0")) / 1e6,  # Divide by 1e6
            size=float(t.get("sz", "0")),
            side=t.get("side", "B"),  # B for buy, A for ask
            hash=t.get("hash")
        )
    
    def _normalize_l2(self, l2_data: Dict, coin: str, timestamp: int) -> L2Update:
        """
        Normalize Hyperliquid L2 data to L2Update.
        """
        return L2Update(
            timestamp=timestamp,
            coin=coin,
            bids=l2_data.get("bids", []),
            asks=l2_data.get("asks", []),
            top_n=5
        )
    
    async def get_historical_trades(
        self, 
        coin: str, 
        limit: int = 1000
    ) -> list[Trade]:
        """Get historical trades from Hyperliquid"""
        if not self._info:
            logger.warning("Hyperliquid not connected")
            return []
        
        try:
            # Use info.all_trades method
            trades_data = self._info.all_trades(coin)
            trades = []
            
            for t in trades_data[:limit]:
                trade = Trade(
                    timestamp=int(t.get("time", 0)),
                    coin=coin,
                    price=float(t.get("px", "0")) / 1e6,  # Hyperliquid uses 6 decimal places
                    size=float(t.get("sz", "0")),
                    side=t.get("side", "B"),
                    hash=t.get("hash")
                )
                trades.append(trade)
            
            logger.info(f"📊 Fetched {len(trades)} historical trades for {coin}")
            return trades
            
        except Exception as e:
            logger.error(f"Error fetching historical trades: {e}")
            return []
    
    async def get_l2_snapshot(self, coin: str) -> Optional[OrderbookSnapshot]:
        """Get current L2 orderbook snapshot"""
        if not self._info:
            return None
        
        try:
            l2_data = self._info.l2_book(coin)
            timestamp = int(asyncio.get_event_loop().time() * 1000)
            
            # Convert Hyperliquid format to OrderbookSnapshot
            bids = []
            asks = []
            
            for bid in l2_data.get("bids", []):
                px = float(bid[0]) / 1e6 if isinstance(bid[0], (int, str)) else 0
                sz = float(bid[1]) if isinstance(bid[1], (int, str)) else 0
                bids.append((px, sz))
            
            for ask in l2_data.get("asks", []):
                px = float(ask[0]) / 1e6 if isinstance(ask[0], (int, str)) else 0
                sz = float(ask[1]) if isinstance(ask[1], (int, str)) else 0
                asks.append((px, sz))
            
            return OrderbookSnapshot(
                timestamp=timestamp,
                coin=coin,
                bids=bids,
                asks=asks
            )
        except Exception as e:
            logger.error(f"Error fetching L2 snapshot: {e}")
            return None
    
    async def stream_events(self, coin: str) -> AsyncGenerator[Event, None]:
        """
        Stream normalized events (trades and L2 updates) for a coin.
        
        This is the main async generator that yields normalized Event objects.
        For backtesting, it yields historical trades as events.
        For live mode, it would subscribe to WebSocket for real-time data.
        """
        # First yield historical trades as events
        trades = await self.get_historical_trades(coin, limit=1000)
        
        for trade in trades:
            # Convert Trade to TradeTick
            tick = TradeTick(
                timestamp=trade.timestamp,
                coin=trade.coin,
                price=trade.price,
                size=trade.size,
                side=trade.side,
                hash=trade.hash
            )
            
            event = Event(
                event_type=EventType.TRADE,
                data=tick
            )
            
            yield event
            
            # Small delay to simulate streaming
            await asyncio.sleep(0.001)
    
    async def stream_ticks(self, coin: str) -> AsyncGenerator[Tick, None]:
        """
        Stream real-time ticks (legacy method).
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
    
    Features:
    - Async event streaming via stream_events()
    - Redis publishing to "market:data" channel
    - Context manager support for graceful cleanup
    - Graceful shutdown via stop() method
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
        
        # Running state
        self._running = False
        
        logger.info(f"📡 DataPipeline initialized with source: {self.source_name}")
    
    async def __aenter__(self) -> "DataPipeline":
        """Context manager entry"""
        await self.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit"""
        await self.disconnect()
    
    async def connect(self) -> None:
        """Connect to the data source"""
        await self._source.connect()
        self._running = True
        
        # Initialize Redis connection
        try:
            self._redis = redis.from_url(self.settings.redis.url)
            await self._redis.ping()
            logger.info("📡 Redis connected")
        except Exception as e:
            logger.warning(f"⚠️ Redis connection failed: {e}")
            self._redis = None
    
    async def disconnect(self) -> None:
        """Disconnect from all sources"""
        self._running = False
        await self._source.disconnect()
        
        if self._redis:
            await self._redis.close()
            logger.info("📴 Redis disconnected")
    
    async def stop(self) -> None:
        """Graceful shutdown"""
        await self.disconnect()
        logger.info("DataPipeline stopped")
    
    async def publish_event(self, event: Event) -> None:
        """Publish event to Redis 'market:data' channel as JSON"""
        if self._redis:
            try:
                await self._redis.publish(
                    "market:data",
                    json.dumps(event.model_dump())
                )
            except Exception as e:
                logger.error(f"Error publishing event: {e}")
    
    async def publish_tick(self, tick: Tick) -> None:
        """Publish tick to Redis (legacy method)"""
        if self._redis:
            try:
                await self._redis.publish(
                    "market:data",
                    tick.model_dump_json()
                )
            except Exception as e:
                logger.error(f"Error publishing tick: {e}")
    
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
    
    async def stream_events(self, coin: str) -> AsyncGenerator[Event, None]:
        """
        Stream normalized events from the source.
        
        Yields Event objects containing either TradeTick or L2Update data.
        """
        async for event in self._source.stream_events(coin):
            # Publish to Redis
            await self.publish_event(event)
            yield event
    
    async def stream_ticks(self, coin: str) -> AsyncGenerator[Tick, None]:
        """Stream ticks from the source"""
        async for tick in self._source.stream_ticks(coin):
            yield tick
    
    @property
    def source(self) -> BaseDataSource:
        """Get the underlying source"""
        return self._source
    
    @property
    def is_running(self) -> bool:
        """Check if pipeline is running"""
        return self._running


# Factory function
def create_data_pipeline(source: Optional[str] = None) -> DataPipeline:
    """Factory function to create data pipeline"""
    return DataPipeline(source=source)
