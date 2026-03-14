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
    Includes error handling around all callbacks.
    Publishes to Redis channels: trades:BTC, l2:BTC, etc.
    Maintains buffer of last 500 events per coin for backtest startup.
    """
    
    def __init__(self, redis_client=None):
        self.settings = get_settings()
        self._info = None
        self._ws = None
        self._running = False
        self._subscriptions: Dict[str, Callable] = {}
        self._trades_callbacks: Dict[str, Callable] = {}
        self._l2_callbacks: Dict[str, Callable] = {}
        self._redis = redis_client  # Redis client for publishing
        
        # Event buffers for backtest startup (last 500 events per coin)
        from collections import deque
        coins = self.settings.hyperliquid.coins
        self._trades_buffer: Dict[str, deque] = {coin: deque(maxlen=500) for coin in coins}
        self._l2_buffer: Dict[str, deque] = {coin: deque(maxlen=500) for coin in coins}
    
    @property
    def name(self) -> str:
        return "hyperliquid"
    
    @property
    def ws_url(self) -> str:
        """Get WebSocket URL from settings"""
        return self.settings.hyperliquid.ws_url
    
    @property
    def is_testnet(self) -> bool:
        """Get testnet flag from settings"""
        return self.settings.hyperliquid.testnet
    
    @retry(
        stop=stop_after_attempt(10),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        retry=retry_if_exception_type(Exception)
    )
    async def _connect_with_retry(self) -> None:
        """Connect to Hyperliquid with retry logic"""
        from hyperliquid.info import Info
        from hyperliquid.utils import constants
        
        is_testnet = self.is_testnet
        api_url = self.settings.hyperliquid.api_url or (
            constants.TESTNET_API_URL if is_testnet else constants.MAINNET_API_URL
        )
        
        self._info = Info(api_url)
        self._running = True
        
        logger.info(f"✅ Hyperliquid connected (testnet={is_testnet}, ws_url={self.ws_url})")
    
    async def connect(self) -> None:
        """Connect to Hyperliquid with WebSocket subscriptions"""
        try:
            await self._connect_with_retry()
        except Exception as e:
            logger.error(f"Failed to connect to Hyperliquid after 10 attempts: {e}")
            raise
        
        # Subscribe to WebSocket feeds for all configured coins
        for coin in self.settings.hyperliquid.coins:
            try:
                # Subscribe to trades
                self._info.subscribe(
                    {"type": "trades", "coin": coin},
                    lambda msg, c=coin: self._trades_callback_wrapper(c, msg)
                )
                logger.info(f"📡 Subscribed to {coin} trades via WebSocket")
                
                # Subscribe to L2 orderbook
                self._info.subscribe(
                    {"type": "l2Book", "coin": coin},
                    lambda msg, c=coin: self._l2_callback_wrapper(c, msg)
                )
                logger.info(f"📡 Subscribed to {coin} L2 orderbook via WebSocket")
                
            except Exception as e:
                logger.warning(f"Failed to subscribe to {coin}: {e}")
    
    async def disconnect(self) -> None:
        """Disconnect from Hyperliquid"""
        self._running = False
        
        # Close WebSocket if available
        if self._ws:
            try:
                self._ws.close()
            except Exception as e:
                logger.warning(f"Error closing WebSocket: {e}")
        
        self._info = None
        self._ws = None
        logger.info("📴 Hyperliquid disconnected")
    
    async def stop(self) -> None:
        """Graceful stop - alias for disconnect"""
        await self.disconnect()
        logger.info("HyperliquidSource stopped")
    
    async def subscribe(self, coin: str, callback: Callable) -> None:
        """Subscribe to trades for a coin"""
        self._subscriptions[coin] = callback
        self._trades_callbacks[coin] = callback
        logger.info(f"📡 Subscribed to {coin} trades")
    
    async def subscribe_l2(self, coin: str, callback: Callable) -> None:
        """Subscribe to L2 orderbook for a coin"""
        self._l2_callbacks[coin] = callback
        logger.info(f"📡 Subscribed to {coin} L2 orderbook")
    
    def _trades_callback_wrapper(self, coin: str, data: Any) -> None:
        """Wrapper for trades callback with error handling, buffering and Redis publishing"""
        callback = self._trades_callbacks.get(coin)
        
        try:
            # Handle Hyperliquid WebSocket message format
            if isinstance(data, dict):
                channel = data.get("channel")
                if channel == "trades":
                    trade_data = data.get("data", [])
                    if isinstance(trade_data, list):
                        for t in trade_data:
                            normalized = self._normalize_trade(t, coin)
                            # Store in buffer
                            if coin in self._trades_buffer:
                                self._trades_buffer[coin].append(normalized)
                            # Execute callback if registered
                            if callback:
                                callback(normalized)
                            # Publish to Redis channel: trades:BTC, trades:ETH, etc.
                            self._publish_to_redis(f"trades:{coin}", normalized)
            elif isinstance(data, list):
                for t in data:
                    normalized = self._normalize_trade(t, coin)
                    if coin in self._trades_buffer:
                        self._trades_buffer[coin].append(normalized)
                    if callback:
                        callback(normalized)
                    self._publish_to_redis(f"trades:{coin}", normalized)
        except Exception as e:
            logger.error(f"Error in trades callback for {coin}: {e}")
    
    def _l2_callback_wrapper(self, coin: str, data: Any) -> None:
        """Wrapper for L2 callback with error handling, buffering and Redis publishing"""
        callback = self._l2_callbacks.get(coin)
        
        try:
            # Handle Hyperliquid WebSocket message format for L2
            if isinstance(data, dict):
                channel = data.get("channel")
                if channel == "l2Book":
                    l2_data = data.get("data", {})
                    timestamp = l2_data.get("time", int(asyncio.get_event_loop().time() * 1000))
                    levels = l2_data.get("levels", [[], []])  # [bids, asks]
                    
                    normalized = L2Update(
                        timestamp=timestamp,
                        coin=coin,
                        bids=levels[0] if levels[0] else [],
                        asks=levels[1] if levels[1] else [],
                        top_n=5
                    )
                    
                    # Store in buffer
                    if coin in self._l2_buffer:
                        self._l2_buffer[coin].append(normalized)
                    
                    if callback:
                        callback(normalized)
                    # Publish to Redis channel: l2:BTC, l2:ETH, etc.
                    self._publish_to_redis(f"l2:{coin}", normalized)
        except Exception as e:
            logger.error(f"Error in L2 callback for {coin}: {e}")
    
    def _publish_to_redis(self, channel: str, data: Any) -> None:
        """Publish data to Redis channel"""
        if self._redis:
            try:
                import asyncio
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    # Schedule async publish in the running loop
                    asyncio.create_task(self._async_publish(channel, data))
                else:
                    # No running loop, try to publish synchronously
                    loop.run_until_complete(self._async_publish(channel, data))
            except Exception as e:
                logger.debug(f"Redis publish error (non-critical): {e}")
    
    async def _async_publish(self, channel: str, data: Any) -> None:
        """Async Redis publish helper"""
        if self._redis:
            try:
                await self._redis.publish(channel, json.dumps(data.model_dump() if hasattr(data, 'model_dump') else data))
            except Exception as e:
                logger.debug(f"Redis async publish error: {e}")
    
    def get_buffered_trades(self, coin: str) -> list:
        """Get buffered trades for a coin (last 500)"""
        if coin in self._trades_buffer:
            return list(self._trades_buffer[coin])
        return []
    
    def get_buffered_l2(self, coin: str) -> list:
        """Get buffered L2 updates for a coin (last 500)"""
        if coin in self._l2_buffer:
            return list(self._l2_buffer[coin])
        return []
    
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
        Stream real-time ticks from Redis channel.
        Subscribes to trades:{coin} channel and yields normalized ticks.
        """
        if not self._redis:
            logger.warning("No Redis connection, cannot stream ticks")
            return
        
        pubsub = self._redis.pubsub()
        await pubsub.subscribe(f"trades:{coin}")
        
        try:
            async for message in pubsub.listen():
                if message["type"] == "message":
                    try:
                        data = json.loads(message["data"])
                        tick = Tick(
                            timestamp=data.get("timestamp", 0),
                            coin=data.get("coin", coin),
                            price=float(data.get("price", 0)),
                            size=float(data.get("size", 0)),
                            side=data.get("side", "B"),
                            is_trade=True
                        )
                        yield tick
                    except Exception as e:
                        logger.error(f"Error parsing tick: {e}")
        finally:
            await pubsub.unsubscribe(f"trades:{coin}")
            await pubsub.close()
    
    async def stream_l2(self, coin: str) -> AsyncGenerator[L2Update, None]:
        """
        Stream real-time L2 updates from Redis channel.
        Subscribes to l2:{coin} channel and yields L2 updates.
        """
        if not self._redis:
            logger.warning("No Redis connection, cannot stream L2")
            return
        
        pubsub = self._redis.pubsub()
        await pubsub.subscribe(f"l2:{coin}")
        
        try:
            async for message in pubsub.listen():
                if message["type"] == "message":
                    try:
                        data = json.loads(message["data"])
                        l2 = L2Update(
                            timestamp=data.get("timestamp", 0),
                            coin=data.get("coin", coin),
                            bids=data.get("bids", []),
                            asks=data.get("asks", []),
                            top_n=data.get("top_n", 5)
                        )
                        yield l2
                    except Exception as e:
                        logger.error(f"Error parsing L2: {e}")
        finally:
            await pubsub.unsubscribe(f"l2:{coin}")
            await pubsub.close()


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
    - Async event streaming via stream_events() and stream_ticks()
    - Redis publishing to separate channels (trades:BTC, l2:BTC, etc.)
    - Redis subscribing for consuming data
    - Context manager support for graceful cleanup
    - Graceful shutdown via stop() method
    """
    
    def __init__(self, source: Optional[str] = None):
        self.settings = get_settings()
        
        # Determine source
        self.source_name = source or self.settings.data_source
        
        # Redis for publishing
        self._redis: Optional[redis.Redis] = None
        
        # Initialize source (will be connected to Redis later)
        if self.source_name == "hyperliquid":
            self._source: BaseDataSource = HyperliquidSource()
        elif self.source_name == "sierra_dtc":
            self._source: BaseDataSource = SierraDTCSource()
        else:
            raise ValueError(f"Unknown data source: {self.source_name}")
        
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
        """Connect to the data source and initialize Redis"""
        # Initialize Redis connection first
        try:
            self._redis = redis.from_url(self.settings.redis.url)
            await self._redis.ping()
            logger.info("📡 Redis connected")
        except Exception as e:
            logger.warning(f"⚠️ Redis connection failed: {e}")
            self._redis = None
        
        # Pass Redis to source for publishing
        if hasattr(self._source, '_redis'):
            self._source._redis = self._redis
        
        # Connect to data source
        await self._source.connect()
        self._running = True
    
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
        Stream normalized events from Redis.
        Subscribes to both trades and L2 channels.
        """
        if not self._redis:
            logger.warning("No Redis connection for streaming")
            return
        
        pubsub = self._redis.pubsub()
        await pubsub.subscribe(f"trades:{coin}", f"l2:{coin}")
        
        try:
            async for message in pubsub.listen():
                if message["type"] == "message":
                    channel = message["channel"]
                    try:
                        data = json.loads(message["data"])
                        
                        if channel.startswith("trades:"):
                            tick = TradeTick(
                                timestamp=data.get("timestamp", 0),
                                coin=data.get("coin", coin),
                                price=float(data.get("price", 0)),
                                size=float(data.get("size", 0)),
                                side=data.get("side", "B"),
                                hash=data.get("hash")
                            )
                            yield Event(event_type=EventType.TRADE, data=tick)
                            
                        elif channel.startswith("l2:"):
                            l2 = L2Update(
                                timestamp=data.get("timestamp", 0),
                                coin=data.get("coin", coin),
                                bids=data.get("bids", []),
                                asks=data.get("asks", []),
                                top_n=data.get("top_n", 5)
                            )
                            yield Event(event_type=EventType.L2_UPDATE, data=l2)
                            
                    except Exception as e:
                        logger.error(f"Error parsing event: {e}")
        finally:
            await pubsub.unsubscribe(f"trades:{coin}", f"l2:{coin}")
            await pubsub.close()
    
    async def stream_ticks(self, coin: str) -> AsyncGenerator[Tick, None]:
        """
        Stream real-time ticks from Redis channel.
        Subscribes to trades:{coin} channel and yields normalized ticks.
        """
        if not self._redis:
            logger.warning("No Redis connection, cannot stream ticks")
            return
        
        pubsub = self._redis.pubsub()
        await pubsub.subscribe(f"trades:{coin}")
        
        try:
            async for message in pubsub.listen():
                if message["type"] == "message":
                    try:
                        data = json.loads(message["data"])
                        tick = Tick(
                            timestamp=data.get("timestamp", 0),
                            coin=data.get("coin", coin),
                            price=float(data.get("price", 0)),
                            size=float(data.get("size", 0)),
                            side=data.get("side", "B"),
                            is_trade=True
                        )
                        yield tick
                    except Exception as e:
                        logger.error(f"Error parsing tick: {e}")
        finally:
            await pubsub.unsubscribe(f"trades:{coin}")
            await pubsub.close()
    
    async def stream_l2(self, coin: str) -> AsyncGenerator[L2Update, None]:
        """
        Stream real-time L2 updates from Redis channel.
        Subscribes to l2:{coin} channel and yields L2 updates.
        """
        if not self._redis:
            logger.warning("No Redis connection, cannot stream L2")
            return
        
        pubsub = self._redis.pubsub()
        await pubsub.subscribe(f"l2:{coin}")
        
        try:
            async for message in pubsub.listen():
                if message["type"] == "message":
                    try:
                        data = json.loads(message["data"])
                        l2 = L2Update(
                            timestamp=data.get("timestamp", 0),
                            coin=data.get("coin", coin),
                            bids=data.get("bids", []),
                            asks=data.get("asks", []),
                            top_n=data.get("top_n", 5)
                        )
                        yield l2
                    except Exception as e:
                        logger.error(f"Error parsing L2: {e}")
        finally:
            await pubsub.unsubscribe(f"l2:{coin}")
            await pubsub.close()
    
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
