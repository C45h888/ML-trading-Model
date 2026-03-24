"""
Orderflow Engine - Centralized feature calculation
Features used by ALL strategies: CVD, absorption, imbalance, delta, volume profile

Supports both trade events and L2 orderbook updates.
"""
from collections import deque
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Union
import logging
import pandas as pd
import numpy as np

from brain_agent.core.models import TradeTick, L2Update, Event, EventType

logger = logging.getLogger(__name__)


@dataclass
class OrderflowFeatures:
    """Container for calculated orderflow features"""
    # Cumulative Volume Delta
    cvd: float = 0.0
    
    # Absorption metrics
    absorption_score: float = 0.0
    absorption_count: int = 0
    
    # Imbalance metrics
    bid_ask_imbalance: float = 0.0
    volume_imbalance: float = 0.0
    
    # Delta divergence
    delta_divergence: float = 0.0
    
    # Volume profile
    volume_profile_poc: float = 0.0  # Point of Control
    volume_profile_va: float = 0.0   # Value Area
    
    # Trade statistics
    total_buy_volume: float = 0.0
    total_sell_volume: float = 0.0
    trade_count: int = 0
    
    # Price statistics
    high_price: float = 0.0
    low_price: float = 0.0
    close_price: float = 0.0
    
    # L2-specific metrics
    l2_bid_volume: float = 0.0
    l2_ask_volume: float = 0.0
    l2_spread: float = 0.0
    l2_best_bid: float = 0.0
    l2_best_ask: float = 0.0
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for strategies"""
        return {
            "cvd": self.cvd,
            "absorption_score": self.absorption_score,
            "absorption_count": self.absorption_count,
            "bid_ask_imbalance": self.bid_ask_imbalance,
            "volume_imbalance": self.volume_imbalance,
            "delta_divergence": self.delta_divergence,
            "volume_profile_poc": self.volume_profile_poc,
            "volume_profile_va": self.volume_profile_va,
            "total_buy_volume": self.total_buy_volume,
            "total_sell_volume": self.total_sell_volume,
            "trade_count": self.trade_count,
            "high_price": self.high_price,
            "low_price": self.low_price,
            "close_price": self.close_price,
            "l2_bid_volume": self.l2_bid_volume,
            "l2_ask_volume": self.l2_ask_volume,
            "l2_spread": self.l2_spread,
            "l2_best_bid": self.l2_best_bid,
            "l2_best_ask": self.l2_best_ask
        }
    

class OrderflowEngine:
    """
    Centralized orderflow feature calculation.
    Used by all strategies for consistent feature set.
    """
    
    # Constants
    MAX_IMBALANCE_RATIO = 20.0  # Cap to avoid extreme values from near-zero division
    
    def __init__(
        self, 
        window: int = 500,
        absorption_threshold: float = 3.0,
        absorption_window: int = 5
    ):
        self.window = window
        self.absorption_threshold = absorption_threshold
        self.absorption_window = absorption_window
        
        # State
        self.cvd = 0.0
        self.trades_buffer: deque = deque(maxlen=window)
        self.l2_buffer: deque = deque(maxlen=100)
        
        # Running totals
        self.total_buy_volume = 0.0
        self.total_sell_volume = 0.0
        self.trade_count = 0
        
        # Price tracking
        self.high_price = 0.0
        self.low_price = float('inf')
        self.last_price = 0.0
        
        # Absorption tracking
        self.absorption_events: deque = deque(maxlen=50)
    
    def process_tick(self, tick: Dict) -> OrderflowFeatures:
        """
        Process a single tick and return calculated features.
        
        Args:
            tick: Dict with keys: timestamp, coin, price, size, side
            
        Returns:
            OrderflowFeatures with all calculated metrics
        """
        price = float(tick["price"])
        size = float(tick["size"])
        side = tick["side"]
        
        # Determine side multiplier (+1 for buy, -1 for sell)
        side_mult = 1 if side.upper() in ["B", "BUY", "LONG"] else -1
        
        # Update CVD
        self.cvd += side_mult * size
        
        # Update totals
        if side_mult > 0:
            self.total_buy_volume += size
        else:
            self.total_sell_volume += size
        
        self.trade_count += 1
        
        # Update price tracking
        self.last_price = price
        if price > self.high_price:
            self.high_price = price
        if price < self.low_price:
            self.low_price = price
        
        # Add to buffer
        self.trades_buffer.append({
            "timestamp": tick.get("timestamp", 0),
            "price": price,
            "size": size,
            "side": side,
            "side_mult": side_mult
        })
        
        # Calculate features
        features = self._calculate_features()
        
        return features
    
    def process_l2(self, l2_data: Dict) -> OrderflowFeatures:
        """
        Process L2 orderbook data with improved calculations.
        
        Calculates:
        - bid_ask_imbalance = sum(bid_sz) / sum(ask_sz) over top 5
        - absorption using max size at best bid/ask with small spread change
        """
        bids = l2_data.get("bids", [])
        asks = l2_data.get("asks", [])
        
        if not bids or not asks:
            return self._calculate_features()
        
        # Parse bids and asks (handle both tuple and dict formats)
        parsed_bids = []
        parsed_asks = []
        
        for bid in bids[:10]:
            if isinstance(bid, dict):
                px = float(bid.get("px", "0"))  # px is decimal USD string e.g. '71760.0'
                sz = float(bid.get("sz", "0"))
            else:
                px, sz = float(bid[0]), float(bid[1])
            parsed_bids.append((px, sz))

        for ask in asks[:10]:
            if isinstance(ask, dict):
                px = float(ask.get("px", "0"))
                sz = float(ask.get("sz", "0"))
            else:
                px, sz = float(ask[0]), float(ask[1])
            parsed_asks.append((px, sz))
        
        if not parsed_bids or not parsed_asks:
            return self._calculate_features()
        
        # Calculate volumes over top 5 levels
        bid_volume = sum(sz for _, sz in parsed_bids[:5])
        ask_volume = sum(sz for _, sz in parsed_asks[:5])
        
        total_volume = bid_volume + ask_volume
        
        # Calculate bid_ask_imbalance = sum(bid_sz) / sum(ask_sz) over top N
        # Cap at MAX_IMBALANCE_RATIO to avoid extreme values from near-zero ask_volume
        if ask_volume > 0:
            imbalance_ratio = bid_volume / ask_volume
            imbalance_ratio = min(imbalance_ratio, self.MAX_IMBALANCE_RATIO)
        else:
            imbalance_ratio = 1.0
        
        if total_volume > 0:
            self.l2_buffer.append({
                "bid_volume": bid_volume,
                "ask_volume": ask_volume,
                "imbalance": (bid_volume - ask_volume) / total_volume,
                "imbalance_ratio": imbalance_ratio,
                "best_bid": parsed_bids[0][0] if parsed_bids else 0,
                "best_ask": parsed_asks[0][0] if parsed_asks else 0,
                "spread": parsed_asks[0][0] - parsed_bids[0][0] if parsed_bids and parsed_asks else 0
            })
        
        # Calculate absorption using L2 levels
        # Large size at best bid/ask with small spread = absorption
        self._calculate_l2_absorption(parsed_bids, parsed_asks)
        
        return self._calculate_features()
    
    def _calculate_l2_absorption(self, bids: List, asks: List) -> None:
        """
        Calculate absorption from L2 data.
        Large orders at best bid/ask with small spread changes indicate absorption.
        """
        if not bids or not asks:
            return
        
        best_bid_px, best_bid_sz = bids[0]
        best_ask_px, best_ask_sz = asks[0]
        
        spread = best_ask_px - best_bid_px
        
        if spread > 0 and self.last_price > 0:
            # Check if price is near best levels
            price_near_bid = self.last_price <= best_bid_px * 1.001  # Within 0.1%
            price_near_ask = self.last_price >= best_ask_px * 0.999  # Within 0.1%
            
            if price_near_bid or price_near_ask:
                # Large size at best level = potential absorption
                max_size = max(best_bid_sz, best_ask_sz)
                avg_size = sum(sz for _, sz in bids[:3] + asks[:3]) / 6
                
                if avg_size > 0:
                    l2_absorption = (max_size / avg_size) * (1 / (spread + 0.0001))
                    
                    if l2_absorption > self.absorption_threshold:
                        self.absorption_events.append({
                            "type": "L2",
                            "score": l2_absorption,
                            "spread": spread
                        })
    
    def process_event(self, event: Event) -> OrderflowFeatures:
        """
        Process a unified Event (trade or L2 update).
        
        Args:
            event: Event object with event_type and data
            
        Returns:
            OrderflowFeatures with all calculated metrics
        """
        try:
            if event.event_type == EventType.TRADE:
                # Process as trade
                tick = event.data
                tick_dict = {
                    "timestamp": tick.timestamp,
                    "coin": tick.coin,
                    "price": tick.price,
                    "size": tick.size,
                    "side": tick.side
                }
                return self.process_tick(tick_dict)
                
            elif event.event_type == EventType.L2_UPDATE:
                # Process as L2 update — px is already decimal USD string, no /1e6
                l2 = event.data
                l2_dict = {
                    "bids": [(float(b.get("px", "0")), float(b.get("sz", "0"))) for b in l2.bids],
                    "asks": [(float(a.get("px", "0")), float(a.get("sz", "0"))) for a in l2.asks]
                }
                return self.process_l2(l2_dict)
            
            else:
                logger.warning(f"Unknown event type: {event.event_type}")
                return self._calculate_features()
                
        except Exception as e:
            logger.error(f"Error processing event: {e}")
            return self._calculate_features()
    
    def _calculate_features(self) -> OrderflowFeatures:
        """Calculate all orderflow features from current state"""
        
        # Convert buffer to DataFrame for calculations
        if len(self.trades_buffer) == 0 and len(self.l2_buffer) == 0:
            return OrderflowFeatures()
        
        # Create features with current state
        features = OrderflowFeatures(
            cvd=self.cvd,
            total_buy_volume=self.total_buy_volume,
            total_sell_volume=self.total_sell_volume,
            trade_count=self.trade_count,
            high_price=self.high_price,
            low_price=self.low_price if self.low_price != float('inf') else 0.0,
            close_price=self.last_price
        )
        
        # Calculate absorption from trades
        trade_absorption = 0.0
        if len(self.trades_buffer) > 0:
            try:
                df = pd.DataFrame(self.trades_buffer)
                trade_absorption = self._calculate_absorption(df)
            except Exception as e:
                logger.warning(f"Error calculating trade absorption: {e}")
        
        # Calculate L2-based absorption (if we have recent L2 data)
        l2_absorption = 0.0
        if len(self.absorption_events) > 0:
            # Get the most recent absorption event
            recent_events = [e for e in self.absorption_events if e.get("type") == "L2"]
            if recent_events:
                l2_absorption = recent_events[-1].get("score", 0.0)
        
        # Combine both absorption scores (favor the higher one)
        features.absorption_score = max(trade_absorption, l2_absorption)
        
        features.absorption_count = len(self.absorption_events)
        
        # Calculate volume imbalance
        if self.total_buy_volume + self.total_sell_volume > 0:
            features.volume_imbalance = (
                self.total_buy_volume - self.total_sell_volume
            ) / (self.total_buy_volume + self.total_sell_volume)
        
        # Calculate L2 imbalance (from most recent)
        if len(self.l2_buffer) > 0:
            try:
                latest_l2 = self.l2_buffer[-1]
                # Use ratio (bid/ask) so strategy thresholds like >2.5 are meaningful
                features.bid_ask_imbalance = latest_l2.get("imbalance_ratio", 1.0)
                features.l2_bid_volume = latest_l2.get("bid_volume", 0.0)
                features.l2_ask_volume = latest_l2.get("ask_volume", 0.0)
                features.l2_spread = latest_l2.get("spread", 0.0)
                features.l2_best_bid = latest_l2.get("best_bid", 0.0)
                features.l2_best_ask = latest_l2.get("best_ask", 0.0)
            except Exception as e:
                logger.warning(f"Error getting L2 features: {e}")
        
        # Calculate delta divergence
        if len(self.trades_buffer) > 0:
            try:
                df = pd.DataFrame(self.trades_buffer)
                features.delta_divergence = self._calculate_delta_divergence(df)
            except Exception as e:
                logger.warning(f"Error calculating delta divergence: {e}")
        
        # Calculate volume profile POC
        if len(self.trades_buffer) > 0:
            try:
                df = pd.DataFrame(self.trades_buffer)
                features.volume_profile_poc = self._calculate_volume_profile_poc(df)
            except Exception as e:
                logger.warning(f"Error calculating volume profile POC: {e}")
        
        return features
    
    def _calculate_absorption(self, df: pd.DataFrame) -> float:
        """
        Calculate absorption score.
        Higher score = more absorption detected (large orders not moving price)
        """
        if len(df) < self.absorption_window:
            return 0.0
        
        # Get recent trades
        recent = df.iloc[-self.absorption_window:]
        
        # Calculate average volume
        avg_vol = recent["size"].mean()
        if avg_vol == 0:
            return 0.0
        
        max_vol = recent["size"].max()
        
        # Calculate price movement
        price_range = recent["price"].max() - recent["price"].min()
        
        if price_range == 0:
            # No price movement despite volume = strong absorption
            absorption = (max_vol / avg_vol) * 2
        else:
            # High volume / low price movement = absorption
            absorption = (max_vol / avg_vol) * (1 / (price_range + 0.0001))
        
        # Track absorption events
        if absorption > self.absorption_threshold:
            self.absorption_events.append({
                "timestamp": recent.iloc[-1].get("timestamp", 0),
                "score": absorption
            })
        
        return min(absorption, 10.0)  # Cap at 10
    
    def _calculate_delta_divergence(self, df: pd.DataFrame) -> float:
        """
        Calculate delta divergence.
        Positive = buying pressure despite price dropping (bullish divergence)
        Negative = selling pressure despite price rising (bearish divergence)
        """
        if len(df) < 20:
            return 0.0
        
        # Get recent window
        recent = df.iloc[-20:]
        
        # Price change
        price_change = recent["price"].iloc[-1] - recent["price"].iloc[0]
        
        # Delta change (CVD)
        delta_change = recent["side_mult"].sum() * recent["size"].sum()
        
        if abs(price_change) < 0.0001:
            return 0.0
        
        # Divergence = delta / price_change
        divergence = delta_change / (price_change * recent["price"].mean())
        
        return divergence
    
    def _calculate_volume_profile_poc(self, df: pd.DataFrame) -> float:
        """Calculate Point of Control (price level with most volume)"""
        if len(df) == 0:
            return self.last_price
        
        # Bin prices and sum volumes
        if len(df) < 10:
            return df["price"].mean()
        
        # Create price bins
        price_min = df["price"].min()
        price_max = df["price"].max()
        
        if price_max == price_min:
            return price_max
        
        # Use pandas cut for binning
        bins = 20
        df["price_bin"] = pd.cut(
            df["price"], 
            bins=bins, 
            labels=False
        )
        
        # Volume by bin
        volume_by_bin = df.groupby("price_bin")["size"].sum()
        
        if len(volume_by_bin) == 0:
            return df["price"].mean()
        
        # POC = bin with most volume
        poc_bin = volume_by_bin.idxmax()
        
        # Convert bin back to price
        bin_size = (price_max - price_min) / bins
        poc_price = price_min + (poc_bin + 0.5) * bin_size
        
        return poc_price
    
    def get_snapshot(self) -> Dict:
        """Get current state snapshot — includes all fields used by DB logging and strategy."""
        features = self._calculate_features()
        return {
            "cvd": features.cvd,
            "absorption_score": features.absorption_score,
            "bid_ask_imbalance": features.bid_ask_imbalance,
            "volume_imbalance": features.volume_imbalance,
            "delta_divergence": features.delta_divergence,
            "volume_profile_poc": features.volume_profile_poc,
            "trade_count": features.trade_count,
            "total_buy_volume": features.total_buy_volume,
            "total_sell_volume": features.total_sell_volume,
            "l2_bid_volume": features.l2_bid_volume,
            "l2_ask_volume": features.l2_ask_volume,
            "l2_spread": features.l2_spread,
            "high": features.high_price,
            "low": features.low_price,
            "close": features.close_price,
        }
    
    def reset(self):
        """Reset engine state"""
        self.cvd = 0.0
        self.trades_buffer.clear()
        self.l2_buffer.clear()
        self.total_buy_volume = 0.0
        self.total_sell_volume = 0.0
        self.trade_count = 0
        self.high_price = 0.0
        self.low_price = float('inf')
        self.last_price = 0.0
        self.absorption_events.clear()
