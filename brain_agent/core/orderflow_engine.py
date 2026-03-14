"""
Orderflow Engine - Centralized feature calculation
Features used by ALL strategies: CVD, absorption, imbalance, delta, volume profile
"""
from collections import deque
from dataclasses import dataclass, field
from typing import Dict, List, Optional
import pandas as pd
import numpy as np


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
    

class OrderflowEngine:
    """
    Centralized orderflow feature calculation.
    Used by all strategies for consistent feature set.
    """
    
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
        """Process L2 orderbook data"""
        bids = l2_data.get("bids", [])
        asks = l2_data.get("asks", [])
        
        if not bids or not asks:
            return self._calculate_features()
        
        # Calculate bid/ask imbalance
        bid_volume = sum(size for _, size in bids[:10])
        ask_volume = sum(size for _, size in asks[:10])
        
        total_volume = bid_volume + ask_volume
        if total_volume > 0:
            self.l2_buffer.append({
                "bid_volume": bid_volume,
                "ask_volume": ask_volume,
                "imbalance": (bid_volume - ask_volume) / total_volume
            })
        
        return self._calculate_features()
    
    def _calculate_features(self) -> OrderflowFeatures:
        """Calculate all orderflow features from current state"""
        
        # Convert buffer to DataFrame for calculations
        if len(self.trades_buffer) == 0:
            return OrderflowFeatures()
        
        df = pd.DataFrame(self.trades_buffer)
        
        features = OrderflowFeatures(
            cvd=self.cvd,
            total_buy_volume=self.total_buy_volume,
            total_sell_volume=self.total_sell_volume,
            trade_count=self.trade_count,
            high_price=self.high_price,
            low_price=self.low_price if self.low_price != float('inf') else 0.0,
            close_price=self.last_price
        )
        
        # Calculate absorption score
        features.absorption_score = self._calculate_absorption(df)
        features.absorption_count = len(self.absorption_events)
        
        # Calculate volume imbalance
        if self.total_buy_volume + self.total_sell_volume > 0:
            features.volume_imbalance = (
                self.total_buy_volume - self.total_sell_volume
            ) / (self.total_buy_volume + self.total_sell_volume)
        
        # Calculate L2 imbalance (from most recent)
        if len(self.l2_buffer) > 0:
            latest_l2 = self.l2_buffer[-1]
            features.bid_ask_imbalance = latest_l2.get("imbalance", 0.0)
        
        # Calculate delta divergence
        features.delta_divergence = self._calculate_delta_divergence(df)
        
        # Calculate volume profile POC
        features.volume_profile_poc = self._calculate_volume_profile_poc(df)
        
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
        """Get current state snapshot"""
        features = self._calculate_features()
        return {
            "cvd": features.cvd,
            "absorption_score": features.absorption_score,
            "bid_ask_imbalance": features.bid_ask_imbalance,
            "volume_imbalance": features.volume_imbalance,
            "delta_divergence": features.delta_divergence,
            "volume_profile_poc": features.volume_profile_poc,
            "trade_count": features.trade_count,
            "high": features.high_price,
            "low": features.low_price,
            "close": features.close_price
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
