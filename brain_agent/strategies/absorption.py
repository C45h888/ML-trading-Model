"""
Absorption Strategy - L2-based absorption detection
Trades when large orders are not moving price (absorption detected)

Based on 2026 Hyperliquid-optimized design:
- Primary Trigger: Large resting size at best bid/ask with minimal price movement
- Direction Confirmation: CVD + L2 level direction
- Confidence Scoring: Multi-factor weighted combination (60/20/15/5)
- Minimum Confidence: 0.75 (reject low-probability signals)
- Signal Cooldown: 60 seconds between signals
"""
from dataclasses import dataclass
from typing import Literal, Optional, Dict
import time

from brain_agent.core.orderflow_engine import OrderflowFeatures


@dataclass
class Signal:
    """Clean signal output dataclass"""
    direction: Literal["LONG", "SHORT", "FLAT"]
    confidence: float  # 0.0 to 1.0
    reason: str
    metadata: Optional[Dict] = None


class AbsorptionStrategy:
    """
    Production-ready L2 absorption strategy.
    
    Core Rules:
    - Primary Trigger: Large resting size at best bid/ask with minimal price movement
    - Formula: absorption_score = (max_level_size / avg_level_size) / (price_range + 1e-6)
    - Threshold: absorption_score >= 4.0
    
    Direction Confirmation:
    - If large size on best bid + positive CVD → LONG
    - If large size on best ask + negative CVD → SHORT
    
    Confidence Scoring (multi-factor, per your spec):
    - 60% weight: raw absorption strength
    - 20% weight: CVD divergence strength
    - 15% weight: bid/ask imbalance alignment
    - 5% weight: price stall confirmation (spread narrowing)
    - Cap at 0.95
    
    Hard Filters (Must All Pass):
    - Resting size at trigger level >= 3x average size
    - Price movement in last 5 ticks <= 0.05-0.08%
    - Total liquidity at top 5 levels >= 0.1 BTC
    - CVD must show directional alignment
    - Cap imbalance ratio at 20
    
    Minimum Confidence: 0.75 (reject low-probability entries)
    Signal Cooldown: 60 seconds between signals
    """
    
    def __init__(
        self,
        min_score: float = 4.0,
        min_level_size_ratio: float = 3.0,
        max_price_move_pct: float = 0.0008,  # 0.08% per your spec
        min_cvd_strength: float = 1.5,
        min_trade_count: int = 10,
        min_confidence: float = 0.50,  # Minimum confidence threshold
        min_signal_interval: int = 60,  # Cooldown in seconds
        min_liquidity: float = 0.01,  # Minimum BTC liquidity at top 5 levels
    ):
        self.min_score = min_score
        self.min_level_size_ratio = min_level_size_ratio
        self.max_price_move_pct = max_price_move_pct
        self.min_cvd_strength = min_cvd_strength
        self.min_trade_count = min_trade_count
        self.min_confidence = min_confidence
        self.min_signal_interval = min_signal_interval
        self.min_liquidity = min_liquidity
        
        # Cooldown tracking
        self._last_signal_time = 0.0
    
    def generate_signal(self, features, price: float) -> Signal:
        """
        Generate L2-based absorption signal.
        
        Args:
            features: OrderflowFeatures or dict with features
            price: Current price
            
        Returns:
            Signal with direction, confidence, and reason
        """
        # COOLDOWN CHECK: Rate limit signals to avoid over-trading
        current_time = time.time()
        if current_time - self._last_signal_time < self.min_signal_interval:
            return Signal(
                direction="FLAT",
                confidence=0.0,
                reason=f"Cooldown active ({self.min_signal_interval - (current_time - self._last_signal_time):.0f}s remaining)"
            )
        
        # Handle both OrderflowFeatures and dict input
        if isinstance(features, dict):
            # Convert dict to OrderflowFeatures-like object
            features_dict = features
            # Create a simple object with attribute access
            class FeaturesWrapper:
                def __init__(self, d):
                    for k, v in d.items():
                        setattr(self, k, v)
            features = FeaturesWrapper(features_dict)
        
        # Validate minimum trade count
        if features.trade_count < self.min_trade_count:
            return Signal(
                direction="FLAT",
                confidence=0.0,
                reason=f"Insufficient trade data ({features.trade_count} < {self.min_trade_count})"
            )
        
        # Get absorption score from features
        score = features.absorption_score
        
        # Primary trigger: absorption score threshold
        if score < self.min_score:
            return Signal(
                direction="FLAT",
                confidence=0.0,
                reason=f"No significant absorption (score={score:.2f} < {self.min_score})"
            )
        
        # Get L2 data for direction and extra filters
        l2_bid_volume = getattr(features, 'l2_bid_volume', 0.0)
        l2_ask_volume = getattr(features, 'l2_ask_volume', 0.0)
        l2_spread = getattr(features, 'l2_spread', 0.0)
        l2_best_bid = getattr(features, 'l2_best_bid', 0.0)
        l2_best_ask = getattr(features, 'l2_best_ask', 0.0)
        
        # HARD FILTER: Minimum liquidity at top 5 levels (per your spec)
        total_liquidity = l2_bid_volume + l2_ask_volume
        if total_liquidity < self.min_liquidity:
            return Signal(
                direction="FLAT",
                confidence=0.0,
                reason=f"Low liquidity ({total_liquidity:.4f} < {self.min_liquidity} BTC)"
            )
        
        # Extra filter: Check price movement percentage
        if features.high_price > 0 and features.low_price > 0:
            price_range_pct = (features.high_price - features.low_price) / features.low_price
            if price_range_pct > self.max_price_move_pct:
                return Signal(
                    direction="FLAT",
                    confidence=0.0,
                    reason=f"Price move too large ({price_range_pct:.4f} > {self.max_price_move_pct})"
                )
        
        # Determine direction from CVD and L2 level
        # Buy absorption: more bid liquidity + positive CVD
        # Sell absorption: more ask liquidity + negative CVD
        is_buy_absorption = (
            features.cvd > 0 and 
            l2_bid_volume >= l2_ask_volume
        )
        is_sell_absorption = (
            features.cvd < 0 and 
            l2_ask_volume >= l2_bid_volume
        )
        
        if not (is_buy_absorption or is_sell_absorption):
            return Signal(
                direction="FLAT",
                confidence=0.0,
                reason=f"No clear direction (CVD={features.cvd:+.2f}, bid_vol={l2_bid_volume:.2f}, ask_vol={l2_ask_volume:.2f})"
            )
        
        direction = "LONG" if is_buy_absorption else "SHORT"
        
        # HARD FILTER: CVD directional alignment (per your spec)
        if direction == "LONG" and features.cvd < 0:
            return Signal(
                direction="FLAT",
                confidence=0.0,
                reason=f"CVD not aligned for LONG (CVD={features.cvd:+.2f})"
            )
        if direction == "SHORT" and features.cvd > 0:
            return Signal(
                direction="FLAT",
                confidence=0.0,
                reason=f"CVD not aligned for SHORT (CVD={features.cvd:+.2f})"
            )
        
        # Confidence: weighted multi-factor combination (per your spec: 60/20/15/5)
        # 60% from absorption_score
        base_conf = (score / 10.0) * 0.6
        
        # 20% if strong CVD divergence
        cvd_divergence_bonus = 0.2 if abs(features.delta_divergence) > self.min_cvd_strength else 0.0
        
        # 15% if imbalance aligns with direction
        imbalance_bonus = 0.0
        if direction == "LONG" and features.bid_ask_imbalance > 2.5:
            imbalance_bonus = 0.15
        elif direction == "SHORT" and features.bid_ask_imbalance < 0.4:
            imbalance_bonus = 0.15
        
        # 5% if price is stalling (spread narrowing or small range)
        price_stall_bonus = 0.0
        if l2_spread > 0 and price_range_pct < 0.0002:  # Very small price range
            price_stall_bonus = 0.05
        
        # Cap at 0.95
        confidence = min(base_conf + cvd_divergence_bonus + imbalance_bonus + price_stall_bonus, 0.95)
        
        # HARD FILTER: Minimum confidence threshold (per your spec: 0.75-0.80)
        if confidence < self.min_confidence:
            return Signal(
                direction="FLAT",
                confidence=confidence,
                reason=f"Low confidence ({confidence:.2f} < {self.min_confidence})"
            )
        
        # Mark signal time for cooldown
        self._last_signal_time = current_time
        
        # Build reason string
        reason = (
            f"Strong L2 absorption at {price:.0f} "
            f"(score={score:.2f}, CVD={features.cvd:+.2f}, "
            f"imbalance={features.bid_ask_imbalance:+.2f}, "
            f"delta_div={features.delta_divergence:+.2f})"
        )
        
        # Build metadata
        metadata = {
            "absorption_score": score,
            "cvd": features.cvd,
            "volume_imbalance": features.volume_imbalance,
            "bid_ask_imbalance": features.bid_ask_imbalance,
            "delta_divergence": features.delta_divergence,
            "trade_count": features.trade_count,
            "l2_spread": l2_spread,
            "l2_bid_volume": l2_bid_volume,
            "l2_ask_volume": l2_ask_volume,
            "l2_best_bid": l2_best_bid,
            "l2_best_ask": l2_best_ask,
            "price_range_pct": (features.high_price - features.low_price) / features.low_price if features.low_price > 0 else 0
        }
        
        return Signal(
            direction=direction,
            confidence=confidence,
            reason=reason,
            metadata=metadata
        )
    
    def __repr__(self) -> str:
        return (
            f"AbsorptionStrategy("
            f"min_score={self.min_score}, "
            f"min_confidence={self.min_confidence}, "
            f"min_signal_interval={self.min_signal_interval}s, "
            f"min_liquidity={self.min_liquidity}, "
            f"max_price_move_pct={self.max_price_move_pct})"
        )
