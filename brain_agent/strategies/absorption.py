"""
Absorption Strategy - L2-based absorption detection
Trades when large orders are not moving price (absorption detected)

Based on 2026 Hyperliquid-optimized design:
- Primary Trigger: Large resting size at best bid/ask with minimal price movement
- Direction Confirmation: CVD + L2 level direction
- Confidence Scoring: Multi-factor weighted combination
"""
from dataclasses import dataclass
from typing import Literal, Optional, Dict

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
    
    Confidence Scoring (multi-factor):
    - Base: 60% from absorption_score
    - +20% if strong CVD divergence (abs(delta_divergence) > 1.5)
    - +20% if imbalance aligns
    - Cap at 0.95
    """
    
    def __init__(
        self,
        min_score: float = 4.0,
        min_level_size_ratio: float = 3.0,
        max_price_move_pct: float = 0.0005,  # 0.05%
        min_cvd_strength: float = 1.5,
        min_trade_count: int = 10
    ):
        self.min_score = min_score
        self.min_level_size_ratio = min_level_size_ratio
        self.max_price_move_pct = max_price_move_pct
        self.min_cvd_strength = min_cvd_strength
        self.min_trade_count = min_trade_count
    
    def generate_signal(self, features, price: float) -> Signal:
        """
        Generate L2-based absorption signal.
        
        Args:
            features: OrderflowFeatures or dict with features
            price: Current price
            
        Returns:
            Signal with direction, confidence, and reason
        """
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
        
        # Confidence: weighted multi-factor combination
        # Base: 60% from absorption_score
        base_conf = (score / 10.0) * 0.6
        
        # +20% if strong CVD divergence
        cvd_divergence_bonus = 0.2 if abs(features.delta_divergence) > self.min_cvd_strength else 0.0
        
        # +20% if imbalance aligns
        imbalance_bonus = 0.0
        if direction == "LONG" and features.bid_ask_imbalance > 2.5:
            imbalance_bonus = 0.2
        elif direction == "SHORT" and features.bid_ask_imbalance < 0.4:
            imbalance_bonus = 0.2
        
        # Cap at 0.95
        confidence = min(base_conf + cvd_divergence_bonus + imbalance_bonus, 0.95)
        
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
            f"min_level_size_ratio={self.min_level_size_ratio}, "
            f"max_price_move_pct={self.max_price_move_pct})"
        )
