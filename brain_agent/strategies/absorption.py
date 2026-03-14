"""
Absorption Strategy - Detect absorbing liquidity
Trades when large orders are not moving price (absorption detected)

L2-Aware: Uses bid_ask_imbalance and L2 metrics for better signal generation
"""
from typing import Dict
from .base_strategy import BaseStrategy, StrategySignal, Signal
from brain_agent.core.orderflow_engine import OrderflowFeatures


class AbsorptionStrategy(BaseStrategy):
    """
    Absorption trading strategy.
    
    Detects when large market orders are being absorbed by limit orders
    without moving price significantly - indicating potential reversal.
    
    Signal Logic:
    - LONG: Strong absorption + positive CVD (buyers absorbing selling)
    - SHORT: Strong absorption + negative CVD (sellers absorbing buying)
    
    Enhanced with L2 data:
    - Uses bid_ask_imbalance for direction confirmation
    - Validates L2 spread for liquidity
    """
    
    def __init__(
        self,
        min_absorption_score: float = 4.0,
        min_volume: float = 10.0,
        min_trade_count: int = 10,
        confidence_scaling: float = 10.0,
        min_l2_spread: float = 0.0  # Minimum L2 spread to consider valid
    ):
        super().__init__(name="AbsorptionStrategy")
        self.min_absorption_score = min_absorption_score
        self.min_volume = min_volume
        self.min_trade_count = min_trade_count
        self.confidence_scaling = confidence_scaling
        self.min_l2_spread = min_l2_spread
    
    def generate_signal(
        self, 
        features: OrderflowFeatures, 
        price: float
    ) -> StrategySignal:
        """
        Generate absorption-based trading signal using research-backed formula.
        
        Formula:
        - score = (max_level_size / avg_level_size_last_10) / (price_range_last_5 + 1e-6)
        - LONG: score > 4.0 + large buy volume absorbed at best bid + positive CVD
        - SHORT: score > 4.0 + large sell volume absorbed at best ask + negative CVD
        - Confidence: min(score / 8.0 + 0.2 * cvd_strength, 0.95)
        
        Args:
            features: OrderflowFeatures from OrderflowEngine (type-safe)
            price: Current price
            
        Returns:
            StrategySignal with direction and confidence
        """
        # Use base class validation first
        if not self.validate(features):
            return StrategySignal(
                signal=Signal.FLAT,
                confidence=0.0,
                reason="Invalid features"
            )
        
        # Extract features using attribute access (type-safe)
        absorption_score = features.absorption_score
        cvd = features.cvd
        volume_imbalance = features.volume_imbalance
        trade_count = features.trade_count
        bid_ask_imbalance = features.bid_ask_imbalance
        
        # L2-specific features
        l2_spread = getattr(features, 'l2_spread', 0.0)
        l2_best_bid = getattr(features, 'l2_best_bid', 0.0)
        l2_best_ask = getattr(features, 'l2_best_ask', 0.0)
        l2_bid_volume = getattr(features, 'l2_bid_volume', 0.0)
        l2_ask_volume = getattr(features, 'l2_ask_volume', 0.0)
        
        # Validate minimum trade count
        if trade_count < self.min_trade_count:
            return StrategySignal(
                signal=Signal.FLAT,
                confidence=0.0,
                reason=f"Insufficient trade data ({trade_count} < {self.min_trade_count})"
            )
        
        # Research-backed absorption score calculation
        # score = (max_level_size / avg_level_size_last_10) / (price_range_last_5 + 1e-6)
        # Using existing absorption_score as base
        if l2_bid_volume > 0 and l2_ask_volume > 0:
            max_level = max(l2_bid_volume, l2_ask_volume)
            avg_level = (l2_bid_volume + l2_ask_volume) / 2
            price_range = features.high_price - features.low_price if features.high_price > 0 else 0
            
            # Enhanced absorption score
            research_score = (max_level / (avg_level + 1e-6)) / (price_range + 1e-6)
            # Combine with existing score
            absorption_score = (absorption_score + research_score) / 2
        
        # Check absorption threshold
        if absorption_score < self.min_absorption_score:
            return StrategySignal(
                signal=Signal.FLAT,
                confidence=0.0,
                reason=f"Absorption score {absorption_score:.2f} below threshold {self.min_absorption_score}"
            )
        
        # Check volume threshold
        abs_cvd = abs(cvd)
        if abs_cvd < self.min_volume:
            return StrategySignal(
                signal=Signal.FLAT,
                confidence=0.0,
                reason=f"CVD {abs_cvd:.2f} below minimum volume {self.min_volume}"
            )
        
        # Validate L2 spread if we have L2 data
        if l2_spread > 0 and l2_spread < self.min_l2_spread:
            return StrategySignal(
                signal=Signal.FLAT,
                confidence=0.0,
                reason=f"L2 spread {l2_spread:.2f} too tight (min: {self.min_l2_spread})"
            )
        
        # Generate signal based on absorption + CVD direction + L2 imbalance
        # Use both volume_imbalance and bid_ask_imbalance for confirmation
        combined_imbalance = (volume_imbalance + bid_ask_imbalance) / 2
        
        # Calculate CVD strength for confidence
        cvd_strength = min(abs(cvd) / 1000, 1.0)  # Normalize to 0-1
        
        # Research-backed confidence: min(score / 8.0 + 0.2 * cvd_strength, 0.95)
        base_confidence = absorption_score / 8.0 + 0.2 * cvd_strength
        confidence = min(base_confidence, 0.95)
        
        if cvd > 0 and combined_imbalance > 0:
            # Buying pressure being absorbed = potential bullish reversal
            direction = Signal.LONG
            reason = (
                f"Absorption at bid (score={absorption_score:.2f}, CVD={cvd:+.1f}) | "
                f"Imb={combined_imbalance:+.2f} | L2_spread={l2_spread:.2f}"
            )
            
        elif cvd < 0 and combined_imbalance < 0:
            # Selling pressure being absorbed = potential bearish reversal
            direction = Signal.SHORT
            reason = (
                f"Absorption at ask (score={absorption_score:.2f}, CVD={cvd:+.1f}) | "
                f"Imb={combined_imbalance:+.2f} | L2_spread={l2_spread:.2f}"
            )
        
        else:
            # Absorption but unclear direction
            return StrategySignal(
                signal=Signal.FLAT,
                confidence=0.0,
                reason=f"Absorption without clear direction (CVD={cvd:+.1f}, imbalance={combined_imbalance:+.2f})"
            )
        
        return StrategySignal(
            signal=direction,
            confidence=confidence,
            reason=reason,
            metadata={
                "absorption_score": absorption_score,
                "cvd": cvd,
                "volume_imbalance": volume_imbalance,
                "bid_ask_imbalance": bid_ask_imbalance,
                "trade_count": trade_count,
                "l2_spread": l2_spread,
                "l2_best_bid": l2_best_bid,
                "l2_best_ask": l2_best_ask,
                "cvd_strength": cvd_strength
            }
        )
    
    def __repr__(self) -> str:
        return (
            f"AbsorptionStrategy("
            f"min_absorption={self.min_absorption_score}, "
            f"min_volume={self.min_volume}, "
            f"min_l2_spread={self.min_l2_spread})"
        )
