"""
Absorption Strategy - Detect absorbing liquidity
Trades when large orders are not moving price (absorption detected)
"""
from typing import Dict, Tuple
from .base_strategy import BaseStrategy, StrategySignal, Signal


class AbsorptionStrategy(BaseStrategy):
    """
    Absorption trading strategy.
    
    Detects when large market orders are being absorbed by limit orders
    without moving price significantly - indicating potential reversal.
    
    Signal Logic:
    - LONG: Strong absorption + positive CVD (buyers absorbing selling)
    - SHORT: Strong absorption + negative CVD (sellers absorbing buying)
    """
    
    def __init__(
        self,
        min_absorption_score: float = 4.0,
        min_volume: float = 10.0,
        min_trade_count: int = 10,
        confidence_scaling: float = 10.0
    ):
        super().__init__(name="AbsorptionStrategy")
        self.min_absorption_score = min_absorption_score
        self.min_volume = min_volume
        self.min_trade_count = min_trade_count
        self.confidence_scaling = confidence_scaling
    
    def generate_signal(
        self, 
        features: Dict, 
        price: float
    ) -> StrategySignal:
        """
        Generate absorption-based trading signal.
        
        Args:
            features: OrderflowFeatures from OrderflowEngine
            price: Current price
            
        Returns:
            StrategySignal with direction and confidence
        """
        # Extract features
        absorption_score = features.get("absorption_score", 0.0)
        cvd = features.get("cvd", 0.0)
        volume_imbalance = features.get("volume_imbalance", 0.0)
        trade_count = features.get("trade_count", 0)
        
        # Validate minimum trade count
        if trade_count < self.min_trade_count:
            return StrategySignal(
                signal=Signal.FLAT,
                confidence=0.0,
                reason="Insufficient trade data"
            )
        
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
        
        # Generate signal based on absorption + CVD direction
        if cvd > 0 and volume_imbalance > 0:
            # Buying pressure being absorbed = potential bullish reversal
            direction = Signal.LONG
            confidence = min(absorption_score / self.confidence_scaling, 0.95)
            reason = (
                f"🐂 Absorption detected (score={absorption_score:.2f}) | "
                f"CVD={cvd:.2f} | Bullish absorption"
            )
            
        elif cvd < 0 and volume_imbalance < 0:
            # Selling pressure being absorbed = potential bearish reversal
            direction = Signal.SHORT
            confidence = min(absorption_score / self.confidence_scaling, 0.95)
            reason = (
                f"🐻 Absorption detected (score={absorption_score:.2f}) | "
                f"CVD={cvd:.2f} | Bearish absorption"
            )
        
        else:
            # Absorption but unclear direction
            return StrategySignal(
                signal=Signal.FLAT,
                confidence=0.0,
                reason=f"Absorption without clear direction (CVD={cvd:.2f}, imbalance={volume_imbalance:.2f})"
            )
        
        return StrategySignal(
            signal=direction,
            confidence=confidence,
            reason=reason,
            metadata={
                "absorption_score": absorption_score,
                "cvd": cvd,
                "volume_imbalance": volume_imbalance,
                "trade_count": trade_count
            }
        )
    
    def __repr__(self) -> str:
        return (
            f"AbsorptionStrategy("
            f"min_absorption={self.min_absorption_score}, "
            f"min_volume={self.min_volume})"
        )
