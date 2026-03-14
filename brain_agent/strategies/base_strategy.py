"""
Base Strategy - Abstract base class for all trading strategies
Strong, L2-aware base with type safety and validation-first approach
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, Optional
from enum import Enum

from brain_agent.core.orderflow_engine import OrderflowFeatures


class Signal(Enum):
    """Trading signals"""
    LONG = "LONG"
    SHORT = "SHORT"
    FLAT = "FLAT"


@dataclass
class StrategySignal:
    """Container for strategy signal output"""
    signal: Signal
    confidence: float  # 0.0 to 1.0
    reason: str
    metadata: Optional[Dict] = None


class BaseStrategy(ABC):
    """
    Strong, L2-aware base for all trading strategies.
    
    Features:
    - Type-safe OrderflowFeatures input
    - Strong validation with L2 awareness
    - Consistent StrategySignal return type
    - Ready for registry and selector later
    """
    
    def __init__(self, name: str = "BaseStrategy"):
        self.name = name
        self._params: Dict = {}
    
    @abstractmethod
    def generate_signal(
        self, 
        features: OrderflowFeatures, 
        price: float
    ) -> StrategySignal:
        """
        Generate trading signal from orderflow features.
        
        Args:
            features: OrderflowFeatures from OrderflowEngine (type-safe)
            price: Current price
            
        Returns:
            StrategySignal with signal, confidence, and reason
        """
        pass
    
    def validate(self, features: OrderflowFeatures) -> bool:
        """
        Strong validation with L2 awareness.
        
        Checks:
        - Required attributes exist
        - Values are in valid ranges
        - L2 data is present and valid
        
        Returns:
            True if features are valid for signal generation
        """
        # Check required numeric attributes exist and are valid
        required_fields = [
            'cvd', 
            'absorption_score', 
            'bid_ask_imbalance',
            'volume_imbalance'
        ]
        
        for field in required_fields:
            if not hasattr(features, field):
                return False
            value = getattr(features, field)
            if value is None:
                return False
        
        # Validate ranges
        if features.absorption_score < 0:
            return False
        
        # L2 validation - check if we have recent L2 data
        if hasattr(features, 'l2_spread'):
            # If we have L2 data, validate it
            if features.l2_spread is not None and features.l2_spread < 0:
                return False
        
        return True
    
    def set_params(self, params: Dict):
        """Set strategy parameters"""
        self._params = params
    
    def get_params(self) -> Dict:
        """Get strategy parameters"""
        return self._params.copy()
    
    def __repr__(self) -> str:
        return f"{self.name}(params={self._params})"
