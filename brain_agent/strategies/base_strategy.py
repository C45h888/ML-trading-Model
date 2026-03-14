"""
Base Strategy - Abstract base class for all trading strategies
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Tuple, Dict, Optional
from enum import Enum


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
    Abstract base class for all trading strategies.
    
    All strategies must implement generate_signal() method.
    """
    
    def __init__(self, name: str = "BaseStrategy"):
        self.name = name
        self._params: Dict = {}
    
    @abstractmethod
    def generate_signal(
        self, 
        features: Dict, 
        price: float
    ) -> StrategySignal:
        """
        Generate trading signal from orderflow features.
        
        Args:
            features: Dict of OrderflowFeatures from OrderflowEngine
            price: Current price
            
        Returns:
            StrategySignal with signal, confidence, and reason
        """
        pass
    
    def set_params(self, params: Dict):
        """Set strategy parameters"""
        self._params = params
    
    def get_params(self) -> Dict:
        """Get strategy parameters"""
        return self._params.copy()
    
    def validate_features(self, features: Dict) -> bool:
        """Validate that required features are present"""
        required = [
            "cvd", 
            "absorption_score", 
            "bid_ask_imbalance",
            "volume_imbalance",
            "delta_divergence"
        ]
        return all(k in features for k in required)
    
    def __repr__(self) -> str:
        return f"{self.name}({self._params})"
