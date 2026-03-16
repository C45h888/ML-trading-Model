# Strategies module - trading strategies
from typing import Dict, Type, Optional
from brain_agent.strategies.base_strategy import BaseStrategy
from brain_agent.strategies.absorption import AbsorptionStrategy


# Strategy Registry - maps strategy names to their classes
STRATEGY_REGISTRY: Dict[str, Type[BaseStrategy]] = {
    "absorption": AbsorptionStrategy,
    # Future strategies can be added here:
    # "momentum": MomentumStrategy,
    # "mean_reversion": MeanReversionStrategy,
}


def get_strategy(strategy_name: str, **kwargs) -> Optional[BaseStrategy]:
    """
    Get a strategy instance by name.
    
    Args:
        strategy_name: Name of the strategy (e.g., "absorption")
        **kwargs: Parameters to pass to the strategy constructor
        
    Returns:
        Strategy instance or None if not found
    """
    strategy_class = STRATEGY_REGISTRY.get(strategy_name.lower())
    if strategy_class:
        return strategy_class(**kwargs)
    return None


def list_strategies() -> list:
    """List all available strategy names."""
    return list(STRATEGY_REGISTRY.keys())


__all__ = [
    "BaseStrategy",
    "AbsorptionStrategy", 
    "STRATEGY_REGISTRY",
    "get_strategy",
    "list_strategies",
]
