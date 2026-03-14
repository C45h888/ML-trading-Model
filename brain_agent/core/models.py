"""
Data Models - Pydantic models for type-safe data handling
"""
from typing import Optional, List, Tuple, Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field, computed_field


class Tick(BaseModel):
    """Normalized tick data from any source"""
    timestamp: int
    coin: str
    price: float
    size: float
    side: str = Field(description="B for buy, S for sell")
    is_trade: bool = True
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return self.model_dump()


class OrderbookSnapshot(BaseModel):
    """L2 orderbook snapshot"""
    timestamp: int
    coin: str
    bids: List[Tuple[float, float]] = Field(default_factory=list, description="List of (price, size)")
    asks: List[Tuple[float, float]] = Field(default_factory=list, description="List of (price, size)")
    
    @computed_field
    @property
    def spread(self) -> float:
        """Calculate bid-ask spread"""
        if not self.bids or not self.asks:
            return 0.0
        best_bid = self.bids[0][0] if self.bids else 0.0
        best_ask = self.asks[0][0] if self.asks else 0.0
        return best_ask - best_bid
    
    @computed_field
    @property
    def mid_price(self) -> float:
        """Calculate mid price"""
        if not self.bids or not self.asks:
            return 0.0
        best_bid = self.bids[0][0] if self.bids else 0.0
        best_ask = self.asks[0][0] if self.asks else 0.0
        return (best_bid + best_ask) / 2
    
    @computed_field
    @property
    def bid_volume(self) -> float:
        """Total bid volume"""
        return sum(size for _, size in self.bids)
    
    @computed_field
    @property
    def ask_volume(self) -> float:
        """Total ask volume"""
        return sum(size for _, size in self.asks)
    
    @computed_field
    @property
    def imbalance(self) -> float:
        """Bid-ask volume imbalance (-1 to 1)"""
        total = self.bid_volume + self.ask_volume
        if total == 0:
            return 0.0
        return (self.bid_volume - self.ask_volume) / total


class OrderflowFeatures(BaseModel):
    """Calculated orderflow features"""
    cvd: float = 0.0
    absorption_score: float = 0.0
    absorption_count: int = 0
    bid_ask_imbalance: float = 0.0
    volume_imbalance: float = 0.0
    delta_divergence: float = 0.0
    volume_profile_poc: float = 0.0
    volume_profile_va_high: float = 0.0
    volume_profile_va_low: float = 0.0
    trade_count: int = 0
    total_buy_volume: float = 0.0
    total_sell_volume: float = 0.0
    high_price: float = 0.0
    low_price: float = 0.0
    close_price: float = 0.0
    
    def to_dict(self) -> Dict[str, float]:
        """Convert to flat dictionary for strategies"""
        return self.model_dump()


class Trade(BaseModel):
    """Historical trade data"""
    timestamp: int
    coin: str
    price: float
    size: float
    side: str
    hash: Optional[str] = None
    
    def to_tick(self) -> Tick:
        """Convert to Tick model"""
        return Tick(
            timestamp=self.timestamp,
            coin=self.coin,
            price=self.price,
            size=self.size,
            side=self.side,
            is_trade=True
        )


class Signal(BaseModel):
    """Trading signal output"""
    direction: str = Field(description="LONG, SHORT, or FLAT")
    confidence: float = Field(ge=0.0, le=1.0)
    reason: str
    metadata: Dict[str, Any] = Field(default_factory=dict)
    timestamp: int = Field(default_factory=lambda: int(datetime.now().timestamp() * 1000))
    
    def is_long(self) -> bool:
        return self.direction == "LONG"
    
    def is_short(self) -> bool:
        return self.direction == "SHORT"
    
    def is_flat(self) -> bool:
        return self.direction == "FLAT"
    
    def __str__(self) -> str:
        return f"{self.direction} ({self.confidence:.1%}): {self.reason}"
