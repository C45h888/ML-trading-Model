"""
Risk Manager - Position sizing and risk controls for Brain Agent.

Provides:
- Rate limiting (max trades per minute/hour)
- Position sizing based on confidence and portfolio
- Daily loss limits
- Max drawdown protection
"""
import logging
from dataclasses import dataclass, field
from typing import Optional, Tuple
from datetime import datetime, timezone
from collections import deque

logger = logging.getLogger(__name__)


@dataclass
class RiskLimits:
    """Risk control parameters"""
    max_trades_per_minute: int = 3
    max_trades_per_hour: int = 20
    max_daily_trades: int = 50  # Maximum trades per day
    max_position_size_pct: float = 10.0  # % of portfolio
    daily_loss_limit_pct: float = -2.0  # -2%
    max_drawdown_pct: float = -5.0  # -5%
    min_trade_interval_seconds: float = 10.0  # Minimum between trades


@dataclass
class Portfolio:
    """Portfolio state tracking"""
    initial_equity: float = 10000.0  # Starting capital
    current_equity: float = 10000.0
    peak_equity: float = 10000.0
    
    # Trade history for rate limiting
    recent_trades: deque = field(default_factory=lambda: deque(maxlen=100))
    
    # Daily tracking
    daily_trades: int = 0
    daily_pnl_pct: float = 0.0
    last_reset_date: str = ""
    
    def reset_daily_if_needed(self):
        """Reset daily counters if it's a new day"""
        today = datetime.now(timezone.utc).date().isoformat()
        if self.last_reset_date != today:
            self.daily_trades = 0
            self.daily_pnl_pct = 0.0
            self.last_reset_date = today


class RiskManager:
    """
    Risk management for trading signals.
    
    Controls:
    - Rate limiting: max trades per minute/hour
    - Position sizing: scales with confidence
    - Loss limits: daily loss and max drawdown
    """
    
    def __init__(
        self,
        limits: Optional[RiskLimits] = None,
        portfolio: Optional[Portfolio] = None,
    ):
        self.limits = limits or RiskLimits()
        self.portfolio = portfolio or Portfolio()
        
        # Hourly tracking
        self.hourly_trades = deque(maxlen=60)  # Track last 60 minutes
        
        logger.info(
            "RiskManager initialized",
            max_trades_per_minute=self.limits.max_trades_per_minute,
            max_trades_per_hour=self.limits.max_trades_per_hour,
            max_position_size_pct=self.limits.max_position_size_pct,
            daily_loss_limit=self.limits.daily_loss_limit_pct,
        )
    
    def can_trade(self, signal_direction: str) -> Tuple[bool, str]:
        """
        Check if a new trade is allowed based on risk controls.
        
        Args:
            signal_direction: Direction of the signal (LONG/SHORT)
            
        Returns:
            Tuple of (allowed: bool, reason: str)
        """
        from time import time
        
        current_time = time()
        
        # Reset daily if needed
        self.portfolio.reset_daily_if_needed()
        
        # Check 1: Rate limit per minute
        recent_minute = [t for t in self.portfolio.recent_trades 
                        if current_time - t < 60]
        if len(recent_minute) >= self.limits.max_trades_per_minute:
            return False, f"Rate limit: {self.limits.max_trades_per_minute} trades/min"
        
        # Check 2: Rate limit per hour
        self.hourly_trades = deque([t for t in self.hourly_trades 
                                   if current_time - t < 3600], maxlen=60)
        if len(self.hourly_trades) >= self.limits.max_trades_per_hour:
            return False, f"Rate limit: {self.limits.max_trades_per_hour} trades/hour"
        
        # Check 3: Daily trade limit
        if self.portfolio.daily_trades >= self.limits.max_daily_trades:
            return False, f"Daily trade limit reached ({self.limits.max_daily_trades})"
        
        # Check 4: Daily loss limit
        if self.portfolio.daily_pnl_pct <= self.limits.daily_loss_limit_pct:
            return False, f"Daily loss limit hit ({self.portfolio.daily_pnl_pct:.2f}%)"
        
        # Check 5: Max drawdown
        drawdown = (self.portfolio.peak_equity - self.portfolio.current_equity) / self.portfolio.peak_equity * 100
        if drawdown >= abs(self.limits.max_drawdown_pct):
            return False, f"Max drawdown reached ({drawdown:.2f}%)"
        
        # Check 6: Minimum time between trades
        if self.portfolio.recent_trades:
            last_trade_time = max(self.portfolio.recent_trades)
            if current_time - last_trade_time < self.limits.min_trade_interval_seconds:
                return False, f"Min trade interval ({self.limits.min_trade_interval_seconds}s)"
        
        return True, "OK"
    
    def calculate_position_size(
        self,
        confidence: float,
        entry_price: float,
    ) -> float:
        """
        Calculate position size based on confidence and portfolio.
        
        Uses Kelly Criterion simplified: size = confidence * max_position
        
        Args:
            confidence: Signal confidence (0.0 to 1.0)
            entry_price: Entry price for the trade
            
        Returns:
            Position size in BTC (notional value)
        """
        # Base position size as % of portfolio
        base_size_pct = self.limits.max_position_size_pct / 100.0
        
        # Scale by confidence (higher confidence = larger position)
        # Minimum 50% of base, maximum 100% of base
        confidence_factor = 0.5 + (confidence * 0.5)  # 0.5 to 1.0
        
        position_value = self.portfolio.current_equity * base_size_pct * confidence_factor
        
        # Convert to BTC
        position_size = position_value / entry_price
        
        logger.debug(
            "Position size calculated",
            confidence=confidence,
            position_value=position_value,
            position_size=position_size,
        )
        
        return position_size
    
    def record_trade(
        self,
        direction: str,
        entry_price: float,
        size: float,
        confidence: float,
    ) -> None:
        """
        Record a trade for risk tracking.
        
        Args:
            direction: LONG or SHORT
            entry_price: Entry price
            size: Position size in BTC
            confidence: Signal confidence
        """
        from time import time
        
        current_time = time()
        
        # Record trade time for rate limiting
        self.portfolio.recent_trades.append(current_time)
        self.hourly_trades.append(current_time)
        self.portfolio.daily_trades += 1
        
        logger.info(
            "Trade recorded",
            direction=direction,
            entry_price=entry_price,
            size=size,
            confidence=confidence,
            daily_trades=self.portfolio.daily_trades,
        )
    
    def record_outcome(
        self,
        exit_price: float,
        direction: str,
        size: float,
    ) -> None:
        """
        Record trade outcome for P&L and drawdown tracking.
        
        Args:
            exit_price: Exit price
            direction: LONG or SHORT
            size: Position size in BTC
        """
        # Calculate P&L
        if direction == "LONG":
            pnl = (exit_price - 0) * size  # Simplified - would need entry price
        else:
            pnl = (0 - exit_price) * size
        
        pnl_pct = pnl / self.portfolio.current_equity * 100
        
        # Update equity
        self.portfolio.current_equity += pnl
        
        # Update peak equity
        if self.portfolio.current_equity > self.portfolio.peak_equity:
            self.portfolio.peak_equity = self.portfolio.current_equity
        
        # Update daily P&L
        self.portfolio.daily_pnl_pct += pnl_pct
        
        logger.info(
            "Trade outcome recorded",
            pnl=pnl,
            pnl_pct=pnl_pct,
            current_equity=self.portfolio.current_equity,
            daily_pnl_pct=self.portfolio.daily_pnl_pct,
        )
    
    def get_stats(self) -> dict:
        """Get current risk manager statistics"""
        from time import time
        
        current_time = time()
        recent_minute = len([t for t in self.portfolio.recent_trades 
                           if current_time - t < 60])
        hourly = len([t for t in self.hourly_trades 
                     if current_time - t < 3600])
        
        return {
            "trades_last_minute": recent_minute,
            "trades_last_hour": hourly,
            "daily_trades": self.portfolio.daily_trades,
            "daily_pnl_pct": self.portfolio.daily_pnl_pct,
            "current_equity": self.portfolio.current_equity,
            "peak_equity": self.portfolio.peak_equity,
            "max_drawdown_pct": (
                (self.portfolio.peak_equity - self.portfolio.current_equity) 
                / self.portfolio.peak_equity * 100
            ),
        }
