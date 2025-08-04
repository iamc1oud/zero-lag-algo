"""
Market data model for forex price information.
"""

from dataclasses import dataclass
from datetime import datetime


@dataclass
class MarketData:
    """
    Represents market data for a forex symbol at a specific time.
    
    Attributes:
        symbol: The forex symbol (e.g., "EURUSD")
        timestamp: When the data was recorded
        open: Opening price
        high: Highest price
        low: Lowest price
        close: Closing price
        volume: Trading volume
    """
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    
    def __post_init__(self):
        """Validate market data after initialization."""
        if not self.symbol:
            raise ValueError("Symbol cannot be empty")
        
        prices = [self.open, self.high, self.low, self.close]
        if any(price <= 0 for price in prices):
            raise ValueError("All prices must be positive")
        
        if self.high < max(self.open, self.close) or self.low > min(self.open, self.close):
            raise ValueError("High/Low prices are inconsistent with Open/Close")
        
        if self.volume < 0:
            raise ValueError("Volume cannot be negative")
    
    def to_dict(self) -> dict:
        """Convert market data to dictionary representation."""
        return {
            'symbol': self.symbol,
            'timestamp': self.timestamp.isoformat(),
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'close': self.close,
            'volume': self.volume
        }
    
    @property
    def typical_price(self) -> float:
        """Calculate typical price (HLC/3)."""
        return (self.high + self.low + self.close) / 3
    
    @property
    def weighted_price(self) -> float:
        """Calculate volume-weighted price for VWAP calculations."""
        return self.typical_price * self.volume