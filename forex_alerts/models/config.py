"""
Configuration data model for the Forex Alert System.
"""

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any


@dataclass
class Config:
    """
    Configuration settings for the Forex Alert System.
    
    Attributes:
        symbols: List of forex symbols to monitor
        ema_length: Length for EMA calculation (default: 15)
        update_frequency: Data update frequency in seconds (default: 60)
        notification_methods: List of notification methods to use
        email_config: Email configuration dictionary (optional)
        data_retention_hours: Hours to retain historical data (default: 24)
    """
    symbols: List[str] = field(default_factory=list)
    ema_length: int = 15
    update_frequency: int = 60  # seconds
    notification_methods: List[str] = field(default_factory=lambda: ["console"])
    email_config: Optional[Dict[str, Any]] = None
    data_retention_hours: int = 24
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        if self.ema_length <= 0:
            raise ValueError(f"Invalid ema_length: {self.ema_length}. Must be positive")
        
        if self.update_frequency <= 0:
            raise ValueError(f"Invalid update_frequency: {self.update_frequency}. Must be positive")
        
        if self.data_retention_hours <= 0:
            raise ValueError(f"Invalid data_retention_hours: {self.data_retention_hours}. Must be positive")
        
        valid_methods = ["console", "email", "desktop"]
        for method in self.notification_methods:
            if method not in valid_methods:
                raise ValueError(f"Invalid notification method: {method}. Must be one of {valid_methods}")
    
    def validate_for_monitoring(self):
        """Validate configuration is ready for monitoring (requires symbols)."""
        if not self.symbols:
            raise ValueError("At least one symbol must be specified for monitoring")
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary representation."""
        return {
            'symbols': self.symbols,
            'ema_length': self.ema_length,
            'update_frequency': self.update_frequency,
            'notification_methods': self.notification_methods,
            'email_config': self.email_config,
            'data_retention_hours': self.data_retention_hours
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Config':
        """Create configuration from dictionary."""
        return cls(**data)