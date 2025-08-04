"""
Data fetching service for forex market data using yfinance API.
"""

import time
import logging
from typing import Dict, List, Optional
from datetime import datetime, timedelta
import pandas as pd
import yfinance as yf
from ..models.market_data import MarketData


class DataFetcher:
    """
    Handles fetching forex market data from yfinance API with error handling
    and exponential backoff for API failures.
    """
    
    def __init__(self, symbols: List[str], interval: str = "1m"):
        """
        Initialize DataFetcher with forex symbols and update interval.
        
        Args:
            symbols: List of forex symbols (e.g., ["EURUSD", "GBPUSD"])
            interval: Data interval (1m, 5m, 15m, 30m, 1h, 1d)
        """
        self.symbols = [self._format_forex_symbol(symbol) for symbol in symbols]
        self.interval = interval
        self.logger = logging.getLogger(__name__)
        self._max_retries = 5
        self._base_delay = 1.0  # Base delay for exponential backoff
        
    def _format_forex_symbol(self, symbol: str) -> str:
        """
        Format forex symbol for yfinance API by appending '=X' if needed.
        
        Args:
            symbol: Raw forex symbol (e.g., "EURUSD" or "EURUSD=X")
            
        Returns:
            Formatted symbol for yfinance (e.g., "EURUSD=X")
        """
        if not symbol.endswith("=X"):
            return f"{symbol}=X"
        return symbol
    
    def validate_symbol(self, symbol: str) -> bool:
        """
        Validate that a forex symbol is available in yfinance.
        
        Args:
            symbol: Forex symbol to validate
            
        Returns:
            True if symbol is valid, False otherwise
        """
        try:
            formatted_symbol = self._format_forex_symbol(symbol)
            ticker = yf.Ticker(formatted_symbol)
            
            # Try to fetch a small amount of data to validate
            data = ticker.history(period="1d", interval="1d")
            return not data.empty
            
        except Exception as e:
            self.logger.warning(f"Symbol validation failed for {symbol}: {e}")
            return False
    
    def get_forex_data(self, symbol: str, period: str = "1d") -> Optional[pd.DataFrame]:
        """
        Fetch forex data for a single symbol with retry logic.
        
        Args:
            symbol: Forex symbol to fetch
            period: Time period (1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max)
            
        Returns:
            DataFrame with OHLCV data or None if failed
        """
        formatted_symbol = self._format_forex_symbol(symbol)
        
        for attempt in range(self._max_retries):
            try:
                ticker = yf.Ticker(formatted_symbol)
                data = ticker.history(period=period, interval=self.interval)
                
                if data.empty:
                    self.logger.warning(f"No data returned for {formatted_symbol}")
                    return None
                
                # Add symbol column for identification
                data['Symbol'] = symbol
                return data
                
            except Exception as e:
                delay = self._calculate_backoff_delay(attempt)
                self.logger.warning(
                    f"Attempt {attempt + 1} failed for {formatted_symbol}: {e}. "
                    f"Retrying in {delay:.1f}s"
                )
                
                if attempt < self._max_retries - 1:
                    time.sleep(delay)
                else:
                    self.logger.error(f"All retry attempts failed for {formatted_symbol}")
                    
        return None
    
    def fetch_latest_data(self) -> Dict[str, pd.DataFrame]:
        """
        Fetch latest data for all configured symbols.
        
        Returns:
            Dictionary mapping symbol names to their DataFrames
        """
        results = {}
        
        for symbol in self.symbols:
            # Remove =X suffix for cleaner symbol names in results
            clean_symbol = symbol.replace("=X", "")
            data = self.get_forex_data(clean_symbol, period="1d")
            
            if data is not None:
                results[clean_symbol] = data
                self.logger.info(f"Successfully fetched data for {clean_symbol}")
            else:
                self.logger.error(f"Failed to fetch data for {clean_symbol}")
                
        return results
    
    def _calculate_backoff_delay(self, attempt: int) -> float:
        """
        Calculate exponential backoff delay with jitter.
        
        Args:
            attempt: Current attempt number (0-based)
            
        Returns:
            Delay in seconds
        """
        import random
        
        # Exponential backoff: base_delay * 2^attempt
        delay = self._base_delay * (2 ** attempt)
        
        # Cap at 60 seconds
        delay = min(delay, 60.0)
        
        # Add jitter (±20%)
        jitter = delay * 0.2 * (random.random() - 0.5)
        
        return delay + jitter
    
    def get_current_price(self, symbol: str) -> Optional[float]:
        """
        Get the current/latest price for a forex symbol.
        
        Args:
            symbol: Forex symbol
            
        Returns:
            Current price or None if unavailable
        """
        data = self.get_forex_data(symbol, period="1d")
        
        if data is not None and not data.empty:
            return float(data['Close'].iloc[-1])
        
        return None