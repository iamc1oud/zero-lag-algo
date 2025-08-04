"""
In-memory data storage service for forex market data.
"""

import logging
from typing import Dict, Optional, List
from datetime import datetime, timedelta
import pandas as pd
from threading import Lock
from ..models.market_data import MarketData


class DataStorage:
    """
    Manages in-memory storage of historical forex market data with
    data retention policies and efficient access methods.
    """
    
    def __init__(self, retention_hours: int = 24):
        """
        Initialize DataStorage with configurable data retention.
        
        Args:
            retention_hours: Hours to retain historical data (default: 24)
        """
        self.retention_hours = retention_hours
        self.logger = logging.getLogger(__name__)
        
        # Thread-safe storage for market data
        self._data_lock = Lock()
        self._storage: Dict[str, pd.DataFrame] = {}
        
        # Track last cleanup time
        self._last_cleanup = datetime.now()
        self._cleanup_interval = timedelta(hours=1)  # Cleanup every hour
    
    def store_data(self, symbol: str, data: pd.DataFrame) -> None:
        """
        Store market data for a symbol, merging with existing data.
        
        Args:
            symbol: Forex symbol (e.g., "EURUSD")
            data: DataFrame with OHLCV data and datetime index
        """
        if data.empty:
            self.logger.warning(f"Attempted to store empty data for {symbol}")
            return
        
        with self._data_lock:
            if symbol in self._storage:
                # Merge with existing data, avoiding duplicates
                existing_data = self._storage[symbol]
                
                # Combine and remove duplicates based on index
                combined_data = pd.concat([existing_data, data])
                combined_data = combined_data[~combined_data.index.duplicated(keep='last')]
                combined_data = combined_data.sort_index()
                
                self._storage[symbol] = combined_data
                self.logger.debug(f"Merged data for {symbol}, total records: {len(combined_data)}")
            else:
                # Store new data
                self._storage[symbol] = data.copy()
                self.logger.debug(f"Stored new data for {symbol}, records: {len(data)}")
        
        # Trigger cleanup if needed
        self._maybe_cleanup()
    
    def get_historical_data(self, symbol: str, periods: int = 100) -> Optional[pd.DataFrame]:
        """
        Retrieve historical data for a symbol.
        
        Args:
            symbol: Forex symbol
            periods: Number of most recent periods to return
            
        Returns:
            DataFrame with historical data or None if not available
        """
        with self._data_lock:
            if symbol not in self._storage:
                self.logger.debug(f"No data available for {symbol}")
                return None
            
            data = self._storage[symbol]
            
            if data.empty:
                return None
            
            # Return the most recent periods
            if len(data) <= periods:
                return data.copy()
            else:
                return data.tail(periods).copy()
    
    def get_latest_price(self, symbol: str) -> Optional[float]:
        """
        Get the latest closing price for a symbol.
        
        Args:
            symbol: Forex symbol
            
        Returns:
            Latest closing price or None if not available
        """
        with self._data_lock:
            if symbol not in self._storage or self._storage[symbol].empty:
                return None
            
            data = self._storage[symbol]
            return float(data['Close'].iloc[-1])
    
    def get_latest_data_point(self, symbol: str) -> Optional[MarketData]:
        """
        Get the latest complete data point for a symbol.
        
        Args:
            symbol: Forex symbol
            
        Returns:
            MarketData object with latest data or None if not available
        """
        with self._data_lock:
            if symbol not in self._storage or self._storage[symbol].empty:
                return None
            
            data = self._storage[symbol]
            latest_row = data.iloc[-1]
            
            try:
                return MarketData(
                    symbol=symbol,
                    timestamp=latest_row.name,  # Index is the timestamp
                    open=float(latest_row['Open']),
                    high=float(latest_row['High']),
                    low=float(latest_row['Low']),
                    close=float(latest_row['Close']),
                    volume=int(latest_row['Volume'])
                )
            except (KeyError, ValueError) as e:
                self.logger.error(f"Error creating MarketData for {symbol}: {e}")
                return None
    
    def get_data_range(self, symbol: str, start_time: datetime, end_time: datetime) -> Optional[pd.DataFrame]:
        """
        Get data for a symbol within a specific time range.
        
        Args:
            symbol: Forex symbol
            start_time: Start of time range
            end_time: End of time range
            
        Returns:
            DataFrame with data in the specified range or None
        """
        with self._data_lock:
            if symbol not in self._storage or self._storage[symbol].empty:
                return None
            
            data = self._storage[symbol]
            
            # Filter by time range
            mask = (data.index >= start_time) & (data.index <= end_time)
            filtered_data = data.loc[mask]
            
            return filtered_data.copy() if not filtered_data.empty else None
    
    def cleanup_old_data(self) -> None:
        """
        Remove data older than the retention period.
        """
        cutoff_time = datetime.now() - timedelta(hours=self.retention_hours)
        
        with self._data_lock:
            symbols_to_remove = []
            
            for symbol, data in self._storage.items():
                if data.empty:
                    symbols_to_remove.append(symbol)
                    continue
                
                # Filter out old data
                recent_data = data[data.index >= cutoff_time]
                
                if recent_data.empty:
                    symbols_to_remove.append(symbol)
                else:
                    self._storage[symbol] = recent_data
                    
                    # Log cleanup if significant data was removed
                    removed_count = len(data) - len(recent_data)
                    if removed_count > 0:
                        self.logger.debug(
                            f"Cleaned up {removed_count} old records for {symbol}, "
                            f"retained {len(recent_data)} records"
                        )
            
            # Remove symbols with no recent data
            for symbol in symbols_to_remove:
                del self._storage[symbol]
                self.logger.debug(f"Removed all data for {symbol} (no recent data)")
        
        self._last_cleanup = datetime.now()
        self.logger.info(f"Data cleanup completed, retained data for {len(self._storage)} symbols")
    
    def _maybe_cleanup(self) -> None:
        """
        Trigger cleanup if enough time has passed since last cleanup.
        """
        if datetime.now() - self._last_cleanup >= self._cleanup_interval:
            self.cleanup_old_data()
    
    def get_storage_stats(self) -> Dict[str, any]:
        """
        Get statistics about current data storage.
        
        Returns:
            Dictionary with storage statistics
        """
        with self._data_lock:
            stats = {
                'symbols_count': len(self._storage),
                'total_records': sum(len(data) for data in self._storage.values()),
                'symbols': list(self._storage.keys()),
                'retention_hours': self.retention_hours,
                'last_cleanup': self._last_cleanup.isoformat()
            }
            
            # Add per-symbol statistics
            symbol_stats = {}
            for symbol, data in self._storage.items():
                if not data.empty:
                    symbol_stats[symbol] = {
                        'records': len(data),
                        'oldest_record': data.index.min().isoformat(),
                        'newest_record': data.index.max().isoformat(),
                        'latest_price': float(data['Close'].iloc[-1])
                    }
            
            stats['symbol_details'] = symbol_stats
            
        return stats
    
    def clear_symbol_data(self, symbol: str) -> bool:
        """
        Clear all data for a specific symbol.
        
        Args:
            symbol: Forex symbol to clear
            
        Returns:
            True if data was cleared, False if symbol not found
        """
        with self._data_lock:
            if symbol in self._storage:
                del self._storage[symbol]
                self.logger.info(f"Cleared all data for {symbol}")
                return True
            else:
                self.logger.warning(f"No data found for {symbol} to clear")
                return False
    
    def clear_all_data(self) -> None:
        """
        Clear all stored data for all symbols.
        """
        with self._data_lock:
            symbol_count = len(self._storage)
            self._storage.clear()
            self.logger.info(f"Cleared all data for {symbol_count} symbols")
    
    def has_data(self, symbol: str) -> bool:
        """
        Check if data exists for a symbol.
        
        Args:
            symbol: Forex symbol to check
            
        Returns:
            True if data exists and is not empty
        """
        with self._data_lock:
            return symbol in self._storage and not self._storage[symbol].empty
    
    def get_data_age(self, symbol: str) -> Optional[timedelta]:
        """
        Get the age of the most recent data for a symbol.
        
        Args:
            symbol: Forex symbol
            
        Returns:
            Time since the most recent data point or None if no data
        """
        with self._data_lock:
            if symbol not in self._storage or self._storage[symbol].empty:
                return None
            
            latest_timestamp = self._storage[symbol].index.max()
            return datetime.now() - latest_timestamp.to_pydatetime()