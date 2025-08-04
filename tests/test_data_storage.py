"""
Unit tests for DataStorage class.
"""

import pytest
import pandas as pd
from datetime import datetime, timedelta
from unittest.mock import patch, Mock
import threading
import time

from forex_alerts.services.data_storage import DataStorage
from forex_alerts.models.market_data import MarketData


class TestDataStorage:
    """Test cases for DataStorage class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.data_storage = DataStorage(retention_hours=24)
        
        # Create mock data with datetime index
        self.mock_data = pd.DataFrame({
            'Open': [1.0800, 1.0805, 1.0810],
            'High': [1.0815, 1.0820, 1.0825],
            'Low': [1.0795, 1.0800, 1.0805],
            'Close': [1.0805, 1.0810, 1.0815],
            'Volume': [1000, 1500, 1200]
        }, index=pd.date_range('2024-01-01 10:00:00', periods=3, freq='1min'))
        
        # Create older mock data for cleanup tests
        self.old_data = pd.DataFrame({
            'Open': [1.0700, 1.0705],
            'High': [1.0715, 1.0720],
            'Low': [1.0695, 1.0700],
            'Close': [1.0705, 1.0710],
            'Volume': [800, 900]
        }, index=pd.date_range('2024-01-01 08:00:00', periods=2, freq='1min'))
    
    def test_init(self):
        """Test DataStorage initialization."""
        storage = DataStorage(retention_hours=12)
        assert storage.retention_hours == 12
        assert len(storage._storage) == 0
        assert storage._cleanup_interval == timedelta(hours=1)
    
    def test_store_data_new_symbol(self):
        """Test storing data for a new symbol."""
        self.data_storage.store_data("EURUSD", self.mock_data)
        
        assert self.data_storage.has_data("EURUSD")
        stored_data = self.data_storage.get_historical_data("EURUSD")
        assert len(stored_data) == 3
        pd.testing.assert_frame_equal(stored_data, self.mock_data)
    
    def test_store_data_merge_existing(self):
        """Test merging data with existing symbol data."""
        # Store initial data
        self.data_storage.store_data("EURUSD", self.mock_data)
        
        # Create additional data
        new_data = pd.DataFrame({
            'Open': [1.0820, 1.0825],
            'High': [1.0835, 1.0840],
            'Low': [1.0815, 1.0820],
            'Close': [1.0825, 1.0830],
            'Volume': [1300, 1400]
        }, index=pd.date_range('2024-01-01 10:03:00', periods=2, freq='1min'))
        
        # Store additional data
        self.data_storage.store_data("EURUSD", new_data)
        
        # Verify merged data
        stored_data = self.data_storage.get_historical_data("EURUSD")
        assert len(stored_data) == 5  # 3 + 2
        assert stored_data.index.is_monotonic_increasing
    
    def test_store_data_duplicate_handling(self):
        """Test handling of duplicate timestamps."""
        # Store initial data
        self.data_storage.store_data("EURUSD", self.mock_data)
        
        # Create overlapping data with one overlapping timestamp and one new
        overlapping_data = pd.DataFrame({
            'Open': [1.0801, 1.0806],  # Different values
            'High': [1.0816, 1.0821],
            'Low': [1.0796, 1.0801],
            'Close': [1.0806, 1.0811],
            'Volume': [1001, 1501]
        }, index=pd.date_range('2024-01-01 10:01:00', periods=2, freq='1min'))
        
        # Store overlapping data
        self.data_storage.store_data("EURUSD", overlapping_data)
        
        # Verify that latest values are kept and data is properly merged
        stored_data = self.data_storage.get_historical_data("EURUSD")
        # Original: 10:00, 10:01, 10:02
        # New: 10:01 (overlaps), 10:02 (overlaps) 
        # Result should be: 10:00 (original), 10:01 (updated), 10:02 (updated)
        assert len(stored_data) == 3  # No duplicates, overlapping timestamps updated
        assert stored_data.loc['2024-01-01 10:01:00', 'Open'] == 1.0801  # Updated value
        assert stored_data.loc['2024-01-01 10:02:00', 'Open'] == 1.0806  # Updated value
    
    def test_store_empty_data(self):
        """Test storing empty DataFrame."""
        empty_data = pd.DataFrame()
        self.data_storage.store_data("EURUSD", empty_data)
        
        assert not self.data_storage.has_data("EURUSD")
    
    def test_get_historical_data_not_found(self):
        """Test getting historical data for non-existent symbol."""
        result = self.data_storage.get_historical_data("NONEXISTENT")
        assert result is None
    
    def test_get_historical_data_with_periods(self):
        """Test getting limited number of historical periods."""
        # Store more data than requested periods
        extended_data = pd.DataFrame({
            'Open': [1.08 + i*0.001 for i in range(10)],
            'High': [1.081 + i*0.001 for i in range(10)],
            'Low': [1.079 + i*0.001 for i in range(10)],
            'Close': [1.0805 + i*0.001 for i in range(10)],
            'Volume': [1000 + i*100 for i in range(10)]
        }, index=pd.date_range('2024-01-01 10:00:00', periods=10, freq='1min'))
        
        self.data_storage.store_data("EURUSD", extended_data)
        
        # Request only 5 periods
        result = self.data_storage.get_historical_data("EURUSD", periods=5)
        assert len(result) == 5
        
        # Should be the most recent 5 periods
        expected_start = extended_data.index[-5]
        assert result.index[0] == expected_start
    
    def test_get_latest_price(self):
        """Test getting latest price."""
        self.data_storage.store_data("EURUSD", self.mock_data)
        
        latest_price = self.data_storage.get_latest_price("EURUSD")
        assert latest_price == 1.0815  # Last close price
    
    def test_get_latest_price_not_found(self):
        """Test getting latest price for non-existent symbol."""
        result = self.data_storage.get_latest_price("NONEXISTENT")
        assert result is None
    
    def test_get_latest_data_point(self):
        """Test getting latest complete data point."""
        self.data_storage.store_data("EURUSD", self.mock_data)
        
        latest_data = self.data_storage.get_latest_data_point("EURUSD")
        
        assert isinstance(latest_data, MarketData)
        assert latest_data.symbol == "EURUSD"
        assert latest_data.close == 1.0815
        assert latest_data.volume == 1200
        assert latest_data.timestamp == self.mock_data.index[-1]
    
    def test_get_latest_data_point_not_found(self):
        """Test getting latest data point for non-existent symbol."""
        result = self.data_storage.get_latest_data_point("NONEXISTENT")
        assert result is None
    
    def test_get_data_range(self):
        """Test getting data within a specific time range."""
        self.data_storage.store_data("EURUSD", self.mock_data)
        
        start_time = datetime(2024, 1, 1, 10, 0, 30)
        end_time = datetime(2024, 1, 1, 10, 1, 30)
        
        result = self.data_storage.get_data_range("EURUSD", start_time, end_time)
        
        # Should return middle record only
        assert len(result) == 1
        assert result.index[0] == pd.Timestamp('2024-01-01 10:01:00')
    
    def test_get_data_range_not_found(self):
        """Test getting data range for non-existent symbol."""
        start_time = datetime(2024, 1, 1, 10, 0, 0)
        end_time = datetime(2024, 1, 1, 11, 0, 0)
        
        result = self.data_storage.get_data_range("NONEXISTENT", start_time, end_time)
        assert result is None
    
    def test_cleanup_old_data(self):
        """Test cleanup of old data."""
        # Create storage with short retention
        storage = DataStorage(retention_hours=1)
        
        # Store old data
        old_time = datetime.now() - timedelta(hours=2)
        old_data = pd.DataFrame({
            'Open': [1.0700],
            'High': [1.0715],
            'Low': [1.0695],
            'Close': [1.0705],
            'Volume': [800]
        }, index=[old_time])
        
        # Store recent data
        recent_time = datetime.now() - timedelta(minutes=30)
        recent_data = pd.DataFrame({
            'Open': [1.0800],
            'High': [1.0815],
            'Low': [1.0795],
            'Close': [1.0805],
            'Volume': [1000]
        }, index=[recent_time])
        
        storage.store_data("EURUSD", old_data)
        storage.store_data("EURUSD", recent_data)
        
        # Verify both data points exist
        assert len(storage.get_historical_data("EURUSD")) == 2
        
        # Trigger cleanup
        storage.cleanup_old_data()
        
        # Verify only recent data remains
        remaining_data = storage.get_historical_data("EURUSD")
        assert len(remaining_data) == 1
        assert remaining_data.index[0] == recent_time
    
    def test_cleanup_removes_empty_symbols(self):
        """Test that cleanup removes symbols with no recent data."""
        storage = DataStorage(retention_hours=1)
        
        # Store only old data
        old_time = datetime.now() - timedelta(hours=2)
        old_data = pd.DataFrame({
            'Open': [1.0700],
            'High': [1.0715],
            'Low': [1.0695],
            'Close': [1.0705],
            'Volume': [800]
        }, index=[old_time])
        
        storage.store_data("EURUSD", old_data)
        assert storage.has_data("EURUSD")
        
        # Trigger cleanup
        storage.cleanup_old_data()
        
        # Verify symbol is removed
        assert not storage.has_data("EURUSD")
    
    @patch('forex_alerts.services.data_storage.datetime')
    def test_maybe_cleanup_triggers(self, mock_datetime):
        """Test that _maybe_cleanup triggers when interval has passed."""
        # Mock current time
        current_time = datetime(2024, 1, 1, 12, 0, 0)
        mock_datetime.now.return_value = current_time
        
        # Set last cleanup to more than an hour ago
        self.data_storage._last_cleanup = current_time - timedelta(hours=2)
        
        with patch.object(self.data_storage, 'cleanup_old_data') as mock_cleanup:
            self.data_storage._maybe_cleanup()
            mock_cleanup.assert_called_once()
    
    def test_get_storage_stats(self):
        """Test getting storage statistics."""
        self.data_storage.store_data("EURUSD", self.mock_data)
        self.data_storage.store_data("GBPUSD", self.mock_data)
        
        stats = self.data_storage.get_storage_stats()
        
        assert stats['symbols_count'] == 2
        assert stats['total_records'] == 6  # 3 records per symbol
        assert set(stats['symbols']) == {"EURUSD", "GBPUSD"}
        assert stats['retention_hours'] == 24
        
        # Check symbol details
        assert 'EURUSD' in stats['symbol_details']
        assert stats['symbol_details']['EURUSD']['records'] == 3
        assert stats['symbol_details']['EURUSD']['latest_price'] == 1.0815
    
    def test_clear_symbol_data(self):
        """Test clearing data for a specific symbol."""
        self.data_storage.store_data("EURUSD", self.mock_data)
        self.data_storage.store_data("GBPUSD", self.mock_data)
        
        # Clear one symbol
        result = self.data_storage.clear_symbol_data("EURUSD")
        
        assert result is True
        assert not self.data_storage.has_data("EURUSD")
        assert self.data_storage.has_data("GBPUSD")
    
    def test_clear_symbol_data_not_found(self):
        """Test clearing data for non-existent symbol."""
        result = self.data_storage.clear_symbol_data("NONEXISTENT")
        assert result is False
    
    def test_clear_all_data(self):
        """Test clearing all data."""
        self.data_storage.store_data("EURUSD", self.mock_data)
        self.data_storage.store_data("GBPUSD", self.mock_data)
        
        self.data_storage.clear_all_data()
        
        assert not self.data_storage.has_data("EURUSD")
        assert not self.data_storage.has_data("GBPUSD")
        assert len(self.data_storage._storage) == 0
    
    def test_has_data(self):
        """Test checking if data exists for a symbol."""
        assert not self.data_storage.has_data("EURUSD")
        
        self.data_storage.store_data("EURUSD", self.mock_data)
        assert self.data_storage.has_data("EURUSD")
        
        # Test with empty data
        empty_data = pd.DataFrame()
        self.data_storage.store_data("GBPUSD", empty_data)
        assert not self.data_storage.has_data("GBPUSD")
    
    def test_get_data_age(self):
        """Test getting age of most recent data."""
        # Create data with known timestamp
        past_time = datetime.now() - timedelta(minutes=30)
        data_with_time = pd.DataFrame({
            'Open': [1.0800],
            'High': [1.0815],
            'Low': [1.0795],
            'Close': [1.0805],
            'Volume': [1000]
        }, index=[past_time])
        
        self.data_storage.store_data("EURUSD", data_with_time)
        
        age = self.data_storage.get_data_age("EURUSD")
        
        # Should be approximately 30 minutes (allowing for test execution time)
        assert timedelta(minutes=29) <= age <= timedelta(minutes=31)
    
    def test_get_data_age_not_found(self):
        """Test getting data age for non-existent symbol."""
        result = self.data_storage.get_data_age("NONEXISTENT")
        assert result is None
    
    def test_thread_safety(self):
        """Test thread safety of data storage operations."""
        def store_data_worker(symbol_suffix):
            """Worker function to store data from multiple threads."""
            for i in range(10):
                data = pd.DataFrame({
                    'Open': [1.08 + i*0.001],
                    'High': [1.081 + i*0.001],
                    'Low': [1.079 + i*0.001],
                    'Close': [1.0805 + i*0.001],
                    'Volume': [1000 + i*10]
                }, index=[datetime.now() + timedelta(seconds=i)])
                
                self.data_storage.store_data(f"SYMBOL{symbol_suffix}", data)
                time.sleep(0.001)  # Small delay to simulate real usage
        
        # Start multiple threads
        threads = []
        for i in range(3):
            thread = threading.Thread(target=store_data_worker, args=(i,))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Verify data integrity
        stats = self.data_storage.get_storage_stats()
        assert stats['symbols_count'] == 3
        
        for i in range(3):
            symbol = f"SYMBOL{i}"
            assert self.data_storage.has_data(symbol)
            data = self.data_storage.get_historical_data(symbol)
            assert len(data) == 10  # All records should be stored


class TestDataStorageIntegration:
    """Integration tests for DataStorage with realistic scenarios."""
    
    def test_realistic_data_flow(self):
        """Test realistic data storage and retrieval flow."""
        storage = DataStorage(retention_hours=2)
        
        # Simulate receiving data over time
        base_time = datetime.now() - timedelta(hours=1)
        
        for i in range(60):  # 60 minutes of data
            timestamp = base_time + timedelta(minutes=i)
            price = 1.0800 + (i * 0.0001)  # Gradually increasing price
            
            data = pd.DataFrame({
                'Open': [price],
                'High': [price + 0.0005],
                'Low': [price - 0.0005],
                'Close': [price + 0.0002],
                'Volume': [1000 + (i * 10)]
            }, index=[timestamp])
            
            storage.store_data("EURUSD", data)
        
        # Verify all data is stored
        all_data = storage.get_historical_data("EURUSD")
        assert len(all_data) == 60
        
        # Test range queries
        range_start = base_time + timedelta(minutes=30)
        range_end = base_time + timedelta(minutes=40)
        range_data = storage.get_data_range("EURUSD", range_start, range_end)
        assert len(range_data) == 11  # Inclusive range
        
        # Test latest data retrieval
        latest_price = storage.get_latest_price("EURUSD")
        expected_latest = 1.0800 + (59 * 0.0001) + 0.0002
        assert abs(latest_price - expected_latest) < 0.0001
        
        # Test cleanup doesn't affect recent data
        storage.cleanup_old_data()
        remaining_data = storage.get_historical_data("EURUSD")
        assert len(remaining_data) == 60  # All data should still be there