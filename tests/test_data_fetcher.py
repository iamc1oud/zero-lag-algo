"""
Unit tests for DataFetcher class.
"""

import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta
import time

from forex_alerts.services.data_fetcher import DataFetcher


class TestDataFetcher:
    """Test cases for DataFetcher class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.symbols = ["EURUSD", "GBPUSD"]
        self.data_fetcher = DataFetcher(self.symbols, interval="1m")
        
        # Create mock data
        self.mock_data = pd.DataFrame({
            'Open': [1.0800, 1.0805, 1.0810],
            'High': [1.0815, 1.0820, 1.0825],
            'Low': [1.0795, 1.0800, 1.0805],
            'Close': [1.0805, 1.0810, 1.0815],
            'Volume': [1000, 1500, 1200]
        }, index=pd.date_range('2024-01-01 10:00:00', periods=3, freq='1min'))
    
    def test_init(self):
        """Test DataFetcher initialization."""
        assert self.data_fetcher.symbols == ["EURUSD=X", "GBPUSD=X"]
        assert self.data_fetcher.interval == "1m"
        assert self.data_fetcher._max_retries == 5
        assert self.data_fetcher._base_delay == 1.0
    
    def test_format_forex_symbol(self):
        """Test forex symbol formatting."""
        # Test symbol without =X suffix
        assert self.data_fetcher._format_forex_symbol("EURUSD") == "EURUSD=X"
        
        # Test symbol with =X suffix already
        assert self.data_fetcher._format_forex_symbol("EURUSD=X") == "EURUSD=X"
        
        # Test other symbols
        assert self.data_fetcher._format_forex_symbol("GBPJPY") == "GBPJPY=X"
    
    @patch('forex_alerts.services.data_fetcher.yf.Ticker')
    def test_validate_symbol_success(self, mock_ticker):
        """Test successful symbol validation."""
        # Mock ticker with valid data
        mock_ticker_instance = Mock()
        mock_ticker_instance.history.return_value = self.mock_data
        mock_ticker.return_value = mock_ticker_instance
        
        result = self.data_fetcher.validate_symbol("EURUSD")
        
        assert result is True
        mock_ticker.assert_called_once_with("EURUSD=X")
        mock_ticker_instance.history.assert_called_once_with(period="1d", interval="1d")
    
    @patch('forex_alerts.services.data_fetcher.yf.Ticker')
    def test_validate_symbol_empty_data(self, mock_ticker):
        """Test symbol validation with empty data."""
        # Mock ticker with empty data
        mock_ticker_instance = Mock()
        mock_ticker_instance.history.return_value = pd.DataFrame()
        mock_ticker.return_value = mock_ticker_instance
        
        result = self.data_fetcher.validate_symbol("INVALID")
        
        assert result is False
    
    @patch('forex_alerts.services.data_fetcher.yf.Ticker')
    def test_validate_symbol_exception(self, mock_ticker):
        """Test symbol validation with exception."""
        # Mock ticker that raises exception
        mock_ticker.side_effect = Exception("API Error")
        
        result = self.data_fetcher.validate_symbol("EURUSD")
        
        assert result is False
    
    @patch('forex_alerts.services.data_fetcher.yf.Ticker')
    def test_get_forex_data_success(self, mock_ticker):
        """Test successful forex data retrieval."""
        # Mock ticker with valid data
        mock_ticker_instance = Mock()
        mock_ticker_instance.history.return_value = self.mock_data
        mock_ticker.return_value = mock_ticker_instance
        
        result = self.data_fetcher.get_forex_data("EURUSD", period="1d")
        
        assert result is not None
        assert 'Symbol' in result.columns
        assert result['Symbol'].iloc[0] == "EURUSD"
        mock_ticker.assert_called_once_with("EURUSD=X")
        mock_ticker_instance.history.assert_called_once_with(period="1d", interval="1m")
    
    @patch('forex_alerts.services.data_fetcher.yf.Ticker')
    def test_get_forex_data_empty_response(self, mock_ticker):
        """Test forex data retrieval with empty response."""
        # Mock ticker with empty data
        mock_ticker_instance = Mock()
        mock_ticker_instance.history.return_value = pd.DataFrame()
        mock_ticker.return_value = mock_ticker_instance
        
        result = self.data_fetcher.get_forex_data("EURUSD")
        
        assert result is None
    
    @patch('forex_alerts.services.data_fetcher.yf.Ticker')
    @patch('time.sleep')
    def test_get_forex_data_retry_logic(self, mock_sleep, mock_ticker):
        """Test retry logic with exponential backoff."""
        # Mock ticker that fails first two attempts, succeeds on third
        mock_ticker_instance = Mock()
        mock_ticker_instance.history.side_effect = [
            Exception("Network error"),
            Exception("API error"),
            self.mock_data
        ]
        mock_ticker.return_value = mock_ticker_instance
        
        result = self.data_fetcher.get_forex_data("EURUSD")
        
        assert result is not None
        assert 'Symbol' in result.columns
        assert mock_ticker_instance.history.call_count == 3
        assert mock_sleep.call_count == 2  # Two retries before success
    
    @patch('forex_alerts.services.data_fetcher.yf.Ticker')
    @patch('time.sleep')
    def test_get_forex_data_max_retries_exceeded(self, mock_sleep, mock_ticker):
        """Test behavior when max retries are exceeded."""
        # Mock ticker that always fails
        mock_ticker_instance = Mock()
        mock_ticker_instance.history.side_effect = Exception("Persistent error")
        mock_ticker.return_value = mock_ticker_instance
        
        result = self.data_fetcher.get_forex_data("EURUSD")
        
        assert result is None
        assert mock_ticker_instance.history.call_count == 5  # Max retries
        assert mock_sleep.call_count == 4  # One less than max retries
    
    def test_calculate_backoff_delay(self):
        """Test exponential backoff delay calculation."""
        # Test increasing delays
        delay_0 = self.data_fetcher._calculate_backoff_delay(0)
        delay_1 = self.data_fetcher._calculate_backoff_delay(1)
        delay_2 = self.data_fetcher._calculate_backoff_delay(2)
        
        # Should be roughly exponential (with jitter)
        assert 0.8 <= delay_0 <= 1.2  # ~1s ± jitter
        assert 1.6 <= delay_1 <= 2.4  # ~2s ± jitter
        assert 3.2 <= delay_2 <= 4.8  # ~4s ± jitter
        
        # Test cap at 60 seconds (allowing for jitter which can add up to 20% more)
        delay_large = self.data_fetcher._calculate_backoff_delay(10)
        assert delay_large <= 72.0  # 60 + 20% jitter
    
    @patch.object(DataFetcher, 'get_forex_data')
    def test_fetch_latest_data_success(self, mock_get_forex_data):
        """Test successful fetching of latest data for all symbols."""
        # Mock successful data retrieval
        mock_get_forex_data.side_effect = [self.mock_data, self.mock_data]
        
        result = self.data_fetcher.fetch_latest_data()
        
        assert len(result) == 2
        assert "EURUSD" in result
        assert "GBPUSD" in result
        assert mock_get_forex_data.call_count == 2
    
    @patch.object(DataFetcher, 'get_forex_data')
    def test_fetch_latest_data_partial_failure(self, mock_get_forex_data):
        """Test fetching data when some symbols fail."""
        # Mock mixed success/failure
        mock_get_forex_data.side_effect = [self.mock_data, None]
        
        result = self.data_fetcher.fetch_latest_data()
        
        assert len(result) == 1
        assert "EURUSD" in result
        assert "GBPUSD" not in result
    
    @patch.object(DataFetcher, 'get_forex_data')
    def test_get_current_price_success(self, mock_get_forex_data):
        """Test successful current price retrieval."""
        mock_get_forex_data.return_value = self.mock_data
        
        price = self.data_fetcher.get_current_price("EURUSD")
        
        assert price == 1.0815  # Last close price in mock data
        mock_get_forex_data.assert_called_once_with("EURUSD", period="1d")
    
    @patch.object(DataFetcher, 'get_forex_data')
    def test_get_current_price_no_data(self, mock_get_forex_data):
        """Test current price retrieval with no data."""
        mock_get_forex_data.return_value = None
        
        price = self.data_fetcher.get_current_price("EURUSD")
        
        assert price is None
    
    @patch.object(DataFetcher, 'get_forex_data')
    def test_get_current_price_empty_data(self, mock_get_forex_data):
        """Test current price retrieval with empty DataFrame."""
        mock_get_forex_data.return_value = pd.DataFrame()
        
        price = self.data_fetcher.get_current_price("EURUSD")
        
        assert price is None


class TestDataFetcherIntegration:
    """Integration tests for DataFetcher (require network access)."""
    
    @pytest.mark.integration
    def test_validate_real_symbol(self):
        """Test validation with real forex symbols (requires network)."""
        data_fetcher = DataFetcher(["EURUSD"], interval="1d")
        
        # Test valid symbol
        assert data_fetcher.validate_symbol("EURUSD") is True
        
        # Test invalid symbol
        assert data_fetcher.validate_symbol("INVALID123") is False
    
    @pytest.mark.integration
    def test_fetch_real_data(self):
        """Test fetching real data (requires network)."""
        data_fetcher = DataFetcher(["EURUSD"], interval="1d")
        
        data = data_fetcher.get_forex_data("EURUSD", period="5d")
        
        if data is not None:  # May fail due to network/API issues
            assert not data.empty
            assert 'Open' in data.columns
            assert 'High' in data.columns
            assert 'Low' in data.columns
            assert 'Close' in data.columns
            assert 'Volume' in data.columns
            assert 'Symbol' in data.columns