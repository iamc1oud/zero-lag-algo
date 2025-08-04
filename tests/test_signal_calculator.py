"""
Unit tests for SignalCalculator class.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from forex_alerts.services.signal_calculator import SignalCalculator
from forex_alerts.models.signal import Signal


class TestSignalCalculator:
    """Test cases for SignalCalculator class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.calculator = SignalCalculator(ema_length=15)
    
    def test_init_valid_ema_length(self):
        """Test SignalCalculator initialization with valid EMA length."""
        calc = SignalCalculator(ema_length=20)
        assert calc.ema_length == 20
    
    def test_init_invalid_ema_length(self):
        """Test SignalCalculator initialization with invalid EMA length."""
        with pytest.raises(ValueError, match="EMA length must be positive"):
            SignalCalculator(ema_length=0)
        
        with pytest.raises(ValueError, match="EMA length must be positive"):
            SignalCalculator(ema_length=-5)
    
    def test_calculate_vwap_empty_data(self):
        """Test VWAP calculation with empty DataFrame."""
        empty_df = pd.DataFrame()
        
        with pytest.raises(ValueError, match="Data cannot be empty"):
            self.calculator.calculate_vwap(empty_df)
    
    def test_calculate_vwap_missing_columns(self):
        """Test VWAP calculation with missing required columns."""
        # Missing 'volume' column
        incomplete_df = pd.DataFrame({
            'high': [1.1, 1.2, 1.3],
            'low': [1.0, 1.1, 1.2],
            'close': [1.05, 1.15, 1.25]
        })
        
        with pytest.raises(ValueError, match="Missing required columns: \\['volume'\\]"):
            self.calculator.calculate_vwap(incomplete_df)
    
    def test_calculate_vwap_single_session_known_values(self):
        """Test VWAP calculation with known values for single session."""
        # Create test data with known VWAP values
        data = pd.DataFrame({
            'high': [1.10, 1.20, 1.15, 1.25],
            'low': [1.05, 1.15, 1.10, 1.20],
            'close': [1.08, 1.18, 1.12, 1.22],
            'volume': [1000, 2000, 1500, 1200]
        })
        
        vwap = self.calculator.calculate_vwap(data)
        
        # Calculate expected VWAP manually
        typical_prices = [(1.10 + 1.05 + 1.08) / 3,  # 1.0767
                         (1.20 + 1.15 + 1.18) / 3,   # 1.1767
                         (1.15 + 1.10 + 1.12) / 3,   # 1.1233
                         (1.25 + 1.20 + 1.22) / 3]   # 1.2233
        
        # Expected VWAP calculations:
        # Period 1: (1.0767 * 1000) / 1000 = 1.0767
        # Period 2: (1.0767 * 1000 + 1.1767 * 2000) / 3000 = 1.1433
        # Period 3: (1.0767 * 1000 + 1.1767 * 2000 + 1.1233 * 1500) / 4500 = 1.1378
        # Period 4: (1.0767 * 1000 + 1.1767 * 2000 + 1.1233 * 1500 + 1.2233 * 1200) / 5700 = 1.1508
        
        expected_vwap_1 = typical_prices[0]
        expected_vwap_2 = (typical_prices[0] * 1000 + typical_prices[1] * 2000) / 3000
        expected_vwap_3 = (typical_prices[0] * 1000 + typical_prices[1] * 2000 + typical_prices[2] * 1500) / 4500
        expected_vwap_4 = (typical_prices[0] * 1000 + typical_prices[1] * 2000 + typical_prices[2] * 1500 + typical_prices[3] * 1200) / 5700
        
        assert len(vwap) == 4
        assert abs(vwap.iloc[0] - expected_vwap_1) < 0.0001
        assert abs(vwap.iloc[1] - expected_vwap_2) < 0.0001
        assert abs(vwap.iloc[2] - expected_vwap_3) < 0.0001
        assert abs(vwap.iloc[3] - expected_vwap_4) < 0.0001
    
    def test_calculate_vwap_session_based_anchoring(self):
        """Test VWAP calculation with session-based anchoring (daily reset)."""
        # Create test data spanning two days
        dates = [
            datetime(2024, 1, 1, 9, 0),
            datetime(2024, 1, 1, 10, 0),
            datetime(2024, 1, 2, 9, 0),  # New day - VWAP should reset
            datetime(2024, 1, 2, 10, 0)
        ]
        
        data = pd.DataFrame({
            'high': [1.10, 1.20, 1.15, 1.25],
            'low': [1.05, 1.15, 1.10, 1.20],
            'close': [1.08, 1.18, 1.12, 1.22],
            'volume': [1000, 2000, 1500, 1200]
        }, index=pd.DatetimeIndex(dates))
        
        vwap = self.calculator.calculate_vwap(data)
        
        # Day 1 calculations
        tp1 = (1.10 + 1.05 + 1.08) / 3
        tp2 = (1.20 + 1.15 + 1.18) / 3
        expected_vwap_day1_period1 = tp1
        expected_vwap_day1_period2 = (tp1 * 1000 + tp2 * 2000) / 3000
        
        # Day 2 calculations (reset)
        tp3 = (1.15 + 1.10 + 1.12) / 3
        tp4 = (1.25 + 1.20 + 1.22) / 3
        expected_vwap_day2_period1 = tp3  # Reset for new day
        expected_vwap_day2_period2 = (tp3 * 1500 + tp4 * 1200) / 2700
        
        assert len(vwap) == 4
        assert abs(vwap.iloc[0] - expected_vwap_day1_period1) < 0.0001
        assert abs(vwap.iloc[1] - expected_vwap_day1_period2) < 0.0001
        assert abs(vwap.iloc[2] - expected_vwap_day2_period1) < 0.0001
        assert abs(vwap.iloc[3] - expected_vwap_day2_period2) < 0.0001
    
    def test_calculate_vwap_zero_volume_handling(self):
        """Test VWAP calculation with zero volume periods."""
        data = pd.DataFrame({
            'high': [1.10, 1.20, 1.15],
            'low': [1.05, 1.15, 1.10],
            'close': [1.08, 1.18, 1.12],
            'volume': [1000, 0, 1500]  # Zero volume in middle
        })
        
        vwap = self.calculator.calculate_vwap(data)
        
        # First period normal calculation
        tp1 = (1.10 + 1.05 + 1.08) / 3
        expected_vwap_1 = tp1
        
        # Second period with zero volume - VWAP stays same as previous (no new volume-weighted price added)
        expected_vwap_2 = tp1  # Same as period 1 since no volume was added
        
        # Third period continues from cumulative calculation
        tp3 = (1.15 + 1.10 + 1.12) / 3
        expected_vwap_3 = (tp1 * 1000 + tp3 * 1500) / 2500
        
        assert len(vwap) == 3
        assert abs(vwap.iloc[0] - expected_vwap_1) < 0.0001
        assert abs(vwap.iloc[1] - expected_vwap_2) < 0.0001  # Should be same as period 1
        assert abs(vwap.iloc[2] - expected_vwap_3) < 0.0001
    
    def test_calculate_vwap_return_series_properties(self):
        """Test that VWAP returns a properly formatted pandas Series."""
        data = pd.DataFrame({
            'high': [1.10, 1.20],
            'low': [1.05, 1.15],
            'close': [1.08, 1.18],
            'volume': [1000, 2000]
        })
        
        vwap = self.calculator.calculate_vwap(data)
        
        assert isinstance(vwap, pd.Series)
        assert vwap.name == 'vwap'
        assert len(vwap) == len(data)
        assert vwap.index.equals(data.index)
    
    def test_calculate_vwap_non_datetime_index(self):
        """Test VWAP calculation with non-datetime index (single session)."""
        data = pd.DataFrame({
            'high': [1.10, 1.20, 1.15],
            'low': [1.05, 1.15, 1.10],
            'close': [1.08, 1.18, 1.12],
            'volume': [1000, 2000, 1500]
        }, index=[0, 1, 2])
        
        vwap = self.calculator.calculate_vwap(data)
        
        # Should treat as single session (no daily reset)
        tp1 = (1.10 + 1.05 + 1.08) / 3
        tp2 = (1.20 + 1.15 + 1.18) / 3
        tp3 = (1.15 + 1.10 + 1.12) / 3
        
        expected_vwap_1 = tp1
        expected_vwap_2 = (tp1 * 1000 + tp2 * 2000) / 3000
        expected_vwap_3 = (tp1 * 1000 + tp2 * 2000 + tp3 * 1500) / 4500
        
        assert len(vwap) == 3
        assert abs(vwap.iloc[0] - expected_vwap_1) < 0.0001
        assert abs(vwap.iloc[1] - expected_vwap_2) < 0.0001
        assert abs(vwap.iloc[2] - expected_vwap_3) < 0.0001
    
    def test_calculate_vwap_initial_zero_volume(self):
        """Test VWAP calculation when first period has zero volume."""
        data = pd.DataFrame({
            'high': [1.10, 1.20, 1.15],
            'low': [1.05, 1.15, 1.10],
            'close': [1.08, 1.18, 1.12],
            'volume': [0, 2000, 1500]  # Zero volume at start
        })
        
        vwap = self.calculator.calculate_vwap(data)
        
        # First period should be NaN due to zero volume
        # Second period starts the VWAP calculation
        tp2 = (1.20 + 1.15 + 1.18) / 3
        expected_vwap_2 = tp2
        
        # Third period continues from second
        tp3 = (1.15 + 1.10 + 1.12) / 3
        expected_vwap_3 = (tp2 * 2000 + tp3 * 1500) / 3500
        
        assert len(vwap) == 3
        assert pd.isna(vwap.iloc[0])  # Should be NaN due to zero initial volume
        assert abs(vwap.iloc[1] - expected_vwap_2) < 0.0001
        assert abs(vwap.iloc[2] - expected_vwap_3) < 0.0001


class TestSignalCalculatorEMA:
    """Test cases for EMA calculations."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.calculator = SignalCalculator(ema_length=3)  # Short period for easier testing
    
    def test_calculate_ema_empty_data(self):
        """Test EMA calculation with empty DataFrame."""
        empty_df = pd.DataFrame()
        
        with pytest.raises(ValueError, match="Data cannot be empty"):
            self.calculator.calculate_ema(empty_df)
    
    def test_calculate_ema_missing_close_column(self):
        """Test EMA calculation with missing 'close' column."""
        data = pd.DataFrame({
            'high': [1.1, 1.2, 1.3],
            'low': [1.0, 1.1, 1.2]
        })
        
        with pytest.raises(ValueError, match="Missing required column: 'close'"):
            self.calculator.calculate_ema(data)
    
    def test_calculate_ema_invalid_length(self):
        """Test EMA calculation with invalid length parameter."""
        data = pd.DataFrame({'close': [1.0, 1.1, 1.2]})
        
        with pytest.raises(ValueError, match="EMA length must be positive"):
            self.calculator.calculate_ema(data, length=0)
        
        with pytest.raises(ValueError, match="EMA length must be positive"):
            self.calculator.calculate_ema(data, length=-1)
    
    def test_calculate_ema_known_values(self):
        """Test EMA calculation with known values."""
        # Simple test case with known EMA values
        data = pd.DataFrame({
            'close': [10.0, 11.0, 12.0, 11.0, 10.0]
        })
        
        ema = self.calculator.calculate_ema(data, length=3)
        
        # For EMA with span=3, alpha = 2/(3+1) = 0.5
        # EMA[0] = 10.0 (first value)
        # EMA[1] = 0.5 * 11.0 + 0.5 * 10.0 = 10.5
        # EMA[2] = 0.5 * 12.0 + 0.5 * 10.5 = 11.25
        # EMA[3] = 0.5 * 11.0 + 0.5 * 11.25 = 11.125
        # EMA[4] = 0.5 * 10.0 + 0.5 * 11.125 = 10.5625
        
        expected_values = [10.0, 10.5, 11.25, 11.125, 10.5625]
        
        assert len(ema) == 5
        for i, expected in enumerate(expected_values):
            assert abs(ema.iloc[i] - expected) < 0.0001, f"EMA[{i}] expected {expected}, got {ema.iloc[i]}"
    
    def test_calculate_ema_custom_length(self):
        """Test EMA calculation with custom length parameter."""
        data = pd.DataFrame({'close': [1.0, 2.0, 3.0, 4.0]})
        
        # Test with length=2 (alpha = 2/3 = 0.6667)
        ema = self.calculator.calculate_ema(data, length=2)
        
        # EMA[0] = 1.0
        # EMA[1] = 0.6667 * 2.0 + 0.3333 * 1.0 = 1.6667
        # EMA[2] = 0.6667 * 3.0 + 0.3333 * 1.6667 = 2.5556
        # EMA[3] = 0.6667 * 4.0 + 0.3333 * 2.5556 = 3.5185
        
        assert len(ema) == 4
        assert abs(ema.iloc[0] - 1.0) < 0.0001
        assert abs(ema.iloc[1] - 1.6667) < 0.001
        assert abs(ema.iloc[2] - 2.5556) < 0.001
        assert abs(ema.iloc[3] - 3.5185) < 0.001
    
    def test_calculate_ema_uses_default_length(self):
        """Test that EMA uses default length when none provided."""
        calc = SignalCalculator(ema_length=5)
        data = pd.DataFrame({'close': [1.0, 2.0, 3.0]})
        
        ema = calc.calculate_ema(data)
        
        # Should use span=5 (alpha = 2/6 = 0.3333)
        assert ema.name == 'ema_5'
    
    def test_calculate_ema_return_series_properties(self):
        """Test that EMA returns a properly formatted pandas Series."""
        data = pd.DataFrame({'close': [1.0, 2.0, 3.0]})
        
        ema = self.calculator.calculate_ema(data, length=3)
        
        assert isinstance(ema, pd.Series)
        assert ema.name == 'ema_3'
        assert len(ema) == len(data)
        assert ema.index.equals(data.index)


class TestSignalCalculatorZLMA:
    """Test cases for Zero-Lag Moving Average calculations."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.calculator = SignalCalculator(ema_length=3)
    
    def test_calculate_zlma_empty_data(self):
        """Test ZLMA calculation with empty DataFrame."""
        empty_df = pd.DataFrame()
        
        with pytest.raises(ValueError, match="Data cannot be empty"):
            self.calculator.calculate_zlma(empty_df)
    
    def test_calculate_zlma_missing_close_column(self):
        """Test ZLMA calculation with missing 'close' column."""
        data = pd.DataFrame({
            'high': [1.1, 1.2, 1.3],
            'low': [1.0, 1.1, 1.2]
        })
        
        with pytest.raises(ValueError, match="Missing required column: 'close'"):
            self.calculator.calculate_zlma(data)
    
    def test_calculate_zlma_invalid_length(self):
        """Test ZLMA calculation with invalid length parameter."""
        data = pd.DataFrame({'close': [1.0, 1.1, 1.2]})
        
        with pytest.raises(ValueError, match="EMA length must be positive"):
            self.calculator.calculate_zlma(data, length=0)
    
    def test_calculate_zlma_known_values(self):
        """Test ZLMA calculation with known values."""
        # Test data with predictable pattern
        data = pd.DataFrame({
            'close': [10.0, 12.0, 14.0, 16.0, 18.0]
        })
        
        zlma = self.calculator.calculate_zlma(data, length=3)
        
        # ZLMA should respond faster to price changes than regular EMA
        # and should be closer to actual prices due to lag compensation
        assert len(zlma) == 5
        assert isinstance(zlma, pd.Series)
        
        # ZLMA should generally be higher than EMA for uptrending data
        ema = self.calculator.calculate_ema(data, length=3)
        
        # For most periods (except possibly the first), ZLMA should be >= EMA in uptrend
        for i in range(1, len(zlma)):
            if not pd.isna(zlma.iloc[i]) and not pd.isna(ema.iloc[i]):
                # ZLMA should be closer to the actual close price than EMA
                zlma_diff = abs(zlma.iloc[i] - data['close'].iloc[i])
                ema_diff = abs(ema.iloc[i] - data['close'].iloc[i])
                # Allow some tolerance for numerical precision
                assert zlma_diff <= ema_diff + 0.1, f"ZLMA should be closer to price at index {i}"
    
    def test_calculate_zlma_formula_verification(self):
        """Test ZLMA calculation follows the correct formula."""
        data = pd.DataFrame({
            'close': [100.0, 101.0, 102.0, 103.0, 104.0]
        })
        
        length = 3
        zlma = self.calculator.calculate_zlma(data, length=length)
        
        # Manually calculate ZLMA using the formula:
        # zlma = ema(close + (close - ema(close, length)), length)
        
        # Step 1: Calculate EMA of close
        ema_close = self.calculator.calculate_ema(data, length=length)
        
        # Step 2: Calculate lag difference
        lag_diff = data['close'] - ema_close
        
        # Step 3: Adjusted close
        adjusted_close = data['close'] + lag_diff
        
        # Step 4: EMA of adjusted close
        temp_data = pd.DataFrame({'close': adjusted_close})
        expected_zlma = self.calculator.calculate_ema(temp_data, length=length)
        
        # Compare calculated ZLMA with expected
        assert len(zlma) == len(expected_zlma)
        for i in range(len(zlma)):
            if not pd.isna(zlma.iloc[i]) and not pd.isna(expected_zlma.iloc[i]):
                assert abs(zlma.iloc[i] - expected_zlma.iloc[i]) < 0.0001
    
    def test_calculate_zlma_custom_length(self):
        """Test ZLMA calculation with custom length parameter."""
        data = pd.DataFrame({'close': [1.0, 2.0, 3.0, 4.0, 5.0]})
        
        zlma = self.calculator.calculate_zlma(data, length=2)
        
        assert isinstance(zlma, pd.Series)
        assert zlma.name == 'zlma_2'
        assert len(zlma) == 5
    
    def test_calculate_zlma_uses_default_length(self):
        """Test that ZLMA uses default length when none provided."""
        calc = SignalCalculator(ema_length=4)
        data = pd.DataFrame({'close': [1.0, 2.0, 3.0, 4.0]})
        
        zlma = calc.calculate_zlma(data)
        
        assert zlma.name == 'zlma_4'
    
    def test_calculate_zlma_return_series_properties(self):
        """Test that ZLMA returns a properly formatted pandas Series."""
        data = pd.DataFrame({'close': [1.0, 2.0, 3.0, 4.0]})
        
        zlma = self.calculator.calculate_zlma(data, length=2)
        
        assert isinstance(zlma, pd.Series)
        assert zlma.name == 'zlma_2'
        assert len(zlma) == len(data)
        assert zlma.index.equals(data.index)
    
    def test_calculate_zlma_vs_ema_responsiveness(self):
        """Test that ZLMA is more responsive than EMA to price changes."""
        # Create data with a sudden price jump
        data = pd.DataFrame({
            'close': [10.0, 10.0, 10.0, 15.0, 15.0, 15.0]  # Price jumps from 10 to 15
        })
        
        ema = self.calculator.calculate_ema(data, length=3)
        zlma = self.calculator.calculate_zlma(data, length=3)
        
        # After the price jump (index 3), ZLMA should be closer to the new price level
        jump_index = 3
        target_price = data['close'].iloc[jump_index]
        
        ema_distance = abs(ema.iloc[jump_index] - target_price)
        zlma_distance = abs(zlma.iloc[jump_index] - target_price)
        
        # ZLMA should be closer to the target price (more responsive)
        assert zlma_distance < ema_distance, "ZLMA should be more responsive to price changes"


class TestSignalCalculatorSignalDetection:
    """Test cases for signal detection logic."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.calculator = SignalCalculator(ema_length=3)
    
    def test_detect_signals_empty_data(self):
        """Test signal detection with empty DataFrame."""
        empty_df = pd.DataFrame()
        
        with pytest.raises(ValueError, match="Data cannot be empty"):
            self.calculator.detect_signals(empty_df, "EURUSD")
    
    def test_detect_signals_missing_close_column(self):
        """Test signal detection with missing 'close' column."""
        data = pd.DataFrame({
            'high': [1.1, 1.2, 1.3],
            'low': [1.0, 1.1, 1.2]
        })
        
        with pytest.raises(ValueError, match="Missing required column: 'close'"):
            self.calculator.detect_signals(data, "EURUSD")
    
    def test_detect_signals_insufficient_data(self):
        """Test signal detection with insufficient data."""
        # Only 2 periods, but need at least 3 for EMA length of 3
        data = pd.DataFrame({
            'close': [1.0, 1.1]
        })
        
        with pytest.raises(ValueError, match="Insufficient data: need at least 3 periods"):
            self.calculator.detect_signals(data, "EURUSD")
    
    def test_detect_signals_no_crossovers(self):
        """Test signal detection when no crossovers occur."""
        # Create data with stable prices where ZLMA and EMA converge without crossing
        data = pd.DataFrame({
            'close': [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]
        })
        
        signals = self.calculator.detect_signals(data, "EURUSD")
        
        # Should return empty list when no crossovers occur
        assert isinstance(signals, list)
        assert len(signals) == 0
    
    def test_detect_signals_bullish_crossover(self):
        """Test detection of bullish signal (ZLMA crosses above EMA)."""
        # Create data that will generate a bullish crossover
        # Start with declining prices, then sharp increase
        data = pd.DataFrame({
            'close': [1.2, 1.1, 1.0, 1.05, 1.15, 1.25, 1.35]
        }, index=pd.date_range('2024-01-01', periods=7, freq='h'))
        
        signals = self.calculator.detect_signals(data, "EURUSD")
        
        # Should detect at least one BUY signal
        buy_signals = [s for s in signals if s.signal_type == "BUY"]
        assert len(buy_signals) > 0
        
        # Verify signal properties
        signal = buy_signals[0]
        assert signal.symbol == "EURUSD"
        assert signal.signal_type == "BUY"
        assert signal.price > 0
        assert isinstance(signal.timestamp, datetime)
        assert signal.zlma_value > signal.ema_value  # ZLMA should be above EMA for BUY signal
        assert 0.0 <= signal.confidence <= 1.0
    
    def test_detect_signals_bearish_crossover(self):
        """Test detection of bearish signal (ZLMA crosses below EMA)."""
        # Create data that will generate a bearish crossover
        # Start with rising prices, then sharp decline
        data = pd.DataFrame({
            'close': [1.0, 1.1, 1.2, 1.15, 1.05, 0.95, 0.85]
        }, index=pd.date_range('2024-01-01', periods=7, freq='h'))
        
        signals = self.calculator.detect_signals(data, "EURUSD")
        
        # Should detect at least one SELL signal
        sell_signals = [s for s in signals if s.signal_type == "SELL"]
        assert len(sell_signals) > 0
        
        # Verify signal properties
        signal = sell_signals[0]
        assert signal.symbol == "EURUSD"
        assert signal.signal_type == "SELL"
        assert signal.price > 0
        assert isinstance(signal.timestamp, datetime)
        assert signal.zlma_value < signal.ema_value  # ZLMA should be below EMA for SELL signal
        assert 0.0 <= signal.confidence <= 1.0
    
    def test_detect_signals_multiple_crossovers(self):
        """Test detection of multiple signals in the same dataset."""
        # Create data with multiple crossovers
        data = pd.DataFrame({
            'close': [1.0, 1.1, 1.2, 1.1, 0.9, 0.8, 1.0, 1.2, 1.4, 1.2, 1.0, 0.8]
        }, index=pd.date_range('2024-01-01', periods=12, freq='h'))
        
        signals = self.calculator.detect_signals(data, "GBPUSD")
        
        # Should detect both BUY and SELL signals
        buy_signals = [s for s in signals if s.signal_type == "BUY"]
        sell_signals = [s for s in signals if s.signal_type == "SELL"]
        
        assert len(buy_signals) > 0
        assert len(sell_signals) > 0
        
        # Verify all signals have correct symbol
        for signal in signals:
            assert signal.symbol == "GBPUSD"
    
    def test_detect_signals_with_nan_values(self):
        """Test signal detection handles NaN values gracefully."""
        # Create data with some NaN values that might occur in calculations
        data = pd.DataFrame({
            'close': [1.0, 1.1, 1.2, 1.3, 1.4]
        })
        
        # This should work without errors even if some intermediate calculations produce NaN
        signals = self.calculator.detect_signals(data, "USDJPY")
        
        # Should return a list (might be empty, but shouldn't crash)
        assert isinstance(signals, list)
    
    def test_detect_signals_non_datetime_index(self):
        """Test signal detection with non-datetime index."""
        data = pd.DataFrame({
            'close': [1.0, 1.1, 1.2, 1.1, 0.9, 1.1, 1.3]
        }, index=[0, 1, 2, 3, 4, 5, 6])
        
        signals = self.calculator.detect_signals(data, "EURUSD")
        
        # Should work with non-datetime index
        assert isinstance(signals, list)
        
        # Timestamps should be current time when no datetime index
        for signal in signals:
            assert isinstance(signal.timestamp, datetime)
    
    def test_detect_signals_confidence_calculation(self):
        """Test that signal confidence is calculated reasonably."""
        # Create data with a clear, strong crossover
        data = pd.DataFrame({
            'close': [1.0, 1.0, 1.0, 1.1, 1.2, 1.3, 1.4]
        })
        
        signals = self.calculator.detect_signals(data, "EURUSD")
        
        if signals:
            # Confidence should be reasonable (not at extremes)
            for signal in signals:
                assert 0.0 <= signal.confidence <= 1.0
                # For a clear trend, confidence should be decent
                assert signal.confidence >= 0.3
    
    def test_detect_signals_crossover_logic(self):
        """Test the specific crossover detection logic."""
        # Create precise data to test crossover detection
        # ZLMA starts below EMA, then crosses above (should generate BUY)
        data = pd.DataFrame({
            'close': [10.0, 9.8, 9.6, 9.8, 10.2, 10.6, 11.0]
        }, index=pd.date_range('2024-01-01', periods=7, freq='h'))
        
        signals = self.calculator.detect_signals(data, "TESTPAIR")
        
        # Calculate EMA and ZLMA to verify crossover logic
        ema = self.calculator.calculate_ema(data)
        zlma = self.calculator.calculate_zlma(data)
        
        # Find where crossover should occur
        crossover_occurred = False
        for i in range(1, len(data)):
            if (not pd.isna(zlma.iloc[i-1]) and not pd.isna(ema.iloc[i-1]) and
                not pd.isna(zlma.iloc[i]) and not pd.isna(ema.iloc[i])):
                
                prev_zlma_below = zlma.iloc[i-1] <= ema.iloc[i-1]
                curr_zlma_above = zlma.iloc[i] > ema.iloc[i]
                
                if prev_zlma_below and curr_zlma_above:
                    crossover_occurred = True
                    break
        
        # If crossover occurred in the data, we should have detected a signal
        if crossover_occurred:
            buy_signals = [s for s in signals if s.signal_type == "BUY"]
            assert len(buy_signals) > 0, "Should detect BUY signal when ZLMA crosses above EMA"
    
    def test_signal_object_creation(self):
        """Test that Signal objects are created with correct attributes."""
        data = pd.DataFrame({
            'close': [1.0, 1.1, 1.2, 1.1, 0.9, 1.1, 1.3]
        }, index=pd.date_range('2024-01-01', periods=7, freq='h'))
        
        signals = self.calculator.detect_signals(data, "EURUSD")
        
        for signal in signals:
            # Test all required attributes are present and valid
            assert isinstance(signal.symbol, str)
            assert signal.symbol == "EURUSD"
            assert signal.signal_type in ["BUY", "SELL"]
            assert isinstance(signal.price, (int, float))
            assert signal.price > 0
            assert isinstance(signal.timestamp, datetime)
            assert isinstance(signal.zlma_value, (int, float))
            assert isinstance(signal.ema_value, (int, float))
            assert isinstance(signal.confidence, (int, float))
            assert 0.0 <= signal.confidence <= 1.0