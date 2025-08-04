"""
Signal calculator for technical analysis indicators and signal detection.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timezone
from typing import List, Optional
from ..models.signal import Signal


class SignalCalculator:
    """
    Calculates technical analysis indicators and detects trading signals.
    
    This class implements VWAP, EMA, and Zero-Lag Moving Average calculations
    along with signal detection logic for crossovers.
    """
    
    def __init__(self, ema_length: int = 15):
        """
        Initialize the SignalCalculator.
        
        Args:
            ema_length: Length parameter for EMA calculations (default: 15)
        """
        if ema_length <= 0:
            raise ValueError("EMA length must be positive")
        
        self.ema_length = ema_length
    
    def calculate_vwap(self, data: pd.DataFrame) -> pd.Series:
        """
        Calculate Volume Weighted Average Price (VWAP) with session-based anchoring.
        
        VWAP is calculated as the cumulative sum of (price * volume) divided by
        cumulative volume, reset daily for session-based anchoring.
        
        Args:
            data: DataFrame with columns ['high', 'low', 'close', 'volume']
                 with DatetimeIndex for session-based anchoring
        
        Returns:
            pd.Series: VWAP values
        
        Raises:
            ValueError: If required columns are missing or data is empty
        """
        if data.empty:
            raise ValueError("Data cannot be empty")
        
        required_columns = ['high', 'low', 'close', 'volume']
        missing_columns = [col for col in required_columns if col not in data.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # Calculate typical price (HLC/3)
        typical_price = (data['high'] + data['low'] + data['close']) / 3
        
        # Create a copy to avoid modifying original data
        df = data.copy()
        df['typical_price'] = typical_price
        df['price_volume'] = typical_price * data['volume']
        
        # Group by date for session-based anchoring (daily reset)
        if isinstance(data.index, pd.DatetimeIndex):
            df['date'] = data.index.date
        else:
            # If no datetime index, treat as single session
            df['date'] = 0
        
        # Calculate VWAP for each session
        vwap_values = []
        for date, group in df.groupby('date'):
            # Calculate cumulative sums for this session
            cum_price_volume = group['price_volume'].cumsum()
            cum_volume = group['volume'].cumsum()
            
            # Handle zero volume periods - set to NaN where cumulative volume is 0
            session_vwap = cum_price_volume / cum_volume
            session_vwap = session_vwap.where(cum_volume > 0, np.nan)
            vwap_values.extend(session_vwap.values)
        
        # Create series with original index
        vwap_series = pd.Series(vwap_values, index=data.index, name='vwap')
        
        return vwap_series
    
    def calculate_ema(self, data: pd.DataFrame, length: Optional[int] = None) -> pd.Series:
        """
        Calculate Exponential Moving Average (EMA).
        
        EMA gives more weight to recent prices and responds more quickly to price changes
        than a simple moving average.
        
        Args:
            data: DataFrame with 'close' column
            length: EMA period length (uses self.ema_length if not provided)
        
        Returns:
            pd.Series: EMA values
        
        Raises:
            ValueError: If 'close' column is missing or data is empty
        """
        if data.empty:
            raise ValueError("Data cannot be empty")
        
        if 'close' not in data.columns:
            raise ValueError("Missing required column: 'close'")
        
        ema_length = length if length is not None else self.ema_length
        
        if ema_length <= 0:
            raise ValueError("EMA length must be positive")
        
        # Calculate EMA using pandas ewm (exponentially weighted moving average)
        # Alpha = 2 / (length + 1) is the standard EMA smoothing factor
        ema = data['close'].ewm(span=ema_length, adjust=False).mean()
        ema.name = f'ema_{ema_length}'
        
        return ema
    
    def calculate_zlma(self, data: pd.DataFrame, length: Optional[int] = None) -> pd.Series:
        """
        Calculate Zero-Lag Moving Average (ZLMA).
        
        ZLMA attempts to eliminate the lag inherent in moving averages by using
        the formula: zlma = ema(close + (close - ema(close, length)), length)
        
        Args:
            data: DataFrame with 'close' column
            length: EMA period length for ZLMA calculation (uses self.ema_length if not provided)
        
        Returns:
            pd.Series: ZLMA values
        
        Raises:
            ValueError: If 'close' column is missing or data is empty
        """
        if data.empty:
            raise ValueError("Data cannot be empty")
        
        if 'close' not in data.columns:
            raise ValueError("Missing required column: 'close'")
        
        ema_length = length if length is not None else self.ema_length
        
        if ema_length <= 0:
            raise ValueError("EMA length must be positive")
        
        # Step 1: Calculate EMA of close prices
        ema_close = self.calculate_ema(data, ema_length)
        
        # Step 2: Calculate the lag difference (close - ema)
        lag_difference = data['close'] - ema_close
        
        # Step 3: Add the lag difference to close prices
        adjusted_close = data['close'] + lag_difference
        
        # Step 4: Calculate EMA of the adjusted close prices
        # Create temporary DataFrame for the adjusted close
        temp_data = pd.DataFrame({'close': adjusted_close}, index=data.index)
        zlma = self.calculate_ema(temp_data, ema_length)
        zlma.name = f'zlma_{ema_length}'
        
        return zlma
    
    def detect_signals(self, data: pd.DataFrame, symbol: str) -> List[Signal]:
        """
        Detect bullish and bearish signals based on ZLMA and EMA crossovers.
        
        Generates BUY signals when ZLMA crosses above EMA (bullish crossover)
        and SELL signals when ZLMA crosses below EMA (bearish crossover).
        
        Args:
            data: DataFrame with OHLCV data
            symbol: The forex symbol being analyzed
        
        Returns:
            List[Signal]: List of detected signals
        
        Raises:
            ValueError: If required columns are missing or data is insufficient
        """
        if data.empty:
            raise ValueError("Data cannot be empty")
        
        if 'close' not in data.columns:
            raise ValueError("Missing required column: 'close'")
        
        if len(data) < self.ema_length:
            raise ValueError(f"Insufficient data: need at least {self.ema_length} periods")
        
        # Calculate EMA and ZLMA
        ema = self.calculate_ema(data)
        zlma = self.calculate_zlma(data)
        
        signals = []
        
        # Need at least 2 periods to detect crossovers
        for i in range(1, len(data)):
            current_zlma = zlma.iloc[i]
            current_ema = ema.iloc[i]
            prev_zlma = zlma.iloc[i-1]
            prev_ema = ema.iloc[i-1]
            
            # Skip if any values are NaN
            if pd.isna(current_zlma) or pd.isna(current_ema) or pd.isna(prev_zlma) or pd.isna(prev_ema):
                continue
            
            # Detect bullish crossover: ZLMA crosses above EMA
            if prev_zlma <= prev_ema and current_zlma > current_ema:
                signal = Signal(
                    symbol=symbol,
                    signal_type="BUY",
                    price=data['close'].iloc[i],
                    timestamp=data.index[i] if isinstance(data.index, pd.DatetimeIndex) else datetime.now(timezone.utc),
                    zlma_value=current_zlma,
                    ema_value=current_ema,
                    confidence=self._calculate_signal_confidence(current_zlma, current_ema, prev_zlma, prev_ema)
                )
                signals.append(signal)
            
            # Detect bearish crossover: ZLMA crosses below EMA
            elif prev_zlma >= prev_ema and current_zlma < current_ema:
                signal = Signal(
                    symbol=symbol,
                    signal_type="SELL",
                    price=data['close'].iloc[i],
                    timestamp=data.index[i] if isinstance(data.index, pd.DatetimeIndex) else datetime.now(timezone.utc),
                    zlma_value=current_zlma,
                    ema_value=current_ema,
                    confidence=self._calculate_signal_confidence(current_zlma, current_ema, prev_zlma, prev_ema)
                )
                signals.append(signal)
        
        return signals
    
    def _calculate_signal_confidence(self, current_zlma: float, current_ema: float, 
                                   prev_zlma: float, prev_ema: float) -> float:
        """
        Calculate confidence level for a signal based on crossover strength.
        
        Higher confidence is given to signals with:
        - Larger separation between ZLMA and EMA after crossover
        - Cleaner crossover (less noise in previous periods)
        
        Args:
            current_zlma: Current ZLMA value
            current_ema: Current EMA value
            prev_zlma: Previous ZLMA value
            prev_ema: Previous EMA value
        
        Returns:
            float: Confidence level between 0.0 and 1.0
        """
        # Calculate the separation after crossover (as percentage of price)
        current_separation = abs(current_zlma - current_ema)
        avg_price = (current_zlma + current_ema) / 2
        separation_pct = current_separation / avg_price if avg_price > 0 else 0
        
        # Calculate how decisive the crossover was
        prev_separation = abs(prev_zlma - prev_ema)
        prev_avg_price = (prev_zlma + prev_ema) / 2
        prev_separation_pct = prev_separation / prev_avg_price if prev_avg_price > 0 else 0
        
        # Base confidence starts at 0.5
        confidence = 0.5
        
        # Increase confidence based on current separation (up to +0.3)
        confidence += min(separation_pct * 100, 0.3)  # Scale by 100 for typical forex spreads
        
        # Increase confidence if crossover is decisive (up to +0.2)
        if prev_separation_pct > 0:
            decisiveness = separation_pct / prev_separation_pct
            confidence += min(decisiveness * 0.1, 0.2)
        
        # Ensure confidence stays within bounds
        return min(max(confidence, 0.0), 1.0)