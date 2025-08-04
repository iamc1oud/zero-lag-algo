"""
Configuration management service for the Forex Alert System.
"""

import json
import os
from pathlib import Path
from typing import Dict, List, Any, Optional
import yfinance as yf
from ..models.config import Config


class ConfigManager:
    """
    Manages configuration loading, saving, validation, and symbol checking.
    
    Handles user configuration persistence, forex symbol validation using yfinance,
    and provides default configuration values.
    """
    
    DEFAULT_CONFIG_DIR = Path.home() / ".forex_alerts"
    DEFAULT_CONFIG_FILE = "config.json"
    CONFIG_VERSION = "1.0"
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize ConfigManager with optional custom config path.
        
        Args:
            config_path: Optional custom path to configuration file
        """
        if config_path:
            self.config_path = Path(config_path)
        else:
            # Use environment variable if set, otherwise use default
            config_env = os.getenv('FOREX_ALERTS_CONFIG')
            if config_env:
                self.config_path = Path(config_env)
            else:
                self.config_path = self.DEFAULT_CONFIG_DIR / self.DEFAULT_CONFIG_FILE
        
        # Ensure config directory exists
        self.config_path.parent.mkdir(parents=True, exist_ok=True)
    
    def load_config(self) -> Config:
        """
        Load configuration from file or create default if file doesn't exist.
        
        Returns:
            Config: Loaded or default configuration
            
        Raises:
            ValueError: If configuration file is invalid
            FileNotFoundError: If config file exists but cannot be read
        """
        if not self.config_path.exists():
            # Create default config file
            default_config = self.get_default_config()
            self.save_config(default_config)
            return default_config
        
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config_data = json.load(f)
            
            # Handle migration if needed
            original_version = config_data.get('_version', '0.0')
            config_data = self._migrate_config(config_data)
            
            # Merge with defaults to ensure all fields are present
            config_data = self._merge_with_defaults(config_data)
            
            # Filter out metadata fields before creating Config object
            config_fields = {k: v for k, v in config_data.items() 
                           if not k.startswith('_')}
            
            # Validate and create Config object
            config = Config.from_dict(config_fields)
            
            # Save back to file if migration occurred or if merging added new fields
            if original_version == '0.0' or len(config_data) > len(config_fields):
                # Preserve metadata when saving
                metadata = {k: v for k, v in config_data.items() if k.startswith('_')}
                self.save_config(config, preserve_metadata=metadata)
            
            return config
            
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in configuration file: {e}")
        except Exception as e:
            raise ValueError(f"Error loading configuration: {e}")
    
    def save_config(self, config: Config, preserve_metadata: Dict[str, Any] = None) -> None:
        """
        Save configuration to file with version information.
        
        Args:
            config: Configuration object to save
            preserve_metadata: Optional metadata to preserve from previous config
            
        Raises:
            IOError: If configuration cannot be saved
        """
        try:
            config_data = config.to_dict()
            
            # Preserve existing metadata if provided
            if preserve_metadata:
                for key, value in preserve_metadata.items():
                    if key.startswith('_'):
                        config_data[key] = value
            
            # Set version and timestamps
            config_data['_version'] = self.CONFIG_VERSION
            config_data['_created'] = config_data.get('_created', self._get_timestamp())
            config_data['_updated'] = self._get_timestamp()
            
            with open(self.config_path, 'w', encoding='utf-8') as f:
                json.dump(config_data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            raise IOError(f"Error saving configuration: {e}")
    
    def validate_symbols(self, symbols: List[str]) -> List[str]:
        """
        Validate forex symbols using yfinance ticker validation.
        
        Args:
            symbols: List of forex symbols to validate
            
        Returns:
            List[str]: List of valid symbols with proper formatting
            
        Raises:
            ValueError: If no valid symbols are found
        """
        if not symbols:
            raise ValueError("Symbol list cannot be empty")
        
        valid_symbols = []
        invalid_symbols = []
        
        for symbol in symbols:
            # Clean and format symbol
            clean_symbol = symbol.strip().upper()
            
            # Add =X suffix if not present for forex pairs
            if not clean_symbol.endswith('=X') and len(clean_symbol) == 6:
                clean_symbol += '=X'
            
            # Validate symbol with yfinance
            if self._validate_single_symbol(clean_symbol):
                valid_symbols.append(clean_symbol)
            else:
                invalid_symbols.append(symbol)
        
        if not valid_symbols:
            raise ValueError(f"No valid symbols found. Invalid symbols: {invalid_symbols}")
        
        if invalid_symbols:
            print(f"Warning: Invalid symbols ignored: {invalid_symbols}")
        
        return valid_symbols
    
    def _validate_single_symbol(self, symbol: str) -> bool:
        """
        Validate a single symbol using yfinance.
        
        Args:
            symbol: Symbol to validate
            
        Returns:
            bool: True if symbol is valid, False otherwise
        """
        try:
            ticker = yf.Ticker(symbol)
            # Try to get basic info to validate symbol exists
            info = ticker.info
            
            # Check if we got valid data back
            if not info or 'symbol' not in info:
                return False
            
            # Additional check: try to get recent data
            hist = ticker.history(period="1d", interval="1h")
            return not hist.empty
            
        except Exception:
            # Any exception means the symbol is invalid
            return False
    
    def get_default_config(self) -> Config:
        """
        Get default configuration values.
        
        Returns:
            Config: Default configuration object
        """
        return Config(
            symbols=[],  # Will be populated by user input
            ema_length=15,
            update_frequency=60,
            notification_methods=["console"],
            email_config=None,
            data_retention_hours=24
        )
    
    def update_config(self, **kwargs) -> Config:
        """
        Update existing configuration with new values.
        
        Args:
            **kwargs: Configuration parameters to update
            
        Returns:
            Config: Updated configuration object
        """
        current_config = self.load_config()
        config_dict = current_config.to_dict()
        
        # Update with new values
        config_dict.update(kwargs)
        
        # Validate symbols if they were updated
        if 'symbols' in kwargs:
            config_dict['symbols'] = self.validate_symbols(config_dict['symbols'])
        
        # Create new config object and save
        updated_config = Config.from_dict(config_dict)
        self.save_config(updated_config)
        
        return updated_config
    
    def get_config_path(self) -> Path:
        """
        Get the current configuration file path.
        
        Returns:
            Path: Path to configuration file
        """
        return self.config_path
    
    def _migrate_config(self, config_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Migrate configuration from older versions to current version.
        
        Args:
            config_data: Raw configuration data from file
            
        Returns:
            Dict[str, Any]: Migrated configuration data
        """
        # Get current version from config, default to "1.0" if not present
        config_version = config_data.get('version', '1.0')
        
        # Currently only version 1.0 exists, but this structure allows for future migrations
        if config_version == '1.0':
            # No migration needed for current version
            pass
        
        # Add version to config data if not present
        config_data['version'] = self.CONFIG_VERSION
        
        return config_data
    

    

    
    def _merge_with_defaults(self, config_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Merge loaded configuration with default values to ensure all fields are present.
        
        Args:
            config_data: Configuration data from file
            
        Returns:
            Dict[str, Any]: Configuration data merged with defaults
        """
        default_config = self.get_default_config()
        default_dict = default_config.to_dict()
        
        # Start with defaults and update with loaded values
        merged_config = default_dict.copy()
        
        # Remove version from defaults as it's handled separately
        if 'version' in merged_config:
            del merged_config['version']
        
        # Update with loaded values, preserving version
        for key, value in config_data.items():
            if key != 'version':  # Version is handled in migration
                merged_config[key] = value
        
        return merged_config
    
    def _get_timestamp(self) -> str:
        """
        Get current timestamp in ISO format.
        
        Returns:
            str: Current timestamp
        """
        from datetime import datetime
        return datetime.now().isoformat()
    
    def reset_to_defaults(self) -> Config:
        """
        Reset configuration to default values and save to file.
        
        Returns:
            Config: Default configuration object
        """
        default_config = self.get_default_config()
        self.save_config(default_config)
        return default_config
    
    def backup_config(self, backup_suffix: str = None) -> Path:
        """
        Create a backup of the current configuration file.
        
        Args:
            backup_suffix: Optional suffix for backup filename
            
        Returns:
            Path: Path to backup file
            
        Raises:
            FileNotFoundError: If config file doesn't exist
            IOError: If backup cannot be created
        """
        if not self.config_path.exists():
            raise FileNotFoundError("Configuration file does not exist")
        
        if backup_suffix is None:
            from datetime import datetime
            backup_suffix = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        backup_path = self.config_path.with_suffix(f".backup_{backup_suffix}.json")
        
        try:
            import shutil
            shutil.copy2(self.config_path, backup_path)
            return backup_path
        except Exception as e:
            raise IOError(f"Error creating backup: {e}")