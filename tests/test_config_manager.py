"""
Unit tests for ConfigManager class.
"""

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import pandas as pd

from forex_alerts.services.config_manager import ConfigManager
from forex_alerts.models.config import Config


class TestConfigManager(unittest.TestCase):
    """Test cases for ConfigManager class."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Create temporary directory for test configs
        self.temp_dir = tempfile.mkdtemp()
        self.test_config_path = Path(self.temp_dir) / "test_config.json"
        self.config_manager = ConfigManager(str(self.test_config_path))
    
    def tearDown(self):
        """Clean up test fixtures."""
        # Remove test config file if it exists
        if self.test_config_path.exists():
            self.test_config_path.unlink()
        
        # Remove any backup files
        temp_path = Path(self.temp_dir)
        for backup_file in temp_path.glob("*.backup_*.json"):
            backup_file.unlink()
        
        # Remove temp directory
        if temp_path.exists():
            import shutil
            shutil.rmtree(self.temp_dir)
    
    def test_init_default_path(self):
        """Test ConfigManager initialization with default path."""
        manager = ConfigManager()
        expected_path = Path.home() / ".forex_alerts" / "config.json"
        self.assertEqual(manager.config_path, expected_path)
    
    def test_init_custom_path(self):
        """Test ConfigManager initialization with custom path."""
        custom_path = str(Path(self.temp_dir) / "custom_config.json")
        manager = ConfigManager(custom_path)
        self.assertEqual(manager.config_path, Path(custom_path))
    
    @patch.dict(os.environ, {'FOREX_ALERTS_CONFIG': str(Path(tempfile.mkdtemp()) / 'config.json')})
    def test_init_env_path(self):
        """Test ConfigManager initialization with environment variable path."""
        manager = ConfigManager()
        expected_path = Path(os.environ['FOREX_ALERTS_CONFIG'])
        self.assertEqual(manager.config_path, expected_path)
    
    def test_get_default_config(self):
        """Test getting default configuration."""
        default_config = self.config_manager.get_default_config()
        
        self.assertIsInstance(default_config, Config)
        self.assertEqual(default_config.symbols, [])
        self.assertEqual(default_config.ema_length, 15)
        self.assertEqual(default_config.update_frequency, 60)
        self.assertEqual(default_config.notification_methods, ["console"])
        self.assertIsNone(default_config.email_config)
        self.assertEqual(default_config.data_retention_hours, 24)
    
    def test_save_and_load_config(self):
        """Test saving and loading configuration."""
        # Create test config
        test_config = Config(
            symbols=["EURUSD=X", "GBPUSD=X"],
            ema_length=20,
            update_frequency=30,
            notification_methods=["console", "email"],
            data_retention_hours=48
        )
        
        # Save config
        self.config_manager.save_config(test_config)
        self.assertTrue(self.test_config_path.exists())
        
        # Load config
        loaded_config = self.config_manager.load_config()
        
        self.assertEqual(loaded_config.symbols, test_config.symbols)
        self.assertEqual(loaded_config.ema_length, test_config.ema_length)
        self.assertEqual(loaded_config.update_frequency, test_config.update_frequency)
        self.assertEqual(loaded_config.notification_methods, test_config.notification_methods)
        self.assertEqual(loaded_config.data_retention_hours, test_config.data_retention_hours)
    
    def test_load_config_creates_default_if_not_exists(self):
        """Test that load_config creates default config if file doesn't exist."""
        # Ensure file doesn't exist
        self.assertFalse(self.test_config_path.exists())
        
        # Load config should create default
        config = self.config_manager.load_config()
        
        # File should now exist
        self.assertTrue(self.test_config_path.exists())
        
        # Should be default config
        default_config = self.config_manager.get_default_config()
        self.assertEqual(config.ema_length, default_config.ema_length)
        self.assertEqual(config.update_frequency, default_config.update_frequency)
    
    def test_load_config_invalid_json(self):
        """Test loading configuration with invalid JSON."""
        # Write invalid JSON to config file
        with open(self.test_config_path, 'w') as f:
            f.write("invalid json content")
        
        with self.assertRaises(ValueError) as context:
            self.config_manager.load_config()
        
        self.assertIn("Invalid JSON", str(context.exception))
    
    def test_save_config_io_error(self):
        """Test save_config with IO error."""
        # Create config
        test_config = self.config_manager.get_default_config()
        
        # Mock open to raise IOError
        with patch('builtins.open', side_effect=IOError("Permission denied")):
            with self.assertRaises(IOError) as context:
                self.config_manager.save_config(test_config)
            
            self.assertIn("Error saving configuration", str(context.exception))
    
    @patch('forex_alerts.services.config_manager.yf.Ticker')
    def test_validate_single_symbol_valid(self, mock_ticker_class):
        """Test validation of a single valid symbol."""
        # Mock yfinance ticker
        mock_ticker = Mock()
        mock_ticker.info = {'symbol': 'EURUSD=X'}
        mock_ticker.history.return_value = pd.DataFrame({
            'Close': [1.0850, 1.0860],
            'Volume': [1000, 1100]
        })
        mock_ticker_class.return_value = mock_ticker
        
        result = self.config_manager._validate_single_symbol("EURUSD=X")
        self.assertTrue(result)
        
        # Verify yfinance was called correctly
        mock_ticker_class.assert_called_once_with("EURUSD=X")
        mock_ticker.history.assert_called_once_with(period="1d", interval="1h")
    
    @patch('forex_alerts.services.config_manager.yf.Ticker')
    def test_validate_single_symbol_invalid_no_info(self, mock_ticker_class):
        """Test validation of symbol with no info."""
        mock_ticker = Mock()
        mock_ticker.info = {}
        mock_ticker_class.return_value = mock_ticker
        
        result = self.config_manager._validate_single_symbol("INVALID=X")
        self.assertFalse(result)
    
    @patch('forex_alerts.services.config_manager.yf.Ticker')
    def test_validate_single_symbol_invalid_empty_history(self, mock_ticker_class):
        """Test validation of symbol with empty history."""
        mock_ticker = Mock()
        mock_ticker.info = {'symbol': 'INVALID=X'}
        mock_ticker.history.return_value = pd.DataFrame()  # Empty DataFrame
        mock_ticker_class.return_value = mock_ticker
        
        result = self.config_manager._validate_single_symbol("INVALID=X")
        self.assertFalse(result)
    
    @patch('forex_alerts.services.config_manager.yf.Ticker')
    def test_validate_single_symbol_exception(self, mock_ticker_class):
        """Test validation of symbol that raises exception."""
        mock_ticker_class.side_effect = Exception("Network error")
        
        result = self.config_manager._validate_single_symbol("ERROR=X")
        self.assertFalse(result)
    
    @patch.object(ConfigManager, '_validate_single_symbol')
    def test_validate_symbols_success(self, mock_validate):
        """Test successful symbol validation."""
        mock_validate.return_value = True
        
        symbols = ["EURUSD", "GBPUSD", "USDJPY"]
        result = self.config_manager.validate_symbols(symbols)
        
        expected = ["EURUSD=X", "GBPUSD=X", "USDJPY=X"]
        self.assertEqual(result, expected)
        
        # Verify all symbols were validated
        self.assertEqual(mock_validate.call_count, 3)
    
    @patch.object(ConfigManager, '_validate_single_symbol')
    def test_validate_symbols_mixed_valid_invalid(self, mock_validate):
        """Test symbol validation with mix of valid and invalid symbols."""
        # Mock validation: first two valid, third invalid
        mock_validate.side_effect = [True, True, False]
        
        symbols = ["EURUSD", "GBPUSD", "INVALID"]
        
        with patch('builtins.print') as mock_print:
            result = self.config_manager.validate_symbols(symbols)
        
        expected = ["EURUSD=X", "GBPUSD=X"]
        self.assertEqual(result, expected)
        
        # Verify warning was printed
        mock_print.assert_called_once_with("Warning: Invalid symbols ignored: ['INVALID']")
    
    @patch.object(ConfigManager, '_validate_single_symbol')
    def test_validate_symbols_all_invalid(self, mock_validate):
        """Test symbol validation with all invalid symbols."""
        mock_validate.return_value = False
        
        symbols = ["INVALID1", "INVALID2"]
        
        with self.assertRaises(ValueError) as context:
            self.config_manager.validate_symbols(symbols)
        
        self.assertIn("No valid symbols found", str(context.exception))
        self.assertIn("INVALID1", str(context.exception))
        self.assertIn("INVALID2", str(context.exception))
    
    def test_validate_symbols_empty_list(self):
        """Test symbol validation with empty list."""
        with self.assertRaises(ValueError) as context:
            self.config_manager.validate_symbols([])
        
        self.assertIn("Symbol list cannot be empty", str(context.exception))
    
    @patch.object(ConfigManager, '_validate_single_symbol')
    def test_validate_symbols_formatting(self, mock_validate):
        """Test symbol formatting during validation."""
        mock_validate.return_value = True
        
        # Test various input formats
        symbols = ["eurusd", " GBPUSD ", "USDJPY=X", "audcad=x"]
        result = self.config_manager.validate_symbols(symbols)
        
        expected = ["EURUSD=X", "GBPUSD=X", "USDJPY=X", "AUDCAD=X"]
        self.assertEqual(result, expected)
    
    @patch.object(ConfigManager, 'validate_symbols')
    @patch.object(ConfigManager, 'load_config')
    @patch.object(ConfigManager, 'save_config')
    def test_update_config(self, mock_save, mock_load, mock_validate):
        """Test updating configuration."""
        # Mock current config
        current_config = Config(
            symbols=["EURUSD=X"],
            ema_length=15,
            update_frequency=60
        )
        mock_load.return_value = current_config
        mock_validate.return_value = ["GBPUSD=X", "USDJPY=X"]
        
        # Update config
        result = self.config_manager.update_config(
            symbols=["GBPUSD", "USDJPY"],
            ema_length=20
        )
        
        # Verify symbols were validated
        mock_validate.assert_called_once_with(["GBPUSD", "USDJPY"])
        
        # Verify config was saved
        mock_save.assert_called_once()
        
        # Verify returned config has updated values
        self.assertEqual(result.symbols, ["GBPUSD=X", "USDJPY=X"])
        self.assertEqual(result.ema_length, 20)
        self.assertEqual(result.update_frequency, 60)  # Unchanged
    
    def test_get_config_path(self):
        """Test getting configuration file path."""
        path = self.config_manager.get_config_path()
        self.assertEqual(path, self.test_config_path)
    
    def test_migrate_config_current_version(self):
        """Test config migration with current version."""
        config_data = {
            'symbols': ['EURUSD=X'],
            'ema_length': 15,
            '_version': '1.0'
        }
        
        result = self.config_manager._migrate_config(config_data)
        
        # Should remain unchanged for current version
        self.assertEqual(result['symbols'], ['EURUSD=X'])
        self.assertEqual(result['ema_length'], 15)
        self.assertEqual(result['_version'], '1.0')
    
    def test_migrate_config_no_version(self):
        """Test config migration when no version is present."""
        config_data = {
            'symbols': ['EURUSD=X'],
            'ema_length': 15
        }
        
        result = self.config_manager._migrate_config(config_data)
        
        # Should add current version
        self.assertEqual(result['_version'], '1.0')
        self.assertEqual(result['symbols'], ['EURUSD=X'])
        self.assertEqual(result['ema_length'], 15)
    
    def test_merge_with_defaults_complete_config(self):
        """Test merging with defaults when config has all fields."""
        config_data = {
            'symbols': ['EURUSD=X', 'GBPUSD=X'],
            'ema_length': 20,
            'update_frequency': 30,
            'notification_methods': ['email'],
            'email_config': {'smtp_server': 'smtp.gmail.com'},
            'data_retention_hours': 48
        }
        
        result = self.config_manager._merge_with_defaults(config_data)
        
        # Should preserve all provided values
        self.assertEqual(result['symbols'], ['EURUSD=X', 'GBPUSD=X'])
        self.assertEqual(result['ema_length'], 20)
        self.assertEqual(result['update_frequency'], 30)
        self.assertEqual(result['notification_methods'], ['email'])
        self.assertEqual(result['email_config'], {'smtp_server': 'smtp.gmail.com'})
        self.assertEqual(result['data_retention_hours'], 48)
    
    def test_merge_with_defaults_partial_config(self):
        """Test merging with defaults when config has only some fields."""
        config_data = {
            'symbols': ['EURUSD=X'],
            'ema_length': 25
        }
        
        result = self.config_manager._merge_with_defaults(config_data)
        
        # Should use provided values
        self.assertEqual(result['symbols'], ['EURUSD=X'])
        self.assertEqual(result['ema_length'], 25)
        
        # Should use defaults for missing values
        self.assertEqual(result['update_frequency'], 60)
        self.assertEqual(result['notification_methods'], ['console'])
        self.assertIsNone(result['email_config'])
        self.assertEqual(result['data_retention_hours'], 24)
    
    def test_merge_with_defaults_empty_config(self):
        """Test merging with defaults when config is empty."""
        config_data = {}
        
        result = self.config_manager._merge_with_defaults(config_data)
        
        # Should use all default values
        default_config = self.config_manager.get_default_config()
        default_dict = default_config.to_dict()
        
        for key, value in default_dict.items():
            self.assertEqual(result[key], value)
    
    def test_load_config_with_migration_and_merge(self):
        """Test complete load_config flow with migration and merging."""
        # Create config file with partial data and no version
        partial_config = {
            'symbols': ['EURUSD=X'],
            'ema_length': 25
        }
        
        with open(self.test_config_path, 'w') as f:
            json.dump(partial_config, f)
        
        # Load config
        config = self.config_manager.load_config()
        
        # Should have provided values
        self.assertEqual(config.symbols, ['EURUSD=X'])
        self.assertEqual(config.ema_length, 25)
        
        # Should have default values for missing fields
        self.assertEqual(config.update_frequency, 60)
        self.assertEqual(config.notification_methods, ['console'])
        self.assertIsNone(config.email_config)
        self.assertEqual(config.data_retention_hours, 24)
        
        # Verify file was updated with complete config including version
        with open(self.test_config_path, 'r') as f:
            saved_data = json.load(f)
        
        # Should now include version and all default fields
        self.assertIn('_version', saved_data)
        self.assertEqual(saved_data['_version'], '1.0')
        self.assertEqual(saved_data['update_frequency'], 60)
        self.assertEqual(saved_data['notification_methods'], ['console'])

    def test_save_config_with_metadata(self):
        """Test saving configuration includes metadata."""
        test_config = Config(
            symbols=["EURUSD=X"],
            ema_length=20
        )
        
        self.config_manager.save_config(test_config)
        
        # Read raw JSON to check metadata
        with open(self.test_config_path, 'r') as f:
            saved_data = json.load(f)
        
        self.assertIn('_version', saved_data)
        self.assertIn('_created', saved_data)
        self.assertIn('_updated', saved_data)
        self.assertEqual(saved_data['_version'], '1.0')
    
    def test_merge_with_defaults(self):
        """Test merging configuration with defaults."""
        partial_config = {
            'symbols': ['EURUSD=X'],
            'ema_length': 25
        }
        
        merged = self.config_manager._merge_with_defaults(partial_config)
        
        # Should have all default fields
        self.assertEqual(merged['symbols'], ['EURUSD=X'])
        self.assertEqual(merged['ema_length'], 25)  # From partial config
        self.assertEqual(merged['update_frequency'], 60)  # From defaults
        self.assertEqual(merged['notification_methods'], ['console'])  # From defaults
    
    def test_migrate_from_v0_to_v1(self):
        """Test migration from version 0.0 to 1.0."""
        v0_config = {
            'symbols': ['EURUSD=X'],
            'ema_length': 15,
            'notification_methods': 'console'  # String instead of list
        }
        
        migrated = self.config_manager._migrate_from_v0_to_v1(v0_config)
        
        self.assertEqual(migrated['_version'], '1.0')
        self.assertIn('_migrated', migrated)
        self.assertEqual(migrated['notification_methods'], ['console'])  # Should be list now
    
    def test_migrate_config_no_version(self):
        """Test migration of config with no version."""
        old_config = {
            'symbols': ['EURUSD=X'],
            'ema_length': 15
        }
        
        migrated = self.config_manager._migrate_config(old_config)
        
        self.assertEqual(migrated['_version'], '1.0')
        self.assertIn('_migrated', migrated)
    
    def test_migrate_config_current_version(self):
        """Test migration of config with current version."""
        current_config = {
            'symbols': ['EURUSD=X'],
            'ema_length': 15,
            '_version': '1.0'
        }
        
        migrated = self.config_manager._migrate_config(current_config)
        
        # Should be unchanged
        self.assertEqual(migrated, current_config)
    
    def test_load_config_with_migration(self):
        """Test loading config that requires migration."""
        # Write old format config
        old_config = {
            'symbols': ['EURUSD=X'],
            'ema_length': 20,
            'notification_methods': 'email'  # String format (old)
        }
        
        with open(self.test_config_path, 'w') as f:
            json.dump(old_config, f)
        
        # Load should trigger migration
        config = self.config_manager.load_config()
        
        self.assertEqual(config.symbols, ['EURUSD=X'])
        self.assertEqual(config.ema_length, 20)
        self.assertEqual(config.notification_methods, ['email'])  # Should be list now
        
        # Check that file was updated with migration
        with open(self.test_config_path, 'r') as f:
            saved_data = json.load(f)
        
        self.assertEqual(saved_data['_version'], '1.0')
        self.assertIn('_migrated', saved_data)
    
    def test_reset_to_defaults(self):
        """Test resetting configuration to defaults."""
        # First save a custom config
        custom_config = Config(
            symbols=['EURUSD=X'],
            ema_length=25,
            update_frequency=30
        )
        self.config_manager.save_config(custom_config)
        
        # Reset to defaults
        default_config = self.config_manager.reset_to_defaults()
        
        # Should be default values
        self.assertEqual(default_config.symbols, [])
        self.assertEqual(default_config.ema_length, 15)
        self.assertEqual(default_config.update_frequency, 60)
        
        # File should be updated
        loaded_config = self.config_manager.load_config()
        self.assertEqual(loaded_config.ema_length, 15)
    
    def test_backup_config(self):
        """Test creating configuration backup."""
        # Create a config file
        test_config = Config(symbols=['EURUSD=X'])
        self.config_manager.save_config(test_config)
        
        # Create backup
        backup_path = self.config_manager.backup_config("test")
        
        self.assertTrue(backup_path.exists())
        self.assertIn("backup_test", backup_path.name)
        
        # Backup should have same content
        with open(backup_path, 'r') as f:
            backup_data = json.load(f)
        
        self.assertEqual(backup_data['symbols'], ['EURUSD=X'])
    
    def test_backup_config_no_file(self):
        """Test backup when config file doesn't exist."""
        with self.assertRaises(FileNotFoundError):
            self.config_manager.backup_config()
    
    def test_backup_config_auto_suffix(self):
        """Test backup with automatic timestamp suffix."""
        # Create a config file
        test_config = Config(symbols=['EURUSD=X'])
        self.config_manager.save_config(test_config)
        
        # Create backup with auto suffix
        backup_path = self.config_manager.backup_config()
        
        self.assertTrue(backup_path.exists())
        self.assertIn("backup_", backup_path.name)
        
        # Clean up
        backup_path.unlink()
    
    def test_get_timestamp(self):
        """Test timestamp generation."""
        timestamp = self.config_manager._get_timestamp()
        
        # Should be valid ISO format
        from datetime import datetime
        parsed = datetime.fromisoformat(timestamp)
        self.assertIsInstance(parsed, datetime)
    
    def test_load_config_with_partial_data(self):
        """Test loading config with missing fields gets defaults."""
        # Write partial config
        partial_config = {
            'symbols': ['EURUSD=X'],
            'ema_length': 20
            # Missing other fields
        }
        
        with open(self.test_config_path, 'w') as f:
            json.dump(partial_config, f)
        
        config = self.config_manager.load_config()
        
        # Should have provided values
        self.assertEqual(config.symbols, ['EURUSD=X'])
        self.assertEqual(config.ema_length, 20)
        
        # Should have default values for missing fields
        self.assertEqual(config.update_frequency, 60)
        self.assertEqual(config.notification_methods, ['console'])
        self.assertEqual(config.data_retention_hours, 24)


if __name__ == '__main__':
    unittest.main()