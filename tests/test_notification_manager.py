"""
Unit tests for NotificationManager.
"""

import pytest
import smtplib
import subprocess
from datetime import datetime
from unittest.mock import patch, MagicMock, Mock
from io import StringIO
import sys

from forex_alerts.services.notification_manager import NotificationManager, NotificationChannel
from forex_alerts.models.signal import Signal


class TestNotificationManager:
    """Test cases for NotificationManager class."""

    def test_init_default_config(self):
        """Test initialization with default configuration."""
        manager = NotificationManager()

        assert manager.config == {}
        assert manager.enabled_channels == [NotificationChannel.CONSOLE]
        assert manager._console_enabled is True
        assert manager._email_enabled is False
        assert manager._desktop_enabled is False

    def test_init_with_console_config(self):
        """Test initialization with console notification config."""
        config = {'notification_methods': ['console']}
        manager = NotificationManager(config)

        assert manager.config == config
        assert manager.enabled_channels == [NotificationChannel.CONSOLE]
        assert manager._console_enabled is True
        assert manager._email_enabled is False
        assert manager._desktop_enabled is False

    def test_init_with_multiple_channels(self):
        """Test initialization with multiple notification channels."""
        config = {'notification_methods': ['console', 'email', 'desktop']}
        manager = NotificationManager(config)

        expected_channels = [
            NotificationChannel.CONSOLE,
            NotificationChannel.EMAIL,
            NotificationChannel.DESKTOP
        ]
        assert manager.enabled_channels == expected_channels
        assert manager._console_enabled is True
        assert manager._email_enabled is True
        assert manager._desktop_enabled is True

    def test_init_with_invalid_channel(self):
        """Test initialization with invalid notification channel."""
        config = {'notification_methods': ['console', 'invalid', 'email']}
        manager = NotificationManager(config)

        expected_channels = [
            NotificationChannel.CONSOLE, NotificationChannel.EMAIL]
        assert manager.enabled_channels == expected_channels

    def test_init_with_empty_channels_defaults_to_console(self):
        """Test initialization with empty channels defaults to console."""
        config = {'notification_methods': []}
        manager = NotificationManager(config)

        assert manager.enabled_channels == [NotificationChannel.CONSOLE]

    def test_parse_enabled_channels_case_insensitive(self):
        """Test that channel parsing is case insensitive."""
        config = {'notification_methods': ['CONSOLE', 'Email', 'DESKTOP']}
        manager = NotificationManager(config)

        expected_channels = [
            NotificationChannel.CONSOLE,
            NotificationChannel.EMAIL,
            NotificationChannel.DESKTOP
        ]
        assert manager.enabled_channels == expected_channels

    def test_format_console_message_buy_signal(self):
        """Test console message formatting for BUY signal."""
        manager = NotificationManager()
        signal = Signal(
            symbol="EURUSD=X",
            signal_type="BUY",
            price=1.0845,
            timestamp=datetime(2024, 1, 15, 14, 30, 25),
            zlma_value=1.0843,
            ema_value=1.0841,
            confidence=0.95
        )

        message = manager._format_console_message(signal)

        assert "🔔 FOREX ALERT 🔔" in message
        assert "Symbol: EUR/USD" in message
        assert "Signal: BUY 📈" in message
        assert "Price: $1.08450" in message
        assert "Time: 2024-01-15 14:30:25 UTC" in message
        assert "ZLMA: 1.08430 | EMA: 1.08410" in message
        assert "Confidence: 0.95" in message
        assert "=" * 40 in message

    def test_format_console_message_sell_signal(self):
        """Test console message formatting for SELL signal."""
        manager = NotificationManager()
        signal = Signal(
            symbol="GBPUSD=X",
            signal_type="SELL",
            price=1.2567,
            timestamp=datetime(2024, 1, 15, 15, 45, 30),
            zlma_value=1.2565,
            ema_value=1.2570,
            confidence=0.88
        )

        message = manager._format_console_message(signal)

        assert "Symbol: GBP/USD" in message
        assert "Signal: SELL 📉" in message
        assert "Price: $1.25670" in message
        assert "Time: 2024-01-15 15:45:30 UTC" in message
        assert "ZLMA: 1.25650 | EMA: 1.25700" in message
        assert "Confidence: 0.88" in message

    def test_format_console_message_symbol_formatting(self):
        """Test various symbol formatting scenarios."""
        manager = NotificationManager()

        # Test with =X suffix
        signal1 = Signal("EURUSD=X", "BUY", 1.0845,
                         datetime.now(), 1.0843, 1.0841)
        message1 = manager._format_console_message(signal1)
        assert "Symbol: EUR/USD" in message1

        # Test without =X suffix
        signal2 = Signal("GBPJPY", "SELL", 150.25,
                         datetime.now(), 150.23, 150.27)
        message2 = manager._format_console_message(signal2)
        assert "Symbol: GBP/JPY" in message2

        # Test already formatted symbol
        signal3 = Signal("AUD/CAD", "BUY", 0.9123,
                         datetime.now(), 0.9121, 0.9119)
        message3 = manager._format_console_message(signal3)
        assert "Symbol: AUD/CAD" in message3

    @patch('sys.stdout', new_callable=StringIO)
    def test_send_console_notification_success(self, mock_stdout):
        """Test successful console notification sending."""
        manager = NotificationManager({'notification_methods': ['console']})
        signal = Signal("EURUSD=X", "BUY", 1.0845,
                        datetime.now(), 1.0843, 1.0841)

        result = manager._send_console_notification(signal)

        assert result is True
        output = mock_stdout.getvalue()
        assert "🔔 FOREX ALERT 🔔" in output
        assert "EUR/USD" in output
        assert "BUY 📈" in output

    @patch('builtins.print', side_effect=Exception("Print error"))
    def test_send_console_notification_failure(self, mock_print):
        """Test console notification failure handling."""
        manager = NotificationManager({'notification_methods': ['console']})
        signal = Signal("EURUSD=X", "BUY", 1.0845,
                        datetime.now(), 1.0843, 1.0841)

        result = manager._send_console_notification(signal)

        assert result is False

    def test_send_notification_console_only(self):
        """Test sending notification with console channel only."""
        manager = NotificationManager({'notification_methods': ['console']})
        signal = Signal("EURUSD=X", "BUY", 1.0845,
                        datetime.now(), 1.0843, 1.0841)

        with patch.object(manager, '_send_console_notification', return_value=True) as mock_console:
            result = manager.send_notification(signal)

            assert result is True
            mock_console.assert_called_once_with(signal)

    def test_send_notification_multiple_channels(self):
        """Test sending notification with multiple channels."""
        config = {'notification_methods': ['console', 'email', 'desktop']}
        manager = NotificationManager(config)
        signal = Signal("EURUSD=X", "BUY", 1.0845,
                        datetime.now(), 1.0843, 1.0841)

        with patch.object(manager, '_send_console_notification', return_value=True) as mock_console, \
                patch.object(manager, '_send_email_notification', return_value=True) as mock_email, \
                patch.object(manager, '_send_desktop_notification', return_value=True) as mock_desktop:

            result = manager.send_notification(signal)

            assert result is True
            mock_console.assert_called_once_with(signal)
            mock_email.assert_called_once_with(signal)
            mock_desktop.assert_called_once_with(signal)

    def test_send_notification_partial_failure(self):
        """Test sending notification with some channels failing."""
        config = {'notification_methods': ['console', 'email']}
        manager = NotificationManager(config)
        signal = Signal("EURUSD=X", "BUY", 1.0845,
                        datetime.now(), 1.0843, 1.0841)

        with patch.object(manager, '_send_console_notification', return_value=True) as mock_console, \
                patch.object(manager, '_send_email_notification', return_value=False) as mock_email:

            result = manager.send_notification(signal)

            assert result is True  # At least one channel succeeded
            mock_console.assert_called_once_with(signal)
            mock_email.assert_called_once_with(signal)

    def test_send_notification_all_channels_fail(self):
        """Test sending notification when all channels fail."""
        config = {'notification_methods': ['console', 'email']}
        manager = NotificationManager(config)
        signal = Signal("EURUSD=X", "BUY", 1.0845,
                        datetime.now(), 1.0843, 1.0841)

        with patch.object(manager, '_send_console_notification', return_value=False) as mock_console, \
                patch.object(manager, '_send_email_notification', return_value=False) as mock_email:

            result = manager.send_notification(signal)

            assert result is False
            mock_console.assert_called_once_with(signal)
            mock_email.assert_called_once_with(signal)

    @patch('sys.stdout', new_callable=StringIO)
    def test_test_notifications_console_only(self, mock_stdout):
        """Test notification testing with console channel only."""
        manager = NotificationManager({'notification_methods': ['console']})

        results = manager.test_notifications()

        assert results == {'console': True}
        output = mock_stdout.getvalue()
        assert "🧪 Testing Console Notification:" in output
        assert "🔔 FOREX ALERT 🔔" in output
        assert "✅ Successful channels: ['console']" in output

    @patch('sys.stdout', new_callable=StringIO)
    @patch('forex_alerts.services.notification_manager.PLYER_AVAILABLE', True)
    @patch('forex_alerts.services.notification_manager.notification')
    def test_test_notifications_multiple_channels(self, mock_notification, mock_stdout):
        """Test notification testing with multiple channels."""
        mock_notification.notify = Mock()

        config = {'notification_methods': ['console', 'email', 'desktop']}
        manager = NotificationManager(config)

        results = manager.test_notifications()

        # Email should fail without proper configuration, others should succeed
        expected_results = {'console': True, 'email': False, 'desktop': True}
        assert results == expected_results

        output = mock_stdout.getvalue()
        assert "🧪 Testing Console Notification:" in output
        assert "🧪 Testing Email Notification:" in output
        assert "🧪 Testing Desktop Notification:" in output
        assert "✅ Successful channels: ['console', 'desktop']" in output
        assert "❌ Failed channels: ['email']" in output

    def test_get_enabled_channels(self):
        """Test getting list of enabled channels."""
        config = {'notification_methods': ['console', 'email']}
        manager = NotificationManager(config)

        channels = manager.get_enabled_channels()

        assert channels == ['console', 'email']

    def test_is_channel_enabled(self):
        """Test checking if specific channels are enabled."""
        config = {'notification_methods': ['console', 'email']}
        manager = NotificationManager(config)

        assert manager.is_channel_enabled('console') is True
        assert manager.is_channel_enabled('email') is True
        assert manager.is_channel_enabled('desktop') is False
        assert manager.is_channel_enabled('invalid') is False

    def test_is_channel_enabled_case_insensitive(self):
        """Test channel enabled check is case insensitive."""
        config = {'notification_methods': ['console']}
        manager = NotificationManager(config)

        assert manager.is_channel_enabled('CONSOLE') is True
        assert manager.is_channel_enabled('Console') is True
        assert manager.is_channel_enabled('console') is True

    def test_email_notification_no_config(self):
        """Test email notification with no configuration."""
        manager = NotificationManager({'notification_methods': ['email']})
        signal = Signal("EURUSD=X", "BUY", 1.0845,
                        datetime.now(), 1.0843, 1.0841)

        # Should return False when no email config is provided
        result = manager._send_email_notification(signal)
        assert result is False

    @patch('forex_alerts.services.notification_manager.PLYER_AVAILABLE', True)
    @patch('forex_alerts.services.notification_manager.notification')
    def test_desktop_notification_success(self, mock_notification):
        """Test successful desktop notification sending."""
        manager = NotificationManager({'notification_methods': ['desktop']})
        signal = Signal("EURUSD=X", "BUY", 1.0845,
                        datetime.now(), 1.0843, 1.0841)

        result = manager._send_desktop_notification(signal)

        assert result is True
        mock_notification.notify.assert_called_once()

        # Check the notification call arguments
        call_args = mock_notification.notify.call_args[1]
        assert "🔔 Forex Alert: BUY EUR/USD" in call_args['title']
        assert "BUY Signal 📈" in call_args['message']
        assert "$1.08450" in call_args['message']
        assert call_args['app_name'] == "Forex Alert System"
        assert call_args['timeout'] == 10  # Default timeout

    @patch('forex_alerts.services.notification_manager.PLYER_AVAILABLE', False)
    @patch('forex_alerts.services.notification_manager.SUBPROCESS_AVAILABLE', False)
    def test_desktop_notification_all_unavailable(self):
        """Test desktop notification when all methods are unavailable."""
        manager = NotificationManager({'notification_methods': ['desktop']})
        signal = Signal("EURUSD=X", "BUY", 1.0845,
                        datetime.now(), 1.0843, 1.0841)

        result = manager._send_desktop_notification(signal)

        assert result is False

    @patch('forex_alerts.services.notification_manager.PLYER_AVAILABLE', True)
    @patch('forex_alerts.services.notification_manager.SUBPROCESS_AVAILABLE', False)
    @patch('forex_alerts.services.notification_manager.notification')
    def test_desktop_notification_plyer_fails_no_fallback(self, mock_notification):
        """Test desktop notification when plyer fails and no fallback is available."""
        mock_notification.notify.side_effect = Exception("Notification failed")

        manager = NotificationManager({'notification_methods': ['desktop']})
        signal = Signal("EURUSD=X", "BUY", 1.0845,
                        datetime.now(), 1.0843, 1.0841)

        result = manager._send_desktop_notification(signal)

        assert result is False

    @patch('forex_alerts.services.notification_manager.PLYER_AVAILABLE', True)
    @patch('forex_alerts.services.notification_manager.notification')
    def test_desktop_notification_sell_signal(self, mock_notification):
        """Test desktop notification for SELL signal."""
        manager = NotificationManager({'notification_methods': ['desktop']})
        signal = Signal("GBPUSD=X", "SELL", 1.2567,
                        datetime.now(), 1.2565, 1.2570, 0.88)

        result = manager._send_desktop_notification(signal)

        assert result is True
        call_args = mock_notification.notify.call_args[1]
        assert "🔔 Forex Alert: SELL GBP/USD" in call_args['title']
        assert "SELL Signal 📉" in call_args['message']
        assert "$1.25670" in call_args['message']
        assert "0.88" in call_args['message']  # Confidence

    @patch('forex_alerts.services.notification_manager.PLYER_AVAILABLE', True)
    @patch('forex_alerts.services.notification_manager.notification')
    @patch('forex_alerts.services.notification_manager.platform.system')
    def test_desktop_notification_custom_config(self, mock_platform, mock_notification):
        """Test desktop notification with custom configuration."""
        mock_platform.return_value = "Windows"

        config = {
            'notification_methods': ['desktop'],
            'desktop_config': {
                'timeout': 20,
                'app_icon': '/path/to/icon.png'
            }
        }
        manager = NotificationManager(config)
        signal = Signal("EURUSD=X", "BUY", 1.0845,
                        datetime.now(), 1.0843, 1.0841)

        result = manager._send_desktop_notification(signal)

        assert result is True
        call_args = mock_notification.notify.call_args[1]
        assert call_args['timeout'] == 20
        assert call_args['app_icon'] == '/path/to/icon.png'


class TestEmailNotifications:
    """Test cases for email notification functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.valid_email_config = {
            'smtp_server': 'smtp.gmail.com',
            'smtp_port': '587',
            'sender_email': 'test@example.com',
            'sender_password': 'test_password',
            'recipient_email': 'recipient@example.com',
            'use_tls': True
        }

        self.test_signal = Signal(
            symbol="EURUSD=X",
            signal_type="BUY",
            price=1.0845,
            timestamp=datetime(2024, 1, 15, 14, 30, 25),
            zlma_value=1.0843,
            ema_value=1.0841,
            confidence=0.95
        )

    def test_validate_email_config_valid(self):
        """Test email configuration validation with valid config."""
        config = {'email_config': self.valid_email_config}
        manager = NotificationManager(config)

        assert manager.validate_email_config() is True

    def test_validate_email_config_missing_config(self):
        """Test email configuration validation with missing config."""
        manager = NotificationManager({})

        assert manager.validate_email_config() is False

    def test_validate_email_config_missing_fields(self):
        """Test email configuration validation with missing required fields."""
        incomplete_config = {
            'smtp_server': 'smtp.gmail.com',
            'smtp_port': '587',
            # Missing sender_email, sender_password, recipient_email
        }
        config = {'email_config': incomplete_config}
        manager = NotificationManager(config)

        assert manager.validate_email_config() is False

    def test_validate_email_config_empty_fields(self):
        """Test email configuration validation with empty fields."""
        empty_config = {
            'smtp_server': 'smtp.gmail.com',
            'smtp_port': '587',
            'sender_email': '',  # Empty field
            'sender_password': 'test_password',
            'recipient_email': 'recipient@example.com'
        }
        config = {'email_config': empty_config}
        manager = NotificationManager(config)

        assert manager.validate_email_config() is False

    def test_validate_email_config_invalid_port(self):
        """Test email configuration validation with invalid port."""
        invalid_port_config = self.valid_email_config.copy()
        invalid_port_config['smtp_port'] = 'invalid_port'
        config = {'email_config': invalid_port_config}
        manager = NotificationManager(config)

        assert manager.validate_email_config() is False

    def test_create_email_message_buy_signal(self):
        """Test email message creation for BUY signal."""
        config = {'email_config': self.valid_email_config}
        manager = NotificationManager(config)

        message = manager._create_email_message(
            self.test_signal, self.valid_email_config)

        assert message["Subject"] == "🔔 Forex Alert: BUY EUR/USD 📈"
        assert message["From"] == "test@example.com"
        assert message["To"] == "recipient@example.com"

        # Check message has both text and HTML parts
        parts = message.get_payload()
        assert len(parts) == 2
        assert parts[0].get_content_type() == "text/plain"
        assert parts[1].get_content_type() == "text/html"

    def test_create_email_message_sell_signal(self):
        """Test email message creation for SELL signal."""
        sell_signal = Signal(
            symbol="GBPUSD=X",
            signal_type="SELL",
            price=1.2567,
            timestamp=datetime(2024, 1, 15, 15, 45, 30),
            zlma_value=1.2565,
            ema_value=1.2570,
            confidence=0.88
        )

        config = {'email_config': self.valid_email_config}
        manager = NotificationManager(config)

        message = manager._create_email_message(
            sell_signal, self.valid_email_config)

        assert message["Subject"] == "🔔 Forex Alert: SELL GBP/USD 📉"

    def test_create_html_email_body(self):
        """Test HTML email body creation."""
        config = {'email_config': self.valid_email_config}
        manager = NotificationManager(config)

        html_body = manager._create_html_email_body(
            self.test_signal, "EUR/USD")

        assert "🔔 FOREX ALERT 🔔" in html_body
        assert "BUY 📈" in html_body
        assert "EUR/USD" in html_body
        assert "$1.08450" in html_body
        assert "2024-01-15 14:30:25 UTC" in html_body
        assert "1.08430" in html_body  # ZLMA value
        assert "1.08410" in html_body  # EMA value
        assert "0.95" in html_body     # Confidence
        assert "<html>" in html_body
        assert "</html>" in html_body

    def test_create_text_email_body(self):
        """Test plain text email body creation."""
        config = {'email_config': self.valid_email_config}
        manager = NotificationManager(config)

        text_body = manager._create_text_email_body(
            self.test_signal, "EUR/USD")

        assert "🔔 FOREX ALERT 🔔" in text_body
        assert "Symbol: EUR/USD" in text_body
        assert "Signal: BUY 📈" in text_body
        assert "Price: $1.08450" in text_body
        assert "Time: 2024-01-15 14:30:25 UTC" in text_body
        assert "ZLMA: 1.08430 | EMA: 1.08410" in text_body
        assert "Confidence: 0.95" in text_body
        assert "=" * 50 in text_body

    @patch('smtplib.SMTP')
    def test_send_smtp_email_success_tls(self, mock_smtp):
        """Test successful SMTP email sending with TLS."""
        mock_server = Mock()
        mock_smtp.return_value = mock_server

        config = {'email_config': self.valid_email_config}
        manager = NotificationManager(config)
        message = manager._create_email_message(
            self.test_signal, self.valid_email_config)

        result = manager._send_smtp_email(message, self.valid_email_config)

        assert result is True
        mock_smtp.assert_called_once_with('smtp.gmail.com', 587)
        mock_server.starttls.assert_called_once()
        mock_server.login.assert_called_once_with(
            'test@example.com', 'test_password')
        mock_server.sendmail.assert_called_once()
        mock_server.quit.assert_called_once()

    @patch('smtplib.SMTP_SSL')
    def test_send_smtp_email_success_ssl(self, mock_smtp_ssl):
        """Test successful SMTP email sending with SSL."""
        mock_server = Mock()
        mock_smtp_ssl.return_value = mock_server

        ssl_config = self.valid_email_config.copy()
        ssl_config['use_tls'] = False
        ssl_config['smtp_port'] = '465'

        config = {'email_config': ssl_config}
        manager = NotificationManager(config)
        message = manager._create_email_message(self.test_signal, ssl_config)

        result = manager._send_smtp_email(message, ssl_config)

        assert result is True
        mock_smtp_ssl.assert_called_once_with(
            'smtp.gmail.com', 465, context=mock_smtp_ssl.call_args[1]['context'])
        mock_server.login.assert_called_once_with(
            'test@example.com', 'test_password')
        mock_server.sendmail.assert_called_once()
        mock_server.quit.assert_called_once()

    @patch('smtplib.SMTP')
    def test_send_smtp_email_authentication_error(self, mock_smtp):
        """Test SMTP email sending with authentication error."""
        mock_server = Mock()
        mock_server.login.side_effect = smtplib.SMTPAuthenticationError(
            535, "Authentication failed")
        mock_smtp.return_value = mock_server

        config = {'email_config': self.valid_email_config}
        manager = NotificationManager(config)
        message = manager._create_email_message(
            self.test_signal, self.valid_email_config)

        result = manager._send_smtp_email(message, self.valid_email_config)

        assert result is False
        mock_server.login.assert_called_once()

    @patch('smtplib.SMTP')
    def test_send_smtp_email_recipients_refused(self, mock_smtp):
        """Test SMTP email sending with recipients refused error."""
        mock_server = Mock()
        mock_server.sendmail.side_effect = smtplib.SMTPRecipientsRefused({})
        mock_smtp.return_value = mock_server

        config = {'email_config': self.valid_email_config}
        manager = NotificationManager(config)
        message = manager._create_email_message(
            self.test_signal, self.valid_email_config)

        result = manager._send_smtp_email(message, self.valid_email_config)

        assert result is False
        mock_server.sendmail.assert_called_once()

    @patch('smtplib.SMTP')
    def test_send_smtp_email_server_disconnected_retry(self, mock_smtp):
        """Test SMTP email sending with server disconnection and retry."""
        mock_server = Mock()
        # First two attempts fail, third succeeds
        mock_server.sendmail.side_effect = [
            smtplib.SMTPServerDisconnected("Connection lost"),
            smtplib.SMTPServerDisconnected("Connection lost"),
            None  # Success on third attempt
        ]
        mock_smtp.return_value = mock_server

        config = {'email_config': self.valid_email_config}
        manager = NotificationManager(config)
        message = manager._create_email_message(
            self.test_signal, self.valid_email_config)

        result = manager._send_smtp_email(message, self.valid_email_config)

        assert result is True
        assert mock_server.sendmail.call_count == 3

    @patch('smtplib.SMTP')
    def test_send_smtp_email_max_retries_exceeded(self, mock_smtp):
        """Test SMTP email sending with max retries exceeded."""
        mock_server = Mock()
        mock_server.sendmail.side_effect = smtplib.SMTPServerDisconnected(
            "Connection lost")
        mock_smtp.return_value = mock_server

        config = {'email_config': self.valid_email_config}
        manager = NotificationManager(config)
        message = manager._create_email_message(
            self.test_signal, self.valid_email_config)

        result = manager._send_smtp_email(message, self.valid_email_config)

        assert result is False
        assert mock_server.sendmail.call_count == 3  # Max retries

    def test_send_email_notification_missing_config(self):
        """Test email notification sending with missing configuration."""
        manager = NotificationManager({'notification_methods': ['email']})

        result = manager._send_email_notification(self.test_signal)

        assert result is False

    def test_send_email_notification_invalid_config(self):
        """Test email notification sending with invalid configuration."""
        invalid_config = {
            'smtp_server': 'smtp.gmail.com',
            # Missing required fields
        }
        config = {'email_config': invalid_config,
                  'notification_methods': ['email']}
        manager = NotificationManager(config)

        result = manager._send_email_notification(self.test_signal)

        assert result is False

    @patch.object(NotificationManager, '_send_smtp_email')
    def test_send_email_notification_success(self, mock_send_smtp):
        """Test successful email notification sending."""
        mock_send_smtp.return_value = True

        config = {'email_config': self.valid_email_config,
                  'notification_methods': ['email']}
        manager = NotificationManager(config)

        result = manager._send_email_notification(self.test_signal)

        assert result is True
        mock_send_smtp.assert_called_once()

    @patch.object(NotificationManager, '_send_smtp_email')
    def test_send_email_notification_smtp_failure(self, mock_send_smtp):
        """Test email notification sending with SMTP failure."""
        mock_send_smtp.return_value = False

        config = {'email_config': self.valid_email_config,
                  'notification_methods': ['email']}
        manager = NotificationManager(config)

        result = manager._send_email_notification(self.test_signal)

        assert result is False
        mock_send_smtp.assert_called_once()

    @patch('sys.stdout', new_callable=StringIO)
    def test_test_notifications_email_valid_config(self, mock_stdout):
        """Test notification testing with valid email configuration."""
        config = {
            'email_config': self.valid_email_config,
            'notification_methods': ['email']
        }
        manager = NotificationManager(config)

        with patch.object(manager, '_send_email_notification', return_value=True) as mock_email:
            results = manager.test_notifications()

            assert results == {'email': True}
            mock_email.assert_called_once()

            output = mock_stdout.getvalue()
            assert "🧪 Testing Email Notification:" in output
            assert "✅ Successful channels: ['email']" in output

    @patch('sys.stdout', new_callable=StringIO)
    def test_test_notifications_email_invalid_config(self, mock_stdout):
        """Test notification testing with invalid email configuration."""
        config = {'notification_methods': ['email']}  # No email_config
        manager = NotificationManager(config)

        results = manager.test_notifications()

        assert results == {'email': False}

        output = mock_stdout.getvalue()
        assert "🧪 Testing Email Notification:" in output
        assert "❌ Email configuration is invalid or missing" in output
        assert "❌ Failed channels: ['email']" in output


class TestDesktopNotifications:
    """Test cases for desktop notification functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.test_signal = Signal(
            symbol="EURUSD=X",
            signal_type="BUY",
            price=1.0845,
            timestamp=datetime(2024, 1, 15, 14, 30, 25),
            zlma_value=1.0843,
            ema_value=1.0841,
            confidence=0.95
        )

    @patch('forex_alerts.services.notification_manager.PLYER_AVAILABLE', True)
    @patch('forex_alerts.services.notification_manager.notification')
    @patch('forex_alerts.services.notification_manager.platform.system')
    def test_get_desktop_notification_config_windows(self, mock_platform, mock_notification):
        """Test desktop notification configuration for Windows."""
        mock_platform.return_value = "Windows"

        manager = NotificationManager({'notification_methods': ['desktop']})
        config = manager._get_desktop_notification_config()

        assert config['timeout'] == 15  # Windows default
        assert config['app_icon'] is None

    @patch('forex_alerts.services.notification_manager.PLYER_AVAILABLE', True)
    @patch('forex_alerts.services.notification_manager.notification')
    @patch('forex_alerts.services.notification_manager.platform.system')
    def test_get_desktop_notification_config_macos(self, mock_platform, mock_notification):
        """Test desktop notification configuration for macOS."""
        mock_platform.return_value = "Darwin"

        manager = NotificationManager({'notification_methods': ['desktop']})
        config = manager._get_desktop_notification_config()

        assert config['timeout'] == 10  # macOS default
        assert config['app_icon'] is None

    @patch('forex_alerts.services.notification_manager.PLYER_AVAILABLE', True)
    @patch('forex_alerts.services.notification_manager.notification')
    @patch('forex_alerts.services.notification_manager.platform.system')
    def test_get_desktop_notification_config_linux(self, mock_platform, mock_notification):
        """Test desktop notification configuration for Linux."""
        mock_platform.return_value = "Linux"

        manager = NotificationManager({'notification_methods': ['desktop']})
        config = manager._get_desktop_notification_config()

        assert config['timeout'] == 8  # Linux default
        assert config['app_icon'] is None

    @patch('forex_alerts.services.notification_manager.PLYER_AVAILABLE', True)
    @patch('forex_alerts.services.notification_manager.notification')
    def test_get_desktop_notification_config_custom_override(self, mock_notification):
        """Test desktop notification configuration with custom overrides."""
        config = {
            'notification_methods': ['desktop'],
            'desktop_config': {
                'timeout': 25,
                'app_icon': '/custom/icon.png'
            }
        }
        manager = NotificationManager(config)
        notification_config = manager._get_desktop_notification_config()

        assert notification_config['timeout'] == 25
        assert notification_config['app_icon'] == '/custom/icon.png'

    @patch('forex_alerts.services.notification_manager.PLYER_AVAILABLE', True)
    @patch('forex_alerts.services.notification_manager.SUBPROCESS_AVAILABLE', True)
    @patch('forex_alerts.services.notification_manager.notification')
    @patch('forex_alerts.services.notification_manager.subprocess.run')
    def test_validate_desktop_notifications_success(self, mock_subprocess, mock_notification):
        """Test successful desktop notification validation."""
        mock_notification.notify = Mock()  # Ensure notify attribute exists
        mock_subprocess.return_value = Mock()  # Mock successful subprocess call

        manager = NotificationManager({'notification_methods': ['desktop']})
        result = manager.validate_desktop_notifications()

        assert result is True

    @patch('forex_alerts.services.notification_manager.PLYER_AVAILABLE', False)
    @patch('forex_alerts.services.notification_manager.SUBPROCESS_AVAILABLE', False)
    def test_validate_desktop_notifications_all_unavailable(self):
        """Test desktop notification validation when all methods are unavailable."""
        manager = NotificationManager({'notification_methods': ['desktop']})
        result = manager.validate_desktop_notifications()

        assert result is False

    @patch('forex_alerts.services.notification_manager.PLYER_AVAILABLE', True)
    @patch('forex_alerts.services.notification_manager.SUBPROCESS_AVAILABLE', True)
    @patch('forex_alerts.services.notification_manager.notification')
    @patch('forex_alerts.services.notification_manager.subprocess.run')
    def test_validate_desktop_notifications_plyer_fails_native_succeeds(self, mock_subprocess, mock_notification):
        """Test desktop notification validation when plyer fails but native succeeds."""
        # Remove the notify attribute to simulate missing method
        if hasattr(mock_notification, 'notify'):
            delattr(mock_notification, 'notify')

        mock_subprocess.return_value = Mock()  # Mock successful native call

        manager = NotificationManager({'notification_methods': ['desktop']})
        result = manager.validate_desktop_notifications()

        assert result is True  # Should succeed with native fallback

    @patch('forex_alerts.services.notification_manager.PLYER_AVAILABLE', False)
    @patch('forex_alerts.services.notification_manager.SUBPROCESS_AVAILABLE', True)
    @patch('forex_alerts.services.notification_manager.subprocess.run')
    def test_validate_desktop_notifications_native_only(self, mock_subprocess):
        """Test desktop notification validation with native method only."""
        mock_subprocess.return_value = Mock()  # Mock successful native call

        manager = NotificationManager({'notification_methods': ['desktop']})
        result = manager.validate_desktop_notifications()

        assert result is True  # Should succeed with native method

    @patch('forex_alerts.services.notification_manager.PLYER_AVAILABLE', True)
    @patch('forex_alerts.services.notification_manager.SUBPROCESS_AVAILABLE', True)
    @patch('forex_alerts.services.notification_manager.notification')
    @patch('forex_alerts.services.notification_manager.subprocess.run')
    @patch('forex_alerts.services.notification_manager.platform.system')
    @patch('forex_alerts.services.notification_manager.platform.release')
    def test_get_desktop_notification_status(self, mock_release, mock_system, mock_subprocess, mock_notification):
        """Test getting desktop notification status information."""
        mock_system.return_value = "Darwin"
        mock_release.return_value = "21.6.0"
        mock_notification.notify = Mock()
        mock_subprocess.return_value = Mock()

        manager = NotificationManager({'notification_methods': ['desktop']})
        status = manager.get_desktop_notification_status()

        expected_status = {
            'platform': 'Darwin',
            'platform_version': '21.6.0',
            'plyer_available': True,
            'subprocess_available': True,
            'notifications_enabled': True,
            'validation_passed': True,
            'config': {
                'timeout': 10,
                'app_icon': None
            }
        }

        assert status == expected_status

    @patch('forex_alerts.services.notification_manager.PLYER_AVAILABLE', False)
    @patch('forex_alerts.services.notification_manager.SUBPROCESS_AVAILABLE', False)
    @patch('forex_alerts.services.notification_manager.platform.system')
    @patch('forex_alerts.services.notification_manager.platform.release')
    def test_get_desktop_notification_status_all_unavailable(self, mock_release, mock_system):
        """Test getting desktop notification status when all methods are unavailable."""
        mock_system.return_value = "Windows"
        mock_release.return_value = "10"

        manager = NotificationManager(
            {'notification_methods': ['console']})  # Desktop not enabled
        status = manager.get_desktop_notification_status()

        expected_status = {
            'platform': 'Windows',
            'platform_version': '10',
            'plyer_available': False,
            'subprocess_available': False,
            'notifications_enabled': False,
            'validation_passed': False,
            'config': {
                'timeout': 15,  # Windows default
                'app_icon': None
            }
        }

        assert status == expected_status

    @patch('sys.stdout', new_callable=StringIO)
    @patch('forex_alerts.services.notification_manager.PLYER_AVAILABLE', True)
    @patch('forex_alerts.services.notification_manager.SUBPROCESS_AVAILABLE', True)
    @patch('forex_alerts.services.notification_manager.notification')
    @patch('forex_alerts.services.notification_manager.subprocess.run')
    def test_test_notifications_desktop_success(self, mock_subprocess, mock_notification, mock_stdout):
        """Test notification testing with successful desktop notification."""
        mock_notification.notify = Mock()
        mock_subprocess.return_value = Mock()

        manager = NotificationManager({'notification_methods': ['desktop']})
        results = manager.test_notifications()

        assert results == {'desktop': True}

        output = mock_stdout.getvalue()
        assert "🧪 Testing Desktop Notification:" in output
        assert "✅ Desktop notification system is available" in output
        assert "✅ Desktop notification sent successfully" in output
        assert "✅ Successful channels: ['desktop']" in output

    @patch('sys.stdout', new_callable=StringIO)
    @patch('forex_alerts.services.notification_manager.PLYER_AVAILABLE', False)
    @patch('forex_alerts.services.notification_manager.SUBPROCESS_AVAILABLE', False)
    def test_test_notifications_desktop_unavailable(self, mock_stdout):
        """Test notification testing when desktop notifications are unavailable."""
        manager = NotificationManager({'notification_methods': ['desktop']})
        results = manager.test_notifications()

        assert results == {'desktop': False}

        output = mock_stdout.getvalue()
        assert "🧪 Testing Desktop Notification:" in output
        assert "❌ Desktop notifications are not available or not working" in output
        assert "❌ Failed channels: ['desktop']" in output

    @patch('sys.stdout', new_callable=StringIO)
    @patch('forex_alerts.services.notification_manager.PLYER_AVAILABLE', True)
    @patch('forex_alerts.services.notification_manager.SUBPROCESS_AVAILABLE', True)
    @patch('forex_alerts.services.notification_manager.notification')
    @patch('forex_alerts.services.notification_manager.subprocess.run')
    def test_test_notifications_desktop_send_failure(self, mock_subprocess, mock_notification, mock_stdout):
        """Test notification testing when desktop notification sending fails."""
        mock_notification.notify = Mock()
        mock_subprocess.return_value = Mock()

        manager = NotificationManager({'notification_methods': ['desktop']})

        # Mock the _send_desktop_notification to fail
        with patch.object(manager, '_send_desktop_notification', return_value=False):
            results = manager.test_notifications()

        assert results == {'desktop': False}

        output = mock_stdout.getvalue()
        assert "🧪 Testing Desktop Notification:" in output
        assert "✅ Desktop notification system is available" in output
        assert "❌ Failed channels: ['desktop']" in output

    @patch('forex_alerts.services.notification_manager.PLYER_AVAILABLE', False)
    @patch('forex_alerts.services.notification_manager.SUBPROCESS_AVAILABLE', True)
    @patch('forex_alerts.services.notification_manager.subprocess.run')
    @patch('forex_alerts.services.notification_manager.platform.system')
    def test_send_native_desktop_notification_macos(self, mock_system, mock_subprocess):
        """Test native desktop notification on macOS."""
        mock_system.return_value = "Darwin"
        mock_subprocess.return_value = Mock()

        manager = NotificationManager({'notification_methods': ['desktop']})
        result = manager._send_native_desktop_notification(
            "Test Title", "Test Message")

        assert result is True
        mock_subprocess.assert_called_once()
        # Check that osascript was called with correct arguments
        # Get the first positional argument (the command list)
        call_args = mock_subprocess.call_args[0][0]
        assert call_args[0] == 'osascript'
        assert call_args[1] == '-e'
        assert 'display notification "Test Message" with title "Test Title"' in call_args[2]

    @patch('forex_alerts.services.notification_manager.PLYER_AVAILABLE', False)
    @patch('forex_alerts.services.notification_manager.SUBPROCESS_AVAILABLE', True)
    @patch('forex_alerts.services.notification_manager.subprocess.run')
    @patch('forex_alerts.services.notification_manager.platform.system')
    def test_send_native_desktop_notification_linux(self, mock_system, mock_subprocess):
        """Test native desktop notification on Linux."""
        mock_system.return_value = "Linux"
        mock_subprocess.return_value = Mock()

        manager = NotificationManager({'notification_methods': ['desktop']})
        result = manager._send_native_desktop_notification(
            "Test Title", "Test Message")

        assert result is True
        mock_subprocess.assert_called_once_with(
            ['notify-send', 'Test Title', 'Test Message'],
            check=True,
            capture_output=True
        )

    @patch('forex_alerts.services.notification_manager.PLYER_AVAILABLE', False)
    @patch('forex_alerts.services.notification_manager.SUBPROCESS_AVAILABLE', True)
    @patch('forex_alerts.services.notification_manager.subprocess.run')
    @patch('forex_alerts.services.notification_manager.platform.system')
    def test_send_native_desktop_notification_windows(self, mock_system, mock_subprocess):
        """Test native desktop notification on Windows."""
        mock_system.return_value = "Windows"
        mock_subprocess.return_value = Mock()

        manager = NotificationManager({'notification_methods': ['desktop']})
        result = manager._send_native_desktop_notification(
            "Test Title", "Test Message")

        assert result is True
        mock_subprocess.assert_called_once()
        # Check that PowerShell was called with correct arguments
        # Get the first positional argument (the command list)
        call_args = mock_subprocess.call_args[0][0]
        assert call_args[0] == 'powershell'
        assert call_args[1] == '-Command'
        assert 'Test Title' in call_args[2]
        assert 'Test Message' in call_args[2]

    @patch('forex_alerts.services.notification_manager.PLYER_AVAILABLE', False)
    @patch('forex_alerts.services.notification_manager.SUBPROCESS_AVAILABLE', True)
    @patch('forex_alerts.services.notification_manager.subprocess.run')
    @patch('forex_alerts.services.notification_manager.platform.system')
    def test_send_native_desktop_notification_unsupported_platform(self, mock_system, mock_subprocess):
        """Test native desktop notification on unsupported platform."""
        mock_system.return_value = "FreeBSD"

        manager = NotificationManager({'notification_methods': ['desktop']})
        result = manager._send_native_desktop_notification(
            "Test Title", "Test Message")

        assert result is False
        mock_subprocess.assert_not_called()

    @patch('forex_alerts.services.notification_manager.PLYER_AVAILABLE', False)
    @patch('forex_alerts.services.notification_manager.SUBPROCESS_AVAILABLE', False)
    def test_send_native_desktop_notification_subprocess_unavailable(self):
        """Test native desktop notification when subprocess is unavailable."""
        manager = NotificationManager({'notification_methods': ['desktop']})
        result = manager._send_native_desktop_notification(
            "Test Title", "Test Message")

        assert result is False

    @patch('forex_alerts.services.notification_manager.PLYER_AVAILABLE', False)
    @patch('forex_alerts.services.notification_manager.SUBPROCESS_AVAILABLE', True)
    @patch('forex_alerts.services.notification_manager.subprocess.run')
    @patch('forex_alerts.services.notification_manager.platform.system')
    def test_send_native_desktop_notification_command_failure(self, mock_system, mock_subprocess):
        """Test native desktop notification when command fails."""
        mock_system.return_value = "Linux"
        mock_subprocess.side_effect = subprocess.CalledProcessError(
            1, 'notify-send')

        manager = NotificationManager({'notification_methods': ['desktop']})
        result = manager._send_native_desktop_notification(
            "Test Title", "Test Message")

        assert result is False


if __name__ == "__main__":
    pytest.main([__file__])
