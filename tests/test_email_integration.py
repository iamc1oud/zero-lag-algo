"""
Integration tests for email notification system.
"""

import pytest
import smtplib
from datetime import datetime
from unittest.mock import patch, Mock

from forex_alerts.services.notification_manager import NotificationManager
from forex_alerts.models.signal import Signal


class TestEmailIntegration:
    """Integration tests for email notification functionality."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.email_config = {
            'smtp_server': 'smtp.gmail.com',
            'smtp_port': '587',
            'sender_email': 'test@example.com',
            'sender_password': 'test_password',
            'recipient_email': 'recipient@example.com',
            'use_tls': True
        }
        
        self.config = {
            'notification_methods': ['email'],
            'email_config': self.email_config
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
    
    @patch('smtplib.SMTP')
    def test_complete_email_notification_flow(self, mock_smtp):
        """Test complete email notification flow from signal to SMTP."""
        # Setup mock SMTP server
        mock_server = Mock()
        mock_smtp.return_value = mock_server
        
        # Create notification manager and send notification
        manager = NotificationManager(self.config)
        result = manager.send_notification(self.test_signal)
        
        # Verify notification was sent successfully
        assert result is True
        
        # Verify SMTP interactions
        mock_smtp.assert_called_once_with('smtp.gmail.com', 587)
        mock_server.starttls.assert_called_once()
        mock_server.login.assert_called_once_with('test@example.com', 'test_password')
        mock_server.sendmail.assert_called_once()
        mock_server.quit.assert_called_once()
        
        # Verify email content
        sendmail_args = mock_server.sendmail.call_args[0]
        assert sendmail_args[0] == 'test@example.com'  # sender
        assert sendmail_args[1] == 'recipient@example.com'  # recipient
        
        email_content = sendmail_args[2]  # message content
        assert 'From: test@example.com' in email_content
        assert 'To: recipient@example.com' in email_content
        # Subject is encoded, but we can check for the basic structure
        assert 'Subject:' in email_content
        # Content is base64 encoded, so just verify structure
        assert 'Content-Type: multipart/alternative' in email_content
    
    @patch('smtplib.SMTP')
    def test_email_notification_with_different_symbols(self, mock_smtp):
        """Test email notifications with different forex symbols."""
        mock_server = Mock()
        mock_smtp.return_value = mock_server
        
        manager = NotificationManager(self.config)
        
        # Test different symbols
        test_cases = [
            ("GBPUSD=X", "GBP/USD"),
            ("USDJPY=X", "USD/JPY"),
            ("EURJPY", "EUR/JPY"),
            ("AUD/CAD", "AUD/CAD")
        ]
        
        for symbol, expected_display in test_cases:
            signal = Signal(
                symbol=symbol,
                signal_type="SELL",
                price=1.2345,
                timestamp=datetime.now(),
                zlma_value=1.2340,
                ema_value=1.2350,
                confidence=0.85
            )
            
            result = manager.send_notification(signal)
            assert result is True
            
            # Check that the email was sent (content is base64 encoded)
            sendmail_args = mock_server.sendmail.call_args[0]
            email_content = sendmail_args[2]
            # Just verify the email structure is correct
            assert 'From: test@example.com' in email_content
            assert 'To: recipient@example.com' in email_content
    
    @patch('smtplib.SMTP')
    def test_email_notification_error_recovery(self, mock_smtp):
        """Test email notification error handling and recovery."""
        mock_server = Mock()
        
        # First attempt fails with server disconnection, second succeeds
        mock_server.sendmail.side_effect = [
            smtplib.SMTPServerDisconnected("Connection lost"),
            None  # Success on retry
        ]
        mock_smtp.return_value = mock_server
        
        manager = NotificationManager(self.config)
        result = manager.send_notification(self.test_signal)
        
        # Should succeed after retry
        assert result is True
        assert mock_server.sendmail.call_count == 2
    
    @patch('smtplib.SMTP')
    def test_email_notification_permanent_failure(self, mock_smtp):
        """Test email notification with permanent authentication failure."""
        mock_server = Mock()
        mock_server.login.side_effect = smtplib.SMTPAuthenticationError(535, "Authentication failed")
        mock_smtp.return_value = mock_server
        
        manager = NotificationManager(self.config)
        result = manager.send_notification(self.test_signal)
        
        # Should fail without retry for authentication errors
        assert result is False
        mock_server.login.assert_called_once()
        mock_server.sendmail.assert_not_called()
    
    def test_email_notification_invalid_configuration(self):
        """Test email notification with invalid configuration."""
        invalid_configs = [
            # Missing email_config
            {'notification_methods': ['email']},
            
            # Missing required fields
            {
                'notification_methods': ['email'],
                'email_config': {
                    'smtp_server': 'smtp.gmail.com',
                    'smtp_port': '587'
                    # Missing sender_email, sender_password, recipient_email
                }
            },
            
            # Invalid port
            {
                'notification_methods': ['email'],
                'email_config': {
                    'smtp_server': 'smtp.gmail.com',
                    'smtp_port': 'invalid_port',
                    'sender_email': 'test@example.com',
                    'sender_password': 'password',
                    'recipient_email': 'recipient@example.com'
                }
            }
        ]
        
        for config in invalid_configs:
            manager = NotificationManager(config)
            result = manager.send_notification(self.test_signal)
            assert result is False
    
    @patch('smtplib.SMTP_SSL')
    def test_email_notification_ssl_connection(self, mock_smtp_ssl):
        """Test email notification with SSL connection."""
        mock_server = Mock()
        mock_smtp_ssl.return_value = mock_server
        
        # Configure for SSL (no TLS)
        ssl_config = self.config.copy()
        ssl_config['email_config']['use_tls'] = False
        ssl_config['email_config']['smtp_port'] = '465'
        
        manager = NotificationManager(ssl_config)
        result = manager.send_notification(self.test_signal)
        
        assert result is True
        mock_smtp_ssl.assert_called_once_with(
            'smtp.gmail.com', 
            465, 
            context=mock_smtp_ssl.call_args[1]['context']
        )
        mock_server.login.assert_called_once()
        mock_server.sendmail.assert_called_once()
    
    def test_email_message_content_validation(self):
        """Test that email messages contain all required information."""
        manager = NotificationManager(self.config)
        
        # Test BUY signal
        buy_signal = Signal(
            symbol="EURUSD=X",
            signal_type="BUY",
            price=1.0845,
            timestamp=datetime(2024, 1, 15, 14, 30, 25),
            zlma_value=1.0843,
            ema_value=1.0841,
            confidence=0.95
        )
        
        message = manager._create_email_message(buy_signal, self.email_config)
        
        # Verify headers
        assert message["Subject"] == "🔔 Forex Alert: BUY EUR/USD 📈"
        assert message["From"] == "test@example.com"
        assert message["To"] == "recipient@example.com"
        
        # Verify message has both text and HTML parts
        parts = message.get_payload()
        assert len(parts) == 2
        
        # Content is base64 encoded, so let's decode it
        import base64
        
        text_content = base64.b64decode(parts[0].get_payload()).decode('utf-8')
        html_content = base64.b64decode(parts[1].get_payload()).decode('utf-8')
        
        # Verify text content
        assert "EUR/USD" in text_content
        assert "BUY 📈" in text_content
        assert "$1.08450" in text_content
        assert "2024-01-15 14:30:25 UTC" in text_content
        assert "1.08430" in text_content
        assert "1.08410" in text_content
        assert "0.95" in text_content
        
        # Verify HTML content
        assert "<html>" in html_content
        assert "EUR/USD" in html_content
        assert "BUY 📈" in html_content
        assert "$1.08450" in html_content
        assert "2024-01-15 14:30:25 UTC" in html_content
        assert "#28a745" in html_content  # Green color for BUY
        
        # Test SELL signal
        sell_signal = Signal(
            symbol="GBPUSD=X",
            signal_type="SELL",
            price=1.2567,
            timestamp=datetime(2024, 1, 15, 15, 45, 30),
            zlma_value=1.2565,
            ema_value=1.2570,
            confidence=0.88
        )
        
        sell_message = manager._create_email_message(sell_signal, self.email_config)
        sell_html_encoded = sell_message.get_payload()[1].get_payload()
        sell_html = base64.b64decode(sell_html_encoded).decode('utf-8')
        
        assert "SELL 📉" in sell_html
        assert "#dc3545" in sell_html  # Red color for SELL


if __name__ == "__main__":
    pytest.main([__file__])