"""
Notification manager for sending trading signal alerts through multiple channels.
"""

import logging
import smtplib
import ssl
import platform
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Dict, List, Optional, Any
from enum import Enum

try:
    from plyer import notification
    PLYER_AVAILABLE = True
except ImportError:
    PLYER_AVAILABLE = False

# Try to import alternative notification libraries for fallback
try:
    import subprocess
    SUBPROCESS_AVAILABLE = True
except ImportError:
    SUBPROCESS_AVAILABLE = False

from ..models.signal import Signal


class NotificationChannel(Enum):
    """Supported notification channels."""
    CONSOLE = "console"
    EMAIL = "email"
    DESKTOP = "desktop"


class NotificationManager:
    """
    Manages multiple notification channels for trading signal alerts.
    
    Supports console, email, and desktop notifications with configurable
    formatting and testing capabilities.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize notification manager with configuration.
        
        Args:
            config: Configuration dictionary containing notification settings
        """
        self.config = config or {}
        self.logger = logging.getLogger(__name__)
        self.enabled_channels = self._parse_enabled_channels()
        
        # Initialize channel handlers
        self._console_enabled = NotificationChannel.CONSOLE in self.enabled_channels
        self._email_enabled = NotificationChannel.EMAIL in self.enabled_channels
        self._desktop_enabled = NotificationChannel.DESKTOP in self.enabled_channels
        
        self.logger.info(f"NotificationManager initialized with channels: {[c.value for c in self.enabled_channels]}")
    
    def _parse_enabled_channels(self) -> List[NotificationChannel]:
        """Parse enabled notification channels from configuration."""
        channels = []
        notification_methods = self.config.get('notification_methods', ['console'])
        
        for method in notification_methods:
            try:
                channel = NotificationChannel(method.lower())
                channels.append(channel)
            except ValueError:
                self.logger.warning(f"Unknown notification method: {method}")
        
        # Default to console if no valid channels
        if not channels:
            channels = [NotificationChannel.CONSOLE]
            
        return channels
    
    def send_notification(self, signal: Signal) -> bool:
        """
        Send notification for a trading signal through all enabled channels.
        
        Args:
            signal: The trading signal to notify about
            
        Returns:
            bool: True if at least one notification was sent successfully
        """
        success_count = 0
        
        if self._console_enabled:
            if self._send_console_notification(signal):
                success_count += 1
        
        if self._email_enabled:
            if self._send_email_notification(signal):
                success_count += 1
        
        if self._desktop_enabled:
            if self._send_desktop_notification(signal):
                success_count += 1
        
        return success_count > 0
    
    def _send_console_notification(self, signal: Signal) -> bool:
        """
        Send notification to console with formatted message.
        
        Args:
            signal: The trading signal to display
            
        Returns:
            bool: True if notification was sent successfully
        """
        try:
            formatted_message = self._format_console_message(signal)
            print(formatted_message)
            self.logger.info(f"Console notification sent for {signal.symbol} {signal.signal_type}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to send console notification: {e}")
            return False
    
    def _send_email_notification(self, signal: Signal) -> bool:
        """
        Send email notification via SMTP.
        
        Args:
            signal: The trading signal to email
            
        Returns:
            bool: True if notification was sent successfully
        """
        try:
            email_config = self.config.get('email_config')
            if not email_config:
                self.logger.error("Email configuration not found")
                return False
            
            # Validate required email configuration
            required_fields = ['smtp_server', 'smtp_port', 'sender_email', 'sender_password', 'recipient_email']
            for field in required_fields:
                if field not in email_config:
                    self.logger.error(f"Missing required email configuration field: {field}")
                    return False
            
            # Create email message
            message = self._create_email_message(signal, email_config)
            
            # Send email via SMTP
            return self._send_smtp_email(message, email_config)
            
        except Exception as e:
            self.logger.error(f"Failed to send email notification: {e}")
            return False
    
    def _create_email_message(self, signal: Signal, email_config: Dict[str, Any]) -> MIMEMultipart:
        """
        Create formatted email message for trading signal.
        
        Args:
            signal: The trading signal to format
            email_config: Email configuration dictionary
            
        Returns:
            MIMEMultipart: Formatted email message
        """
        # Create message container
        message = MIMEMultipart("alternative")
        
        # Format symbol for display
        display_symbol = signal.symbol.replace("=X", "").replace("USD", "/USD")
        if not "/" in display_symbol and len(display_symbol) == 6:
            display_symbol = f"{display_symbol[:3]}/{display_symbol[3:]}"
        
        # Email subject
        signal_emoji = "📈" if signal.signal_type == "BUY" else "📉"
        subject = f"🔔 Forex Alert: {signal.signal_type} {display_symbol} {signal_emoji}"
        
        # Email headers
        message["Subject"] = subject
        message["From"] = email_config['sender_email']
        message["To"] = email_config['recipient_email']
        
        # Create HTML and text versions
        html_body = self._create_html_email_body(signal, display_symbol)
        text_body = self._create_text_email_body(signal, display_symbol)
        
        # Attach parts
        text_part = MIMEText(text_body, "plain")
        html_part = MIMEText(html_body, "html")
        
        message.attach(text_part)
        message.attach(html_part)
        
        return message
    
    def _create_html_email_body(self, signal: Signal, display_symbol: str) -> str:
        """
        Create HTML email body for trading signal.
        
        Args:
            signal: The trading signal
            display_symbol: Formatted symbol for display
            
        Returns:
            str: HTML email body
        """
        signal_emoji = "📈" if signal.signal_type == "BUY" else "📉"
        signal_color = "#28a745" if signal.signal_type == "BUY" else "#dc3545"
        time_str = signal.timestamp.strftime("%Y-%m-%d %H:%M:%S UTC")
        
        html = f"""
        <html>
          <body style="font-family: Arial, sans-serif; margin: 0; padding: 20px; background-color: #f8f9fa;">
            <div style="max-width: 600px; margin: 0 auto; background-color: white; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1);">
              <div style="background-color: {signal_color}; color: white; padding: 20px; border-radius: 8px 8px 0 0; text-align: center;">
                <h1 style="margin: 0; font-size: 24px;">🔔 FOREX ALERT 🔔</h1>
              </div>
              
              <div style="padding: 30px;">
                <div style="text-align: center; margin-bottom: 30px;">
                  <h2 style="margin: 0; color: {signal_color}; font-size: 28px;">
                    {signal.signal_type} {signal_emoji}
                  </h2>
                  <h3 style="margin: 10px 0 0 0; color: #333; font-size: 24px;">
                    {display_symbol}
                  </h3>
                </div>
                
                <table style="width: 100%; border-collapse: collapse; margin-bottom: 20px;">
                  <tr style="border-bottom: 1px solid #eee;">
                    <td style="padding: 12px 0; font-weight: bold; color: #666;">Price:</td>
                    <td style="padding: 12px 0; text-align: right; font-size: 18px; font-weight: bold;">
                      ${signal.price:.5f}
                    </td>
                  </tr>
                  <tr style="border-bottom: 1px solid #eee;">
                    <td style="padding: 12px 0; font-weight: bold; color: #666;">Time:</td>
                    <td style="padding: 12px 0; text-align: right;">{time_str}</td>
                  </tr>
                  <tr style="border-bottom: 1px solid #eee;">
                    <td style="padding: 12px 0; font-weight: bold; color: #666;">ZLMA:</td>
                    <td style="padding: 12px 0; text-align: right;">{signal.zlma_value:.5f}</td>
                  </tr>
                  <tr style="border-bottom: 1px solid #eee;">
                    <td style="padding: 12px 0; font-weight: bold; color: #666;">EMA:</td>
                    <td style="padding: 12px 0; text-align: right;">{signal.ema_value:.5f}</td>
                  </tr>
                  <tr>
                    <td style="padding: 12px 0; font-weight: bold; color: #666;">Confidence:</td>
                    <td style="padding: 12px 0; text-align: right; font-weight: bold;">
                      {signal.confidence:.2f}
                    </td>
                  </tr>
                </table>
                
                <div style="background-color: #f8f9fa; padding: 15px; border-radius: 5px; text-align: center; color: #666; font-size: 12px;">
                  This is an automated trading signal alert from your Forex Alert System.
                </div>
              </div>
            </div>
          </body>
        </html>
        """
        return html
    
    def _create_text_email_body(self, signal: Signal, display_symbol: str) -> str:
        """
        Create plain text email body for trading signal.
        
        Args:
            signal: The trading signal
            display_symbol: Formatted symbol for display
            
        Returns:
            str: Plain text email body
        """
        signal_emoji = "📈" if signal.signal_type == "BUY" else "📉"
        time_str = signal.timestamp.strftime("%Y-%m-%d %H:%M:%S UTC")
        
        text = f"""
🔔 FOREX ALERT 🔔

Symbol: {display_symbol}
Signal: {signal.signal_type} {signal_emoji}
Price: ${signal.price:.5f}
Time: {time_str}
ZLMA: {signal.zlma_value:.5f} | EMA: {signal.ema_value:.5f}
Confidence: {signal.confidence:.2f}

{'=' * 50}

This is an automated trading signal alert from your Forex Alert System.
        """
        return text.strip()
    
    def _send_smtp_email(self, message: MIMEMultipart, email_config: Dict[str, Any]) -> bool:
        """
        Send email via SMTP server with error handling and retries.
        
        Args:
            message: The email message to send
            email_config: Email configuration dictionary
            
        Returns:
            bool: True if email was sent successfully
        """
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # Create SMTP connection
                smtp_server = email_config['smtp_server']
                smtp_port = int(email_config['smtp_port'])
                use_tls = email_config.get('use_tls', True)
                
                if use_tls:
                    # Use TLS connection
                    context = ssl.create_default_context()
                    server = smtplib.SMTP(smtp_server, smtp_port)
                    server.starttls(context=context)
                else:
                    # Use SSL connection
                    context = ssl.create_default_context()
                    server = smtplib.SMTP_SSL(smtp_server, smtp_port, context=context)
                
                # Login and send email
                server.login(email_config['sender_email'], email_config['sender_password'])
                
                text = message.as_string()
                server.sendmail(
                    email_config['sender_email'],
                    email_config['recipient_email'],
                    text
                )
                
                server.quit()
                
                self.logger.info(f"Email notification sent successfully to {email_config['recipient_email']}")
                return True
                
            except smtplib.SMTPAuthenticationError as e:
                self.logger.error(f"SMTP authentication failed: {e}")
                return False  # Don't retry authentication errors
                
            except smtplib.SMTPRecipientsRefused as e:
                self.logger.error(f"SMTP recipients refused: {e}")
                return False  # Don't retry recipient errors
                
            except smtplib.SMTPServerDisconnected as e:
                retry_count += 1
                self.logger.warning(f"SMTP server disconnected (attempt {retry_count}/{max_retries}): {e}")
                if retry_count >= max_retries:
                    self.logger.error("Max retries reached for SMTP server disconnection")
                    return False
                
            except (smtplib.SMTPException, ConnectionError, TimeoutError) as e:
                retry_count += 1
                self.logger.warning(f"SMTP error (attempt {retry_count}/{max_retries}): {e}")
                if retry_count >= max_retries:
                    self.logger.error("Max retries reached for SMTP errors")
                    return False
                
            except Exception as e:
                self.logger.error(f"Unexpected error sending email: {e}")
                return False
        
        return False
    
    def _send_desktop_notification(self, signal: Signal) -> bool:
        """
        Send desktop notification using system notification services.
        
        Args:
            signal: The trading signal to display
            
        Returns:
            bool: True if notification was sent successfully
        """
        # Format symbol for display
        display_symbol = signal.symbol.replace("=X", "").replace("USD", "/USD")
        if not "/" in display_symbol and len(display_symbol) == 6:
            display_symbol = f"{display_symbol[:3]}/{display_symbol[3:]}"
        
        # Create notification title and message
        signal_emoji = "📈" if signal.signal_type == "BUY" else "📉"
        title = f"🔔 Forex Alert: {signal.signal_type} {display_symbol}"
        
        # Format message with key details
        time_str = signal.timestamp.strftime("%H:%M:%S")
        message = (
            f"{signal.signal_type} Signal {signal_emoji}\n"
            f"Price: ${signal.price:.5f}\n"
            f"Time: {time_str}\n"
            f"Confidence: {signal.confidence:.2f}"
        )
        
        # Try plyer first
        if PLYER_AVAILABLE:
            try:
                # Get platform-specific notification settings
                notification_config = self._get_desktop_notification_config()
                
                # Send notification using plyer
                notification.notify(
                    title=title,
                    message=message,
                    app_name="Forex Alert System",
                    timeout=notification_config['timeout'],
                    app_icon=notification_config.get('app_icon')
                )
                
                self.logger.info(f"Desktop notification sent via plyer for {signal.symbol} {signal.signal_type}")
                return True
                
            except Exception as e:
                self.logger.warning(f"Plyer notification failed, trying fallback: {e}")
                # Continue to fallback methods
        
        # Fallback to platform-specific native methods
        return self._send_native_desktop_notification(title, message)
    
    def _send_native_desktop_notification(self, title: str, message: str) -> bool:
        """
        Send desktop notification using native platform methods as fallback.
        
        Args:
            title: Notification title
            message: Notification message
            
        Returns:
            bool: True if notification was sent successfully
        """
        if not SUBPROCESS_AVAILABLE:
            self.logger.error("Desktop notifications not available: no notification method available")
            return False
        
        system = platform.system().lower()
        
        try:
            if system == 'darwin':  # macOS
                # Use osascript to display notification
                script = f'''
                display notification "{message}" with title "{title}" sound name "default"
                '''
                subprocess.run(['osascript', '-e', script], check=True, capture_output=True)
                self.logger.info(f"Desktop notification sent via osascript (macOS)")
                return True
                
            elif system == 'linux':
                # Use notify-send on Linux
                subprocess.run(['notify-send', title, message], check=True, capture_output=True)
                self.logger.info(f"Desktop notification sent via notify-send (Linux)")
                return True
                
            elif system == 'windows':
                # Use PowerShell on Windows
                ps_script = f'''
                Add-Type -AssemblyName System.Windows.Forms
                $notification = New-Object System.Windows.Forms.NotifyIcon
                $notification.Icon = [System.Drawing.SystemIcons]::Information
                $notification.BalloonTipTitle = "{title}"
                $notification.BalloonTipText = "{message}"
                $notification.Visible = $true
                $notification.ShowBalloonTip(5000)
                '''
                subprocess.run(['powershell', '-Command', ps_script], check=True, capture_output=True)
                self.logger.info(f"Desktop notification sent via PowerShell (Windows)")
                return True
                
            else:
                self.logger.error(f"Desktop notifications not supported on platform: {system}")
                return False
                
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Native desktop notification failed: {e}")
            return False
        except FileNotFoundError as e:
            self.logger.error(f"Desktop notification command not found: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Failed to send native desktop notification: {e}")
            return False
    
    def _get_desktop_notification_config(self) -> Dict[str, Any]:
        """
        Get platform-specific desktop notification configuration.
        
        Returns:
            Dict[str, Any]: Configuration dictionary with platform-specific settings
        """
        system = platform.system().lower()
        
        # Default configuration
        config = {
            'timeout': 10,  # seconds
            'app_icon': None
        }
        
        # Platform-specific adjustments
        if system == 'windows':
            # Windows notifications typically stay longer
            config['timeout'] = 15
        elif system == 'darwin':  # macOS
            # macOS notifications are handled by Notification Center
            config['timeout'] = 10
        elif system == 'linux':
            # Linux notifications vary by desktop environment
            config['timeout'] = 8
        
        # Allow configuration override
        desktop_config = self.config.get('desktop_config', {})
        config.update(desktop_config)
        
        return config
    
    def validate_desktop_notifications(self) -> bool:
        """
        Validate that desktop notifications are available and working.
        
        Returns:
            bool: True if desktop notifications are available
        """
        system = platform.system()
        self.logger.info(f"Validating desktop notifications on {system}")
        
        # Check plyer availability first
        plyer_available = False
        if PLYER_AVAILABLE:
            try:
                # Check if we can access the notification module
                if hasattr(notification, 'notify'):
                    plyer_available = True
                    self.logger.info("Plyer notification system is available")
                else:
                    self.logger.warning("Plyer notification module not properly initialized")
            except Exception as e:
                self.logger.warning(f"Plyer notification validation failed: {e}")
        
        # Check native notification availability as fallback
        native_available = False
        if SUBPROCESS_AVAILABLE:
            try:
                system_lower = system.lower()
                if system_lower == 'darwin':
                    # Check if osascript is available
                    subprocess.run(['osascript', '-e', 'return'], check=True, capture_output=True)
                    native_available = True
                    self.logger.info("macOS osascript notification system is available")
                elif system_lower == 'linux':
                    # Check if notify-send is available
                    subprocess.run(['which', 'notify-send'], check=True, capture_output=True)
                    native_available = True
                    self.logger.info("Linux notify-send notification system is available")
                elif system_lower == 'windows':
                    # Check if PowerShell is available
                    subprocess.run(['powershell', '-Command', 'echo test'], check=True, capture_output=True)
                    native_available = True
                    self.logger.info("Windows PowerShell notification system is available")
            except (subprocess.CalledProcessError, FileNotFoundError) as e:
                self.logger.warning(f"Native notification system validation failed: {e}")
            except Exception as e:
                self.logger.warning(f"Native notification validation error: {e}")
        
        # Return True if either plyer or native notifications are available
        available = plyer_available or native_available
        if available:
            self.logger.info("Desktop notifications are available")
        else:
            self.logger.error("No desktop notification system is available")
        
        return available
    
    def _format_console_message(self, signal: Signal) -> str:
        """
        Format trading signal for console display.
        
        Args:
            signal: The trading signal to format
            
        Returns:
            str: Formatted message string
        """
        # Determine emoji based on signal type
        emoji = "📈" if signal.signal_type == "BUY" else "📉"
        
        # Format symbol for display (remove =X suffix if present)
        display_symbol = signal.symbol.replace("=X", "").replace("USD", "/USD")
        if not "/" in display_symbol and len(display_symbol) == 6:
            # Format pairs like EURUSD -> EUR/USD
            display_symbol = f"{display_symbol[:3]}/{display_symbol[3:]}"
        
        # Format timestamp
        time_str = signal.timestamp.strftime("%Y-%m-%d %H:%M:%S UTC")
        
        # Create formatted message
        message = f"""
🔔 FOREX ALERT 🔔
Symbol: {display_symbol}
Signal: {signal.signal_type} {emoji}
Price: ${signal.price:.5f}
Time: {time_str}
ZLMA: {signal.zlma_value:.5f} | EMA: {signal.ema_value:.5f}
Confidence: {signal.confidence:.2f}
{'=' * 40}"""
        
        return message
    
    def validate_email_config(self, email_config: Optional[Dict[str, Any]] = None) -> bool:
        """
        Validate email configuration has all required fields.
        
        Args:
            email_config: Email configuration to validate (uses self.config if None)
            
        Returns:
            bool: True if configuration is valid
        """
        if email_config is None:
            email_config = self.config.get('email_config')
        
        if not email_config:
            return False
        
        required_fields = ['smtp_server', 'smtp_port', 'sender_email', 'sender_password', 'recipient_email']
        for field in required_fields:
            if field not in email_config or not email_config[field]:
                self.logger.error(f"Missing or empty required email configuration field: {field}")
                return False
        
        # Validate port is numeric
        try:
            int(email_config['smtp_port'])
        except (ValueError, TypeError):
            self.logger.error("SMTP port must be a valid integer")
            return False
        
        return True
    
    def test_notifications(self) -> Dict[str, bool]:
        """
        Test all enabled notification channels with a sample signal.
        
        Returns:
            Dict[str, bool]: Results of notification tests by channel
        """
        # Create test signal
        test_signal = Signal(
            symbol="EURUSD=X",
            signal_type="BUY",
            price=1.0845,
            timestamp=datetime.now(),
            zlma_value=1.0843,
            ema_value=1.0841,
            confidence=0.95
        )
        
        results = {}
        
        if self._console_enabled:
            print("\n🧪 Testing Console Notification:")
            results['console'] = self._send_console_notification(test_signal)
        
        if self._email_enabled:
            print("\n🧪 Testing Email Notification:")
            if not self.validate_email_config():
                print("❌ Email configuration is invalid or missing")
                results['email'] = False
            else:
                results['email'] = self._send_email_notification(test_signal)
        
        if self._desktop_enabled:
            print("\n🧪 Testing Desktop Notification:")
            if not self.validate_desktop_notifications():
                print("❌ Desktop notifications are not available or not working")
                results['desktop'] = False
            else:
                print("✅ Desktop notification system is available")
                results['desktop'] = self._send_desktop_notification(test_signal)
                if results['desktop']:
                    print("✅ Desktop notification sent successfully")
        
        # Summary
        successful_channels = [channel for channel, success in results.items() if success]
        failed_channels = [channel for channel, success in results.items() if not success]
        
        print(f"\n✅ Successful channels: {successful_channels}")
        if failed_channels:
            print(f"❌ Failed channels: {failed_channels}")
        
        return results
    
    def get_enabled_channels(self) -> List[str]:
        """
        Get list of enabled notification channels.
        
        Returns:
            List[str]: List of enabled channel names
        """
        return [channel.value for channel in self.enabled_channels]
    
    def is_channel_enabled(self, channel: str) -> bool:
        """
        Check if a specific notification channel is enabled.
        
        Args:
            channel: Channel name to check
            
        Returns:
            bool: True if channel is enabled
        """
        try:
            channel_enum = NotificationChannel(channel.lower())
            return channel_enum in self.enabled_channels
        except ValueError:
            return False
    
    def get_desktop_notification_status(self) -> Dict[str, Any]:
        """
        Get detailed status information about desktop notification capabilities.
        
        Returns:
            Dict[str, Any]: Status information including platform, availability, and configuration
        """
        status = {
            'platform': platform.system(),
            'platform_version': platform.release(),
            'plyer_available': PLYER_AVAILABLE,
            'subprocess_available': SUBPROCESS_AVAILABLE,
            'notifications_enabled': self._desktop_enabled,
            'validation_passed': False,
            'config': self._get_desktop_notification_config()
        }
        
        if self._desktop_enabled:
            status['validation_passed'] = self.validate_desktop_notifications()
        
        return status