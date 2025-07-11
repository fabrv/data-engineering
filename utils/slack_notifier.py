import os
import requests
import json
from datetime import datetime
from typing import Optional, Dict, Any


class SlackNotifier:
    def __init__(self, webhook_url: Optional[str] = None):
        self.webhook_url = webhook_url or os.getenv('SLACK_WEBHOOK_URL')
        if not self.webhook_url:
            print("Warning: No Slack webhook URL provided")
    
    def send_message(self, message: str, color: str = "good", title: str = None) -> bool:
        """Send a message to Slack"""
        if not self.webhook_url:
            print(f"Slack notification (would send): {message}")
            return False
        
        payload = {
            "attachments": [{
                "color": color,
                "title": title or "Pipeline Notification",
                "text": message,
                "footer": "CitiBike ETL Pipeline",
                "ts": int(datetime.now().timestamp())
            }]
        }
        
        try:
            response = requests.post(
                self.webhook_url,
                data=json.dumps(payload),
                headers={'Content-Type': 'application/json'},
                timeout=10
            )
            
            if response.status_code == 200:
                print(f"Slack notification sent: {message}")
                return True
            else:
                print(f"Failed to send Slack notification: {response.status_code}")
                return False
                
        except Exception as e:
            print(f"Error sending Slack notification: {e}")
            return False
    
    def notify_failure(self, step_name: str, error_message: str, context: Dict[str, Any] = None):
        """Send failure notification"""
        title = f"🚨 Pipeline Failure: {step_name}"
        message = f"Step '{step_name}' failed with error:\n```{error_message}```"
        
        if context:
            message += f"\n\nContext:\n"
            for key, value in context.items():
                message += f"• {key}: {value}\n"
        
        self.send_message(message, color="danger", title=title)
    
    def notify_success(self, step_name: str, details: Dict[str, Any] = None):
        """Send success notification"""
        title = f"✅ Pipeline Success: {step_name}"
        message = f"Step '{step_name}' completed successfully"
        
        if details:
            message += f"\n\nDetails:\n"
            for key, value in details.items():
                message += f"• {key}: {value}\n"
        
        self.send_message(message, color="good", title=title)
    
    def notify_completion(self, total_steps: int, duration: str = None):
        """Send pipeline completion notification"""
        title = "🎉 Pipeline Completed"
        message = f"CitiBike ETL pipeline completed successfully!\n"
        message += f"Total steps processed: {total_steps}"
        
        if duration:
            message += f"\nTotal duration: {duration}"
        
        self.send_message(message, color="good", title=title)


# Global instance
slack_notifier = SlackNotifier()


def notify_failure(step_name: str, error_message: str, context: Dict[str, Any] = None):
    """Convenience function for failure notifications"""
    slack_notifier.notify_failure(step_name, error_message, context)


def notify_success(step_name: str, details: Dict[str, Any] = None):
    """Convenience function for success notifications"""
    slack_notifier.notify_success(step_name, details)


def notify_completion(total_steps: int, duration: str = None):
    """Convenience function for completion notifications"""
    slack_notifier.notify_completion(total_steps, duration)