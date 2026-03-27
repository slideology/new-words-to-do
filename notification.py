import logging

from config import FEISHU_CONFIG, NOTIFICATION_CONFIG
from webhook_sender import WebhookSender


class NotificationManager:
    def __init__(self):
        self.method = NOTIFICATION_CONFIG["method"]
        self.webhook_sender = None
        if self.method == "feishu" and FEISHU_CONFIG["webhook_url"]:
            self.webhook_sender = WebhookSender(FEISHU_CONFIG["webhook_url"])

    def is_enabled(self):
        return self.method == "feishu" and self.webhook_sender is not None

    def send_notification(self, event_type, payload):
        if not self.is_enabled():
            logging.warning("Feishu notification is disabled or not configured")
            return False

        if event_type == "daily_summary":
            return self.webhook_sender.send_card(
                self.webhook_sender.build_daily_summary_card(payload)
            )
        if event_type == "rising_alert":
            return self.webhook_sender.send_card(
                self.webhook_sender.build_rising_alert_card(payload)
            )
        if event_type == "error":
            return self.webhook_sender.send_card(
                self.webhook_sender.build_error_card(payload)
            )
        if event_type == "opportunity_alert":
            return self.webhook_sender.send_card(
                self.webhook_sender.build_opportunity_alert_card(payload)
            )

        logging.error("Unsupported notification event type: %s", event_type)
        return False
