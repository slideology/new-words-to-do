import logging

import requests


logger = logging.getLogger(__name__)


class WebhookSender:
    def __init__(self, webhook_url, timeout=10):
        self.webhook_url = webhook_url
        self.timeout = timeout

    def build_daily_summary_card(self, payload):
        summary = payload["summary"]
        workbook_url = payload.get("workbook_url", "")

        lines = [
            f"**运行日期**: {summary['run_date']}",
            f"**Run ID**: `{summary['run_id']}`",
            f"**状态**: {summary['status']}",
            f"**关键词来源**: {summary.get('keyword_source', 'unknown')}",
            f"**时间范围**: {summary['timeframe_requested']} -> {summary['timeframe_actual']}",
            f"**地区**: {summary['geo'] or 'Global'}",
            f"**关键词总数**: {summary['keywords_total']}",
            f"**成功**: {summary['keywords_success']}",
            f"**失败**: {summary['keywords_failed']}",
            f"**高增长告警数**: {summary['rising_alert_count']}",
        ]
        if summary.get("report_csv_path"):
            lines.append(f"**本地日报**: `{summary['report_csv_path']}`")
        if summary.get("data_directory"):
            lines.append(f"**数据目录**: `{summary['data_directory']}`")
        if workbook_url:
            lines.append(f"**飞书表格**: [打开工作簿]({workbook_url})")
        if summary.get("error_message"):
            lines.append(f"**错误摘要**: {self._truncate(summary['error_message'])}")

        card = {
            "config": {"wide_screen_mode": True},
            "header": {
                "title": {"tag": "plain_text", "content": "Google Trends 每日报告"},
                "template": "blue",
            },
            "elements": [
                {"tag": "markdown", "content": "\n".join(lines)},
            ],
        }
        return {"msg_type": "interactive", "card": card}

    def build_rising_alert_card(self, payload):
        summary = payload["summary"]
        alerts = payload.get("alerts", [])[:20]
        lines = [
            f"**Run ID**: `{summary['run_id']}`",
            f"**关键词来源**: {summary.get('keyword_source', 'unknown')}",
            f"**时间范围**: {summary['timeframe_actual']}",
            f"**地区**: {summary['geo'] or 'Global'}",
            f"**高增长条目**: {len(payload.get('alerts', []))}",
            "",
        ]
        if alerts:
            lines.append("**告警明细:**")
            for item in alerts:
                category = item.get("keyword_category", "")
                suffix = f" / {category}" if category else ""
                lines.append(
                    f"- **{item['keyword']}**{suffix} / {self._truncate(item['related_query'], 80)} / `{item['value']}`"
                )
        else:
            lines.append("未发现超过阈值的高增长趋势。")

        card = {
            "config": {"wide_screen_mode": True},
            "header": {
                "title": {"tag": "plain_text", "content": "Google Trends 高增长告警"},
                "template": "red",
            },
            "elements": [
                {"tag": "markdown", "content": "\n".join(lines)},
            ],
        }
        return {"msg_type": "interactive", "card": card}

    def build_error_card(self, payload):
        summary = payload["summary"]
        error_message = self._truncate(payload.get("error_message", ""), 300)
        lines = [
            f"**Run ID**: `{summary['run_id']}`",
            f"**运行日期**: {summary['run_date']}",
            f"**状态**: {summary['status']}",
            f"**关键词来源**: {summary.get('keyword_source', 'unknown')}",
            f"**时间范围**: {summary['timeframe_requested']} -> {summary['timeframe_actual']}",
            f"**地区**: {summary['geo'] or 'Global'}",
            f"**错误摘要**: {error_message or '未知错误'}",
        ]

        card = {
            "config": {"wide_screen_mode": True},
            "header": {
                "title": {"tag": "plain_text", "content": "Google Trends 执行异常"},
                "template": "grey",
            },
            "elements": [
                {"tag": "markdown", "content": "\n".join(lines)},
            ],
        }
        return {"msg_type": "interactive", "card": card}

    def build_opportunity_alert_card(self, payload):
        summary = payload["summary"]
        opportunities = payload.get("opportunities", [])[:10]
        workbook_url = payload.get("workbook_url", "")
        lines = [
            f"**Run ID**: `{summary['run_id']}`",
            f"**关键词来源**: {summary.get('keyword_source', 'unknown')}",
            f"**机会词数量**: {len(payload.get('opportunities', []))}",
            "",
        ]
        for item in opportunities:
            lines.append(
                "- **{candidate}** / seed `{seed}` / demand `{demand}` / pay `{pay}` / {decision} / {reason}".format(
                    candidate=self._truncate(item.get("candidate_keyword", ""), 60),
                    seed=self._truncate(item.get("seed_keyword", ""), 40),
                    demand=item.get("ai_demand_score", ""),
                    pay=item.get("ai_payment_intent_score", ""),
                    decision=item.get("decision", ""),
                    reason=self._truncate(item.get("ai_short_reason", ""), 90),
                )
            )
        if workbook_url:
            lines.extend(["", f"**飞书表格**: [打开工作簿]({workbook_url})"])

        card = {
            "config": {"wide_screen_mode": True},
            "header": {
                "title": {"tag": "plain_text", "content": "Google Trends 新词机会提醒"},
                "template": "orange",
            },
            "elements": [
                {"tag": "markdown", "content": "\n".join(lines)},
            ],
        }
        return {"msg_type": "interactive", "card": card}

    def send_card(self, payload):
        try:
            response = requests.post(
                self.webhook_url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=self.timeout,
            )
            response.raise_for_status()
            result = response.json()
            if result.get("StatusCode") == 0 or result.get("code") == 0:
                logger.info("Feishu webhook notification sent successfully")
                return True
            logger.error("Feishu webhook notification failed: %s", result)
            return False
        except Exception as exc:
            logger.error("Failed to send Feishu webhook notification: %s", exc)
            return False

    @staticmethod
    def _truncate(value, max_length=120):
        text = str(value or "").strip()
        if len(text) <= max_length:
            return text
        return f"{text[: max_length - 3]}..."
