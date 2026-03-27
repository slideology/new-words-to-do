import json
from pathlib import Path

from config import FEISHU_CONFIG
from feishu_integration import create_feishu_client


DAILY_SUMMARY_HEADERS = [
    "run_id",
    "run_date",
    "started_at",
    "finished_at",
    "keyword_source",
    "timeframe_requested",
    "timeframe_actual",
    "geo",
    "keywords_total",
    "keywords_success",
    "keywords_failed",
    "rising_alert_count",
    "report_csv_path",
    "data_directory",
    "status",
    "error_message",
]

TREND_DETAILS_HEADERS = [
    "run_id",
    "run_date",
    "collected_at",
    "keyword_source",
    "keyword_category",
    "keyword",
    "trend_type",
    "related_query",
    "value",
    "is_rising_alert",
    "timeframe_actual",
    "geo",
    "source_json_file",
]

OPPORTUNITY_REVIEW_HEADERS = [
    "run_id",
    "keyword_source",
    "seed_keyword",
    "candidate_keyword",
    "trend_type",
    "seven_day_value",
    "thirty_day_value",
    "growth_persistence",
    "is_new_term",
    "serp_result_count",
    "serp_ads_present",
    "serp_product_pages",
    "serp_forum_pages",
    "serp_pricing_pages",
    "serp_summary",
    "ai_demand_score",
    "ai_payment_intent_score",
    "ai_commercial_intent",
    "ai_topic_type",
    "ai_target_user",
    "ai_why_now",
    "ai_short_reason",
    "ai_noise_flag",
    "decision",
    "collected_at",
]


def load_state(path):
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_state(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def ensure_workbook(client):
    state_path = Path(FEISHU_CONFIG["state_file"])
    state = load_state(state_path)
    spreadsheet_token = state.get("spreadsheet_token", "")
    spreadsheet_url = state.get("spreadsheet_url", "")
    sheet_ids = state.get("sheet_ids", {})

    if not spreadsheet_token:
        spreadsheet = client.create_spreadsheet(FEISHU_CONFIG["workbook_title"], as_user=True)
        spreadsheet_token = spreadsheet["spreadsheet_token"]
        spreadsheet_url = spreadsheet.get("url", "")
        first_sheet_id = client.get_sheet_id_by_token(spreadsheet_token, as_user=True)
        client.rename_sheet(
            spreadsheet_token,
            first_sheet_id,
            FEISHU_CONFIG["sheet_titles"]["daily_summary"],
            as_user=True,
        )
        sheet_ids["daily_summary"] = first_sheet_id

    for key, title in FEISHU_CONFIG["sheet_titles"].items():
        existing_id = sheet_ids.get(key)
        if existing_id:
            try:
                client.rename_sheet(spreadsheet_token, existing_id, title, as_user=True)
                continue
            except Exception:
                pass
        sheet_ids[key] = client.ensure_sheet(spreadsheet_token, title, as_user=True)

    state = {
        "spreadsheet_token": spreadsheet_token,
        "spreadsheet_url": spreadsheet_url,
        "sheet_ids": sheet_ids,
    }
    save_state(state_path, state)
    return state


class FeishuWorkbook:
    def __init__(self, client, spreadsheet_token, spreadsheet_url, sheet_ids):
        self.client = client
        self.spreadsheet_token = spreadsheet_token
        self.spreadsheet_url = spreadsheet_url
        self.sheet_ids = sheet_ids

    @classmethod
    def from_config(cls):
        client = create_feishu_client()
        state = ensure_workbook(client)
        return cls(
            client=client,
            spreadsheet_token=state["spreadsheet_token"],
            spreadsheet_url=state.get("spreadsheet_url", ""),
            sheet_ids=state["sheet_ids"],
        )

    def append_daily_summary(self, summary_row):
        values = [[summary_row.get(header, "") for header in DAILY_SUMMARY_HEADERS]]
        self.client.append_rows(
            self.spreadsheet_token,
            self.sheet_ids["daily_summary"],
            DAILY_SUMMARY_HEADERS,
            values,
            as_user=True,
        )

    def append_trend_details(self, detail_rows):
        if not detail_rows:
            return
        values = [
            [row.get(header, "") for header in TREND_DETAILS_HEADERS]
            for row in detail_rows
        ]
        self.client.append_rows(
            self.spreadsheet_token,
            self.sheet_ids["trend_details"],
            TREND_DETAILS_HEADERS,
            values,
            as_user=True,
        )

    def append_opportunity_reviews(self, rows):
        if not rows:
            return
        values = [
            [row.get(header, "") for header in OPPORTUNITY_REVIEW_HEADERS]
            for row in rows
        ]
        self.client.append_rows(
            self.spreadsheet_token,
            self.sheet_ids["opportunity_reviews"],
            OPPORTUNITY_REVIEW_HEADERS,
            values,
            as_user=True,
        )
