import os
from dotenv import load_dotenv

load_dotenv()

NOTIFICATION_CONFIG = {
    "method": os.getenv("TRENDS_NOTIFICATION_METHOD", "feishu"),
}

FEISHU_CONFIG = {
    "enabled": os.getenv("FEISHU_ENABLED", "true").lower() in {"1", "true", "yes", "on"},
    "webhook_url": os.getenv("FEISHU_WEBHOOK_URL", ""),
    "app_id": os.getenv("FEISHU_APP_ID", ""),
    "app_secret": os.getenv("FEISHU_APP_SECRET", ""),
    "auth_mode": "user",
    "redirect_uri": os.getenv("FEISHU_REDIRECT_URI", "http://127.0.0.1:8787/callback"),
    "user_token_file": os.getenv("FEISHU_USER_TOKEN_FILE", ".feishu_user_token.json"),
    "workbook_title": os.getenv("FEISHU_WORKBOOK_TITLE", "Google Trends 监控台账"),
    "state_file": os.getenv("FEISHU_STATE_FILE", "artifacts/feishu_workbook/state.json"),
    "sheet_titles": {
        "daily_summary": os.getenv("FEISHU_DAILY_SUMMARY_SHEET", "每日汇总"),
        "trend_details": os.getenv("FEISHU_TREND_DETAILS_SHEET", "趋势明细"),
        "opportunity_reviews": os.getenv("FEISHU_OPPORTUNITY_SHEET", "机会评估"),
    },
    "scopes": [
        "offline_access",
        "sheets:spreadsheet",
        "sheets:spreadsheet:readonly",
        "wiki:wiki:readonly",
        "wiki:node:read",
    ],
}

KEYWORDS = [
    "Image",
    "Video",
    "Music",
    "Voice",
    "Text",
]

KEYWORD_LIBRARY_CONFIG = {
    "enabled": os.getenv("KEYWORD_LIBRARY_ENABLED", "true").lower() in {"1", "true", "yes", "on"},
    "wiki_url": os.getenv(
        "KEYWORD_LIBRARY_WIKI_URL",
        "https://gcnbv8csilt1.feishu.cn/wiki/XSo2wePs0iq1u4kaMpic72gAnEc?sheet=lzAJZS",
    ),
    "spreadsheet_token": os.getenv("KEYWORD_LIBRARY_SPREADSHEET_TOKEN", "Va2hsQStjhnU9CtQzqPc8Fgdnke"),
    "sheet_id": os.getenv("KEYWORD_LIBRARY_SHEET_ID", "lzAJZS"),
    "artifact_file": os.getenv("KEYWORD_LIBRARY_ARTIFACT_FILE", "artifacts/keyword_library/library.json"),
    "default_run_source": os.getenv("KEYWORD_LIBRARY_DEFAULT_RUN_SOURCE", "primary"),
    "max_rows": int(os.getenv("KEYWORD_LIBRARY_MAX_ROWS", "300")),
    "primary_limit": int(os.getenv("KEYWORD_LIBRARY_PRIMARY_LIMIT", "40")),
    "primary_ai_limit": int(os.getenv("KEYWORD_LIBRARY_PRIMARY_AI_LIMIT", "24")),
    "primary_game_limit": int(os.getenv("KEYWORD_LIBRARY_PRIMARY_GAME_LIMIT", "16")),
    "primary_per_category_limit": int(os.getenv("KEYWORD_LIBRARY_PRIMARY_PER_CATEGORY_LIMIT", "3")),
    "rotation_group_size": int(os.getenv("KEYWORD_LIBRARY_ROTATION_GROUP_SIZE", "25")),
    "primary_force_include": [
        keyword.strip()
        for keyword in os.getenv("KEYWORD_LIBRARY_PRIMARY_FORCE_INCLUDE", "").split(",")
        if keyword.strip()
    ],
}

TRENDS_CONFIG = {
    "timeframe": os.getenv("TRENDS_TIMEFRAME", "now 7-d"),
    "geo": "",
    "related_queries_source": os.getenv("TRENDS_RELATED_QUERIES_SOURCE", "browser").strip().lower(),
    "browser_hl": os.getenv("TRENDS_BROWSER_HL", "en-US"),
    "browser_channel": os.getenv(
        "TRENDS_BROWSER_CHANNEL",
        os.getenv("GOOGLE_SEARCH_BROWSER_CHANNEL", "chrome"),
    ),
    "browser_profile_dir": os.getenv(
        "TRENDS_BROWSER_PROFILE_DIR",
        "artifacts/querytrends/browser_profile",
    ),
    "browser_remote_debugging_url": os.getenv(
        "TRENDS_BROWSER_REMOTE_DEBUGGING_URL",
        os.getenv("GOOGLE_SEARCH_REMOTE_DEBUGGING_URL", "http://127.0.0.1:9444"),
    ),
    "browser_page_timeout_ms": int(
        os.getenv(
            "TRENDS_BROWSER_PAGE_TIMEOUT_MS",
            os.getenv("GOOGLE_SEARCH_PAGE_TIMEOUT_MS", "30000"),
        )
    ),
    "browser_download_dir": os.getenv(
        "TRENDS_BROWSER_DOWNLOAD_DIR",
        "artifacts/querytrends/downloads",
    ),
}

RATE_LIMIT_CONFIG = {
    "max_retries": 3,
    "min_delay_between_queries": 10,
    "max_delay_between_queries": 20,
    "batch_size": 5,
    "batch_interval": 300,
    "max_requests_per_minute": int(os.getenv("TRENDS_MAX_REQUESTS_PER_MINUTE", "12")),
    "max_requests_per_hour": int(os.getenv("TRENDS_MAX_REQUESTS_PER_HOUR", "120")),
    "shared_state_file": os.getenv(
        "TRENDS_RATE_LIMIT_STATE_FILE",
        "artifacts/querytrends/request_limiter_state.json",
    ),
    "quota_behavior": os.getenv("TRENDS_QUOTA_BEHAVIOR", "fail_fast").strip().lower(),
    "quota_retry_wait_min_seconds": int(os.getenv("TRENDS_QUOTA_RETRY_WAIT_MIN_SECONDS", "300")),
    "quota_retry_wait_max_seconds": int(os.getenv("TRENDS_QUOTA_RETRY_WAIT_MAX_SECONDS", "360")),
    "empty_response_retry_wait_min_seconds": int(
        os.getenv("TRENDS_EMPTY_RESPONSE_RETRY_WAIT_MIN_SECONDS", "60")
    ),
    "empty_response_retry_wait_max_seconds": int(
        os.getenv("TRENDS_EMPTY_RESPONSE_RETRY_WAIT_MAX_SECONDS", "120")
    ),
}

SCHEDULE_CONFIG = {
    "hour": 23,
    "minute": 5,
    "random_delay_minutes": 15,
}

MONITOR_CONFIG = {
    "rising_threshold": 500,
}

OPPORTUNITY_PIPELINE_CONFIG = {
    "enabled": os.getenv("OPPORTUNITY_PIPELINE_ENABLED", "true").lower() in {"1", "true", "yes", "on"},
    "seven_day_timeframe": os.getenv("OPPORTUNITY_SEVEN_DAY_TIMEFRAME", TRENDS_CONFIG["timeframe"]),
    "thirty_day_timeframe": os.getenv("OPPORTUNITY_THIRTY_DAY_TIMEFRAME", "today 1-m"),
    "seven_day_threshold": int(
        os.getenv("OPPORTUNITY_SEVEN_DAY_THRESHOLD", str(MONITOR_CONFIG["rising_threshold"]))
    ),
    "thirty_day_min_value": int(os.getenv("OPPORTUNITY_THIRTY_DAY_MIN_VALUE", "2")),
    "recent_window_days": int(os.getenv("OPPORTUNITY_RECENT_WINDOW_DAYS", "7")),
    "max_candidates_per_run": int(os.getenv("OPPORTUNITY_MAX_CANDIDATES_PER_RUN", "8")),
    "comparison_baseline": os.getenv("OPPORTUNITY_COMPARISON_BASELINE", "GPTs"),
    "google_search_delay_seconds": float(os.getenv("GOOGLE_SEARCH_DELAY_SECONDS", "8")),
    "google_search_timeout_seconds": int(os.getenv("GOOGLE_SEARCH_TIMEOUT_SECONDS", "20")),
    "google_search_hl": os.getenv("GOOGLE_SEARCH_HL", "en"),
    "google_search_gl": os.getenv("GOOGLE_SEARCH_GL", "us"),
    "google_search_browser_channel": os.getenv("GOOGLE_SEARCH_BROWSER_CHANNEL", "chrome"),
    "google_search_browser_profile_dir": os.getenv(
        "GOOGLE_SEARCH_BROWSER_PROFILE_DIR",
        "artifacts/opportunity_pipeline/browser_profile",
    ),
    "google_search_remote_debugging_url": os.getenv(
        "GOOGLE_SEARCH_REMOTE_DEBUGGING_URL",
        "http://127.0.0.1:9444",
    ),
    "google_search_page_timeout_ms": int(os.getenv("GOOGLE_SEARCH_PAGE_TIMEOUT_MS", "30000")),
    "google_search_user_agent": os.getenv(
        "GOOGLE_SEARCH_USER_AGENT",
        (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
        ),
    ),
    "ai_enabled": os.getenv("OPPORTUNITY_AI_ENABLED", "true").lower() in {"1", "true", "yes", "on"},
    "opportunity_alert_enabled": os.getenv("OPPORTUNITY_ALERT_ENABLED", "true").lower()
    in {"1", "true", "yes", "on"},
    "cache_file": os.getenv("OPPORTUNITY_CACHE_FILE", "artifacts/opportunity_pipeline/cache.json"),
    "gemini_api_key": os.getenv("GEMINI_API_KEY", ""),
    "gemini_model": os.getenv("GEMINI_MODEL", "gemini-2.5-flash"),
    "gemini_base_url": os.getenv(
        "GEMINI_BASE_URL",
        "https://generativelanguage.googleapis.com/v1beta",
    ),
}

LOGGING_CONFIG = {
    "log_file": "trends_monitor.log",
    "level": "INFO",
    "format": "%(asctime)s - %(levelname)s - %(message)s",
}

STORAGE_CONFIG = {
    "data_dir_prefix": "data_",
    "report_filename_prefix": "daily_report_",
    "json_filename_prefix": "related_queries_",
}
