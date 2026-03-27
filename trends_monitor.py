import argparse
import logging
import os
import random
import shutil
import time
import uuid
from datetime import datetime, timedelta

import backoff
import pandas as pd
import schedule

from config import (
    FEISHU_CONFIG,
    KEYWORD_LIBRARY_CONFIG,
    KEYWORDS,
    LOGGING_CONFIG,
    MONITOR_CONFIG,
    OPPORTUNITY_PIPELINE_CONFIG,
    RATE_LIMIT_CONFIG,
    SCHEDULE_CONFIG,
    STORAGE_CONFIG,
    TRENDS_CONFIG,
)
from feishu_workbook import FeishuWorkbook
from keyword_library import (
    get_keywords_for_source,
    load_keyword_library_payload,
    sync_keyword_library,
)
from notification import NotificationManager
from opportunity_analyzer import DEFAULT_ANALYSIS, OpportunityAnalyzer
from serp_collector import GoogleSerpCollector, SerpSummary
from querytrends import batch_get_queries, close_browser_related_queries_collector, save_related_queries
from trend_validator import validate_rising_candidates


logging.basicConfig(
    level=getattr(logging, LOGGING_CONFIG["level"]),
    format=LOGGING_CONFIG["format"],
    handlers=[
        logging.FileHandler(LOGGING_CONFIG["log_file"]),
        logging.StreamHandler(),
    ],
)


notification_manager = NotificationManager()


def create_daily_directory():
    today = datetime.now().strftime("%Y%m%d")
    directory = f"{STORAGE_CONFIG['data_dir_prefix']}{today}"
    os.makedirs(directory, exist_ok=True)
    return directory


def get_date_range_timeframe(timeframe):
    if not timeframe.startswith("last-"):
        return timeframe

    try:
        days = int(timeframe.split("-")[1])
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        return f"{start_date.strftime('%Y-%m-%d')} {end_date.strftime('%Y-%m-%d')}"
    except (ValueError, IndexError):
        logging.warning("Invalid timeframe format: %s, fallback to 'now 1-d'", timeframe)
        return "now 1-d"


def generate_daily_report(results, directory):
    report_data = []

    for keyword, data in results.items():
        if data and isinstance(data.get("rising"), pd.DataFrame):
            for _, row in data["rising"].iterrows():
                report_data.append(
                    {
                        "keyword": keyword,
                        "related_keywords": row["query"],
                        "value": row["value"],
                        "type": "rising",
                    }
                )

        if data and isinstance(data.get("top"), pd.DataFrame):
            for _, row in data["top"].iterrows():
                report_data.append(
                    {
                        "keyword": keyword,
                        "related_keywords": row["query"],
                        "value": row["value"],
                        "type": "top",
                    }
                )

    if not report_data:
        return None

    filename = f"{STORAGE_CONFIG['report_filename_prefix']}{datetime.now().strftime('%Y%m%d')}.csv"
    report_file = os.path.join(directory, filename)
    pd.DataFrame(report_data).to_csv(report_file, index=False)
    return report_file


def is_rising_alert_value(value):
    if isinstance(value, str):
        return value.strip().lower() == "breakout"
    if pd.isna(value):
        return False
    try:
        return float(value) > MONITOR_CONFIG["rising_threshold"]
    except (TypeError, ValueError):
        return False


def build_detail_rows(run_context, keyword, data, source_json_file, keyword_meta):
    detail_rows = []
    collected_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for trend_type in ("top", "rising"):
        dataframe = data.get(trend_type)
        if not isinstance(dataframe, pd.DataFrame):
            continue

        for _, row in dataframe.iterrows():
            value = row["value"]
            is_rising_alert = trend_type == "rising" and is_rising_alert_value(value)
            detail_rows.append(
                {
                    "run_id": run_context["run_id"],
                    "run_date": run_context["run_date"],
                    "collected_at": collected_at,
                    "keyword_source": run_context["keyword_source"],
                    "keyword_category": keyword_meta.get("category", ""),
                    "keyword": keyword,
                    "trend_type": trend_type,
                    "related_query": row["query"],
                    "value": value,
                    "is_rising_alert": "true" if is_rising_alert else "false",
                    "timeframe_actual": run_context["timeframe_actual"],
                    "geo": run_context["geo"],
                    "source_json_file": source_json_file,
                }
            )

    return detail_rows


def collect_rising_alerts(detail_rows):
    return [
        {
            "keyword": row["keyword"],
            "related_query": row["related_query"],
            "value": row["value"],
            "keyword_category": row.get("keyword_category", ""),
        }
        for row in detail_rows
        if row["trend_type"] == "rising" and row["is_rising_alert"] == "true"
    ]


def build_summary_row(
    run_context,
    report_csv_path,
    data_directory,
    keywords_total,
    keywords_success,
    keywords_failed,
    rising_alert_count,
    status,
    error_message="",
):
    return {
        "run_id": run_context["run_id"],
        "run_date": run_context["run_date"],
        "started_at": run_context["started_at"],
        "finished_at": run_context["finished_at"],
        "keyword_source": run_context["keyword_source"],
        "timeframe_requested": run_context["timeframe_requested"],
        "timeframe_actual": run_context["timeframe_actual"],
        "geo": run_context["geo"],
        "keywords_total": keywords_total,
        "keywords_success": keywords_success,
        "keywords_failed": keywords_failed,
        "rising_alert_count": rising_alert_count,
        "report_csv_path": report_csv_path or "",
        "data_directory": data_directory or "",
        "status": status,
        "error_message": error_message,
    }


def resolve_run_keywords(requested_source=None, manual_keywords=None, refresh_keyword_library=False):
    if manual_keywords:
        return {
            "keywords": manual_keywords,
            "keyword_source": "manual",
            "keyword_index": {},
            "library_payload": None,
        }

    if not KEYWORD_LIBRARY_CONFIG["enabled"]:
        return {
            "keywords": list(KEYWORDS),
            "keyword_source": "static",
            "keyword_index": {},
            "library_payload": None,
        }

    if refresh_keyword_library:
        payload, _ = sync_keyword_library()
    else:
        payload = load_keyword_library_payload(refresh_if_missing=True)

    keyword_source = requested_source or KEYWORD_LIBRARY_CONFIG["default_run_source"]
    keywords = get_keywords_for_source(payload, keyword_source)
    return {
        "keywords": keywords,
        "keyword_source": keyword_source,
        "keyword_index": payload.get("keyword_index", {}),
        "library_payload": payload,
    }


@backoff.on_exception(
    backoff.expo,
    Exception,
    max_tries=RATE_LIMIT_CONFIG["max_retries"],
    jitter=backoff.full_jitter,
)
def get_trends_with_retry(keywords_batch, timeframe):
    return batch_get_queries(
        keywords_batch,
        timeframe=timeframe,
        geo=TRENDS_CONFIG["geo"],
        delay_between_queries=random.uniform(
            RATE_LIMIT_CONFIG["min_delay_between_queries"],
            RATE_LIMIT_CONFIG["max_delay_between_queries"],
        ),
    )


def send_daily_summary_notification(summary_row, workbook_url):
    return notification_manager.send_notification(
        "daily_summary",
        {
            "summary": summary_row,
            "workbook_url": workbook_url,
        },
    )


def send_rising_alert_notification(summary_row, alerts):
    if not alerts:
        return False
    return notification_manager.send_notification(
        "rising_alert",
        {
            "summary": summary_row,
            "alerts": alerts,
        },
    )


def send_error_notification(summary_row, error_message):
    return notification_manager.send_notification(
        "error",
        {
            "summary": summary_row,
            "error_message": error_message,
        },
    )


def send_opportunity_alert_notification(summary_row, opportunities, workbook_url):
    if not opportunities or not OPPORTUNITY_PIPELINE_CONFIG["opportunity_alert_enabled"]:
        return False
    return notification_manager.send_notification(
        "opportunity_alert",
        {
            "summary": summary_row,
            "opportunities": opportunities,
            "workbook_url": workbook_url,
        },
    )


def build_opportunity_review_row(run_context, candidate, serp_summary, analysis):
    decision = analysis.get("decision", "watch")
    collected_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return {
        "run_id": run_context["run_id"],
        "keyword_source": run_context["keyword_source"],
        "seed_keyword": candidate.seed_keyword,
        "candidate_keyword": candidate.candidate_keyword,
        "trend_type": candidate.trend_type,
        "seven_day_value": candidate.seven_day_value,
        "thirty_day_value": candidate.thirty_day_value,
        "growth_persistence": candidate.growth_persistence,
        "is_new_term": "true" if candidate.is_new_term else "false",
        "serp_result_count": serp_summary.result_count,
        "serp_ads_present": "true" if serp_summary.ads_present else "false",
        "serp_product_pages": serp_summary.product_pages,
        "serp_forum_pages": serp_summary.forum_pages,
        "serp_pricing_pages": serp_summary.pricing_pages,
        "serp_summary": serp_summary.summary,
        "ai_demand_score": analysis.get("demand_score", ""),
        "ai_payment_intent_score": analysis.get("payment_intent_score", ""),
        "ai_commercial_intent": analysis.get("commercial_intent", ""),
        "ai_topic_type": analysis.get("topic_type", ""),
        "ai_target_user": analysis.get("target_user", ""),
        "ai_why_now": analysis.get("why_now", ""),
        "ai_short_reason": analysis.get("short_reason", ""),
        "ai_noise_flag": (
            str(analysis.get("noise_flag", "")).lower()
            if analysis.get("noise_flag", "") != ""
            else ""
        ),
        "decision": decision,
        "collected_at": collected_at,
    }


def run_opportunity_pipeline(run_context, alerts, keyword_index, pipeline_options):
    if not pipeline_options["enabled"] or not alerts:
        return []

    candidates = validate_rising_candidates(
        alerts,
        run_context["keyword_source"],
        keyword_index,
        run_context["run_date"],
    )
    candidates = [
        candidate
        for candidate in candidates
        if candidate.validation_status == "validated" and candidate.is_new_term
    ]
    if not candidates:
        return []

    collector = None if pipeline_options["skip_serp"] else GoogleSerpCollector()
    analyzer = OpportunityAnalyzer()
    opportunity_rows = []

    try:
        for candidate in candidates[: pipeline_options["max_candidates"]]:
            serp_summary = (
                SerpSummary(
                    status="skipped",
                    result_count=0,
                    ads_present=False,
                    product_pages=0,
                    forum_pages=0,
                    pricing_pages=0,
                    summary="SERP collection skipped by CLI flag.",
                    results=[],
                )
                if collector is None
                else collector.search(candidate.candidate_keyword)
            )

            if pipeline_options["skip_ai"] or not analyzer.is_enabled():
                analysis = dict(DEFAULT_ANALYSIS)
                analysis.update(
                    {
                        "demand_score": "",
                        "payment_intent_score": "",
                        "commercial_intent": "unavailable",
                        "topic_type": "",
                        "target_user": "",
                        "why_now": "",
                        "short_reason": "AI analysis skipped or API key missing.",
                        "noise_flag": "",
                        "decision": "watch",
                    }
                )
            else:
                try:
                    analysis = analyzer.analyze(candidate, serp_summary)
                except Exception as exc:
                    logging.warning("AI analysis failed for %s: %s", candidate.candidate_keyword, exc)
                    analysis = dict(DEFAULT_ANALYSIS)
                    analysis.update(
                        {
                            "demand_score": "",
                            "payment_intent_score": "",
                            "commercial_intent": "error",
                            "topic_type": "",
                            "target_user": "",
                            "why_now": "",
                            "short_reason": f"AI analysis failed: {exc}",
                            "noise_flag": "",
                            "decision": "watch",
                        }
                    )

            opportunity_rows.append(
                build_opportunity_review_row(run_context, candidate, serp_summary, analysis)
            )
    finally:
        if collector is not None:
            collector.close()

    return opportunity_rows


def process_trends(keywords, keyword_source, keyword_index=None, pipeline_options=None):
    keyword_index = keyword_index or {}
    pipeline_options = pipeline_options or {
        "enabled": OPPORTUNITY_PIPELINE_CONFIG["enabled"],
        "max_candidates": OPPORTUNITY_PIPELINE_CONFIG["max_candidates_per_run"],
        "skip_serp": False,
        "skip_ai": False,
    }
    started_at = datetime.now()
    run_context = {
        "run_id": uuid.uuid4().hex[:12],
        "run_date": started_at.strftime("%Y-%m-%d"),
        "started_at": started_at.strftime("%Y-%m-%d %H:%M:%S"),
        "keyword_source": keyword_source,
        "timeframe_requested": TRENDS_CONFIG["timeframe"],
        "timeframe_actual": get_date_range_timeframe(TRENDS_CONFIG["timeframe"]),
        "geo": TRENDS_CONFIG["geo"] or "Global",
        "finished_at": "",
    }

    directory = None
    report_file = None
    all_results = {}
    detail_rows = []
    opportunity_rows = []
    failed_keywords = []
    workbook = None
    summary_synced = False
    detail_sync_attempted = False

    try:
        logging.info(
            "Starting trends processing run_id=%s source=%s timeframe=%s geo=%s",
            run_context["run_id"],
            run_context["keyword_source"],
            run_context["timeframe_actual"],
            run_context["geo"],
        )
        directory = create_daily_directory()

        for index in range(0, len(keywords), RATE_LIMIT_CONFIG["batch_size"]):
            keywords_batch = keywords[index : index + RATE_LIMIT_CONFIG["batch_size"]]
            logging.info("Processing batch: %s", keywords_batch)
            results = get_trends_with_retry(keywords_batch, run_context["timeframe_actual"])

            for keyword in keywords_batch:
                data = results.get(keyword)
                if not data:
                    failed_keywords.append(keyword)
                    continue

                filename = save_related_queries(keyword, data)
                source_json_file = ""
                if filename:
                    destination = os.path.join(directory, os.path.basename(filename))
                    shutil.move(filename, destination)
                    source_json_file = destination

                all_results[keyword] = data
                detail_rows.extend(
                    build_detail_rows(
                        run_context,
                        keyword,
                        data,
                        source_json_file,
                        keyword_index.get(keyword, {}),
                    )
                )

            if index + RATE_LIMIT_CONFIG["batch_size"] < len(keywords):
                wait_time = RATE_LIMIT_CONFIG["batch_interval"] + random.uniform(0, 60)
                logging.info("Waiting %.1f seconds before the next batch", wait_time)
                time.sleep(wait_time)

        report_file = generate_daily_report(all_results, directory)
        run_context["finished_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        alerts = collect_rising_alerts(detail_rows)

        # 【优化3：日志准确性】根据实际采集结果决定最终状态
        # - 全部成功：completed
        # - 部分成功：partial_success
        # - 全部失败：all_failed（触发 error 通知，不假装成功）
        if not failed_keywords:
            status = "completed"
        elif len(all_results) > 0:
            status = "partial_success"
        else:
            status = "all_failed"

        error_message = f"所有 {len(keywords)} 个关键词均采集失败，请检查网络和浏览器状态。" if status == "all_failed" else ""

        summary_row = build_summary_row(
            run_context=run_context,
            report_csv_path=report_file,
            data_directory=directory,
            keywords_total=len(keywords),
            keywords_success=len(all_results),
            keywords_failed=len(failed_keywords),
            rising_alert_count=len(alerts),
            status=status,
            error_message=error_message,
        )

        workbook = FeishuWorkbook.from_config()
        workbook.append_daily_summary(summary_row)
        summary_synced = True
        detail_sync_attempted = True
        workbook.append_trend_details(detail_rows)

        # 【优化3】全部失败时发送错误通知，而不是乐观的日常汇报
        if status == "all_failed":
            logging.error("所有关键词采集均失败，触发错误通知。")
            send_error_notification(summary_row, error_message)
        else:
            send_daily_summary_notification(summary_row, workbook.spreadsheet_url)
            if alerts:
                send_rising_alert_notification(summary_row, alerts)
        close_browser_related_queries_collector()
        try:
            opportunity_rows = run_opportunity_pipeline(
                run_context,
                alerts,
                keyword_index,
                pipeline_options,
            )
            if opportunity_rows:
                workbook.append_opportunity_reviews(opportunity_rows)
                opportunity_alerts = [
                    row for row in opportunity_rows if row.get("decision") == "opportunity"
                ]
                if opportunity_alerts:
                    send_opportunity_alert_notification(
                        summary_row,
                        opportunity_alerts,
                        workbook.spreadsheet_url,
                    )
        except Exception as opportunity_exc:
            logging.exception("Opportunity pipeline failed: %s", opportunity_exc)

        logging.info("Trends processing completed successfully for run_id=%s", run_context["run_id"])
        return {
            "summary": summary_row,
            "alerts": alerts,
            "detail_rows": detail_rows,
            "opportunity_rows": opportunity_rows,
            "failed_keywords": failed_keywords,
            "workbook_url": workbook.spreadsheet_url,
        }
    except Exception as exc:
        close_browser_related_queries_collector()
        logging.exception("Error in trends processing")
        run_context["finished_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        error_message = str(exc)
        alerts = collect_rising_alerts(detail_rows)
        summary_row = build_summary_row(
            run_context=run_context,
            report_csv_path=report_file,
            data_directory=directory,
            keywords_total=len(keywords),
            keywords_success=len(all_results),
            keywords_failed=max(len(keywords) - len(all_results), len(failed_keywords)),
            rising_alert_count=len(alerts),
            status="failed",
            error_message=error_message,
        )

        workbook_url = ""
        try:
            if workbook is None:
                workbook = FeishuWorkbook.from_config()
            workbook_url = workbook.spreadsheet_url
            if not summary_synced:
                workbook.append_daily_summary(summary_row)
            if detail_rows and not detail_sync_attempted:
                workbook.append_trend_details(detail_rows)
        except Exception as workbook_exc:
            logging.exception("Failed to sync error result to Feishu workbook: %s", workbook_exc)

        send_error_notification(summary_row, error_message)
        return {
            "summary": summary_row,
            "alerts": alerts,
            "detail_rows": detail_rows,
            "opportunity_rows": opportunity_rows,
            "failed_keywords": failed_keywords,
            "workbook_url": workbook_url,
            "error": error_message,
        }


def validate_runtime_config():
    placeholder_values = {
        "your-webhook-token",
        "cli_xxx",
        "xxx",
    }

    def is_placeholder(value):
        text = str(value or "").strip()
        if not text:
            return True
        if text in placeholder_values:
            return True
        return text.endswith("/your-webhook-token")

    if not FEISHU_CONFIG["enabled"]:
        raise RuntimeError("Feishu integration is disabled. Set FEISHU_ENABLED=true.")
    if is_placeholder(FEISHU_CONFIG["webhook_url"]):
        raise RuntimeError("Missing FEISHU_WEBHOOK_URL in .env.")
    if is_placeholder(FEISHU_CONFIG["app_id"]) or is_placeholder(FEISHU_CONFIG["app_secret"]):
        raise RuntimeError("Missing FEISHU_APP_ID or FEISHU_APP_SECRET in .env.")
    if not os.path.exists(FEISHU_CONFIG["user_token_file"]):
        raise RuntimeError(
            "Feishu user token file not found. Run `python setup_feishu_user_auth.py` first."
        )


def run_scheduler(keywords, keyword_source, keyword_index=None, pipeline_options=None):
    schedule_hour = SCHEDULE_CONFIG["hour"]
    schedule_minute = SCHEDULE_CONFIG.get("minute", 0)

    if SCHEDULE_CONFIG.get("random_delay_minutes", 0) > 0:
        random_minutes = random.randint(0, SCHEDULE_CONFIG["random_delay_minutes"])
        total_minutes = schedule_hour * 60 + schedule_minute + random_minutes
        schedule_hour = (total_minutes // 60) % 24
        schedule_minute = total_minutes % 60

    schedule_time = f"{schedule_hour:02d}:{schedule_minute:02d}"
    schedule.every().day.at(schedule_time).do(
        process_trends,
        keywords,
        keyword_source,
        keyword_index,
        pipeline_options,
    )
    logging.info("Scheduler started. Will run daily at %s using source=%s", schedule_time, keyword_source)

    now = datetime.now()
    scheduled_time = now.replace(hour=schedule_hour, minute=schedule_minute, second=0, microsecond=0)
    if now >= scheduled_time:
        next_run = scheduled_time + timedelta(days=1)
        logging.info("Current time is past the scheduled time, waiting for tomorrow")
        time.sleep((next_run - now).total_seconds())

    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Google Trends Monitor")
    parser.add_argument(
        "--test",
        action="store_true",
        help="Run once immediately instead of waiting for the schedule",
    )
    parser.add_argument(
        "--keywords",
        nargs="+",
        help="Override configured keywords for test mode",
    )
    parser.add_argument(
        "--timeframe",
        type=str,
        help="Override the timeframe for query (e.g., '7d', '30d', 'now 7-d', 'today 1-m')",
    )
    parser.add_argument(
        "--keyword-source",
        help="Keyword source to use: primary or rotation_group_n",
    )
    parser.add_argument(
        "--refresh-keyword-library",
        action="store_true",
        help="Refresh keyword library from Feishu before running",
    )
    parser.add_argument(
        "--sync-keyword-library",
        action="store_true",
        help="Sync keyword library artifact from Feishu and exit",
    )
    parser.add_argument(
        "--enable-opportunity-analysis",
        action="store_true",
        help="Force-enable the opportunity pipeline for this run",
    )
    parser.add_argument(
        "--max-opportunity-candidates",
        type=int,
        help="Override the max number of opportunity candidates per run",
    )
    parser.add_argument(
        "--skip-serp",
        action="store_true",
        help="Skip Google SERP collection during opportunity analysis",
    )
    parser.add_argument(
        "--skip-ai",
        action="store_true",
        help="Skip AI scoring during opportunity analysis",
    )
    args = parser.parse_args()

    validate_runtime_config()

    if args.sync_keyword_library:
        payload, artifact_path = sync_keyword_library()
        logging.info(
            "Keyword library synced to %s with %s primary keywords",
            artifact_path,
            payload["stats"]["primary_keyword_count"],
        )
    else:
        # 【优化1】解析命令行提供的 timeframe 并覆盖配置
        if args.timeframe:
            timeframe_map = {
                "7d": "now 7-d",
                "30d": "today 1-m",
                "90d": "today 3-m",
                "12m": "today 12-m",
            }
            TRENDS_CONFIG["timeframe"] = timeframe_map.get(args.timeframe, args.timeframe)
            logging.info("Override timeframe to: %s", TRENDS_CONFIG["timeframe"])

        resolved = resolve_run_keywords(
            requested_source=args.keyword_source,
            manual_keywords=args.keywords,
            refresh_keyword_library=args.refresh_keyword_library,
        )
        keywords = resolved["keywords"]
        keyword_source = resolved["keyword_source"]
        keyword_index = resolved["keyword_index"]

        logging.info(
            "Using keyword source=%s with %s keywords",
            keyword_source,
            len(keywords),
        )

        pipeline_options = {
            "enabled": args.enable_opportunity_analysis or OPPORTUNITY_PIPELINE_CONFIG["enabled"],
            "max_candidates": args.max_opportunity_candidates
            or OPPORTUNITY_PIPELINE_CONFIG["max_candidates_per_run"],
            "skip_serp": args.skip_serp,
            "skip_ai": args.skip_ai,
        }

        if args.test:
            logging.info("Running in test mode")
            process_trends(keywords, keyword_source, keyword_index, pipeline_options)
        else:
            run_scheduler(keywords, keyword_source, keyword_index, pipeline_options)
