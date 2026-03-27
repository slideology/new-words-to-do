import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

import pandas as pd

from config import OPPORTUNITY_PIPELINE_CONFIG, TRENDS_CONFIG
from querytrends import get_interest_over_time


logger = logging.getLogger(__name__)

LOW_SIGNAL_TERMS = {
    "app",
    "api",
    "free",
    "online",
    "apk",
}

GENERIC_BRAND_TERMS = {
    "openai",
    "chatgpt",
    "gemini",
    "claude",
    "cursor",
    "windsurf",
    "felo",
    "bytedance",
}


@dataclass
class CandidateValidation:
    seed_keyword: str
    candidate_keyword: str
    trend_type: str
    seven_day_value: str
    thirty_day_value: float
    growth_persistence: float
    is_new_term: bool
    validation_status: str
    keyword_source: str
    keyword_category: str
    comparison_keyword: str
    recent_nonzero_days: int
    prior_nonzero_days: int
    trend_snapshot: List[Dict[str, object]]


def _normalize_keyword(value):
    return " ".join(str(value or "").strip().lower().split())


def _token_count(value):
    return len([part for part in _normalize_keyword(value).split(" ") if part])


def _load_cache(path):
    cache_path = Path(path)
    if not cache_path.exists():
        return {"seen_candidates": {}}
    try:
        return json.loads(cache_path.read_text(encoding="utf-8"))
    except Exception:
        return {"seen_candidates": {}}


def save_opportunity_cache(cache):
    cache_path = Path(OPPORTUNITY_PIPELINE_CONFIG["cache_file"])
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")


def load_opportunity_cache():
    return _load_cache(OPPORTUNITY_PIPELINE_CONFIG["cache_file"])


def _is_low_signal_term(keyword):
    normalized = _normalize_keyword(keyword)
    if normalized in LOW_SIGNAL_TERMS or normalized in GENERIC_BRAND_TERMS:
        return True
    return _token_count(keyword) <= 1 and normalized not in {
        "poki",
        "crazygames",
        "replit",
        "notion",
    }


def _to_numeric_series(dataframe, candidate_keyword):
    series = dataframe[candidate_keyword].copy()
    series = pd.to_numeric(series, errors="coerce").fillna(0)
    if "isPartial" in dataframe.columns:
        partial_mask = dataframe["isPartial"].astype(bool)
        series = series[~partial_mask]
    return series


def _build_snapshot(dataframe, candidate_keyword, comparison_keyword):
    snapshot_rows = []
    trimmed = dataframe.tail(10)
    for index, row in trimmed.iterrows():
        row_payload = {
            "date": str(index.date() if hasattr(index, "date") else index),
            "candidate_value": float(pd.to_numeric(row.get(candidate_keyword), errors="coerce") or 0),
            "comparison_value": float(pd.to_numeric(row.get(comparison_keyword), errors="coerce") or 0),
        }
        if "isPartial" in row:
            row_payload["is_partial"] = bool(row["isPartial"])
        snapshot_rows.append(row_payload)
    return snapshot_rows


def _validate_candidate(alert, keyword_source, keyword_index, cache, run_date):
    candidate_keyword = str(alert["related_query"]).strip()
    seed_keyword = str(alert["keyword"]).strip()
    
    # 【优化2】对比词不再用 seed_keyword 本身，而是使用配置的基准参照物，默认为 GPTs
    comparison_keyword = OPPORTUNITY_PIPELINE_CONFIG.get("comparison_baseline", "GPTs")
    normalized_candidate = _normalize_keyword(candidate_keyword)

    if cache.get("seen_candidates", {}).get(normalized_candidate, {}).get("last_run_date") == run_date:
        logger.info("Skip already analyzed candidate today: %s", candidate_keyword)
        return None

    if _is_low_signal_term(candidate_keyword):
        logger.info("Skip low-signal candidate: %s", candidate_keyword)
        return None

    timeframe = OPPORTUNITY_PIPELINE_CONFIG["thirty_day_timeframe"]
    dataframe = get_interest_over_time(
        [candidate_keyword, comparison_keyword],
        geo=TRENDS_CONFIG["geo"],
        timeframe=timeframe,
    )
    if not isinstance(dataframe, pd.DataFrame) or candidate_keyword not in dataframe.columns:
        return None

    candidate_series = _to_numeric_series(dataframe, candidate_keyword)
    if candidate_series.empty:
        return None

    recent_window_days = OPPORTUNITY_PIPELINE_CONFIG["recent_window_days"]
    recent_window = candidate_series.tail(recent_window_days)
    prior_window = candidate_series.iloc[:-recent_window_days] if len(candidate_series) > recent_window_days else candidate_series.iloc[:0]

    recent_avg = float(recent_window.mean()) if not recent_window.empty else 0.0
    prior_avg = float(prior_window.mean()) if not prior_window.empty else 0.0
    recent_max = float(recent_window.max()) if not recent_window.empty else 0.0
    prior_max = float(prior_window.max()) if not prior_window.empty else 0.0
    recent_nonzero_days = int((recent_window > 0).sum()) if not recent_window.empty else 0
    prior_nonzero_days = int((prior_window > 0).sum()) if not prior_window.empty else 0

    if prior_avg <= 0:
        growth_persistence = recent_avg if recent_avg > 0 else 0.0
    else:
        growth_persistence = recent_avg / prior_avg

    min_value = OPPORTUNITY_PIPELINE_CONFIG["thirty_day_min_value"]
    passed_validation = recent_max >= min_value and recent_nonzero_days >= 2
    is_new_term = passed_validation and prior_max <= 0 and prior_nonzero_days == 0

    status = "validated" if passed_validation else "rejected_30d"
    validation = CandidateValidation(
        seed_keyword=seed_keyword,
        candidate_keyword=candidate_keyword,
        trend_type="rising",
        seven_day_value=str(alert["value"]),
        thirty_day_value=recent_max,
        growth_persistence=round(growth_persistence, 2),
        is_new_term=is_new_term,
        validation_status=status,
        keyword_source=keyword_source,
        keyword_category=alert.get("keyword_category", keyword_index.get(seed_keyword, {}).get("category", "")),
        comparison_keyword=comparison_keyword,
        recent_nonzero_days=recent_nonzero_days,
        prior_nonzero_days=prior_nonzero_days,
        trend_snapshot=_build_snapshot(dataframe, candidate_keyword, comparison_keyword),
    )
    return validation


def validate_rising_candidates(alerts, keyword_source, keyword_index, run_date):
    cache = load_opportunity_cache()
    unique_alerts = []
    seen = set()
    threshold = OPPORTUNITY_PIPELINE_CONFIG["seven_day_threshold"]

    for alert in alerts:
        normalized = _normalize_keyword(alert["related_query"])
        if normalized in seen:
            continue
        seen.add(normalized)
        value = str(alert.get("value", "")).strip().lower()
        if value == "breakout":
            unique_alerts.append(alert)
            continue
        try:
            if float(alert["value"]) >= threshold:
                unique_alerts.append(alert)
        except (TypeError, ValueError):
            continue

    candidates = []
    max_candidates = OPPORTUNITY_PIPELINE_CONFIG["max_candidates_per_run"]
    for alert in unique_alerts[:max_candidates]:
        try:
            validated = _validate_candidate(alert, keyword_source, keyword_index, cache, run_date)
        except Exception as exc:
            logger.warning("30-day validation failed for %s: %s", alert.get("related_query"), exc)
            continue
        if not validated:
            continue
        candidates.append(validated)
        cache.setdefault("seen_candidates", {})[_normalize_keyword(validated.candidate_keyword)] = {
            "last_run_date": run_date,
            "seed_keyword": validated.seed_keyword,
            "validation_status": validated.validation_status,
            "is_new_term": validated.is_new_term,
        }

    save_opportunity_cache(cache)
    return candidates
