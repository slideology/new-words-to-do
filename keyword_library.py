import json
import logging
import re
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from config import KEYWORD_LIBRARY_CONFIG
from feishu_integration import create_feishu_client


logger = logging.getLogger(__name__)

AI_CATEGORIES = {
    "大模型/接口",
    "部署/托管",
    "生成器",
    "热点IP",
    "生成/编辑",
    "检测/安全",
    "代码/开发",
    "音频/语音",
    "文档/总结",
    "效率/办公",
    "长尾前缀",
    "搜索/对话",
    "部署/无代码",
    "现象级旗舰模型",
    "AI生成设计",
    "开发者低代码",
    "办公效率",
    "搜索对话智能体",
    "部署托管API",
}

GAME_CATEGORIES = {
    "游戏玩法",
    "游戏平台",
    "游戏技术",
    "游戏变现",
    "经典玩法",
    "游戏引擎",
    "发布平台",
    "视觉风格",
    "轻量/即玩",
}

REJECT_PRIMARY_KEYWORDS = {
    "free",
    "online",
    "no sign up",
    "app",
    "apk",
    "api",
    "prompt",
    "github",
    "openai",
    "chatgpt",
    "gemini",
    "claude",
    "grok",
    "bytedance",
}

SINGLE_WORD_ALLOWLIST = {
    "cursor",
    "windsurf",
    "felo",
    "poki",
    "crazygames",
    "kongregate",
    "notion",
    "gamma",
    "tldraw",
    "miro",
    "canva",
    "grammarly",
    "replit",
    "modal",
    "runpod",
    "baseten",
    "deepinfra",
    "suno",
    "haiper",
    "pika",
    "phind",
    "consensus",
    "scispace",
    "chatpdf",
    "chatdoc",
}


def _extract_cell_link(cell):
    if isinstance(cell, list) and cell:
        first = cell[0]
        if isinstance(first, dict):
            return first.get("link") or first.get("text") or ""
    return str(cell or "").strip()


def _normalize_keyword(value):
    return re.sub(r"\s+", " ", str(value or "").strip())


def _keyword_token_count(keyword):
    return len(re.findall(r"[a-z0-9]+", keyword.lower()))


def _parse_source_from_wiki_url(url):
    if not url:
        return "", ""
    parsed = urlparse(url)
    wiki_token = ""
    if parsed.path.startswith("/wiki/"):
        wiki_token = parsed.path.split("/wiki/", 1)[1].split("/", 1)[0]
    query = parse_qs(parsed.query)
    sheet_id = query.get("sheet", [""])[0]
    return wiki_token, sheet_id


def _domain_for_category(category):
    if category in AI_CATEGORIES:
        return "ai"
    if category in GAME_CATEGORIES:
        return "game"
    return "other"


def _domain_for_keyword(keyword, category=""):
    if category:
        return _domain_for_category(category)
    lowered = str(keyword or "").lower()
    if "game" in lowered or "games" in lowered:
        return "game"
    if "ai" in lowered or "llm" in lowered or "gpt" in lowered:
        return "ai"
    return "other"


def _strength_for_keyword(keyword):
    lower = keyword.lower()
    token_count = _keyword_token_count(keyword)
    if lower in REJECT_PRIMARY_KEYWORDS:
        return "weak"
    if token_count >= 2:
        return "strong"
    if lower in SINGLE_WORD_ALLOWLIST:
        return "strong"
    if token_count == 1:
        return "maybe"
    return "weak"


def _score_for_primary(row):
    strength_weight = {
        "strong": 3,
        "maybe": 2,
        "weak": 1,
    }
    score = strength_weight.get(row["strength"], 0) * 100
    token_count = _keyword_token_count(row["keyword"])
    score += min(token_count, 4) * 10
    if row["domain"] == "ai":
        score += 5
    if row["category"] == "长尾前缀":
        score -= 80
    if row["keyword"].lower() in SINGLE_WORD_ALLOWLIST:
        score -= 10
    if row["keyword"].lower() in REJECT_PRIMARY_KEYWORDS:
        score -= 1000
    return score


def _build_manual_row(keyword):
    normalized_keyword = _normalize_keyword(keyword)
    category = "手动保留"
    return {
        "row": 0,
        "category": category,
        "keyword": normalized_keyword,
        "normalized_keyword": normalized_keyword.lower(),
        "link": "",
        "domain": _domain_for_keyword(normalized_keyword, category=""),
        "strength": _strength_for_keyword(normalized_keyword),
    }


def _resolve_sheet_source(client):
    spreadsheet_token = KEYWORD_LIBRARY_CONFIG["spreadsheet_token"]
    sheet_id = KEYWORD_LIBRARY_CONFIG["sheet_id"]
    wiki_url = KEYWORD_LIBRARY_CONFIG["wiki_url"]

    if spreadsheet_token and sheet_id:
        return spreadsheet_token, sheet_id

    wiki_token, url_sheet_id = _parse_source_from_wiki_url(wiki_url)
    if url_sheet_id and not sheet_id:
        sheet_id = url_sheet_id
    if wiki_token and not spreadsheet_token:
        node = client.get_wiki_node(wiki_token, as_user=True)
        spreadsheet_token = node.get("obj_token", "")
    if not spreadsheet_token or not sheet_id:
        raise RuntimeError("Keyword library source is not fully configured.")
    return spreadsheet_token, sheet_id


def fetch_keyword_rows():
    client = create_feishu_client()
    spreadsheet_token, sheet_id = _resolve_sheet_source(client)
    value_range = f"{sheet_id}!A1:C{KEYWORD_LIBRARY_CONFIG['max_rows']}"
    values = client.read_range(spreadsheet_token, value_range, as_user=True)
    if not values:
        raise RuntimeError("Keyword source sheet returned no data.")

    headers = values[0]
    rows = []
    for row_index, row in enumerate(values[1:], start=2):
        category = _normalize_keyword(row[0] if len(row) > 0 else "")
        keyword = _normalize_keyword(row[1] if len(row) > 1 else "")
        link = _extract_cell_link(row[2] if len(row) > 2 else "")
        if not category and not keyword and not link:
            continue
        if not keyword:
            continue
        normalized = keyword.lower()
        rows.append(
            {
                "row": row_index,
                "category": category,
                "keyword": keyword,
                "normalized_keyword": normalized,
                "link": link,
                "domain": _domain_for_category(category),
                "strength": _strength_for_keyword(keyword),
            }
        )

    return {
        "headers": headers,
        "rows": rows,
        "spreadsheet_token": spreadsheet_token,
        "sheet_id": sheet_id,
        "source_url": KEYWORD_LIBRARY_CONFIG["wiki_url"],
    }


def build_keyword_library_payload():
    source_data = fetch_keyword_rows()
    rows = source_data["rows"]
    unique_index = {}
    duplicate_keywords = {}

    for row in rows:
        normalized = row["normalized_keyword"]
        duplicate_keywords[normalized] = duplicate_keywords.get(normalized, 0) + 1
        if normalized not in unique_index:
            unique_index[normalized] = row
            continue
        current = unique_index[normalized]
        if _score_for_primary(row) > _score_for_primary(current):
            unique_index[normalized] = row

    unique_rows = list(unique_index.values())
    unique_rows.sort(
        key=lambda item: (
            -_score_for_primary(item),
            item["category"],
            item["keyword"].lower(),
        )
    )

    ai_candidates = [row for row in unique_rows if row["domain"] == "ai" and row["strength"] != "weak"]
    game_candidates = [row for row in unique_rows if row["domain"] == "game" and row["strength"] != "weak"]
    other_candidates = [row for row in unique_rows if row["domain"] == "other" and row["strength"] != "weak"]

    selected_primary = []
    per_category_limit = KEYWORD_LIBRARY_CONFIG["primary_per_category_limit"]
    selected_count_by_category = {}
    force_include = {
        item.lower(): item
        for item in KEYWORD_LIBRARY_CONFIG.get("primary_force_include", [])
    }

    def add_candidates(candidates, limit):
        added = 0
        for row in candidates:
            if added >= limit:
                break
            if row["keyword"].lower() in REJECT_PRIMARY_KEYWORDS:
                continue
            current_count = selected_count_by_category.get(row["category"], 0)
            if current_count >= per_category_limit:
                continue
            if row["keyword"] in {item["keyword"] for item in selected_primary}:
                continue
            selected_primary.append(row)
            selected_count_by_category[row["category"]] = current_count + 1
            added += 1

    forced_rows = []
    if force_include:
        for row in unique_rows:
            if row["keyword"].lower() in force_include:
                forced_rows.append(row)
        existing_forced = {row["keyword"].lower() for row in forced_rows}
        for lowered_keyword, original_keyword in force_include.items():
            if lowered_keyword in existing_forced:
                continue
            forced_rows.append(_build_manual_row(original_keyword))

    for row in forced_rows:
        if row["keyword"] in {item["keyword"] for item in selected_primary}:
            continue
        selected_primary.append(row)
        selected_count_by_category[row["category"]] = selected_count_by_category.get(row["category"], 0) + 1

    add_candidates(ai_candidates, KEYWORD_LIBRARY_CONFIG["primary_ai_limit"])
    add_candidates(game_candidates, KEYWORD_LIBRARY_CONFIG["primary_game_limit"])
    remaining_limit = max(0, KEYWORD_LIBRARY_CONFIG["primary_limit"] - len(selected_primary))
    if remaining_limit:
        add_candidates(other_candidates + ai_candidates + game_candidates, remaining_limit)

    selected_primary = selected_primary[: KEYWORD_LIBRARY_CONFIG["primary_limit"]]
    combined_rows = list(unique_rows)
    existing_keywords = {row["keyword"] for row in combined_rows}
    for row in selected_primary:
        if row["keyword"] not in existing_keywords:
            combined_rows.append(row)
            existing_keywords.add(row["keyword"])

    primary_terms = [row["keyword"] for row in selected_primary]
    pool_terms = [row["keyword"] for row in combined_rows]

    rotation_seed_rows = [row for row in combined_rows if row["keyword"] not in set(primary_terms)]
    rotation_groups = {}
    group_size = KEYWORD_LIBRARY_CONFIG["rotation_group_size"]
    for start in range(0, len(rotation_seed_rows), group_size):
        group_index = start // group_size + 1
        rotation_groups[f"rotation_group_{group_index}"] = [
            row["keyword"] for row in rotation_seed_rows[start : start + group_size]
        ]

    payload = {
        "source": {
            "wiki_url": source_data["source_url"],
            "spreadsheet_token": source_data["spreadsheet_token"],
            "sheet_id": source_data["sheet_id"],
            "headers": source_data["headers"],
        },
        "stats": {
            "total_rows": len(rows),
            "unique_keywords": len(unique_rows),
            "categories": sorted({row["category"] for row in rows if row["category"]}),
            "duplicate_keywords": {
                key: value for key, value in sorted(duplicate_keywords.items()) if value > 1
            },
            "primary_keyword_count": len(primary_terms),
            "rotation_group_count": len(rotation_groups),
        },
        "raw_rows": rows,
        "keyword_pool": pool_terms,
        "primary_keywords": primary_terms,
        "keyword_index": {
            row["keyword"]: {
                "category": row["category"],
                "domain": row["domain"],
                "strength": row["strength"],
                "link": row["link"],
                "normalized_keyword": row["normalized_keyword"],
            }
            for row in combined_rows
        },
        "rotation_groups": rotation_groups,
    }
    return payload


def save_keyword_library_payload(payload):
    artifact_path = Path(KEYWORD_LIBRARY_CONFIG["artifact_file"])
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return artifact_path


def sync_keyword_library():
    payload = build_keyword_library_payload()
    artifact_path = save_keyword_library_payload(payload)
    logger.info(
        "Keyword library synced: %s primary keywords, %s unique keywords",
        payload["stats"]["primary_keyword_count"],
        payload["stats"]["unique_keywords"],
    )
    return payload, artifact_path


def load_keyword_library_payload(refresh_if_missing=False):
    artifact_path = Path(KEYWORD_LIBRARY_CONFIG["artifact_file"])
    if artifact_path.exists():
        return json.loads(artifact_path.read_text(encoding="utf-8"))
    if refresh_if_missing:
        payload, _ = sync_keyword_library()
        return payload
    raise RuntimeError(
        f"Keyword library artifact not found: {artifact_path}. Run `python sync_keyword_library.py` first."
    )


def get_keywords_for_source(payload, keyword_source):
    if keyword_source == "primary":
        return payload["primary_keywords"]
    if keyword_source in payload.get("rotation_groups", {}):
        return payload["rotation_groups"][keyword_source]
    raise RuntimeError(f"Unsupported keyword source: {keyword_source}")
