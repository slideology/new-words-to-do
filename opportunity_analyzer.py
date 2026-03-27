import json
import logging

import requests

from config import OPPORTUNITY_PIPELINE_CONFIG


logger = logging.getLogger(__name__)


DEFAULT_ANALYSIS = {
    "demand_score": 0,
    "payment_intent_score": 0,
    "commercial_intent": "low",
    "topic_type": "unknown",
    "target_user": "unknown",
    "why_now": "",
    "short_reason": "",
    "noise_flag": True,
}


def derive_decision(analysis):
    demand_score = int(analysis.get("demand_score", 0) or 0)
    payment_score = int(analysis.get("payment_intent_score", 0) or 0)
    commercial_intent = str(analysis.get("commercial_intent", "low")).lower()
    noise_flag = bool(analysis.get("noise_flag", False))

    if noise_flag or demand_score < 35 or payment_score < 25:
        return "ignore"
    if demand_score >= 70 and payment_score >= 60 and commercial_intent in {"medium", "high"}:
        return "opportunity"
    return "watch"


class OpportunityAnalyzer:
    def __init__(self):
        self.api_key = OPPORTUNITY_PIPELINE_CONFIG["gemini_api_key"]
        self.model = OPPORTUNITY_PIPELINE_CONFIG["gemini_model"]
        self.base_url = OPPORTUNITY_PIPELINE_CONFIG["gemini_base_url"].rstrip("/")
        self.timeout = 40

    def is_enabled(self):
        return OPPORTUNITY_PIPELINE_CONFIG["ai_enabled"] and bool(self.api_key)

    def _build_prompt(self, candidate, serp_summary):
        serp_results = serp_summary.results[:5]
        organic_preview = [
            {
                "title": item["title"],
                "url": item["url"],
                "snippet": item["snippet"],
            }
            for item in serp_results
        ]
        return {
            "candidate_keyword": candidate.candidate_keyword,
            "seed_keyword": candidate.seed_keyword,
            "keyword_source": candidate.keyword_source,
            "keyword_category": candidate.keyword_category,
            "seven_day_value": candidate.seven_day_value,
            "thirty_day_value": candidate.thirty_day_value,
            "growth_persistence": candidate.growth_persistence,
            "is_new_term": candidate.is_new_term,
            "recent_nonzero_days": candidate.recent_nonzero_days,
            "prior_nonzero_days": candidate.prior_nonzero_days,
            "serp_status": serp_summary.status,
            "serp_result_count": serp_summary.result_count,
            "serp_ads_present": serp_summary.ads_present,
            "serp_product_pages": serp_summary.product_pages,
            "serp_forum_pages": serp_summary.forum_pages,
            "serp_pricing_pages": serp_summary.pricing_pages,
            "serp_summary": serp_summary.summary,
            "serp_results": organic_preview,
        }

    def _parse_content(self, response_json):
        candidates = response_json.get("candidates", [])
        if not candidates:
            raise RuntimeError(f"Missing candidates in Gemini response: {response_json}")
        parts = candidates[0].get("content", {}).get("parts", [])
        if not parts:
            raise RuntimeError(f"Missing parts in Gemini response: {response_json}")
        text = parts[0].get("text", "")
        if not text:
            raise RuntimeError(f"Missing text in Gemini response: {response_json}")
        return json.loads(text)

    def analyze(self, candidate, serp_summary):
        if not self.is_enabled():
            return dict(DEFAULT_ANALYSIS)

        system_prompt = (
            "You are a startup keyword analyst. "
            "Given Google Trends breakout evidence and a Google SERP summary, "
            "score whether the keyword shows real demand and willingness to pay. "
            "Return valid JSON only."
        )
        user_prompt = {
            "task": (
                "Analyze whether this keyword is an actual emerging opportunity. "
                "Score real search demand and willingness to pay on a 0-100 scale. "
                "Treat noise, meme-only, or purely informational terms as low monetization. "
                "Respond with JSON keys: demand_score, payment_intent_score, "
                "commercial_intent, topic_type, target_user, why_now, short_reason, noise_flag."
            ),
            "input": self._build_prompt(candidate, serp_summary),
        }

        response = requests.post(
            f"{self.base_url}/models/{self.model}:generateContent",
            headers={"Content-Type": "application/json"},
            params={"key": self.api_key},
            json={
                "system_instruction": {
                    "parts": [{"text": system_prompt}],
                },
                "contents": [
                    {
                        "role": "user",
                        "parts": [{"text": json.dumps(user_prompt, ensure_ascii=False)}],
                    }
                ],
                "generationConfig": {
                    "temperature": 0.2,
                    "responseMimeType": "application/json",
                    "responseSchema": {
                        "type": "OBJECT",
                        "properties": {
                            "demand_score": {"type": "NUMBER"},
                            "payment_intent_score": {"type": "NUMBER"},
                            "commercial_intent": {"type": "STRING"},
                            "topic_type": {"type": "STRING"},
                            "target_user": {"type": "STRING"},
                            "why_now": {"type": "STRING"},
                            "short_reason": {"type": "STRING"},
                            "noise_flag": {"type": "BOOLEAN"},
                        },
                        "required": [
                            "demand_score",
                            "payment_intent_score",
                            "commercial_intent",
                            "topic_type",
                            "target_user",
                            "why_now",
                            "short_reason",
                            "noise_flag",
                        ],
                    },
                },
            },
            timeout=self.timeout,
        )
        response.raise_for_status()
        parsed = self._parse_content(response.json())
        analysis = dict(DEFAULT_ANALYSIS)
        analysis.update(parsed)
        analysis["decision"] = derive_decision(analysis)
        return analysis
