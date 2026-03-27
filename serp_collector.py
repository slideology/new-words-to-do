import logging
import random
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List
from urllib.parse import urlparse

from config import OPPORTUNITY_PIPELINE_CONFIG


logger = logging.getLogger(__name__)

FORUM_DOMAINS = {
    "reddit.com",
    "quora.com",
    "stackoverflow.com",
    "discord.com",
    "steamcommunity.com",
}


@dataclass
class SerpSummary:
    status: str
    result_count: int
    ads_present: bool
    product_pages: int
    forum_pages: int
    pricing_pages: int
    summary: str
    results: List[Dict[str, str]]
    error_message: str = ""


class GoogleSerpCollector:
    def __init__(self):
        self.base_delay = OPPORTUNITY_PIPELINE_CONFIG["google_search_delay_seconds"]
        self.page_timeout_ms = OPPORTUNITY_PIPELINE_CONFIG["google_search_page_timeout_ms"]
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None
        self.connection_mode = ""
        self._start_browser()

    def _start_browser(self):
        try:
            from playwright.sync_api import sync_playwright
        except Exception as exc:
            raise RuntimeError(
                "Playwright is not installed. Run `pip install -r requirements.txt` first."
            ) from exc

        profile_dir = Path(OPPORTUNITY_PIPELINE_CONFIG["google_search_browser_profile_dir"])
        profile_dir.mkdir(parents=True, exist_ok=True)
        self.playwright = sync_playwright().start()

        remote_debugging_url = OPPORTUNITY_PIPELINE_CONFIG["google_search_remote_debugging_url"].strip()
        if remote_debugging_url:
            try:
                self.browser = self.playwright.chromium.connect_over_cdp(remote_debugging_url)
                self.context = self.browser.contexts[0] if self.browser.contexts else self.browser.new_context()
                self.page = self.context.new_page()
                self.page.set_default_timeout(self.page_timeout_ms)
                self.connection_mode = f"cdp:{remote_debugging_url}"
                logger.info("Attached SERP collector to remote Chrome via %s", remote_debugging_url)
                return
            except Exception as exc:
                logger.warning(
                    "Failed to attach to remote Chrome %s, fallback to local launch: %s",
                    remote_debugging_url,
                    exc,
                )

        launch_kwargs = {
            "user_data_dir": str(profile_dir),
            "headless": False,
            "ignore_default_args": ["--enable-automation"],
            "args": [
                "--disable-blink-features=AutomationControlled",
                "--start-maximized",
            ],
            "viewport": {"width": 1440, "height": 960},
        }
        channel = OPPORTUNITY_PIPELINE_CONFIG["google_search_browser_channel"]
        try:
            self.context = self.playwright.chromium.launch_persistent_context(
                channel=channel,
                **launch_kwargs,
            )
        except Exception as exc:
            logger.warning("Failed to launch %s channel, fallback to default chromium: %s", channel, exc)
            self.context = self.playwright.chromium.launch_persistent_context(**launch_kwargs)

        self.page = self.context.pages[0] if self.context.pages else self.context.new_page()
        self.page.set_default_timeout(self.page_timeout_ms)
        self.connection_mode = "local_launch"

    def close(self):
        try:
            if self.page:
                self.page.close()
        finally:
            try:
                if self.context and self.connection_mode == "local_launch":
                    self.context.close()
            finally:
                if self.playwright:
                    self.playwright.stop()

    def _sleep_before_request(self):
        delay = self.base_delay + random.uniform(0, 1.5)
        time.sleep(delay)

    @staticmethod
    def _domain(url):
        try:
            return urlparse(url).netloc.lower()
        except Exception:
            return ""

    @staticmethod
    def _looks_blocked(page_text):
        lowered = page_text.lower()
        return (
            "enablejs" in lowered
            or "unusual traffic" in lowered
            or "detected unusual traffic" in lowered
            or "sorry/index" in lowered
            or "captcha" in lowered
        )

    def _fetch_page(self, keyword):
        from urllib.parse import quote_plus

        self._sleep_before_request()
        url = (
            "https://www.google.com/search?q={query}&num=10&hl={hl}&gl={gl}"
        ).format(
            query=quote_plus(keyword),
            hl=OPPORTUNITY_PIPELINE_CONFIG["google_search_hl"],
            gl=OPPORTUNITY_PIPELINE_CONFIG["google_search_gl"],
        )
        self.page.goto(url, wait_until="domcontentloaded")
        self.page.wait_for_timeout(2500)
        page_text = self.page.locator("body").inner_text(timeout=self.page_timeout_ms)
        return page_text

    def _extract_results(self):
        return self.page.evaluate(
            """() => {
                const items = [];
                const seen = new Set();
                const anchors = Array.from(document.querySelectorAll('a')).filter((a) => a.querySelector('h3'));
                for (const anchor of anchors) {
                    const h3 = anchor.querySelector('h3');
                    const title = (h3?.innerText || '').trim();
                    const url = (anchor.href || '').trim();
                    if (!title || !url || !url.startsWith('http')) continue;
                    const dedupeKey = `${title}|${url}`;
                    if (seen.has(dedupeKey)) continue;
                    seen.add(dedupeKey);
                    const container = anchor.closest('div');
                    let snippet = '';
                    if (container) {
                        const textNodes = Array.from(container.querySelectorAll('span, div'))
                          .map((node) => (node.innerText || '').trim())
                          .filter(Boolean)
                          .filter((text) => text !== title);
                        snippet = textNodes.find((text) => text.length > 20) || '';
                    }
                    items.push({ title, url, snippet });
                    if (items.length >= 10) break;
                }
                return items;
            }"""
        )

    def _classify_results(self, results):
        product_pages = 0
        forum_pages = 0
        pricing_pages = 0
        for item in results:
            domain = self._domain(item["url"])
            merged = " ".join([item["title"], item["snippet"], item["url"]]).lower()
            if any(token in merged for token in {"pricing", "plans", "subscription", "buy", "purchase"}):
                pricing_pages += 1
            if any(token in merged for token in {"download", "app store", "google play", "signup", "sign up"}):
                product_pages += 1
            if any(domain.endswith(forum) for forum in FORUM_DOMAINS) or any(
                token in merged for token in {"forum", "reddit", "discord", "community", "thread"}
            ):
                forum_pages += 1
        return product_pages, forum_pages, pricing_pages

    @staticmethod
    def _summarize_results(results):
        if not results:
            return "No organic results parsed from Google search page."
        fragments = []
        for item in results[:5]:
            domain = urlparse(item["url"]).netloc or item["url"]
            fragments.append(f"{item['title']} ({domain})")
        return "; ".join(fragments)[:500]

    def search(self, keyword):
        try:
            page_text = self._fetch_page(keyword)
            if self._looks_blocked(page_text):
                return SerpSummary(
                    status="blocked",
                    result_count=0,
                    ads_present=False,
                    product_pages=0,
                    forum_pages=0,
                    pricing_pages=0,
                    summary="Google returned an anti-bot or captcha page in the visible browser.",
                    results=[],
                    error_message="google_browser_block",
                )

            results = self._extract_results()
            product_pages, forum_pages, pricing_pages = self._classify_results(results)
            summary = self._summarize_results(results)
            return SerpSummary(
                status="ok" if results else "empty",
                result_count=len(results),
                ads_present="Sponsored" in page_text or "广告" in page_text,
                product_pages=product_pages,
                forum_pages=forum_pages,
                pricing_pages=pricing_pages,
                summary=summary,
                results=results,
            )
        except Exception as exc:
            logger.warning("SERP collection failed for %s: %s", keyword, exc)
            return SerpSummary(
                status="error",
                result_count=0,
                ads_present=False,
                product_pages=0,
                forum_pages=0,
                pricing_pages=0,
                summary=f"SERP collection failed: {exc}",
                results=[],
                error_message=str(exc),
            )
