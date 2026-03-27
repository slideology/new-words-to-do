import logging
import random
import re
from pathlib import Path
from urllib.parse import quote_plus

import pandas as pd

from config import TRENDS_CONFIG


logger = logging.getLogger(__name__)

RISING_LABELS = ["Rising", "搜索量上升"]
TOP_LABELS = ["Top", "热门"]
VALUE_PATTERN = re.compile(r"^(\+?[\d,]+%?|breakout|飙升)$", re.IGNORECASE)
RANK_PATTERN = re.compile(r"^\d+$")
DOWNLOAD_TIMEOUT_MS = 15000

# 打开页面最多重试次数
PAGE_OPEN_MAX_RETRIES = 3
# 每次重试之间的随机等待秒数范围（下限、上限）
PAGE_RETRY_WAIT_RANGE = (3, 7)
# 等待图表容器渲染的超时时间（毫秒），超时后降级为固定等待
CHART_RENDER_WAIT_MS = 15000
# 图表容器的 CSS 选择器候选列表，任意一个出现即认为图表已渲染
CHART_CONTAINER_SELECTORS = [
    "widget[type='RELATED_QUERIES']",  # Google Trends 相关查询组件
    "div.trends-bar-chart-table",       # 备用：趋势图表表格区域
]
QUERY_PAGINATION_PATTERNS = [
    re.compile(r"Showing\s+(\d+)-(\d+)\s+of\s+(\d+)\s+queries", re.IGNORECASE),
    re.compile(r"当前显示的是第\s*(\d+)-(\d+)\s*个查询"),
]


class GoogleTrendsBrowserCollector:
    def __init__(self):
        self.page_timeout_ms = TRENDS_CONFIG["browser_page_timeout_ms"]
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None
        self.connection_mode = ""
        self.download_dir = Path(TRENDS_CONFIG["browser_download_dir"])
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self._start_browser()

    def _start_browser(self):
        try:
            from playwright.sync_api import sync_playwright
        except Exception as exc:
            raise RuntimeError(
                "Playwright is not installed. Run `pip install -r requirements.txt` first."
            ) from exc

        profile_dir = Path(TRENDS_CONFIG["browser_profile_dir"])
        profile_dir.mkdir(parents=True, exist_ok=True)
        self.playwright = sync_playwright().start()

        remote_debugging_url = TRENDS_CONFIG["browser_remote_debugging_url"].strip()
        if remote_debugging_url:
            try:
                self.browser = self.playwright.chromium.connect_over_cdp(remote_debugging_url)
                self.context = self.browser.contexts[0] if self.browser.contexts else self.browser.new_context()
                self.page = self.context.new_page()
                self.page.set_default_timeout(self.page_timeout_ms)
                self.connection_mode = f"cdp:{remote_debugging_url}"
                logger.info(
                    "Attached Google Trends browser collector to remote Chrome via %s",
                    remote_debugging_url,
                )
                return
            except Exception as exc:
                logger.warning(
                    "Failed to attach Trends collector to remote Chrome %s, fallback to local launch: %s",
                    remote_debugging_url,
                    exc,
                )

        launch_kwargs = {
            "user_data_dir": str(profile_dir),
            "headless": False,
            "accept_downloads": True,
            "ignore_default_args": ["--enable-automation"],
            "args": [
                "--disable-blink-features=AutomationControlled",
                "--start-maximized",
            ],
            "viewport": {"width": 1440, "height": 960},
        }
        channel = TRENDS_CONFIG["browser_channel"]
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

    def _build_explore_url(self, keyword, geo, timeframe):
        parts = [
            f"hl={quote_plus(TRENDS_CONFIG['browser_hl'])}",
            f"date={quote_plus(timeframe)}",
            f"q={quote_plus(keyword)}",
        ]
        if geo:
            parts.append(f"geo={quote_plus(geo)}")
        return f"https://trends.google.com/trends/explore?{'&'.join(parts)}"

    def _dismiss_cookie_banner(self):
        for label in ("OK, got it", "I agree", "接受", "知道了"):
            try:
                button = self.page.get_by_role("button", name=label)
                if button.count() and button.first.is_visible():
                    button.first.click()
                    self.page.wait_for_timeout(500)
                    return
            except Exception:
                continue

    def _wait_for_chart_render(self):
        """【优化4：智能等待】
        等待图表区域真正渲染完成，而不是固定等待固定毫秒数。
        逐一尝试候选选择器，任意一个出现即返回。
        全部超时时降级为短暂固定等待，保持向后兼容。
        """
        for selector in CHART_CONTAINER_SELECTORS:
            try:
                self.page.wait_for_selector(
                    selector,
                    state="visible",
                    timeout=CHART_RENDER_WAIT_MS,
                )
                logger.debug("Chart container '%s' is visible, proceeding.", selector)
                return
            except Exception:
                # 当前选择器等待超时，尝试下一个
                continue
        # 所有选择器都超时，降级为短暂固定等待
        logger.warning(
            "All chart container selectors timed out; falling back to fixed 3s wait."
        )
        self.page.wait_for_timeout(3000)

    def _open_keyword_page(self, keyword, geo, timeframe):
        """【优化1：重试机制 + 优化4：智能等待】
        打开 Google Trends 关键词页面，并验证'相关查询'模块已渲染。
        如果渲染失败，会自动最多重试 PAGE_OPEN_MAX_RETRIES 次，
        每次重试前随机等待几秒，降低被谷歌拦截的概率。
        """
        url = self._build_explore_url(keyword, geo, timeframe)
        last_error = None

        for attempt in range(1, PAGE_OPEN_MAX_RETRIES + 1):
            try:
                # 导航到页面，等待 DOM 加载完成
                self.page.goto(url, wait_until="domcontentloaded")

                # 【优化4】智能等待图表容器渲染，替代原来固定的 2500ms
                self._wait_for_chart_render()

                self._dismiss_cookie_banner()
                # Cookie 弹窗关闭后给页面少量缓冲时间
                self.page.wait_for_timeout(800)

                # 校验「相关查询」部分是否真的出现在页面文本中
                body_text = self.page.locator("body").inner_text()
                if "Related queries" not in body_text and "相关查询" not in body_text:
                    raise RuntimeError(
                        "Google Trends page loaded, but Related queries module did not render."
                    )
                # 成功，直接返回
                return

            except Exception as exc:
                last_error = exc
                if attempt < PAGE_OPEN_MAX_RETRIES:
                    # 随机等待后重试，避免短时间内连续请求被谷歌限速
                    wait_sec = random.uniform(*PAGE_RETRY_WAIT_RANGE)
                    logger.warning(
                        "[Retry %d/%d] Failed to open page for '%s': %s. "
                        "Waiting %.1fs before retry...",
                        attempt,
                        PAGE_OPEN_MAX_RETRIES,
                        keyword,
                        exc,
                        wait_sec,
                    )
                    self.page.wait_for_timeout(int(wait_sec * 1000))
                else:
                    # 达到最大重试次数，向上抛出原始异常
                    logger.error(
                        "All %d retries exhausted for keyword '%s'. Last error: %s",
                        PAGE_OPEN_MAX_RETRIES,
                        keyword,
                        last_error,
                    )
                    raise last_error

    def _view_listbox(self):
        listboxes = self.page.get_by_role("listbox")
        count = listboxes.count()
        if count <= 0:
            raise RuntimeError("Could not find Related queries view selector.")
        return listboxes.nth(count - 1)

    def _switch_view(self, mode):
        target_labels = RISING_LABELS if mode == "rising" else TOP_LABELS
        listbox = self._view_listbox()
        current_text = listbox.inner_text().strip().lower()
        if any(label.lower() in current_text for label in target_labels):
            return

        listbox.click()
        self.page.wait_for_timeout(300)
        for label in target_labels:
            try:
                option = self.page.get_by_text(label, exact=True)
                if option.count():
                    option.last.click()
                    self.page.wait_for_timeout(1200)
                    return
            except Exception:
                continue
        raise RuntimeError(f"Failed to switch Related queries view to {mode}.")

    def _extract_visible_rows(self):
        rows = self.page.evaluate(
            """() => {
                const valuePattern = /^(\\+?[\\d,]+%?|Breakout|飙升)$/i;
                const rankPattern = /^\\d+$/;
                const anchors = Array.from(document.querySelectorAll('a[href*="/trends/explore?q="]'));
                const items = [];
                for (const anchor of anchors) {
                    const lines = (anchor.innerText || '')
                      .split('\\n')
                      .map((line) => line.trim())
                      .filter(Boolean);
                    if (!lines.length) continue;
                    const value = [...lines].reverse().find((line) => valuePattern.test(line)) || '';
                    const query = lines.find((line) => !rankPattern.test(line) && !valuePattern.test(line)) || '';
                    if (!query || !value) continue;
                    items.push({ query, value, href: anchor.href || '' });
                }
                return items;
            }"""
        )
        visible_count = self._visible_query_count()
        if visible_count > 0 and len(rows) >= visible_count:
            rows = rows[-visible_count:]
        deduped = []
        seen = set()
        for row in rows:
            key = row["query"].strip().lower()
            if key in seen:
                continue
            seen.add(key)
            deduped.append(row)
        return deduped

    def _visible_query_count(self):
        body_text = self.page.locator("body").inner_text()
        for pattern in QUERY_PAGINATION_PATTERNS:
            matches = pattern.findall(body_text)
            if not matches:
                continue
            start, end, *_ = matches[-1]
            try:
                return max(int(end) - int(start) + 1, 0)
            except ValueError:
                return 0
        return 0

    def _related_queries_next_button(self):
        buttons = self.page.get_by_role("button", name="Next")
        count = buttons.count()
        if count <= 0:
            return None
        return buttons.nth(count - 1)

    def _collect_rows_from_dom(self, mode):
        self._switch_view(mode)
        rows = []
        seen = set()
        while True:
            current_rows = self._extract_visible_rows()
            for row in current_rows:
                key = row["query"].strip().lower()
                if key in seen:
                    continue
                seen.add(key)
                rows.append(row)

            next_button = self._related_queries_next_button()
            if next_button is None or next_button.is_disabled():
                break

            next_button.scroll_into_view_if_needed()
            next_button.click()
            self.page.wait_for_timeout(900)

        if not rows:
            raise RuntimeError(f"DOM extraction returned no Related queries rows for {mode}.")
        return rows

    def _csv_download_button(self):
        buttons = self.page.get_by_role("button", name="file_download")
        count = buttons.count()
        if count <= 0:
            raise RuntimeError("Could not find Related queries CSV download button.")
        return buttons.nth(count - 1)

    def _collect_rows_from_csv(self, mode):
        self._switch_view(mode)
        try:
            with self.page.expect_download(timeout=DOWNLOAD_TIMEOUT_MS) as download_info:
                button = self._csv_download_button()
                button.scroll_into_view_if_needed()
                button.click()
            download = download_info.value
        except Exception as exc:
            raise RuntimeError(f"CSV fallback download failed for {mode}: {exc}") from exc

        target_path = self.download_dir / f"related_queries_{mode}.csv"
        download.save_as(str(target_path))
        dataframe = pd.read_csv(target_path)
        columns = {column.lower(): column for column in dataframe.columns}

        query_column = None
        value_column = None
        for key, original in columns.items():
            if query_column is None and "query" in key:
                query_column = original
            if value_column is None and (
                "value" in key or "top" in key or "rising" in key or "search" in key
            ):
                value_column = original
        if query_column is None and len(dataframe.columns) >= 1:
            query_column = dataframe.columns[0]
        if value_column is None and len(dataframe.columns) >= 2:
            value_column = dataframe.columns[1]
        if query_column is None or value_column is None:
            raise RuntimeError(f"CSV fallback could not infer columns for {mode}: {list(dataframe.columns)}")

        return [
            {
                "query": str(row[query_column]).strip(),
                "value": str(row[value_column]).strip(),
            }
            for _, row in dataframe.iterrows()
            if str(row[query_column]).strip()
        ]

    def _normalize_value(self, value):
        if isinstance(value, str):
            cleaned = value.strip()
            if cleaned.lower() in {"breakout", "飙升"}:
                return "Breakout"
            cleaned = cleaned.replace(",", "")
            if cleaned.startswith("+") and cleaned.endswith("%"):
                cleaned = cleaned[1:-1]
            elif cleaned.endswith("%"):
                cleaned = cleaned[:-1]
            if cleaned.isdigit():
                return int(cleaned)
            return value.strip()
        return value

    def _to_dataframe(self, rows):
        normalized_rows = [
            {
                "query": row["query"].strip(),
                "value": self._normalize_value(row["value"]),
            }
            for row in rows
            if row.get("query")
        ]
        return pd.DataFrame(normalized_rows) if normalized_rows else None

    def get_related_queries(self, keyword, geo="", timeframe="today 12-m"):
        self._open_keyword_page(keyword, geo, timeframe)
        results = {}
        for mode in ("rising", "top"):
            try:
                rows = self._collect_rows_from_dom(mode)
            except Exception as dom_exc:
                logger.warning("DOM extraction failed for %s/%s: %s", keyword, mode, dom_exc)
                rows = self._collect_rows_from_csv(mode)
            results[mode] = self._to_dataframe(rows)
        return results
