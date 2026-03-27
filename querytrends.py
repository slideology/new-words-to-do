import atexit
import json
import os
import random
import time
from datetime import datetime
import fcntl

import pandas as pd
from trendspy import Trends

from config import RATE_LIMIT_CONFIG, TRENDS_CONFIG
from trends_browser_collector import GoogleTrendsBrowserCollector


USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

_browser_related_queries_collector = None


def build_request_headers():
    return {
        "referer": "https://www.google.com/",
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    }


def create_trends_client():
    return Trends(hl="zh-CN")


def _close_browser_related_queries_collector():
    global _browser_related_queries_collector
    if _browser_related_queries_collector is not None:
        _browser_related_queries_collector.close()
        _browser_related_queries_collector = None


def close_browser_related_queries_collector():
    _close_browser_related_queries_collector()


atexit.register(_close_browser_related_queries_collector)


def get_browser_related_queries_collector():
    global _browser_related_queries_collector
    if _browser_related_queries_collector is None:
        _browser_related_queries_collector = GoogleTrendsBrowserCollector()
    return _browser_related_queries_collector


class TrendsQuotaExceededFastFail(RuntimeError):
    """Raised when Google Trends quota is exceeded and retries are disabled."""


def _handle_retryable_error(error_msg, quota_context):
    if "API quota exceeded" in error_msg:
        if RATE_LIMIT_CONFIG["quota_behavior"] == "fail_fast":
            raise TrendsQuotaExceededFastFail(
                f"Google Trends quota exceeded during {quota_context}; "
                "fail-fast enabled, please retry later."
            )
        wait_time = random.uniform(
            RATE_LIMIT_CONFIG["quota_retry_wait_min_seconds"],
            RATE_LIMIT_CONFIG["quota_retry_wait_max_seconds"],
        )
        print(f"API配额超限，等待 {wait_time:.1f} 秒后重试...")
        time.sleep(wait_time)
        return True

    if "'NoneType' object has no attribute 'raise_for_status'" in error_msg:
        wait_time = random.uniform(
            RATE_LIMIT_CONFIG["empty_response_retry_wait_min_seconds"],
            RATE_LIMIT_CONFIG["empty_response_retry_wait_max_seconds"],
        )
        print(f"请求返回为空，等待 {wait_time:.1f} 秒后重试...")
        time.sleep(wait_time)
        return True

    return False


def get_related_queries(keyword, geo='', timeframe='today 12-m'):
    """
    获取关键词的相关查询数据，带请求限制。

    【优化2：降级备用通道】
    主通道：如果配置为 'browser'，就用真实浏览器打开 Google Trends 页面抓取。
    备用通道：一旦浏览器主通道全部重试失败，自动切换到 trendspy API 库再试一次。
    如果备用通道也失败，才把异常向上抛。
    """
    related_queries_source = TRENDS_CONFIG["related_queries_source"]

    while True:  # 限流重试循环
        try:
            # 检查请求限制
            request_limiter.wait_if_needed()

            # 添加随机延时
            delay = random.uniform(1, 3)
            time.sleep(delay)

            if related_queries_source == "browser":
                try:
                    # 【主通道】尝试使用浏览器抓取（内部已含重试机制）
                    related_data = get_browser_related_queries_collector().get_related_queries(
                        keyword,
                        geo=geo,
                        timeframe=timeframe,
                    )
                except Exception as browser_exc:
                    # 【优化2：降级备用通道】
                    # 浏览器主通道所有重试均失败，自动切换到 trendspy API
                    print(f"[降级] 浏览器抓取失败: {browser_exc}")
                    print(f"[降级] 正在切换备用通道 (trendspy API) 来尝试获取 '{keyword}' 的数据...")
                    import logging
                    logging.warning(
                        "Browser related queries failed for '%s': %s. Falling back to trendspy API.",
                        keyword, browser_exc
                    )
                    # 关闭并重置浏览器采集器，下次需要时重新建立
                    close_browser_related_queries_collector()
                    # 备用通道：直接调用 trendspy API
                    tr = create_trends_client()
                    headers = build_request_headers()
                    related_data = tr.related_queries(
                        keyword,
                        headers=headers,
                        geo=geo,
                        timeframe=timeframe
                    )
                    print(f"[B计划] trendspy API 成功获取 '{keyword}' 的数据！")
            else:
                # 直接使用 trendspy API
                tr = create_trends_client()
                headers = build_request_headers()
                related_data = tr.related_queries(
                    keyword,
                    headers=headers,
                    geo=geo,
                    timeframe=timeframe
                )
            print(f"成功获取数据！")
            return related_data

        except Exception as e:
            error_msg = str(e)
            print(f"尝试获取数据时出错: {error_msg}")

            if _handle_retryable_error(error_msg, f"related queries for '{keyword}'"):
                continue

            raise


def get_interest_over_time(keywords, geo="", timeframe="today 12-m"):
    """
    获取关键词时间序列趋势数据，带请求限制。
    """
    while True:
        tr = create_trends_client()
        headers = build_request_headers()

        try:
            request_limiter.wait_if_needed()
            delay = random.uniform(1, 3)
            time.sleep(delay)
            series = tr.interest_over_time(
                keywords,
                headers=headers,
                geo=geo,
                timeframe=timeframe,
            )
            print("成功获取时间序列数据！")
            return series
        except Exception as e:
            error_msg = str(e)
            print(f"尝试获取时间序列数据时出错: {error_msg}")

            if _handle_retryable_error(error_msg, f"interest over time for {keywords}"):
                continue

            raise

def batch_get_queries(keywords, geo='', timeframe='today 12-m', delay_between_queries=5):
    """
    批量获取多个关键词的数据，带间隔控制。

    【优化3：日志准确性】
    当某个关键词所有备用通道均失败时，会记录正式的 ERROR 级别日志，
    保证运行日志里不会出现“明明失败却显示成功”的假象。
    """
    results = {}
    import logging

    for keyword in keywords:
        try:
            print(f"\n正在查询关键词: {keyword}")
            results[keyword] = get_related_queries(keyword, geo, timeframe)

            # 在请求之间添加延时
            if keyword != keywords[-1]:  # 如果不是最后一个关键词
                delay = delay_between_queries + random.uniform(0, 2)
                print(f"等待 {delay:.1f} 秒后继续下个查询...")
                time.sleep(delay)

        except TrendsQuotaExceededFastFail as e:
            # 【优化3】配额超限错误：用 ERROR 级别记录，明确标注失败
            logging.error(
                "[DATA MISSING] 关键词 '%s' 采集失败 (配额超限): %s", keyword, e
            )
            print(f"[ERROR] 获取 {keyword} 的数据失败: {str(e)}")
            results[keyword] = None
        except Exception as e:
            # 【优化3】其它错误：同样用 ERROR 级别记录，不再静默失败
            logging.error(
                "[DATA MISSING] 关键词 '%s' 采集失败 (主/备用通道均失败): %s", keyword, e
            )
            print(f"[ERROR] 获取 {keyword} 的数据失败: {str(e)}")
            results[keyword] = None

            # 如果遇到错误，增加额外等待时间
            time.sleep(10)

    return results

def save_related_queries(keyword, related_data):
    """
    保存相关查询数据到JSON文件
    """
    if not related_data:
        return
    
    timestamp = time.strftime('%Y%m%d_%H%M%S')
    json_data = {
        'keyword': keyword,
        'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
        'related_queries': {
            'top': related_data['top'].to_dict(orient='records') if isinstance(related_data.get('top'), pd.DataFrame) else related_data.get('top'),
            'rising': related_data['rising'].to_dict(orient='records') if isinstance(related_data.get('rising'), pd.DataFrame) else related_data.get('rising')
        }
    }
    
    # 保存为JSON文件
    filename = f"related_queries_{keyword}_{timestamp}.json"
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(json_data, f, ensure_ascii=False, indent=2)
    
    return filename

def print_related_queries(related_data):
    """
    打印相关查询词数据
    """
    if not related_data:
        print("没有相关查询数据")
        return
    
    print("\n相关查询词统计:")
    print("=" * 50)
    
    # 打印热门查询
    if 'top' in related_data and related_data['top'] is not None:
        print("\n热门查询:")
        print("-" * 30)
        df = related_data['top']
        if isinstance(df, pd.DataFrame):
            for _, row in df.iterrows():
                print(f"- {row['query']:<30} (相关度: {row['value']})")
    
    # 打印上升趋势查询
    if 'rising' in related_data and related_data['rising'] is not None:
        print("\n上升趋势查询:")
        print("-" * 30)
        df = related_data['rising']
        if isinstance(df, pd.DataFrame):
            for _, row in df.iterrows():
                print(f"- {row['query']:<30} (增长: {row['value']})")


# 主函数
# timeframe可能的值：
# today 12-m：12个月
# now 1-d：1天
# now 7-d：7天
# now 30-d：30天
# now 90-d：90天
# 日期格式：2024-12-28 2024-12-30
def main():
    # 设置要查询的关键词列表
    keywords = ['game']  # 可以添加多个关键词
    geo = ''
    timeframe = 'now 1-d'
    
    print("开始批量查询...")
    print(f"地区: {geo if geo else '全球'}")
    print(f"时间范围: {timeframe}")
    
    try:
        # 批量获取数据
        results = batch_get_queries(
            keywords,
            geo=geo,
            timeframe=timeframe,
            delay_between_queries=100  # 设置请求间隔
        )

        # 处理和保存结果
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        for keyword, data in results.items():
            if data:
                print(f"\n处理 {keyword} 的数据:")
                print_related_queries(data)
                filename = save_related_queries(keyword, data)
                print(f"数据已保存到文件: {filename}")
            else:
                print(f"\n未能获取 {keyword} 的数据")
                
    except Exception as e:
        print(f"批量查询过程中出错: {str(e)}")

class RequestLimiter:
    def __init__(self):
        self.max_requests_per_min = RATE_LIMIT_CONFIG["max_requests_per_minute"]
        self.max_requests_per_hour = RATE_LIMIT_CONFIG["max_requests_per_hour"]
        self.state_file = RATE_LIMIT_CONFIG["shared_state_file"]

    def _ensure_state_file(self):
        directory = os.path.dirname(self.state_file)
        if directory:
            os.makedirs(directory, exist_ok=True)
        if not os.path.exists(self.state_file):
            with open(self.state_file, "w", encoding="utf-8") as handle:
                json.dump({"requests": []}, handle)

    def _reserve_request_slot(self):
        self._ensure_state_file()
        current_time = time.time()

        with open(self.state_file, "r+", encoding="utf-8") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                payload = json.load(handle)
            except json.JSONDecodeError:
                payload = {"requests": []}

            requests = [
                float(timestamp)
                for timestamp in payload.get("requests", [])
                if current_time - float(timestamp) < 3600
            ]
            recent_minute = [timestamp for timestamp in requests if current_time - timestamp < 60]

            if (
                len(recent_minute) < self.max_requests_per_min
                and len(requests) < self.max_requests_per_hour
            ):
                requests.append(current_time)
                handle.seek(0)
                json.dump({"requests": requests}, handle)
                handle.truncate()
                return 0.0, len(recent_minute) + 1, len(requests)

            minute_wait = 0.0
            hour_wait = 0.0
            if len(recent_minute) >= self.max_requests_per_min:
                oldest_minute = min(recent_minute)
                minute_wait = max(60 - (current_time - oldest_minute), 0.0)
            if len(requests) >= self.max_requests_per_hour:
                oldest_hour = min(requests)
                hour_wait = max(3600 - (current_time - oldest_hour), 0.0)
            return max(minute_wait, hour_wait) + random.uniform(0.5, 1.5), len(
                recent_minute
            ), len(requests)

    def wait_if_needed(self):
        """如果需要，等待直到可以发送请求。该状态在不同进程间共享。"""
        while True:
            wait_time, minute_count, hour_count = self._reserve_request_slot()
            if wait_time <= 0:
                return
            print(
                "达到共享请求限制，最近1分钟/1小时请求数为 "
                f"{minute_count}/{hour_count}，等待 {wait_time:.1f} 秒..."
            )
            time.sleep(wait_time)

# 创建全局请求限制器
request_limiter = RequestLimiter()

if __name__ == "__main__":
    main()
