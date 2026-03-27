# Google Trends 飞书监控工具

这是一个用于监控 Google Trends 数据的自动化工具。它会定期查询指定关键词的趋势数据，生成本地 JSON/CSV 报告，并同步到飞书通知和飞书表格。

项目现在支持两层关键词体系：

- `PRIMARY_KEYWORDS` 风格的精选主清单
- 来自飞书词表的候选词库与轮询分组
- 新增“机会评估”流水线：`7天趋势 -> 30天复核 -> Google 搜索页 -> AI评分`
- 支持用 `KEYWORD_LIBRARY_PRIMARY_FORCE_INCLUDE` 强制保留指定主清单词

## 功能特点

- 每日自动查询多个关键词的趋势数据
- 生成本地 CSV 日报和 JSON 原始数据
- 通过飞书群机器人发送日报、告警和异常通知
- 自动创建并维护飞书工作簿
- 将每次执行结果追加到 `每日汇总`、`趋势明细` 和 `机会评估` 三张飞书表
- 从飞书词表同步候选词库，自动生成主清单和轮询组
- 智能请求频率控制，降低触发限制的风险
- 跨进程共享请求限流，减少“刚手动探测完又立刻正式跑”导致的短时 quota
- `related queries` 默认改走浏览器版 Google Trends 采集，保留 `interest_over_time` 现有接口
- 浏览器版 Trends 采集优先读页面 DOM，必要时回退到卡片 CSV 下载
- 对高增长词做 30 天走势复核，识别近几天才冒头的新词
- 抓取 Google 搜索页摘要，并用 AI 结构化评分需求与付费意愿
- SERP 抓取走可见浏览器，不使用无头模式

## 安装说明

```bash
git clone [repository-url]
cd [repository-name]
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

## 飞书配置

编辑 `.env` 并填写以下变量：

```bash
FEISHU_WEBHOOK_URL=https://open.feishu.cn/open-apis/bot/v2/hook/your-webhook-token
FEISHU_ENABLED=true
FEISHU_APP_ID=cli_xxx
FEISHU_APP_SECRET=xxx
FEISHU_REDIRECT_URI=http://127.0.0.1:8787/callback
FEISHU_USER_TOKEN_FILE=.feishu_user_token.json
```

可选工作簿与关键词词库配置：

```bash
FEISHU_WORKBOOK_TITLE=Google Trends 监控台账
FEISHU_STATE_FILE=artifacts/feishu_workbook/state.json
FEISHU_DAILY_SUMMARY_SHEET=每日汇总
FEISHU_TREND_DETAILS_SHEET=趋势明细
FEISHU_OPPORTUNITY_SHEET=机会评估

KEYWORD_LIBRARY_ENABLED=true
KEYWORD_LIBRARY_WIKI_URL=https://gcnbv8csilt1.feishu.cn/wiki/XSo2wePs0iq1u4kaMpic72gAnEc?sheet=lzAJZS
KEYWORD_LIBRARY_SPREADSHEET_TOKEN=Va2hsQStjhnU9CtQzqPc8Fgdnke
KEYWORD_LIBRARY_SHEET_ID=lzAJZS
KEYWORD_LIBRARY_ARTIFACT_FILE=artifacts/keyword_library/library.json
KEYWORD_LIBRARY_DEFAULT_RUN_SOURCE=primary
KEYWORD_LIBRARY_PRIMARY_LIMIT=40
KEYWORD_LIBRARY_PRIMARY_AI_LIMIT=24
KEYWORD_LIBRARY_PRIMARY_GAME_LIMIT=16
KEYWORD_LIBRARY_PRIMARY_PER_CATEGORY_LIMIT=3
KEYWORD_LIBRARY_ROTATION_GROUP_SIZE=25
KEYWORD_LIBRARY_PRIMARY_FORCE_INCLUDE=clicker games
TRENDS_NOTIFICATION_METHOD=feishu

TRENDS_TIMEFRAME=today 7-d
TRENDS_RELATED_QUERIES_SOURCE=browser
TRENDS_BROWSER_HL=en-US
TRENDS_BROWSER_CHANNEL=chrome
TRENDS_BROWSER_PROFILE_DIR=artifacts/querytrends/browser_profile
TRENDS_BROWSER_REMOTE_DEBUGGING_URL=http://127.0.0.1:9444
TRENDS_BROWSER_PAGE_TIMEOUT_MS=30000
TRENDS_BROWSER_DOWNLOAD_DIR=artifacts/querytrends/downloads
TRENDS_MAX_REQUESTS_PER_MINUTE=12
TRENDS_MAX_REQUESTS_PER_HOUR=120
TRENDS_RATE_LIMIT_STATE_FILE=artifacts/querytrends/request_limiter_state.json
TRENDS_QUOTA_BEHAVIOR=fail_fast
OPPORTUNITY_PIPELINE_ENABLED=true
OPPORTUNITY_SEVEN_DAY_TIMEFRAME=today 7-d
OPPORTUNITY_THIRTY_DAY_TIMEFRAME=today 1-m
OPPORTUNITY_MAX_CANDIDATES_PER_RUN=8
GOOGLE_SEARCH_DELAY_SECONDS=8
GOOGLE_SEARCH_HL=en
GOOGLE_SEARCH_GL=us
GOOGLE_SEARCH_BROWSER_CHANNEL=chrome
GOOGLE_SEARCH_REMOTE_DEBUGGING_URL=http://127.0.0.1:9444
OPPORTUNITY_AI_ENABLED=true
GEMINI_API_KEY=your_gemini_api_key
GEMINI_MODEL=gemini-2.5-flash
```

然后按需要修改 [`config.py`](/Users/dahuang/CascadeProjects/new-words-to-do/trendspy-related-keywords/config.py) 中的：

- `TRENDS_CONFIG`
- `RATE_LIMIT_CONFIG`
- `SCHEDULE_CONFIG`
- `MONITOR_CONFIG`
- `KEYWORD_LIBRARY_CONFIG`
- `OPPORTUNITY_PIPELINE_CONFIG`

关于 Google Trends 配额：

- 默认会把请求记录写到 [`artifacts/querytrends/request_limiter_state.json`](/Users/dahuang/CascadeProjects/new-words-to-do/trendspy-related-keywords/artifacts/querytrends/request_limiter_state.json)
- 不同 Python 进程会共享这份限流状态，避免手动探测和正式任务互相“看不见”
- `TRENDS_QUOTA_BEHAVIOR=fail_fast` 时，一旦 Google 返回 quota，当前关键词会快速失败，不再原地等待 5-6 分钟
- 如果你想恢复旧行为，把 `TRENDS_QUOTA_BEHAVIOR` 改成 `wait`

关于 Trends 浏览器采集：

- `TRENDS_RELATED_QUERIES_SOURCE=browser` 时，项目会直接打开 Google Trends 页面抓 `Related queries`
- 这条链路默认优先复用 [`http://127.0.0.1:9444`](http://127.0.0.1:9444) 的调试 Chrome
- `interest_over_time` 仍然使用现有 `trendspy` 接口，不受这次改动影响

## 首次授权

飞书表格写入使用 `user` 身份模式，首次启动前需要先完成一次授权：

```bash
python setup_feishu_user_auth.py
```

执行后会：

1. 打开浏览器进入飞书授权页
2. 回调到 `FEISHU_REDIRECT_URI`
3. 在本地生成 `FEISHU_USER_TOKEN_FILE`

如果你新增了 wiki 只读权限，也建议重新执行一次授权，确保最新 scopes 生效。

## 同步关键词词库

项目可以从飞书词表同步候选词库，并生成：

- `primary_keywords`
- `keyword_pool`
- `rotation_group_n`

同步命令：

```bash
python sync_keyword_library.py
```

或：

```bash
python trends_monitor.py --sync-keyword-library
```

同步产物会写入：

[`artifacts/keyword_library/library.json`](/Users/dahuang/CascadeProjects/new-words-to-do/trendspy-related-keywords/artifacts/keyword_library/library.json)

## 使用说明

测试模式：

```bash
python trends_monitor.py --test
```

临时指定关键词测试：

```bash
python trends_monitor.py --test --keywords "Python" "AI"
```

用主清单测试：

```bash
python trends_monitor.py --test --keyword-source primary
```

开启完整机会流水线测试：

```bash
python trends_monitor.py --test --keyword-source primary --enable-opportunity-analysis
```

仅验证 30 天复核，跳过搜索页和 AI：

```bash
python trends_monitor.py --test --keyword-source primary --skip-serp --skip-ai
```

用轮询组测试：

```bash
python trends_monitor.py --test --keyword-source rotation_group_1
```

先刷新词库再执行：

```bash
python trends_monitor.py --test --refresh-keyword-library
```

定时模式：

```bash
python trends_monitor.py
```

默认会使用 `KEYWORD_LIBRARY_DEFAULT_RUN_SOURCE`。

## 飞书数据输出

### 1. 每日汇总

每次运行追加一行，记录：

- 运行时间
- 关键词来源
- 时间范围
- 成功 / 失败关键词数
- 高增长告警数
- 本地 CSV 路径
- 数据目录
- 运行状态

### 2. 趋势明细

每个 `keyword x related_query x type` 追加一行，记录：

- 关键词来源
- 关键词分类
- 关键词
- 趋势类型（`top` / `rising`）
- 相关查询词
- 数值
- 是否超过高增长阈值
- 原始 JSON 文件路径

### 3. 机会评估

每个通过 30 天复核且像新词的候选词追加一行，记录：

- `seed_keyword` 和 `candidate_keyword`
- 7 天高增长值与 30 天复核值
- 是否像最近几天刚出现的新词
- Google 搜索页摘要
- AI 需求分、付费意愿分、机会判断

### 浏览器抓取说明

- `机会评估` 里的 Google 搜索页抓取会直接打开可见浏览器窗口
- 默认优先附着到 `GOOGLE_SEARCH_REMOTE_DEBUGGING_URL`
- 连不上远程调试实例时，才会退回本机 `chrome` 通道
- 浏览器会复用 `artifacts/opportunity_pipeline/browser_profile`
- 如果机器上没有可用浏览器，先执行：

```bash
python -m playwright install chromium
```

## 本地数据输出

- 每日数据保存在 `data_YYYYMMDD` 目录
- 原始趋势数据保存为 JSON
- 每日汇总报告保存为 CSV
- 关键词词库快照保存为 `artifacts/keyword_library/library.json`
- 机会流水线缓存保存为 `artifacts/opportunity_pipeline/cache.json`

## 最近进展

- 已完成飞书通知改造，日报、告警、异常都走飞书机器人
- 已完成飞书工作簿落库，当前会维护 `每日汇总`、`趋势明细`、`机会评估` 三张表
- 已接入飞书词表同步，支持 `primary` 主清单和 `rotation_group_n` 轮询组
- 已把机会评估链路接入主流程：
  - 先跑 `7天 Google Trends`
  - 对高增长词做 `30天 interest_over_time` 复核
  - 对通过复核且像新词的候选做 Google 搜索结果分析
  - 用 Gemini 输出结构化的需求分和付费意愿分
- 已确认 30 天复核不再靠人工看图，而是直接读取时间序列数据来判断"最近几天才冒头"的新词
- 已把 Google 搜索结果采集切成"可见浏览器模式"，不会使用无头浏览器
- 已把 AI 分析默认模型切到 `gemini-2.5-flash`
- 已把 `related queries` 从 `trendspy` 改成浏览器版 Google Trends 采集，`interest_over_time` 继续保留原实现
- 已新增浏览器版 Trends 采集器，优先附着 `http://127.0.0.1:9444` 的调试 Chrome
- 已实现 `related queries` 的 DOM 优先采集，并加入卡片 CSV 下载 fallback
- 已完成 `clicker games` 的浏览器版抓取验证：
  - `top` 结果能稳定拿到
  - `rising` 结果能稳定拿到
  - 不再混入 `related topics`
- 已完成 `clicker games` 的主流程联调：
  - 7 天 `related queries` 走浏览器版
  - 30 天复核继续走 `interest_over_time`
  - SERP 继续走 `9444` 调试 Chrome
- 已把 Google Trends quota 处理改成跨进程共享限流 + `fail_fast`，避免临时测试卡 5-6 分钟
- 已修复同一进程里 Trends 浏览器采集器和 SERP 采集器的 Playwright 冲突
- **[新增] 优化1 - 重试机制**：`_open_keyword_page` 失败时自动最多重试 3 次，每次间隔随机 3-7 秒
- **[新增] 优化2 - 降级备用通道**：浏览器采集全部重试失败后，自动切换到 trendspy API 再尝试一次
- **[新增] 优化3 - 日志准确性**：关键词采集失败时统一记录 `ERROR` 级别日志；全部失败时最终状态改为 `all_failed` 并触发错误通知，不再假装成功
- **[新增] 优化4 - 智能等待**：等待图表容器 CSS 选择器出现再抓取，替代原来的固定 2500ms 等待；超时后降级为 3s 固定等待
- **[修复] 支持时间范围参数**：新增 `--timeframe 7d/30d/90d` 命令行选项，用于临时覆盖默认查询时间
- **[修复] 时间参数格式错误**：修复了 Google Trends 默认的 7 天参数由于使用了错误的 `today 7-d` 导致网页回退为主页默认（12个月）的问题，统一更正为正确的 `now 7-d`
- **[修复] 机会评估基准词**：机会评估 30 天复核不再以自身作为对比，而是统一切换到配置好的 `comparison_baseline` (默认保留设为 `GPTs`)
- **[修复] 噪音词过滤策略**：将 `gpts` 从候选词过滤黑名单移除，确保基准词自身能够被正常验证

## 下一步待办

- 给机会链路增加“与 seed keyword 的语义相关性过滤”，先挡掉 `march madness` 这类热点噪音
- 评估是否要把 `related topics` 也切到浏览器版采集，和 `related queries` 保持一致
- 把浏览器版 Trends 采集结果做本地缓存，减少同词重复打开页面
- 视运行效果决定是否把 `related queries` 的 CSV fallback 单独做一轮强制验证
- 继续观察 `clicker games` 是否适合作为主监控词，还是更应该下钻到 `cookie clicker`、`auto clicker games` 这类子词
- 复核 `机会评估` sheet 的字段是否还需要补充，比如 SERP 状态、趋势截图链接、人工备注
- 视效果决定是否增加“二次深判”模型，只对 `watch/opportunity` 候选做更重的分析

## 常见问题

### 缺少 webhook 配置

检查 `.env` 中是否设置了 `FEISHU_WEBHOOK_URL`。

### 缺少飞书应用配置

检查 `.env` 中是否设置了 `FEISHU_APP_ID` 和 `FEISHU_APP_SECRET`。

### 缺少用户 token

重新执行：

```bash
python setup_feishu_user_auth.py
```

### 关键词词库同步失败

- 检查 `KEYWORD_LIBRARY_SPREADSHEET_TOKEN` 和 `KEYWORD_LIBRARY_SHEET_ID`
- 如果你想直接通过 wiki open API 解析链接，确认应用 scopes 已包含 `wiki:wiki:readonly` 或 `wiki:node:read`
- 如果 scopes 是刚补的，重新授权一次

### 飞书表格写入失败

- 检查应用权限是否包含 `offline_access` 和 `sheets:spreadsheet`
- 确认授权的飞书用户有创建和维护工作簿的权限
- 查看日志文件 `trends_monitor.log`

### Google Trends 数据采集失败

- 检查网络连接
- 降低关键词数量
- 延长请求间隔
- 查看日志文件获取详细错误信息

### Google 搜索页抓取失败

- Google 可能返回 `enablejs` 或反爬页面
- 这不会中断主趋势任务，只会让机会评估降级
- 如果长期被拦截，先降低运行频率和候选词数量

### AI 评分没有产出

- 检查 `GEMINI_API_KEY`
- 检查 `GEMINI_MODEL`
- 如果你只想先验证 30 天复核链路，可以临时加 `--skip-ai`
