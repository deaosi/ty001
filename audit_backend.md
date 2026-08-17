# 探域数据看板后端审查报告（main.py）

- 审查对象: `D:\test\test01\main.py`（6762 行, FastAPI + SQLite 数据看板后端, 窗口服务, 单进程 uvicorn）
- 交叉核对: `trace_store.py`（SQLite 消息轨迹存储层）、`auth.py`（账号鉴权）、`log_store.py`（任务/系统日志库）
- 审查日期: 见文件修改时间
- 分级: A = BUG/隐患（最高优先）, B = 性能, C = 健壮性, D = 小优化

---

## A. BUG / 隐患（最高优先）

### A1. `/api/cookies` 无管理员校验（main.py:5886-5891）
- **问题**: `update_cookies` 没有任何 `Depends`，全局中间件只要求"已登录"。任何普通用户都能覆写全局 tanyu cookie —— 可注入自己的 tanyu 账号把数据拉进看板（信息泄露），或清空 cookie 让全站抓取瘫痪。与同样写配置的 `trace_config_set`（要求 `require_admin`）权限不一致。
- **修复**: 加 `_: dict = Depends(auth.require_admin)`。

### A2. refresh 双启动竞态（main.py:3971-3994 + 1376-1401）
- **问题**: `/api/refresh` 端点不置 `_refresh_state["running"]`，`refresh_all_groups_async.worker` 要等第一个集团的 `switch_group` + `sync_shops_from_tanyu`（网络耗时数秒, 含退避重试可达 35s）完成后才在 1400 行置 `running=True`。窗口内第二个 `/api/refresh` 通过 `_any_task_running()` 检查 → 起第二个 worker → 两个 worker 并发 `switch_group` 互切集团 cookie → 跨集团串数据。
  连带效应: 调度器 `_run_task` → `refresh(req)` 后 `_await_state`（309-335）3s 宽限期内看不到 `running=True`，把仍在跑的任务判为完成，提前启动队尾任务。
- **修复**: 端点在 `with _lock` 内同步置 `_refresh_state["running"] = True`（与 trace_overview:4823 / today:5224 / prefetch:4043 的做法一致）再起线程。

### A3. worker 先清 running 再恢复原集团（main.py:4986-4997 trace、5322-5331 today、1907-1916 staff、1439-1452 refresh）
- **问题**: 四个 worker 的 `finally` 都是先 `running=False` 再执行恢复集团的 `switch_group(orig_gid)`。恢复耗时数秒（含 sync_shops_from_tanyu），期间 `_any_task_running()` 已为 False → 新请求/调度器可启动新 worker → 新旧 worker 的 switch_group 并发出网互踩 cookie（正是设计想防的"单并发防串数据"裂缝）。
- **修复**: 先恢复集团（try/finally 兜底置 False）再清 `running`，或恢复动作完成前保持 `running=True`。

### A4. config.json 多写者 read-modify-write 丢更新（main.py:1586-1617 switch_group、1554-1583 _capture_cookie_expiry、6067-6084 _capture_wx_cookies、5932-6003 _login_worker、6170-6234 wx_qr_select、5886-5891 update_cookies）
- **问题**: 各写者独立 load→改→save，`_atomic_write_text` 只保证文件不撕裂、不防并发覆盖（丢更新）。典型场景: 登录 worker 保存新 cookie 与另一用户 switch_group 保存新 group-id cookie 互相覆盖 → "登录成功但 cookie 未生效/集团错乱"。现有注释只规避了夜间进程写者，主进程内部多个写者未串行化。
- **修复**: 加模块级 `_config_lock`，所有写路径在锁内 load→mutate→save（或保存前锁内重读再合并）。

### A5. `_fetch_trace_range` 触顶细分区间越界（main.py:2541-2552）
- **问题**: 区间 total ≥ TRACE_QUERY_CAP(10000) 时按 4 小时细分，`_time_slices(b, e, 4)` 最后一段 `start+4h-1s` 可能超出原始 end（如 00:00~10:00 → 第三段 08:00~11:59:59），把区间外最多 4 小时的消息计入核算 → 总数/采纳数虚高。
- **修复**: 子区间 end 取 `min(start + 4h, e)`。

### A6. 触顶截断被当完整数据缓存入库（main.py:2553 + 2677-2695）
- **问题**: 区间 ≤4h 仍触顶时返回 `first_batch`（≤2000 条，代码注释"接受截断"），`stat_trace_daily` 把截断结果当整天数据写入 trace_days 缓存（2681）并 `upsert_shop_day` 进 SQLite（2688），之后 6h/7 天一直展示错误数字——比抓取失败更危险（错误被持久化）。
- **修复**: 截断时抛异常或返回 truncated 标记，调用方不落缓存/库，并提示"该天数据超上限需细分补抓"。

### A7. days/区间跨度无上限 → 资源耗尽（main.py:4304 trace_shop、4695 trace_overview、5415 trace_messages、5041 trace_staff）
- **问题**: `days: int = 7` 未 clamp，start/end 跨度未校验。`days=100000` → 每店 10 万天循环抓取（登录用户即可触发，等同 DoS）。
- **修复**: 统一 `days = max(1, min(days, 60))` 且区间跨度 ≤60 天（参照 `_trend_days_range` 的 1..60 与 prefetch 的 35 天上限）。

### A8. 今日抓取时刻参数无范围校验（main.py:5111-5142 _parse_hhmm / _full_today_ts）
- **问题**: `_parse_hhmm("25:99")` 返回 "25:99"（只做 int 转换不校验范围），`_full_today_ts` 拼出 "2026-08-14 25:99:00" 发上游 → 接口报错/空数据。
- **修复**: 校验 h∈0-23、m/s∈0-59，非法返回 400。

### A9. `_running_task_id` 无条件清空（main.py:247-250）
- **问题**: 调度器 `finally` 直接 `_running_task_id = None`，可能把交互请求刚经 `_register_running_task`（369-387）登记的 direct 任务 id 清掉 → 任务列表 running 显示错、调度器提前判断"无任务"提前弹队。
- **修复**: `if _running_task_id == tid: _running_task_id = None`（参照 `_sync_task_progress` 423-424 的等值判断）。

### A10. `_rate_limit` 无锁竞态（main.py:2172-2181）
- **问题**: `_request_times` 全局列表读写无锁，两个线程可同时通过 `len < RISK_MAX_RPM` 检查后各自 append → 突发超限，正是风控机制要防的场景。
- **修复**: 用 `with _lock:` 包裹过滤+判断+append，或改用带锁的 deque + 时间戳。

### A11. 并发 cache-miss 重复抓取（main.py:4367-4373 trace_shop / 5467-5475 trace_messages 与 overview worker 4850-4999 可同时跑）
- **问题**: 交互端点与 overview worker 可对同一店同一天同时进入 `stat_trace_daily`，双双 miss 后各自向上游抓取 → 双倍请求 + 双倍限速压力。
- **修复**: 每店一把锁（`dict` 存 `threading.Lock`）串行化抓取段。

### A12. 翻页失败静默截断（main.py:2565-2567）
- **问题**: `_fetch_trace_range` 中间页失败只 `print` 后 break，返回部分数据，调用方当完整结果聚合 → 核算数字偏低且无任何标记。
- **修复**: 返回 truncated 标志给调用方、记日志；失败页改用 `_with_network_retry` 退避重试。

### A13. `fromisoformat` 空格分隔依赖 Python 版本（main.py:2541，5133-5142）
- **问题**: `"2026-08-10 00:00:00"`（空格分隔）在 Python <3.11 的 `datetime.fromisoformat` 抛 ValueError → b=e=None → 触顶细分被静默跳过 → 截断（数据不完整）。
- **修复**: 统一用 `"T"` 分隔，或 `strptime` 显式格式解析。

---

## B. 性能

### B1. `trace_store._ensure()` 每次调用跑全量 DDL（trace_store.py:175-182 + 53-172）
- **问题**: 每个读函数（get_shops / get_user_by_id / query_operation_log 等）都在 `_write_lock` 下新建写连接 + 执行 ~15 条 `CREATE ... IF NOT EXISTS` + PRAGMA —— 读路径与写路径全串行、每次调用都有 DDL 开销。get_shops 被 main.py 高频调用（load_all_shops / staff_shops / _staff_list_per_shop / _today_target_shops）。
- **修复**: 初始化一次后仅做轻量 `SELECT 1` 探活；读路径不拿写锁。

### B2. `rebuild_daily` 每次 `upsert_shop_day` 全店重建（trace_store.py:540-570、573-631）
- **问题**: 每晚每店每天 upsert 都触发全店消息扫描 + 全窗口 trace_daily 重写 → 单店 O(天数²) 写放大（35 天窗口 = 35 次全店扫描）。
- **修复**: 只重建当天行，或整店抓完攒批重建一次。

### B3. trace_days 累积文件整文件重写（main.py:2681-2685）
- **问题**: 每店每天 `save_cache("trace_days", ...)` 重写全部窗口数据：繁忙店 35 天 × 万条 = 单文件几十 MB，一晚重写 35 次；`_load_trace_days_cache` 每次还要整读 + json 解析。
- **修复**: 改按天分文件（`trace_days/{shop_id}/{day}.json`），或直接以 SQLite 为准（JSON 已冗余）。

### B4. 核算 worker 内存峰值（main.py:2738-2749、4570-4581）
- **问题**: `stat_trace_daily` / `_aggregate_stat_rows` 为每店构建全量 `raw_messages`（35 天 × 万条 × 2 份列表），而 overview worker（4906-4933）只用 total/adopted/byStaff。
- **修复**: 加 `include_messages=False` 参数，worker/DB 快路径不构造 messages。

### B5. `query_shop_aggregate` 全列加载（trace_store.py:1020-1044）
- **问题**: 聚合路径也 SELECT 全列并 `json.loads(content_json)` 每行。
- **修复**: 按调用方需要只取列（聚合不需要 content）。

### B6. N+1 式重复查询
- **问题**:
  - `_staff_list_per_shop` 每次调用重新 `get_shops()` + 读 staff_names.json（main.py:2367、2374-2375），被 overview 每请求、trace_staff、worker 收尾各调一次；
  - `_today_target_shops` 每集团循环内 `get_shops()`（main.py:5164-5168）；
  - `stat_trace_daily` → `_staff_list_from_agg` → `_load_staff_names()` 每店读一次文件（main.py:2334-2335）。
- **修复**: 调用方传入 shop_map/name_map，或进程内 TTL 缓存。

### B7. config.json 每请求重读（main.py:593-613 共 32 处调用 + get_headers 819 + _use_sqlite_trace 6284-6290）
- **问题**: 每次出网请求、每次 trace 请求都做文件 IO + json 解析 + 平台推断。
- **修复**: 带 mtime 失效的进程内缓存（夜间进程写者靠 mtime 变化自动失效，不破坏跨进程一致性）。

### B8. 任务/操作者身份掩码 N+1（main.py:5579-5585 + 771-814）
- **问题**: `tasks_list` 对每条任务调 `_mask_admin_label` → `_label_is_admin` → 按 uid/用户名查 DB，每次轮询 10~100 条 = 2×N 查询。
- **修复**: 批量取用户映射或进程内用户缓存。

### B9. 磁盘缓存目录无清理（main.py:1132-1134、1118-1129）
- **问题**: `data/{summary,table,trend,trace_days}/` 按 key 累积永不过期文件（days 参数 1..60 可产生大量 trend 键）。
- **修复**: 启动/每日按 mtime 清理 >2×TTL 的缓存文件。

### B10. `_aggregate_shop_gen_rate` 逐店读 summary 缓存文件（main.py:3337-3344）
- **问题**: 每次抓取平台 overview 本地回落都几十次文件读。
- **修复**: 进程内聚合结果缓存（TTL）。

### B11. `week_coverage` 全表扫描 + 大 dict 构建（trace_store.py:919-999）
- **问题**: `/api/history/weeks` 与历史周总览每次全量（店铺 × 天）扫描并做 set 运算。
- **修复**: 按日缓存（如 1h）。

### B12. operation_log 无保留期裁剪（trace_store.py:121-134，main.py 无 prune 调用）
- **问题**: 每次非轮询请求插一行，无限增长；`list_operation_clients` 全表 GROUP BY。
- **修复**: 启动/每日 prune（>90 天），参照 `log_store.prune_all`。

### B13. 无连接复用（main.py:839、893、2266 等所有 requests 调用）
- **问题**: 每次请求新建 TCP + TLS 连接。
- **修复**: 每 worker 线程一个 `requests.Session()` 保持 keep-alive。

---

## C. 健壮性

### C1. `resp.json()` 无 try/except（main.py:839、893、2266、1470、1602）
- **问题**: 上游返回非 JSON（维护页/代理错误）抛 JSONDecodeError；多数路径被通用 except 吞掉无日志，switch_group 会被 `groups_switch` 误报 400。
- **修复**: 包 try，记录状态码 + 响应前 200 字节，非 JSON 按网络错误走重试。

### C2. `_login_worker` 浏览器泄漏 + `login_close` 不真关（main.py:5943-5996、6025-6032）
- **问题**: 异常路径（goto 失败/超时）browser 未 close → 孤儿 chromium 进程；`/api/login/close` 只改状态，worker 继续轮询到 15 分钟并可能覆盖状态。
- **修复**: try/finally browser.close()；login_close 设置取消事件让 worker 退出。

### C3. 调度器 `_RequeueTask` 无次数上限（main.py:237-242）
- **问题**: 持续被交互请求抢占的任务无限重排 → 队尾饥饿。
- **修复**: task 加 requeue 计数，超 N 次标 error。

### C4. import_trace 非事务（main.py:3144-3152）
- **问题**: `clear_import_data` 后逐 upsert，中途异常 → 数据半清半留。
- **修复**: 包单事务或先写临时数据再整体替换。

### C5. `_prefetch_group` 未按 FETCH_PLATFORMS 过滤（main.py:6397）
- **问题**: 与 `refresh_all_async`（1311）不一致，若集团含平台 0/4 店铺会被误抓。
- **修复**: `[s for s in load_shops() if s.get("platform") in FETCH_PLATFORMS]`。

### C6. `_parse_kpi_sheet` 跨年周推断（main.py:3000-3008）
- **问题**: "12.28-1.3" 用今年年份 → 归到错误周。
- **修复**: 月份 < 当前月-1 时用下一年，或要求 RPA 周表带年份。

### C7. `_wx_qr_state` 全局单例互踩（main.py:6045-6162）
- **问题**: 多人并发扫码互相覆盖 scene；`last_poll` 全局 1s 节流让一个用户的轮询阻塞另一个。
- **修复**: 按会话隔离或加"进行中"互斥。

### C8. overview worker 目标为空时写 0 结果盘缓存（main.py:4864 + 4981-4982）
- **问题**: `platform` 无匹配集团 → shops=[] → completed total=0 → `save_trace_overview_cache` 落盘，同区间后续 6h 命中全 0。
- **修复**: 空目标不写缓存。

### C9. `/api/db/status` 直连 sqlite + COUNT 全表（main.py:6586-6615）
- **问题**: 绕开 trace_store 连接管理，写锁长占时可能 locked（已 try 兜底但无提示）；COUNT(*) 随窗口增长变慢。
- **修复**: 复用 trace_store 查询函数，COUNT 结果缓存。

---

## D. 小优化

- **D1.** 聚合逻辑三份重复：`stat_trace`（2443-2510，死代码无调用）、`stat_trace_daily`（2706-2759）、`_aggregate_stat_rows`（4537-4591）→ 合并为一份并删除死代码。
- **D2.** `import random` 在文件中部（2034）；多处函数内 import（942、1169、1193、4005、4152、4080、5164、6295 等）→ 统一移到模块顶部。
- **D3.** 魔法数字：TTL 21600 散落（1120、3625、3656），`_period_max_age` 重复 6*3600 → 抽常量。
- **D4.** `date_range`/`_split_bounds` 内部重复 `import datetime` → 用模块级 import。
- **D5.** `trace_config_get`（4262）每次调 `get_current_group()` 向 tanyu 出网 → 加 30s 缓存。

---

## 总结（最重要的 5 个问题）

1. **抓取任务并发互踩集团 cookie（A2+A3）**: refresh 的 running 置位太晚、四个 worker 恢复集团前就清 running，任何时刻都可能出现两个 worker 并发 switch_group，导致跨集团数据串污染 —— 这是"单并发防串数据"设计上最致命的裂缝，应先在端点同步置位、再调整 finally 顺序。
2. **触顶截断被当完整数据缓存入库（A6）且细分区间越界多算（A5）**: >1 万条的天要么被截断成 2000 条并持久化、要么尾部多算 4 小时，核算采纳率数字失真且难察觉。
3. **config.json 多写者丢更新（A4）**: 登录/切集团/扫码三条写路径无锁，互相覆盖导致登录态或集团 cookie 丢失，需统一加写锁。
4. **`/api/cookies` 无管理员校验（A1）与 days/区间无上限（A7）**: 前者任何登录用户可注入自己的 tanyu 账号（数据泄露）、后者一个超长 days 参数即可触发海量抓取。
5. **限速器无锁 + 双 worker 缓存击穿（A10/A11）**: 限速窗口竞态可突破 30 RPM 撞上风控，同店并发 miss 会双倍请求，都会直接触发探域风控停表。
