# 探域数据看板 数据管道审查报告(SQLite 抓取/存储/调度)

- 审查对象: `trace_store.py`(1337 行, SQLite 存取层) / `log_store.py` / `nightly_fetch.py` / `launch_resident.py` / `main.py` 中抓取相关部分
- 审查日期: 2026-08-14

---

## A. 数据正确性 / 完整性(最高优先)

### A1. 覆盖判定只查 MIN/MAX, 中间缺天被静默算成 0 ✅已修复
- 位置: `trace_store.py:db_window_covers` + `main.py:trace_overview` 快路径
- 问题: 判定条件是 `MIN(d.day) <= start and end <= MAX(d.day)`。若某晚某店抓取失败, trace_daily 中间缺一行, MIN/MAX 依然覆盖 → 快路径直接聚合返回, 缺天被静默计 0, 永不回退在线抓取。
- 修复: 改为逐格校验 `COUNT(DISTINCT (third_shop_id, day)) == 店铺数 × 期望天数`; `upsert_shop_day` 对"已抓取但 0 消息"的天也写 total=0 行, 使覆盖可逐格判定。

### A2. trace_id 为 NULL 的消息重复膨胀(force-refetch 竞态) ⚠️部分修复
- 位置: `trace_store.py:_msg_row`、`upsert_shop_day`
- 问题: 唯一索引是部分索引(`WHERE trace_id IS NOT NULL`), 无 traceId 的消息走 INSERT OR IGNORE 无约束去重, 每晚 force 重抓"昨天"会重复插入, trace_daily 数字虚高。
- 修复: upsert_shop_day 现在按 (shop, day) 直接重写当天聚合(0 条也写), 不再依赖去重; 残余重复行需一次 `DELETE FROM messages WHERE trace_id IS NULL` 手工清理(未执行, 待确认是否有此类行)。

### A3. 翻页中途失败 → 部分数据被当作完整天固化 ✅已修复(抛 TraceTruncatedError)
- 位置: `main.py:_fetch_trace_range`
- 问题: 翻页循环里单页异常只 print 后 break, 返回部分结果, 被当完整天缓存入库, 永不补齐。
- 修复: 4 小时细分后仍触顶时抛出 `TraceTruncatedError`, 调用方不落缓存/库并记日志(供人工补抓)。

### A4. cache 有、DB 无 → 永不自愈
- 位置: `main.py:stat_trace_daily`
- 问题: 每天抓完先 save_cache 再写 DB, DB 写入失败只 log; 次日命中缓存, 不重放 DB 写入。
- 状态: 未修(风险低: DB 写入失败极罕见, 且夜间 prefetch 每晚补写)。

### A5. `prefetch_trace_window` prune=True 无满窗守卫(数据丢失路径)
- 位置: `main.py:prefetch_trace_window` + `main.py --prefetch`
- 问题: `main.py --prefetch` 直接调 `prefetch_trace_window()` 默认 prune=True, 缺天时照样裁剪 → 永久数据洞。
- 状态: 未修(当前计划任务 TanyuDashboardTracePrefetch 是否用此路径待确认; nightly_fetch.py 是安全的)。

### A6. 夜间进程与常驻进程互斥标志非原子(check-then-set)
- 位置: `nightly_fetch.py:_lock` + `main.py:set_nightly_fetch_flag`
- 状态: 未修(建议改 O_EXCL 独占创建; 目前双开概率低)。

### A7. 时区: 全链路本地 naive 时间
- 位置: `rebuild_daily`/`prune_window`/`_split_bounds`
- 状态: 未修(服务器时区稳定为本地, 风险低; 建议加启动断言)。

### A8. SQLite 双进程写无 busy 重试
- 位置: `trace_store.py:_conn`(timeout=15)
- 状态: 未修(busy_timeout 15s 已较大, 双进程并发写窗口小)。

### A9. `upsert_shop_day` 两段提交读者可见不一致 ✅已消除
- 修复: 消息写入 + 当天聚合合并为同一事务, 单次 commit。

### A10. `_check_risk` 对 429 一刀切全局停抓
- 位置: `main.py:_check_risk`
- 状态: 未修(设计取舍: 保守止损优先; 可改为 429 退避重试)。

## B. 性能

- **B1. `_ensure()` 每次写调用全量跑 schema 初始化 ✅已修复**: 进程级 `_schema_ready` 标志, 首次成功建表后读路径跳过 DDL。
- **B2. `rebuild_daily` 每次单天 upsert 全店 35 天全量重扫 ✅已修复**: upsert_shop_day 只写当天聚合行, 不再触发全店重建(rebuild_daily 保留给平台回填等批量场景)。
- **B3. prune 后无 VACUUM/checkpoint ✅已修复**: prune_window 提交后执行 `PRAGMA wal_checkpoint(TRUNCATE)` 收缩 WAL。
- **B4. 冗余索引 `idx_oplog_ts ON operation_log(id)` ✅已删除**: id 已是 INTEGER PRIMARY KEY 自带索引, 纯写入开销。
- **B5. operation_log 无保留期 ✅已修复**: 新增 `prune_operation_log(keep_days=90)`, 挂到 `_maybe_prune_tasks` 每小时懒清理。
- B6. `append_system_log` 全表 NOT IN 子查询 — 未修(表小, 影响低)。
- B7. 缺 `(platform, msg_time)` 复合索引 — 未修(现有 msg_time 索引够用)。

## C. 健壮性

- C1. 日志无轮转 — 未修(server.log/nightly_fetch.log 无限追加; 建议加大小截断)。
- C2. launch_resident 无守护、pythonw 硬编码 — 未修(建议 watchdog)。
- C3. 磁盘满无预检 — 未修(D 盘剩余 296GB, 风险低)。
- C4. `_rate_limit` 无锁 ✅已修复: 独立 `_rate_lock` 保护滑动窗口。
- C5. 风控状态进程内不共享 — 未修(夜间进程独立判断, 靠 401/403 撞停)。
- C6. `_lock()` 异常时无锁放行 — 未修(低概率)。

## D. 数据库迁移策略

- D1. users 补列 try/except 吞真实错误 — 未修(建议 PRAGMA table_info 判断)。
- D2. 未来演进: messages 冗余 day 列 / fetch_meta 表 / trace_daily updated_at — 未修(建议按需实施)。

---

## 总结(最重要的 5 个问题)

1. **覆盖判定 MIN/MAX 缺陷(A1)**: ✅已修复为逐格校验 + 0 行写入, 中间缺天不再被静默算 0。
2. **触顶截断被当完整数据(A3)**: ✅已修复为抛异常不落库。
3. **upsert 两段提交不一致 + 全店重建(A9/B2)**: ✅已合并单事务 + 只写当天聚合。
4. **operation_log 无限增长(B5)**: ✅已加 90 天保留期懒清理。
5. **`_ensure` 每次全量 DDL(B1)**: ✅已加进程级标志跳过。
