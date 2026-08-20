# -*- coding: utf-8 -*-
"""快速重建 trace_daily: 从 data/trace_days/*.json 缓存做 Python 聚合 + 批量 INSERT。

不走 upsert_shop_day(那条路径要逐条 INSERT/UPDATE messages, 对 300 万行的 messages
表做无索引的 (third_shop_id, trace_id) UPDATE, 实测 5686 格要跑数小时)。

只重建 trace_daily 聚合行, 绝不动 messages 表。之后跑 prune_window(30) 滚动裁剪
回 30 天窗口(与被删前的库状态一致)。

天猫1/天猫2(平台 10/11)无消息缓存, 其 trace_daily 行无法恢复(源数据来自 Excel 导入)。
"""
import glob
import json
import os
import sys

PROJ = "D:/test/test01"
sys.path.insert(0, PROJ)
os.chdir(PROJ)

import trace_store  # noqa: E402

ADOPTED = (1, 2, 3)
CACHE_DIR = os.path.join(PROJ, "data", "trace_days")

# 1) 清空被误删后残留的部分行
trace_store._ensure()
with trace_store._write_lock:
    c = trace_store._conn(write=True)
    try:
        c.execute("DELETE FROM trace_daily")
        c.commit()
        print("[1] trace_daily 已清空")
    finally:
        c.close()

# 2) 逐缓存聚合
rows = []  # (shop_id, day, total, adopted, counts_json, by_staff_json, by_type_json)
shops_done = 0
empty_days = 0
for path in sorted(glob.glob(os.path.join(CACHE_DIR, "*.json"))):
    shop_id = os.path.basename(path)[:-5]
    try:
        with open(path, encoding="utf-8") as f:
            cache = json.load(f)
    except Exception as e:
        print(f"[skip] {shop_id} 读缓存失败: {e}")
        continue
    days = cache.get("days") or {}
    if not days:
        continue
    shops_done += 1
    for day_str, msgs in days.items():
        if not msgs:
            empty_days += 1
            continue
        counts = {1: 0, 2: 0, 3: 0, None: 0}
        by_staff, by_type = {}, {}
        total = adopted = 0
        for m in msgs:
            st = m.get("sendType")
            counts[st] = counts.get(st, 0) + 1
            staff = m.get("sellerAccount") or m.get("staffName") or "未知"
            e = by_staff.setdefault(staff, {"total": 0, "adopted": 0})
            e["total"] += 1
            if st in ADOPTED:
                e["adopted"] += 1
            t = m.get("type") or "OTHER"
            by_type[t] = by_type.get(t, 0) + 1
            total += 1
            if st in ADOPTED:
                adopted += 1
        rows.append((
            shop_id, day_str, total, adopted,
            json.dumps(counts, ensure_ascii=False),
            json.dumps(sorted(by_staff.items(), key=lambda kv: -kv[1]["total"]), ensure_ascii=False),
            json.dumps(by_type, ensure_ascii=False),
        ))

print(f"[2] 聚合完成: {len(rows)} 个 (shop, day) 行, 涉及 {shops_done} 店, 空消息天 {empty_days}")

# 3) 批量写回
with trace_store._write_lock:
    c = trace_store._conn(write=True)
    try:
        c.executemany(
            """INSERT INTO trace_daily(third_shop_id, day, total, adopted,
                                       counts_json, by_staff_json, by_type_json)
               VALUES(?,?,?,?,?,?,?)
               ON CONFLICT(third_shop_id, day) DO UPDATE SET
                 total=excluded.total, adopted=excluded.adopted,
                 counts_json=excluded.counts_json,
                 by_staff_json=excluded.by_staff_json,
                 by_type_json=excluded.by_type_json""",
            rows,
        )
        c.commit()
        print(f"[3] 已写回 {len(rows)} 行")
    finally:
        c.close()

# 4) 滚动裁剪回 30 天窗口(与被删前一致; 天猫导入店铺无限期保留)
n = trace_store.prune_window(keep_days=30)
print(f"[4] prune_window(30) 完成, 删除消息 {n} 行")

# 5) 汇总
with trace_store._write_lock:
    c = trace_store._conn()
    try:
        total = c.execute("select count(*) from trace_daily").fetchone()[0]
        shops = c.execute("select count(distinct third_shop_id) from trace_daily").fetchone()[0]
        days = c.execute("select count(distinct day) from trace_daily").fetchone()[0]
        print(f"[verify] trace_daily = {total} 行 / {shops} 店 / {days} 天")
    finally:
        c.close()