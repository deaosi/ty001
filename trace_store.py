# -*- coding: utf-8 -*-
"""
探域数据看板 - SQLite 消息轨迹存储层(30 天滚动窗口)

数据源是 main.py 的逐日 JSON 缓存(data/trace_days), 本模块把它们投影进
SQLite(data/trace.db), 供核算/展示做纯本地聚合, 交互路径零上游请求。

设计要点:
  - WAL 模式 + 进程内单写锁: 常驻 uvicorn(读)与夜间预抓(写)可并发不互锁
  - 原始消息(raw) 按 30 天滚动窗口留存, 每日裁剪; 按日聚合缓存表提供零扫描总览
  - 口径与 stat_trace_daily 完全一致: adopted = send_type IN (1,2,3)
  - 去重靠 (third_shop_id, trace_id) 唯一索引, 重抓/重复运行幂等
"""
import datetime
import json
import sqlite3
import threading
import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DB_FILE = BASE_DIR / "data" / "trace.db"

# 发送状态: 1=自动发送 2=侧边栏点击发送 3=编辑后发送 (None=未发送)
ADOPTED_SEND_TYPES = (1, 2, 3)

_write_lock = threading.Lock()  # 单写者串行化(写连接每次短开短关)
_local = threading.local()      # 每线程一个只读连接, 避免跨线程复用游标


# ---------- 连接 ----------
def _conn(write=False):
    if not write:
        c = getattr(_local, "conn", None)
        if c is None:
            c = sqlite3.connect(str(DB_FILE), timeout=15)
            c.row_factory = sqlite3.Row
            _local.conn = c
        return c
    c = sqlite3.connect(str(DB_FILE), timeout=15)
    c.row_factory = sqlite3.Row
    return c


def _init_schema(c):
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA synchronous=NORMAL")
    c.execute("PRAGMA foreign_keys=ON")
    c.executescript(
        """
        CREATE TABLE IF NOT EXISTS meta (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS shops (
            third_shop_id     TEXT PRIMARY KEY,
            shop_name         TEXT NOT NULL,
            platform          INTEGER NOT NULL,
            seller_id         TEXT,
            if_agent_receipt  INTEGER DEFAULT 0,
            platform_name     TEXT,
            updated_at        INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS messages (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            trace_id       TEXT,
            third_shop_id  TEXT NOT NULL,
            platform       INTEGER NOT NULL,
            msg_time       INTEGER NOT NULL,
            send_type      INTEGER,
            seller_account TEXT,
            type           TEXT,
            content_json   TEXT,
            fetched_at     INTEGER NOT NULL
        );
        CREATE UNIQUE INDEX IF NOT EXISTS uq_messages_shop_trace
            ON messages(third_shop_id, trace_id) WHERE trace_id IS NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_messages_shop_time
            ON messages(third_shop_id, msg_time);
        CREATE INDEX IF NOT EXISTS idx_messages_time
            ON messages(msg_time);
        CREATE TABLE IF NOT EXISTS trace_daily (
            third_shop_id TEXT NOT NULL,
            day           TEXT NOT NULL,
            total         INTEGER NOT NULL,
            adopted       INTEGER NOT NULL,
            counts_json   TEXT,
            by_staff_json TEXT,
            by_type_json  TEXT,
            PRIMARY KEY (third_shop_id, day)
        );
        CREATE INDEX IF NOT EXISTS idx_trace_daily_day
            ON trace_daily(day);
        """
    )
    c.execute(
        "INSERT OR IGNORE INTO meta(key, value) VALUES('schema_version', '1')"
    )


def _ensure():
    with _write_lock:
        c = _conn(write=True)
        try:
            _init_schema(c)
            c.commit()
        finally:
            c.close()


def db_path():
    return str(DB_FILE)


def init_db():
    """启动时调用: 建表 + 幂等"""
    if not DB_FILE.exists():
        _ensure()
        return
    # 已存在: 轻量校验 schema_version, 缺表则重建(不覆盖数据)
    c = _conn(write=True)
    try:
        _init_schema(c)
        c.commit()
    finally:
        c.close()


# ---------- 写入 ----------
def upsert_shops(shops):
    """店铺维度表与 shops.json 同步(切换店铺组后调用)"""
    if not shops:
        return
    _ensure()
    now = int(time.time())
    with _write_lock:
        c = _conn(write=True)
        try:
            c.executemany(
                """INSERT INTO shops(third_shop_id, shop_name, platform, seller_id,
                                     if_agent_receipt, platform_name, updated_at)
                   VALUES(?,?,?,?,?,?,?)
                   ON CONFLICT(third_shop_id) DO UPDATE SET
                     shop_name=excluded.shop_name, platform=excluded.platform,
                     seller_id=excluded.seller_id,
                     if_agent_receipt=excluded.if_agent_receipt,
                     platform_name=excluded.platform_name, updated_at=excluded.updated_at""",
                [
                    (
                        s.get("thirdShopId"),
                        s.get("shopName", ""),
                        s.get("platform", 0),
                        s.get("sellerId"),
                        1 if s.get("ifAgentReceipt") else 0,
                        s.get("platformName", ""),
                        now,
                    )
                    for s in shops
                    if s.get("thirdShopId")
                ],
            )
            c.commit()
        finally:
            c.close()


def _msg_row(shop_id, platform, day, m):
    """把一条裁剪后的消息 dict 映射为 messages 行; 缺 time 的行丢弃(无法按天裁剪)"""
    ts = m.get("time") or m.get("createTime") or m.get("createAt")
    if not isinstance(ts, (int, float)):
        return None
    content = m.get("content")
    return (
        m.get("traceId") or m.get("id") or None,
        shop_id,
        platform,
        int(ts),
        m.get("sendType"),
        m.get("sellerAccount") or m.get("staffName") or None,
        m.get("type") or "OTHER",
        json.dumps(content, ensure_ascii=False) if content is not None else None,
        int(time.time()),
    )


def upsert_shop_day(shop_id, platform, day, msgs):
    """写入某店铺某天的消息(幂等: 同 trace_id 覆盖), 并刷新该店当日聚合"""
    if not msgs:
        return
    _ensure()
    rows = [_msg_row(shop_id, platform, day, m) for m in msgs]
    rows = [r for r in rows if r is not None]
    if not rows:
        return
    with _write_lock:
        c = _conn(write=True)
        try:
            # 部分唯一索引 (third_shop_id, trace_id) WHERE trace_id IS NOT NULL
            # 不支持 ON CONFLICT 目标子句; 用 INSERT OR IGNORE 去重 + UPDATE 覆盖
            c.executemany(
                """INSERT OR IGNORE INTO messages(trace_id, third_shop_id, platform, msg_time,
                                        send_type, seller_account, type, content_json, fetched_at)
                   VALUES(?,?,?,?,?,?,?,?,?)""",
                rows,
            )
            # 已存在的行(trace_id 非空)覆盖为最新抓取内容
            c.executemany(
                """UPDATE messages SET platform=?, msg_time=?, send_type=?,
                   seller_account=?, type=?, content_json=?, fetched_at=?
                   WHERE third_shop_id=? AND trace_id=?""",
                [(r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[1], r[0]) for r in rows if r[0]],
            )
            c.commit()
        finally:
            c.close()
    rebuild_daily(shop_id)


def rebuild_daily(shop_id):
    """从 messages 重建某店全部按日聚合(upsert), 保证 trace_daily 与 raw 一致"""
    _ensure()
    with _write_lock:
        c = _conn(write=True)
        try:
            cur = c.execute(
                """SELECT msg_time, send_type, seller_account, type
                   FROM messages WHERE third_shop_id = ? ORDER BY msg_time""",
                (shop_id,),
            )
            rows = cur.fetchall()
            agg = {}  # day -> {total, adopted, counts, by_staff, by_type}
            for r in rows:
                st = r["send_type"]
                ts = r["msg_time"]
                day = time.strftime("%Y-%m-%d", time.localtime(ts / 1000))
                a = agg.setdefault(day, {
                    "total": 0, "adopted": 0,
                    "counts": {1: 0, 2: 0, 3: 0, None: 0},
                    "by_staff": {}, "by_type": {},
                })
                a["total"] += 1
                if st in ADOPTED_SEND_TYPES:
                    a["adopted"] += 1
                a["counts"][st] = a["counts"].get(st, 0) + 1
                staff = r["seller_account"] or "未知"
                e = a["by_staff"].setdefault(staff, {"total": 0, "adopted": 0})
                e["total"] += 1
                if st in ADOPTED_SEND_TYPES:
                    e["adopted"] += 1
                t = r["type"] or "OTHER"
                a["by_type"][t] = a["by_type"].get(t, 0) + 1
            c.executemany(
                """INSERT INTO trace_daily(third_shop_id, day, total, adopted,
                                           counts_json, by_staff_json, by_type_json)
                   VALUES(?,?,?,?,?,?,?)
                   ON CONFLICT(third_shop_id, day) DO UPDATE SET
                     total=excluded.total, adopted=excluded.adopted,
                     counts_json=excluded.counts_json,
                     by_staff_json=excluded.by_staff_json,
                     by_type_json=excluded.by_type_json""",
                [
                    (
                        shop_id, day,
                        a["total"], a["adopted"],
                        json.dumps(a["counts"], ensure_ascii=False),
                        json.dumps(
                            sorted(a["by_staff"].items(), key=lambda kv: -kv[1]["total"]),
                            ensure_ascii=False,
                        ),
                        json.dumps(a["by_type"], ensure_ascii=False),
                    )
                    for day, a in agg.items()
                ],
            )
            c.commit()
        finally:
            c.close()


# ---------- 查询 ----------
def db_window_covers(start, end):
    """DB 覆盖区间是否完整包含 [start, end](含两端); 缺库/空库返回 False"""
    if not DB_FILE.exists():
        return False
    try:
        c = _conn()
        row = c.execute(
            "SELECT MIN(day) AS lo, MAX(day) AS hi FROM trace_daily"
        ).fetchone()
        if not row or not row["lo"] or not row["hi"]:
            return False
        return row["lo"] <= start and end <= row["hi"]
    except Exception:
        return False


def db_day_window_covers(hist_start, hist_end):
    """判断库是否完整覆盖 [hist_start, hist_end] 这一段历史区间(不含今天)

    用于"历史走库 + 今天实时"合并路径: hist_end 通常是昨天(今天之前),
    只要库的整日窗口覆盖这段历史, 就认为历史部分可以纯本地聚合。
    """
    if not DB_FILE.exists():
        return False
    try:
        c = _conn()
        row = c.execute(
            "SELECT MIN(day) AS lo, MAX(day) AS hi FROM trace_daily"
        ).fetchone()
        if not row or not row["lo"] or not row["hi"]:
            return False
        return row["lo"] <= hist_start and hist_end <= row["hi"]
    except Exception:
        return False


def query_daily(shop_id, start, end):
    """该店在区间内的逐日聚合(用于折线图/核算总览)"""
    if not DB_FILE.exists():
        return []
    try:
        c = _conn()
        return [
            dict(r)
            for r in c.execute(
                """SELECT day, total, adopted FROM trace_daily
                   WHERE third_shop_id = ? AND day BETWEEN ? AND ? ORDER BY day""",
                (shop_id, start, end),
            )
        ]
    except Exception:
        return []


def query_shop_aggregate(shop_id, start, end, from_ms, to_ms):
    """该店区间原始消息(供 byStaff/byType/messages 聚合), 按 msg_time 排序"""
    if not DB_FILE.exists():
        return []
    try:
        c = _conn()
        return [
            {
                "time": r["msg_time"],
                "sendType": r["send_type"],
                "sellerAccount": r["seller_account"],
                "type": r["type"],
                "content": json.loads(r["content_json"]) if r["content_json"] else "",
                "traceId": r["trace_id"],
            }
            for r in c.execute(
                """SELECT msg_time, send_type, seller_account, type, content_json, trace_id
                   FROM messages
                   WHERE third_shop_id = ? AND msg_time BETWEEN ? AND ?
                   ORDER BY msg_time""",
                (shop_id, from_ms, to_ms),
            )
        ]
    except Exception:
        return []


def overview_aggregate(start, end, platform=None):
    """跨店总览聚合(从 trace_daily 出发): 每店 total/adopted + 全量 byStaff 合并"""
    if not DB_FILE.exists():
        return {"shop_list": [], "staff_agg": {}}
    try:
        c = _conn()
        if platform is None:
            rows = c.execute(
                """SELECT third_shop_id, day, total, adopted, by_staff_json
                   FROM trace_daily WHERE day BETWEEN ? AND ? ORDER BY third_shop_id, day""",
                (start, end),
            ).fetchall()
        else:
            rows = c.execute(
                """SELECT d.third_shop_id, d.day, d.total, d.adopted, d.by_staff_json
                   FROM trace_daily d
                   JOIN shops s ON s.third_shop_id = d.third_shop_id
                   WHERE d.day BETWEEN ? AND ? AND s.platform = ?
                   ORDER BY d.third_shop_id, d.day""",
                (start, end, platform),
            ).fetchall()
        shop_map = {}
        staff_agg = {}
        for r in rows:
            sid = r["third_shop_id"]
            sm = shop_map.setdefault(sid, {"total": 0, "adopted": 0})
            sm["total"] += r["total"]
            sm["adopted"] += r["adopted"]
            try:
                staff_rows = json.loads(r["by_staff_json"])
            except Exception:
                staff_rows = []
            for acct, v in staff_rows:
                e = staff_agg.setdefault(acct, {"total": 0, "adopted": 0})
                e["total"] += v["total"]
                e["adopted"] += v["adopted"]
        return {"shop_list": list(shop_map.items()), "staff_agg": staff_agg}
    except Exception:
        return {"shop_list": [], "staff_agg": {}}


def repair_shop_platform(shop_id, platform):
    """把某店 platform=0 的历史消息纠正为正确平台, 并重建该店按日聚合

    早期回填无平台信息, 大量消息 platform=0; 集团店铺表同步后据此归位。
    仅更新仍为 0 的行(避免覆盖已归位的实时抓取), 返回更新行数。
    """
    _ensure()
    with _write_lock:
        c = _conn(write=True)
        try:
            cur = c.execute(
                "UPDATE messages SET platform = ? WHERE third_shop_id = ? AND platform = 0",
                (platform, shop_id),
            )
            updated = cur.rowcount
            c.commit()
        finally:
            c.close()
    if updated:
        rebuild_daily(shop_id)
    return updated


def prune_window(keep_days=30):
    """滚动裁剪: 保留最近 keep_days 个完整本地日历日(删除更早), 返回删除的消息行数

    整日窗口语义: 库内只存"已定型的完整日"(最新是昨天, 今天实时不进库),
    所以保留窗口 = [昨天 - (keep_days-1) .. 昨天], 共 keep_days 个完整日。
    删除边界: 最早完整日 00:00 之前的消息与按日聚合全部删除。
    """
    if not DB_FILE.exists():
        return 0
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)
    # 保留窗口的最早完整日(含): 昨天往前 (keep_days - 1) 天
    keep_start = yesterday - datetime.timedelta(days=keep_days - 1)
    keep_start_str = keep_start.isoformat()
    # 最早完整日 00:00 的本地时间毫秒数(删除边界)
    cutoff_ms = int(time.mktime(datetime.datetime(keep_start.year, keep_start.month, keep_start.day).timetuple()) * 1000)
    with _write_lock:
        c = _conn(write=True)
        try:
            cur = c.execute(
                "DELETE FROM messages WHERE msg_time < ?", (cutoff_ms,)
            )
            deleted = cur.rowcount
            c.execute("DELETE FROM trace_daily WHERE day < ?", (keep_start_str,))
            c.execute("DELETE FROM shops WHERE third_shop_id NOT IN (SELECT DISTINCT third_shop_id FROM messages)")
            c.commit()
            return deleted
        finally:
            c.close()


# 兼容旧引用: main.py 旧代码用 _trace_days_cache, 本模块不引入
TRACE_DAYS_CACHE_TTL = 60
