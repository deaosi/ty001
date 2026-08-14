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
        # 连接可能被个别函数(如 get_shops 的独立连接)之外的路径 close 掉:
        # 复用前探测一次, 已关闭则重开, 避免同线程后续查询全部失败
        try:
            c.execute("SELECT 1")
        except sqlite3.Error:
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
            generation_rate REAL,
            PRIMARY KEY (third_shop_id, day)
        );
        CREATE INDEX IF NOT EXISTS idx_trace_daily_day
            ON trace_daily(day);
        CREATE TABLE IF NOT EXISTS imported_staff_kpi (
            third_shop_id TEXT NOT NULL,
            week_start    TEXT NOT NULL,
            account       TEXT NOT NULL,
            receive_cnt   INTEGER,
            inquiry_cnt   INTEGER,
            order_cnt     INTEGER,
            conversion    REAL,
            satisfaction  REAL,
            PRIMARY KEY (third_shop_id, week_start, account)
        );
        CREATE TABLE IF NOT EXISTS imported_roster (
            third_shop_id TEXT NOT NULL,
            account       TEXT NOT NULL,
            nick          TEXT,
            is_excluded   INTEGER DEFAULT 0,
            PRIMARY KEY (third_shop_id, account)
        );
        CREATE TABLE IF NOT EXISTS operation_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ts          TEXT NOT NULL,
            client_id   TEXT,
            client_name TEXT,
            ip          TEXT,
            method      TEXT NOT NULL,
            path        TEXT NOT NULL,
            query       TEXT,
            status      INTEGER,
            user_id     INTEGER
        );
        CREATE INDEX IF NOT EXISTS idx_oplog_ts ON operation_log(id);
        CREATE INDEX IF NOT EXISTS idx_oplog_client ON operation_log(client_id);
        CREATE TABLE IF NOT EXISTS users (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            username      TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            role          TEXT NOT NULL DEFAULT 'user',
            status        TEXT NOT NULL DEFAULT 'active',
            expire_date   TEXT,
            created_at    TEXT NOT NULL,
            last_login_at TEXT,
            note          TEXT,
            allow_legacy  INTEGER NOT NULL DEFAULT 0
        );
        """
    )
    # 旧库的 users 表没有 allow_legacy 列(旧版看板访问开关): 幂等补列
    try:
        c.execute("ALTER TABLE users ADD COLUMN allow_legacy INTEGER NOT NULL DEFAULT 0")
    except Exception:
        pass
    # 旧库的 trace_daily 可能没有 generation_rate 列(导入平台概览生成率卡用):
    # 幂等补列(已存在则忽略)
    try:
        c.execute("ALTER TABLE trace_daily ADD COLUMN generation_rate REAL")
    except Exception:
        pass
    # 旧库的 operation_log 可能没有 user_id 列(账号系统上线前建的库):
    # 幂等补列 + 建索引(索引必须等补列完成后, 否则旧表会报"无此列")
    try:
        c.execute("ALTER TABLE operation_log ADD COLUMN user_id INTEGER")
    except Exception:
        pass
    try:
        c.execute("CREATE INDEX IF NOT EXISTS idx_oplog_user ON operation_log(user_id)")
    except Exception:
        pass
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


def get_shops(platform=None):
    """读店铺维度表(跨集团全部店铺), 可按平台过滤; 用于核算/总览展示店铺归属

    与 main.load_shops(shops.json, 仅当前激活集团)不同: 这里覆盖三个集团的店铺,
    且 platform_name 已在本库归一(京东=7 等), 不会随激活集团变化。
    使用独立连接(不触碰线程共享读连接), 否则 close 会污染后续查询。
    """
    _ensure()
    c = _conn(write=True)
    try:
        if platform is None:
            rows = c.execute(
                "SELECT third_shop_id, shop_name, platform, platform_name, seller_id "
                "FROM shops ORDER BY third_shop_id"
            ).fetchall()
        else:
            rows = c.execute(
                "SELECT third_shop_id, shop_name, platform, platform_name, seller_id "
                "FROM shops WHERE platform = ? ORDER BY third_shop_id",
                (platform,),
            ).fetchall()
        return [
            {
                "thirdShopId": r["third_shop_id"],
                "shopName": r["shop_name"],
                "platform": r["platform"],
                "platformName": r["platform_name"] or str(r["platform"]),
                "sellerId": r["seller_id"],
            }
            for r in rows
        ]
    finally:
        c.close()


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


# ---------- 操作日志(会话可追溯, 登录后关联用户) ----------
def log_operation(client_id, client_name, ip, method, path, query, status, user_id=None):
    """写一条操作日志: 按浏览器身份(client_id/昵称)区分操作者, 登录后同时记 user_id。

    无账号系统下靠前端 localStorage 生成的唯一 client_id 标识"这台浏览器是谁",
    配合可选昵称让日志可读; 登录后 user_id 关联真实账号(管理员查日志的主体身份)。
    记录失败不抛异常(日志绝不能阻塞业务请求)。
    """
    try:
        _ensure()
        c = _conn(write=True)
        try:
            c.execute(
                "INSERT INTO operation_log(ts, client_id, client_name, ip, method, path, query, status, user_id) "
                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                 (client_id or "")[:64], (client_name or "")[:32], (ip or "")[:48],
                 method, path, (query or "")[:256], int(status) if status else None,
                 int(user_id) if user_id else None),
            )
            c.commit()
        finally:
            c.close()
    except Exception:
        pass


def query_operation_log(limit=100, client_id=None, client_name=None, user_id=None):
    """最近操作日志(倒序); 可按客户端/昵称/用户筛选, 带关联用户名"""
    _ensure()
    c = _conn(write=True)
    try:
        sql = ("SELECT l.id, l.ts, l.client_id, l.client_name, l.ip, l.method, l.path, "
               "l.query, l.status, l.user_id, u.username AS user_name "
               "FROM operation_log l LEFT JOIN users u ON u.id = l.user_id")
        conds, args = [], []
        if client_id:
            conds.append("l.client_id = ?")
            args.append(client_id)
        if client_name:
            conds.append("l.client_name = ?")
            args.append(client_name)
        if user_id:
            conds.append("l.user_id = ?")
            args.append(int(user_id))
        if conds:
            sql += " WHERE " + " AND ".join(conds)
        sql += " ORDER BY l.id DESC LIMIT ?"
        args.append(max(1, min(int(limit), 500)))
        rows = c.execute(sql, args).fetchall()
        return [dict(r) for r in rows]
    finally:
        c.close()


def list_operation_clients():
    """去重操作者(client_id + 最近昵称/IP)供前端筛选下拉"""
    _ensure()
    c = _conn(write=True)
    try:
        rows = c.execute(
            "SELECT client_id, client_name, ip, MAX(id) AS last_id "
            "FROM operation_log WHERE client_id != '' "
            "GROUP BY client_id ORDER BY last_id DESC"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        c.close()


def count_operation_log(client_id=None, user_id=None):
    """操作日志总条数(日志中心展示用), 可按客户端/用户过滤"""
    _ensure()
    c = _conn(write=True)
    try:
        conds, args = [], []
        if client_id:
            conds.append("client_id = ?"); args.append(client_id)
        if user_id:
            conds.append("user_id = ?"); args.append(int(user_id))
        sql = "SELECT COUNT(*) n FROM operation_log"
        if conds:
            sql += " WHERE " + " AND ".join(conds)
        return c.execute(sql, args).fetchone()["n"]
    finally:
        c.close()


def delete_operation_log(ids=None, older_than=None, client_id=None):
    """删除操作日志: 按 id 列表 / 按时间前 / 按客户端。返回删除条数。"""
    _ensure()
    c = _conn(write=True)
    try:
        conds, args = [], []
        if ids:
            ph = ",".join("?" * len(ids))
            conds.append(f"id IN ({ph})"); args.extend(int(i) for i in ids)
        if older_than:
            conds.append("id < ?"); args.append(int(older_than))
        if client_id:
            conds.append("client_id = ?"); args.append(client_id)
        if not conds:
            return 0
        cur = c.execute("DELETE FROM operation_log WHERE " + " AND ".join(conds), args)
        c.commit()
        return cur.rowcount
    finally:
        c.close()


# ---------- 账号系统: 用户 ----------
def create_user(username, password_hash, role="user", expire_date=None, note=None):
    """创建用户; 用户名已存在抛 ValueError; 返回不含密码哈希的用户 dict"""
    _ensure()
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    c = _conn(write=True)
    try:
        c.execute(
            "INSERT INTO users(username, password_hash, role, status, expire_date, created_at, note) "
            "VALUES(?,?,?,?,?,?,?)",
            (username, password_hash, role, "active", expire_date, now, note),
        )
        c.commit()
        uid = c.execute("SELECT id FROM users WHERE username = ?", (username,)).fetchone()["id"]
        return {"id": uid, "username": username, "role": role, "status": "active",
                "expire_date": expire_date, "created_at": now, "last_login_at": None, "note": note}
    except sqlite3.IntegrityError:
        raise ValueError(f"用户名「{username}」已存在")
    finally:
        c.close()


def get_user_by_username(username):
    """按用户名查用户(含密码哈希, 仅供登录校验用); 不存在返回 None"""
    _ensure()
    c = _conn(write=True)
    try:
        r = c.execute("SELECT * FROM users WHERE username = ?", (username,)).fetchone()
        return dict(r) if r else None
    finally:
        c.close()


def get_user_by_id(user_id):
    """按 ID 查用户(含密码哈希, 仅供鉴权依赖内部校验用); 不存在返回 None"""
    _ensure()
    c = _conn(write=True)
    try:
        r = c.execute("SELECT * FROM users WHERE id = ?", (int(user_id),)).fetchone()
        return dict(r) if r else None
    finally:
        c.close()


def list_users():
    """全部用户(最新注册在前); 不含密码哈希"""
    _ensure()
    c = _conn(write=True)
    try:
        rows = c.execute(
            "SELECT id, username, role, status, expire_date, created_at, last_login_at, note, allow_legacy "
            "FROM users ORDER BY id DESC"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        c.close()


def set_user_note(user_id, note):
    """更新用户备注(管理员)"""
    _ensure()
    c = _conn(write=True)
    try:
        c.execute("UPDATE users SET note = ? WHERE id = ?", (note or None, int(user_id)))
        c.commit()
    finally:
        c.close()


def set_user_allow_legacy(user_id, allow):
    """设置用户是否开放旧版看板(管理员; 管理员本身恒有权限)"""
    _ensure()
    c = _conn(write=True)
    try:
        c.execute("UPDATE users SET allow_legacy = ? WHERE id = ?", (1 if allow else 0, int(user_id)))
        c.commit()
    finally:
        c.close()


def user_allow_legacy(user_id) -> bool:
    """用户是否开放旧版看板"""
    _ensure()
    c = _conn(write=True)
    try:
        r = c.execute("SELECT allow_legacy FROM users WHERE id = ?", (int(user_id),)).fetchone()
        return bool(r and r["allow_legacy"])
    finally:
        c.close()


def update_user_password(user_id, password_hash):
    _ensure()
    c = _conn(write=True)
    try:
        c.execute("UPDATE users SET password_hash = ? WHERE id = ?", (password_hash, int(user_id)))
        c.commit()
    finally:
        c.close()


def set_user_status(user_id, status):
    """status: active / banned"""
    _ensure()
    c = _conn(write=True)
    try:
        c.execute("UPDATE users SET status = ? WHERE id = ?", (status, int(user_id)))
        c.commit()
    finally:
        c.close()


def set_user_expire(user_id, expire_date):
    """expire_date: 'YYYY-MM-DD', None=永久"""
    _ensure()
    c = _conn(write=True)
    try:
        c.execute("UPDATE users SET expire_date = ? WHERE id = ?", (expire_date, int(user_id)))
        c.commit()
    finally:
        c.close()


def touch_last_login(user_id):
    _ensure()
    c = _conn(write=True)
    try:
        c.execute("UPDATE users SET last_login_at = ? WHERE id = ?",
                  (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), int(user_id)))
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


def upsert_import_shop_day(shop_id, day, total, adopted, by_staff_map=None,
                           generation_rate=None):
    """写入导入平台(天猫1/2)某店某天的聚合行到 trace_daily

    与 rebuild_daily 同构: total/adopted + by_staff_json(排序一致, 读链路同口径)。
    counts_json/by_type_json 写 null(导入数据无 send_type/type 明细, 读链路不用)。
    generation_rate(话术生成率)来自 Excel 明细的"生成率"列, 概览第 4 卡用。
    主键 (third_shop_id, day) upsert 幂等, 重复导入覆盖。
    """
    _ensure()
    by_staff = by_staff_map or {}
    with _write_lock:
        c = _conn(write=True)
        try:
            c.execute(
                """INSERT INTO trace_daily(third_shop_id, day, total, adopted,
                                           counts_json, by_staff_json, by_type_json,
                                           generation_rate)
                   VALUES(?,?,?,?,?,?,?,?)
                   ON CONFLICT(third_shop_id, day) DO UPDATE SET
                     total=excluded.total, adopted=excluded.adopted,
                     counts_json=excluded.counts_json,
                     by_staff_json=excluded.by_staff_json,
                     by_type_json=excluded.by_type_json,
                     generation_rate=excluded.generation_rate""",
                (
                    shop_id, day, int(total), int(adopted),
                    None,
                    json.dumps(
                        sorted(by_staff.items(), key=lambda kv: -kv[1]["total"]),
                        ensure_ascii=False,
                    ),
                    None,
                    generation_rate,
                ),
            )
            c.commit()
        finally:
            c.close()


def upsert_import_kpi(rows):
    """写入导入平台周级客服 KPI(接待量/询单量/下单量/转化/满意率)"""
    if not rows:
        return
    _ensure()
    with _write_lock:
        c = _conn(write=True)
        try:
            c.executemany(
                """INSERT INTO imported_staff_kpi(third_shop_id, week_start, account,
                                                  receive_cnt, inquiry_cnt, order_cnt,
                                                  conversion, satisfaction)
                   VALUES(?,?,?,?,?,?,?,?)
                   ON CONFLICT(third_shop_id, week_start, account) DO UPDATE SET
                     receive_cnt=excluded.receive_cnt, inquiry_cnt=excluded.inquiry_cnt,
                     order_cnt=excluded.order_cnt, conversion=excluded.conversion,
                     satisfaction=excluded.satisfaction""",
                [
                    (
                        r["third_shop_id"], r["week_start"], r["account"],
                        r.get("receive_cnt"), r.get("inquiry_cnt"), r.get("order_cnt"),
                        r.get("conversion"), r.get("satisfaction"),
                    )
                    for r in rows
                ],
            )
            c.commit()
        finally:
            c.close()


def upsert_import_roster(rows):
    """写入导入平台在编/剔除客服清单(客服池渲染用, 独立于 tanyu 同步的 staff_names)"""
    if not rows:
        return
    _ensure()
    with _write_lock:
        c = _conn(write=True)
        try:
            c.executemany(
                """INSERT INTO imported_roster(third_shop_id, account, nick, is_excluded)
                   VALUES(?,?,?,?)
                   ON CONFLICT(third_shop_id, account) DO UPDATE SET
                     nick=excluded.nick, is_excluded=excluded.is_excluded""",
                [
                    (r["third_shop_id"], r["account"], r.get("nick"), 1 if r.get("is_excluded") else 0)
                    for r in rows
                ],
            )
            c.commit()
        finally:
            c.close()


def get_import_roster(platform=10):
    """读导入平台在编/剔除客服清单(JOIN shops 按平台过滤)"""
    if not DB_FILE.exists():
        return {}
    try:
        c = _conn()
        rows = c.execute(
            """SELECT r.third_shop_id AS shop_id, r.account, r.nick, r.is_excluded
               FROM imported_roster r
               JOIN shops s ON s.third_shop_id = r.third_shop_id
               WHERE s.platform = ?""",
            (platform,),
        ).fetchall()
        return [
            {
                "shopId": r["shop_id"], "account": r["account"],
                "nick": r["nick"], "isExcluded": bool(r["is_excluded"]),
            }
            for r in rows
        ]
    except Exception:
        return []


def clear_import_data(platform=10):
    """清空导入平台的聚合/客服/KPI 数据(重新导入前先清, 防残留旧日行)"""
    _ensure()
    with _write_lock:
        c = _conn(write=True)
        try:
            c.execute(
                "DELETE FROM trace_daily WHERE third_shop_id IN "
                "(SELECT third_shop_id FROM shops WHERE platform = ?)",
                (platform,),
            )
            c.execute(
                "DELETE FROM imported_staff_kpi WHERE third_shop_id IN "
                "(SELECT third_shop_id FROM shops WHERE platform = ?)",
                (platform,),
            )
            c.execute(
                "DELETE FROM imported_roster WHERE third_shop_id IN "
                "(SELECT third_shop_id FROM shops WHERE platform = ?)",
                (platform,),
            )
            c.commit()
        finally:
            c.close()


# ---------- 查询 ----------
def db_window_covers(start, end, platform=None, shop_filter=None):
    """DB 覆盖区间是否完整包含 [start, end](含两端); 缺库/空库返回 False

    platform/shop_filter 限定覆盖判定的店铺集(默认全库)。必须带平台过滤:
    导入平台(10/11)数据无限期保留会撑大全库 MIN/MAX, 若抓取平台(1/5/7)请求
    早于其 ~35 天窗口的自定义区间, 全库口径会误判为已覆盖, 走纯 DB 聚合返回
    全 0 而不回退在线抓取——把抓取平台更早区间的真实数据藏成 0。
    """
    if not DB_FILE.exists():
        return False
    try:
        c = _conn()
        where = ""
        args = []
        if platform is not None:
            where += " AND s.platform = ?"
            args.append(platform)
        if shop_filter:
            placeholders = ",".join("?" * len(shop_filter))
            where += f" AND d.third_shop_id IN ({placeholders})"
            args.extend(shop_filter)
        row = c.execute(
            f"""SELECT MIN(d.day) AS lo, MAX(d.day) AS hi
                FROM trace_daily d
                JOIN shops s ON s.third_shop_id = d.third_shop_id
                WHERE 1=1{where}""",
            args,
        ).fetchone()
        if not row or not row["lo"] or not row["hi"]:
            return False
        return row["lo"] <= start and end <= row["hi"]
    except Exception:
        return False


def db_window_overlaps(start, end, platform=None, shop_filter=None):
    """DB 区间与 [start, end] 是否有重叠(至少一天数据); 缺库/空库返回 False

    与 db_window_covers(完整包含)不同: 只要求区间内有数据即可, 用于导入平台
    (10/11)客服池等场景——导入数据无预抓、新鲜度取决于手动上传, "近7天"窗口
    末端(昨天)常缺数据, 完整覆盖判定会把整个区间短路成空。有重叠即聚合,
    空天自然为 0, 避免用户上传到昨天之前就把客服池/核算整片空白。
    """
    if not DB_FILE.exists():
        return False
    try:
        c = _conn()
        where = ""
        args = []
        if platform is not None:
            where += " AND s.platform = ?"
            args.append(platform)
        if shop_filter:
            placeholders = ",".join("?" * len(shop_filter))
            where += f" AND d.third_shop_id IN ({placeholders})"
            args.extend(shop_filter)
        row = c.execute(
            f"""SELECT MIN(d.day) AS lo, MAX(d.day) AS hi
                FROM trace_daily d
                JOIN shops s ON s.third_shop_id = d.third_shop_id
                WHERE 1=1{where}""",
            args,
        ).fetchone()
        if not row or not row["lo"] or not row["hi"]:
            return False
        # 标准闭区间重叠: lo <= end and hi >= start
        return row["lo"] <= end and start <= row["hi"]
    except Exception:
        return False


def db_window_range(start, end, platform=None, shop_filter=None):
    """返回 DB 实际覆盖区间 [lo, hi](最早/最晚有数据的天), 无数据返回 None

    与 db_window_covers(完整包含)互补: 完整覆盖失败但区间内部分天有数据时,
    展示场景(如客服账号池)仍可聚合已有天并提示"部分覆盖", 避免单天缺失
    (夜间抓取偶发失败)把整个客服池短路成空白。空店无行、空天无行的语义
    与 covers/overlaps 一致——MIN/MAX 只反映真正有数据的天。
    """
    if not DB_FILE.exists():
        return None
    try:
        c = _conn()
        where = ""
        args = []
        if platform is not None:
            where += " AND s.platform = ?"
            args.append(platform)
        if shop_filter:
            placeholders = ",".join("?" * len(shop_filter))
            where += f" AND d.third_shop_id IN ({placeholders})"
            args.extend(shop_filter)
        row = c.execute(
            f"""SELECT MIN(d.day) AS lo, MAX(d.day) AS hi
                FROM trace_daily d
                JOIN shops s ON s.third_shop_id = d.third_shop_id
                WHERE 1=1{where}""",
            args,
        ).fetchone()
        if not row or not row["lo"] or not row["hi"]:
            return None
        return row["lo"], row["hi"]
    except Exception:
        return None


def count_shops_per_day(start, end, platform=None, shop_filter=None):
    """窗口内每天"有消息的店铺数": {day: count}; 用于客服池缺抓检测

    trace_daily 每天每店至多一行(有消息才重建), 空店/空天无行。某天店铺数
    骤降(<窗口最高的一半)即该天疑似未抓全(夜间抓取失败是整平台同时缺)。
    单条 SQL, 不逐店读缓存文件, 与 staff_aggregate_per_shop 同数据源。
    """
    if not DB_FILE.exists():
        return {}
    try:
        c = _conn()
        where = "d.day BETWEEN ? AND ?"
        args = [start, end]
        if platform is not None:
            where += " AND s.platform = ?"
            args.append(platform)
        if shop_filter:
            placeholders = ",".join("?" * len(shop_filter))
            where += f" AND d.third_shop_id IN ({placeholders})"
            args.extend(shop_filter)
        rows = c.execute(
            f"""SELECT d.day AS day, COUNT(DISTINCT d.third_shop_id) AS n
                FROM trace_daily d
                JOIN shops s ON s.third_shop_id = d.third_shop_id
                WHERE {where}
                GROUP BY d.day""",
            args,
        ).fetchall()
        return {r["day"]: r["n"] for r in rows}
    except Exception:
        return {}


def week_coverage(platform=None):
    """按自然周统计每店覆盖天数 + 缺失天列表(基于 trace_daily, 纯本地只读)

    用于历史周下拉选项与"缺N天"徽标。返回按周降序的列表, 每项:
      {week_start: "YYYY-MM-DD(周一)", week_end, days(应覆盖天数),
       shop_days: {shop_id: set(day 集合)}, missing: {shop_id: [缺天列表]}}
    当前周应覆盖天数=周一~今天; 历史周=周一~周日 7 天。
    此预期天数保持非对称(当前周=周一~今天), 与展示区间(自然周=周一~周日,
    含未来周日)不同系有意设计——未来天物理无数据, 不能标成缺天。
    """
    if not DB_FILE.exists():
        return []
    try:
        c = _conn()
        today = datetime.date.today()
        where = ""
        args = []
        if platform is not None:
            where = " AND s.platform = ?"
            args.append(platform)
        # 回溯上限: tanyu 抓取平台窗口 ~35 天, 63 天覆盖足够(拉多无意义)。
        # 导入平台(10/11)无限期存储(无裁剪窗口), 不限回溯——否则历史周下拉
        # 只到 ~63 天, 更早导入的周完全不可见。platform=None(全部平台视图)
        # 同样可能含导入平台数据, 且导入平台店铺 join 结果与 tanyu 平台混行,
        # 63 天过滤会把更早的导入周一起裁掉——故凡非明确抓取平台(1/5/7)都解除回溯。
        fetch_platforms = (1, 5, 7)
        lookback = 63 if (platform is not None and platform in fetch_platforms) else 0
        lo_expr = "d.day >= ?" if lookback else "1=1"
        lo_args = [(today - datetime.timedelta(days=lookback)).isoformat()] if lookback else []
        rows = c.execute(
            f"""SELECT d.third_shop_id, d.day
                FROM trace_daily d
                JOIN shops s ON s.third_shop_id = d.third_shop_id
                WHERE {lo_expr}{where}
                ORDER BY d.day""",
            lo_args + args,
        ).fetchall()
    except Exception:
        return []
    # 逐店按天归类
    by_shop = {}
    for r in rows:
        by_shop.setdefault(r["third_shop_id"], set()).add(r["day"])
    if not by_shop:
        return []
    weeks = {}
    for shop_id, days in by_shop.items():
        for d in days:
            try:
                dt = datetime.date.fromisoformat(d)
            except (TypeError, ValueError):
                continue
            ws = dt - datetime.timedelta(days=dt.weekday())  # 周一
            weeks.setdefault(ws, {}).setdefault(shop_id, set()).add(d)
    out = []
    for ws in sorted(weeks, reverse=True):
        we = ws + datetime.timedelta(days=6)
        exp = 7
        if ws <= today <= we:
            exp = (today - ws).days + 1  # 当前周: 周一~今天
        shop_days = weeks[ws]
        missing = {}
        for sid, days in shop_days.items():
            have = set()
            for x in days:
                try:
                    have.add(datetime.date.fromisoformat(x))
                except (TypeError, ValueError):
                    pass
            miss = [(ws + datetime.timedelta(days=i)).isoformat()
                    for i in range(exp) if (ws + datetime.timedelta(days=i)) not in have]
            if miss:
                missing[sid] = miss
        out.append({
            "week_start": ws.isoformat(),
            "week_end": we.isoformat(),
            "days": exp,
            "shop_days": {k: sorted(v) for k, v in shop_days.items()},
            "missing": missing,
        })
    return out


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


def overview_aggregate(start, end, platform=None, shop_filter=None):
    """跨店总览聚合(从 trace_daily 出发): 每店 total/adopted + 全量 byStaff 合并

    shop_filter: 可选店铺子集(集合), 只聚合勾选的店铺(账号池模式)。
    """
    if not DB_FILE.exists():
        return {"shop_list": [], "staff_agg": {}}
    try:
        c = _conn()
        where = "d.day BETWEEN ? AND ?"
        args = [start, end]
        if platform is not None:
            where += " AND s.platform = ?"
            args.append(platform)
        if shop_filter:
            placeholders = ",".join("?" * len(shop_filter))
            where += f" AND d.third_shop_id IN ({placeholders})"
            args.extend(shop_filter)
        rows = c.execute(
            f"""SELECT d.third_shop_id, d.day, d.total, d.adopted, d.by_staff_json
                FROM trace_daily d
                JOIN shops s ON s.third_shop_id = d.third_shop_id
                WHERE {where}
                ORDER BY d.third_shop_id, d.day""",
            args,
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


def overview_aggregate_ms(start_ms, end_ms, platform=None, shop_filter=None):
    """跨店总览聚合(秒级边界): 从 messages 表按 msg_time 毫秒过滤

    与 overview_aggregate 同形状({shop_list: [(sid, {total, adopted})], staff_agg}),
    但 trace_daily 只有整天聚合, 秒级区间必须落到原始消息的 msg_time。
    adopted 口径一致: send_type IN (1,2,3)。
    """
    if not DB_FILE.exists():
        return {"shop_list": [], "staff_agg": {}}
    try:
        c = _conn()
        where = "m.msg_time BETWEEN ? AND ?"
        args = [start_ms, end_ms]
        if platform is not None:
            where += " AND m.platform = ?"
            args.append(platform)
        if shop_filter:
            placeholders = ",".join("?" * len(shop_filter))
            where += f" AND m.third_shop_id IN ({placeholders})"
            args.extend(shop_filter)
        rows = c.execute(
            f"""SELECT m.third_shop_id, m.send_type, m.seller_account
                FROM messages m
                WHERE {where}
                ORDER BY m.third_shop_id, m.msg_time""",
            args,
        ).fetchall()
        shop_map = {}
        staff_agg = {}
        for r in rows:
            sid = r["third_shop_id"]
            sm = shop_map.setdefault(sid, {"total": 0, "adopted": 0})
            sm["total"] += 1
            st = r["send_type"]
            if st in ADOPTED_SEND_TYPES:
                sm["adopted"] += 1
            acct = r["seller_account"] or "未知"
            e = staff_agg.setdefault(acct, {"total": 0, "adopted": 0})
            e["total"] += 1
            if st in ADOPTED_SEND_TYPES:
                e["adopted"] += 1
        return {"shop_list": list(shop_map.items()), "staff_agg": staff_agg}
    except Exception:
        return {"shop_list": [], "staff_agg": {}}


def staff_aggregate(start, end, platform=None, shop_filter=None):
    """按时间段聚合客服维度(从 trace_daily.by_staff_json 出发, 秒级)

    用于客服账号池的独立时间筛选: 返回 {account: {total, adopted}},
    跨店/跨平台合并。DB 未覆盖区间时返回空。
    """
    agg = overview_aggregate(start, end, platform, shop_filter)
    return agg["staff_agg"]


def staff_aggregate_per_shop(start, end, platform=None, shop_filter=None):
    """按 (店铺, 客服) 组合聚合客服维度(不去重/不跨店合并)

    用于客服账号池"按店铺区分客服": 同一客服账号在不同店铺各自成行,
    (店铺,客服) 组合数才是该口径下的客服总数。返回:
      {"by_shop": {shop_id: {account: {total, adopted}}}, "combos": N}
    shop_filter: 可选店铺子集(集合)。DB 未覆盖区间时返回空结构。
    """
    if not DB_FILE.exists():
        return {"by_shop": {}, "combos": 0}
    try:
        c = _conn()
        where = "d.day BETWEEN ? AND ?"
        args = [start, end]
        if platform is not None:
            where += " AND s.platform = ?"
            args.append(platform)
        if shop_filter:
            placeholders = ",".join("?" * len(shop_filter))
            where += f" AND d.third_shop_id IN ({placeholders})"
            args.extend(shop_filter)
        rows = c.execute(
            f"""SELECT d.third_shop_id, d.by_staff_json
                FROM trace_daily d
                JOIN shops s ON s.third_shop_id = d.third_shop_id
                WHERE {where}
                ORDER BY d.third_shop_id, d.day""",
            args,
        ).fetchall()
        by_shop = {}
        combos = 0
        for r in rows:
            sid = r["third_shop_id"]
            shop_agg = by_shop.setdefault(sid, {})
            try:
                staff_rows = json.loads(r["by_staff_json"])
            except Exception:
                staff_rows = []
            for acct, v in staff_rows:
                e = shop_agg.setdefault(acct, {"total": 0, "adopted": 0})
                e["total"] += v["total"]
                e["adopted"] += v["adopted"]
        for shop_agg in by_shop.values():
            combos += len(shop_agg)
        return {"by_shop": by_shop, "combos": combos}
    except Exception:
        return {"by_shop": {}, "combos": 0}


def avg_generation_rate(start, end, platform=None, shop_filter=None):
    """区间内 trace_daily.generation_rate 的非空均值(导入平台生成率卡)

    generation_rate 只在导入平台(10/11)有值(Excel 生成率列), 抓取平台恒 None。
    返回 float|None(区间内无任何非空行时返回 None, 前端显示 '—')。
    """
    if not DB_FILE.exists():
        return None
    try:
        c = _conn()
        where = "d.day BETWEEN ? AND ? AND d.generation_rate IS NOT NULL"
        args = [start, end]
        if platform is not None:
            where += " AND s.platform = ?"
            args.append(platform)
        if shop_filter:
            placeholders = ",".join("?" * len(shop_filter))
            where += f" AND d.third_shop_id IN ({placeholders})"
            args.extend(shop_filter)
        rows = c.execute(
            f"""SELECT d.generation_rate AS g
                FROM trace_daily d
                JOIN shops s ON s.third_shop_id = d.third_shop_id
                WHERE {where}""",
            args,
        ).fetchall()
        vals = [r["g"] for r in rows if r["g"] is not None]
        return (sum(vals) / len(vals)) if vals else None
    except Exception:
        return None


def staff_aggregate_per_shop_ms(start_ms, end_ms, platform=None, shop_filter=None):
    """按 (店铺, 客服) 组合聚合客服维度(秒级边界): 从 messages 表按 msg_time 过滤

    与 staff_aggregate_per_shop 同形状({"by_shop", "combos"}), 供 _staff_list_per_shop 消费。
    trace_daily 只有整天聚合, 秒级区间必须落到原始消息的 msg_time。
    """
    if not DB_FILE.exists():
        return {"by_shop": {}, "combos": 0}
    try:
        c = _conn()
        where = "m.msg_time BETWEEN ? AND ?"
        args = [start_ms, end_ms]
        if platform is not None:
            where += " AND m.platform = ?"
            args.append(platform)
        if shop_filter:
            placeholders = ",".join("?" * len(shop_filter))
            where += f" AND m.third_shop_id IN ({placeholders})"
            args.extend(shop_filter)
        rows = c.execute(
            f"""SELECT m.third_shop_id, m.send_type, m.seller_account
                FROM messages m
                WHERE {where}
                ORDER BY m.third_shop_id, m.msg_time""",
            args,
        ).fetchall()
        by_shop = {}
        for r in rows:
            sid = r["third_shop_id"]
            shop_agg = by_shop.setdefault(sid, {})
            acct = r["seller_account"] or "未知"
            e = shop_agg.setdefault(acct, {"total": 0, "adopted": 0})
            e["total"] += 1
            if r["send_type"] in ADOPTED_SEND_TYPES:
                e["adopted"] += 1
        combos = sum(len(a) for a in by_shop.values())
        return {"by_shop": by_shop, "combos": combos}
    except Exception:
        return {"by_shop": {}, "combos": 0}


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

    注意: 只裁消息与按日聚合, 绝不动 shops 表。店铺清单由 sync_shops_from_tanyu
    (每晚 switch_group 时 upsert_shops)维护; 若按"messages 是否有行"删店铺,
    会把 0 消息的空店(整段空档期)误删出列表, 白天店铺/客服池/核算筛选缺店。
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
            # 导入平台(天猫1/2)店铺没有 messages 行、天级数据是人工登记, 裁剪必须排除:
            # 否则会把导入的天级聚合当过期删掉、把导入店铺删出 shops 表。
            imp_sql = "(SELECT third_shop_id FROM shops WHERE platform IN (10, 11))"
            c.execute(
                f"DELETE FROM trace_daily WHERE day < ? "
                f"AND third_shop_id NOT IN {imp_sql}",
                (keep_start_str,),
            )
            c.commit()
            return deleted
        finally:
            c.close()


# 兼容旧引用: main.py 旧代码用 _trace_days_cache, 本模块不引入
TRACE_DAYS_CACHE_TTL = 60
