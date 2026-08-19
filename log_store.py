# -*- coding: utf-8 -*-
"""日志/任务独立轻量存储(SQLite 单文件 data/logs.db), 与主数据 trace.db 解耦。

存储内容:
  tasks        - 任务历史(谁请求了什么任务/状态/进度/结果), 支撑任务列表"最近10条+池子浏览"
  system_logs  - 系统日志(进程环形缓冲的持久化副本), 支撑管理员日志中心分类查看

设计:
  - 独立单文件 DB + WAL + 进程内锁, 与 trace_store 互不干扰。
  - 保留期裁剪: 任务默认 14 天、系统日志 7 天; 系统日志另有条数硬上限防膨胀。
  - 所有写路径短事务, 失败绝不阻塞业务(日志是辅助设施)。
"""
from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DB_FILE = BASE_DIR / "data" / "logs.db"

_lock = threading.Lock()

TASK_RETENTION_DAYS = 14     # 任务记录保留天数(旧语义, 兼容外部调用)
TASK_RETENTION_HOURS = 6     # 任务记录保留小时数: 6 小时后自动清理(需求)
SYSTEM_RETENTION_DAYS = 7    # 系统日志保留天数
MAX_SYSTEM_LOGS = 20000       # 系统日志条数硬上限(防膨胀; SQLite 万级无压力)


def db_path() -> str:
    return str(DB_FILE)


def _conn(write: bool = False):
    DB_FILE.parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(str(DB_FILE), timeout=15)
    c.row_factory = sqlite3.Row
    if write:
        try:
            c.execute("PRAGMA journal_mode=WAL")
        except Exception:
            pass
    return c


def init_db():
    """启动时建表(幂等)。与 trace_store.init_db 互不依赖。"""
    with _lock:
        c = _conn(write=True)
        try:
            c.executescript(
                """
                CREATE TABLE IF NOT EXISTS tasks (
                    id            INTEGER PRIMARY KEY,
                    type          TEXT,
                    label         TEXT,
                    requested_by  TEXT,
                    requested_role TEXT,
                    status        TEXT,
                    progress_json TEXT,
                    result_json   TEXT,
                    error         TEXT,
                    created_at    REAL,
                    started_at    REAL,
                    finished_at   REAL
                );
                CREATE INDEX IF NOT EXISTS idx_tasks_created ON tasks(created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
                CREATE INDEX IF NOT EXISTS idx_tasks_type ON tasks(type);
                CREATE TABLE IF NOT EXISTS system_logs (
                    id  INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts  REAL,
                    tag TEXT,
                    msg TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_syslog_ts ON system_logs(ts DESC);
                """
            )
            # 旧库补列(任务发起人角色, 供普通用户视角对管理员任务掩码)
            cols = [r[1] for r in c.execute("PRAGMA table_info(tasks)").fetchall()]
            if "requested_role" not in cols:
                c.execute("ALTER TABLE tasks ADD COLUMN requested_role TEXT")
            # 旧库补列(任务参数, 供前端"查看"按钮跳回对应平台+时间段视图)
            if "params_json" not in cols:
                c.execute("ALTER TABLE tasks ADD COLUMN params_json TEXT")
            c.commit()
        finally:
            c.close()


# ---------- 任务 ----------
def upsert_task(task: dict):
    """插入新任务或更新已有任务(id 存在则覆盖)。

    task 需含: id/type/label/requested_by/status/created_at + 可选 progress/result/error/started_at/finished_at
    progress/result/params 序列化为 JSON 存储。
    """
    try:
        with _lock:
            c = _conn(write=True)
            try:
                c.execute(
                    """INSERT INTO tasks(id, type, label, requested_by, requested_role, status,
                                         progress_json, result_json, error,
                                         created_at, started_at, finished_at, params_json)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                       ON CONFLICT(id) DO UPDATE SET
                         type=excluded.type, label=excluded.label, requested_by=excluded.requested_by,
                         requested_role=excluded.requested_role,
                         status=excluded.status, progress_json=excluded.progress_json,
                         result_json=excluded.result_json, error=excluded.error,
                         created_at=excluded.created_at, started_at=excluded.started_at,
                         finished_at=excluded.finished_at, params_json=excluded.params_json""",
                    (
                        int(task["id"]), task.get("type"), task.get("label"),
                        task.get("requested_by"), task.get("requested_role"),
                        task.get("status", "queued"),
                        json.dumps(task.get("progress") or {}, ensure_ascii=False),
                        json.dumps(task.get("result"), ensure_ascii=False) if task.get("result") is not None else None,
                        task.get("error"),
                        task.get("created_at", time.time()),
                        task.get("started_at"), task.get("finished_at"),
                        json.dumps(task.get("params") or {}, ensure_ascii=False),
                    ),
                )
                c.commit()
            finally:
                c.close()
    except Exception:
        pass  # 日志是辅助设施, 失败不阻塞业务


def _task_row(row) -> dict:
    d = dict(row)
    try:
        d["progress"] = json.loads(d.pop("progress_json") or "{}")
    except Exception:
        d["progress"] = {}
    try:
        d["result"] = json.loads(d.pop("result_json") or "null")
    except Exception:
        d["result"] = None
    try:
        d["params"] = json.loads(d.pop("params_json") or "{}")
    except Exception:
        d["params"] = {}
    return d


def query_tasks(limit: int = 10, offset: int = 0, status: str | None = None,
                task_type: str | None = None, since: float | None = None) -> list:
    """任务查询(倒序=最近优先)。limit/offset 支撑池子分页; status/type 过滤; since 支撑"UI 只保留6小时"。
    返回 dict 列表(progress/result 已反序列化)。"""
    limit = max(1, min(int(limit), 500))
    offset = max(0, int(offset))
    with _lock:
        c = _conn()
        try:
            where, args = [], []
            if status:
                where.append("status=?"); args.append(status)
            if task_type:
                where.append("type=?"); args.append(task_type)
            if since is not None:
                where.append("created_at>=?"); args.append(since)
            w = (" WHERE " + " AND ".join(where)) if where else ""
            rows = c.execute(
                f"SELECT * FROM tasks{w} ORDER BY created_at DESC LIMIT ? OFFSET ?",
                args + [limit, offset],
            ).fetchall()
            return [_task_row(r) for r in rows]
        finally:
            c.close()


def count_tasks(status: str | None = None, since: float | None = None) -> int:
    with _lock:
        c = _conn()
        try:
            where, args = [], []
            if status:
                where.append("status=?"); args.append(status)
            if since is not None:
                where.append("created_at>=?"); args.append(since)
            w = (" WHERE " + " AND ".join(where)) if where else ""
            return c.execute(f"SELECT COUNT(*) n FROM tasks{w}", args).fetchone()["n"]
        finally:
            c.close()


def delete_task(task_id: int) -> int:
    with _lock:
        c = _conn(write=True)
        try:
            cur = c.execute("DELETE FROM tasks WHERE id=?", (int(task_id),))
            c.commit()
            return cur.rowcount
        finally:
            c.close()


def clear_tasks(status: str | None = None) -> int:
    """清空任务记录(可按状态)。返回删除条数。管理员"清空历史"用。"""
    with _lock:
        c = _conn(write=True)
        try:
            if status:
                cur = c.execute("DELETE FROM tasks WHERE status=?", (status,))
            else:
                cur = c.execute("DELETE FROM tasks")
            c.commit()
            return cur.rowcount
        finally:
            c.close()


def prune_tasks(days: int = TASK_RETENTION_DAYS) -> int:
    cutoff = time.time() - days * 86400
    with _lock:
        c = _conn(write=True)
        try:
            cur = c.execute("DELETE FROM tasks WHERE created_at < ?", (cutoff,))
            c.commit()
            return cur.rowcount
        finally:
            c.close()


# ---------- 系统日志 ----------
def append_system_log(tag: str, msg: str):
    """写一条系统日志(与进程环形缓冲并行)。带条数硬上限裁剪。"""
    try:
        with _lock:
            c = _conn(write=True)
            try:
                c.execute("INSERT INTO system_logs(ts, tag, msg) VALUES(?,?,?)", (time.time(), tag, msg))
                c.execute(
                    "DELETE FROM system_logs WHERE id NOT IN "
                    "(SELECT id FROM system_logs ORDER BY ts DESC LIMIT ?)",
                    (MAX_SYSTEM_LOGS,),
                )
                c.commit()
            finally:
                c.close()
    except Exception:
        pass


def query_system_logs(limit: int = 200, offset: int = 0, tag: str | None = None) -> list:
    limit = max(1, min(int(limit), 1000))
    offset = max(0, int(offset))
    with _lock:
        c = _conn()
        try:
            where, args = [], []
            if tag:
                where.append("tag=?"); args.append(tag)
            w = (" WHERE " + " AND ".join(where)) if where else ""
            rows = c.execute(
                f"SELECT * FROM system_logs{w} ORDER BY ts DESC LIMIT ? OFFSET ?",
                args + [limit, offset],
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            c.close()


def count_system_logs(tag: str | None = None) -> int:
    with _lock:
        c = _conn()
        try:
            where, args = [], []
            if tag:
                where.append("tag=?"); args.append(tag)
            w = (" WHERE " + " AND ".join(where)) if where else ""
            return c.execute(f"SELECT COUNT(*) n FROM system_logs{w}", args).fetchone()["n"]
        finally:
            c.close()


def list_system_tags() -> list:
    """去重系统日志标签(日志中心筛选下拉用)"""
    with _lock:
        c = _conn()
        try:
            rows = c.execute("SELECT tag, COUNT(*) n FROM system_logs GROUP BY tag ORDER BY n DESC").fetchall()
            return [{"tag": r["tag"], "count": r["n"]} for r in rows]
        finally:
            c.close()


def delete_system_logs(older_than: float | None = None, tag: str | None = None) -> int:
    """删除系统日志: 传 older_than 删除该时间之前; 传 tag 删除该标签; 都不传清空。"""
    with _lock:
        c = _conn(write=True)
        try:
            where, args = [], []
            if older_than is not None:
                where.append("ts < ?"); args.append(older_than)
            if tag:
                where.append("tag=?"); args.append(tag)
            w = (" WHERE " + " AND ".join(where)) if where else ""
            cur = c.execute(f"DELETE FROM system_logs{w}", args)
            c.commit()
            return cur.rowcount
        finally:
            c.close()


def prune_system_logs(days: int = SYSTEM_RETENTION_DAYS) -> int:
    return delete_system_logs(older_than=time.time() - days * 86400)


def prune_all():
    """保留期总裁剪(任务 6 小时 + 系统日志 7 天)。可定时/手动调用。"""
    return {"tasks": prune_tasks(days=TASK_RETENTION_HOURS / 24), "system_logs": prune_system_logs()}
