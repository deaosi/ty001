# -*- coding: utf-8 -*-
"""
探域数据看板 - 账号系统认证核心(标准库实现, 无第三方依赖)

  - 密码哈希: hashlib.pbkdf2_hmac('sha256') + 随机 salt, 明文绝不落盘
  - 登录 token: base64("{user_id}:{exp_ts}") + hmac_sha256(SECRET) 签名,
    无状态(重启不丢登录态), 有效期 7 天
  - 配置: config.json 的 auth 字段(secret_key / service_token / register_enabled /
    admin_username / admin_password)。secret_key 与 service_token 首次生成;
    admin_password 用于首次创建管理员, 创建成功后立即清除明文。
  - 鉴权依赖: get_current_user(登录) / require_admin(管理员), 供 main.py 路由用
  - 内部服务 token: verify_service_token 供定时任务(dingtalk_daily_push)放行
"""
import base64
import datetime
import hashlib
import hmac
import json
import os
import re
import secrets
import threading
import time
from pathlib import Path

from fastapi import Depends, HTTPException, Request

import trace_store

BASE_DIR = Path(__file__).resolve().parent
CONFIG_FILE = BASE_DIR / "config.json"

# 登录 token 有效期(秒): 7 天
TOKEN_TTL = 7 * 24 * 3600
# pbkdf2 迭代次数(时间成本可接受; 旧 hash 校验按存储里的迭代次数, 便于未来提升)
PBKDF2_ITERATIONS = 200_000
# 用户名规范: 3~64 位字母/数字/下划线/点/@/连字符(支持邮箱格式, 如 admin@tydiamond.local)
USERNAME_RE = re.compile(r"^[A-Za-z0-9_.@-]{3,64}$")


# ---------- config.json auth 字段读写 ----------
def _read_config() -> dict:
    try:
        return json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _write_config(cfg: dict):
    """原子写 config.json(临时文件 + os.replace), 与 main.save_config 同模式"""
    tmp = CONFIG_FILE.with_name("config.json.tmp")
    tmp.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, CONFIG_FILE)


def _ensure_auth_fields(cfg: dict) -> tuple:
    """补齐 auth 字段; 返回 (cfg, changed)"""
    auth = cfg.setdefault("auth", {})
    changed = False
    if not auth.get("secret_key"):
        auth["secret_key"] = secrets.token_hex(32)
        changed = True
    if not auth.get("service_token"):
        auth["service_token"] = secrets.token_hex(32)
        changed = True
    auth.setdefault("register_enabled", True)
    return cfg, changed


def get_secret() -> str:
    cfg = _read_config()
    cfg, changed = _ensure_auth_fields(cfg)
    if changed:
        _write_config(cfg)
    return cfg["auth"]["secret_key"]


def get_service_token() -> str:
    cfg = _read_config()
    cfg, changed = _ensure_auth_fields(cfg)
    if changed:
        _write_config(cfg)
    return cfg["auth"]["service_token"]


def get_register_enabled() -> bool:
    cfg = _read_config()
    return bool((cfg.get("auth") or {}).get("register_enabled", True))


def set_register_enabled(enabled: bool):
    cfg = _read_config()
    cfg.setdefault("auth", {})["register_enabled"] = bool(enabled)
    _write_config(cfg)


# ---------- 初始管理员 ----------
def ensure_admin():
    """启动时调用: 按 config.auth.admin_username/admin_password 首次创建管理员。

    创建成功后立即从 config 删除 admin_password 明文(避免长期落盘)。
    未配置 admin_username / 同名用户已存在 / 缺密码 → 跳过(幂等)。
    """
    try:
        cfg = _read_config()
        auth = cfg.get("auth") or {}
        name = (auth.get("admin_username") or "").strip()
        pw = auth.get("admin_password") or ""
        if not name:
            return None
        if trace_store.get_user_by_username(name):
            # 已存在(上次创建后未清明文, 或用户手动改名): 确保明文清除
            if "admin_password" in auth:
                auth.pop("admin_password", None)
                _write_config(cfg)
            return None
        if not pw:
            print("[auth] config 指定了 admin_username 但缺少 admin_password, 跳过创建")
            return None
        user = trace_store.create_user(name, hash_password(pw), role="admin",
                                       note="初始管理员(config.json 指定)")
        auth.pop("admin_password", None)
        _write_config(cfg)
        print(f"[auth] 初始管理员「{name}」已创建, config 中的明文密码已清除")
        return user
    except Exception as e:
        print(f"[auth] 初始管理员创建失败: {e}")
        return None


# ---------- 密码哈希 ----------
def hash_password(pw: str) -> str:
    salt = secrets.token_hex(8)
    dk = hashlib.pbkdf2_hmac("sha256", pw.encode("utf-8"), salt.encode("utf-8"),
                             PBKDF2_ITERATIONS)
    return f"pbkdf2_sha256${PBKDF2_ITERATIONS}${salt}${dk.hex()}"


def verify_password(pw: str, stored: str) -> bool:
    """按存储里的迭代次数校验; 任何解析失败都返回 False(不泄露格式)"""
    try:
        _, iters_s, salt, h = stored.split("$")
        dk = hashlib.pbkdf2_hmac("sha256", pw.encode("utf-8"), salt.encode("utf-8"),
                                 int(iters_s))
        return hmac.compare_digest(dk.hex(), h)
    except Exception:
        return False


# ---------- token 签发 / 解析(hmac 签名, 无状态) ----------
def make_token(user_id: int, ttl: int = TOKEN_TTL) -> str:
    exp = int(time.time()) + ttl
    payload = f"{user_id}:{exp}"
    sig = hmac.new(get_secret().encode("utf-8"), payload.encode("utf-8"),
                   hashlib.sha256).hexdigest()
    b64 = base64.urlsafe_b64encode(payload.encode("utf-8")).decode().rstrip("=")
    return f"{b64}.{sig}"


def parse_token(token: str):
    """校验签名+过期; 有效返回 user_id, 否则 None"""
    try:
        b64, sig = token.split(".", 1)
        b64 += "=" * (-len(b64) % 4)  # 补 base64 padding
        payload = base64.urlsafe_b64decode(b64.encode("utf-8")).decode("utf-8")
        if not hmac.compare_digest(
                hmac.new(get_secret().encode("utf-8"), payload.encode("utf-8"),
                         hashlib.sha256).hexdigest(), sig):
            return None
        uid_s, exp_s = payload.split(":", 1)
        if int(exp_s) < time.time():
            return None
        return int(uid_s)
    except Exception:
        return None


# ---------- 表单校验 ----------
def valid_username(name: str) -> bool:
    return bool(USERNAME_RE.match(name or ""))


def valid_password(pw: str) -> bool:
    return isinstance(pw, str) and len(pw) >= 6


# ---------- 注册频率限制(同一来源 IP, 滑动窗口计数) ----------
_REG_DEFAULT_LIMIT = {"window_seconds": 300, "max_per_ip": 3}
_reg_ts: dict = {}
_reg_lock = threading.Lock()


def _register_limit() -> dict:
    """读 config.auth.register_rate_limit(每次实时读取, 改 config 即生效, 无需重启);
    缺省窗口 5 分钟、每 IP 最多 3 次。"""
    cfg = _read_config()
    lim = (cfg.get("auth") or {}).get("register_rate_limit") or {}
    return {
        "window_seconds": int(lim.get("window_seconds") or _REG_DEFAULT_LIMIT["window_seconds"]),
        "max_per_ip": int(lim.get("max_per_ip") or _REG_DEFAULT_LIMIT["max_per_ip"]),
    }


def check_register_allowed(ip: str, client_id: str = "", ua: str = "") -> bool:
    """注册限流: 同一「来源IP + 浏览器指纹」在窗口内注册数达到上限则返回 False(调用方应返回 429)。

    指纹优先用前端持久生成的 client_id(每浏览器唯一), 无则退化为 UA, 再退化为空——
    同一 NAT 出口下不同设备(client_id 不同)各自独立计数, 互不误伤; 只有同一浏览器
    反复注册才受限。按请求计数(含失败尝试), 滑动窗口滚动清理; 仅允许通过的请求才
    计入计数(超限请求不追加)。内存实现, 服务重启后清零。
    """
    now = time.time()
    lim = _register_limit()
    win, mx = lim["window_seconds"], lim["max_per_ip"]
    finger = (client_id or "").strip() or (ua or "").strip()
    key = (ip or "unknown", finger) if finger else (ip or "unknown",)
    with _reg_lock:
        ts = _reg_ts.setdefault(key, [])
        ts[:] = [t for t in ts if now - t < win]
        if len(ts) >= mx:
            return False
        ts.append(now)
        return True


# ---------- 内部服务 token(定时任务) ----------
def verify_service_token(header_value) -> bool:
    """X-Service-Token 头是否匹配 config.auth.service_token(定时任务放行用)"""
    if not header_value:
        return False
    try:
        return hmac.compare_digest(header_value.strip(), get_service_token())
    except Exception:
        return False


# ---------- FastAPI 鉴权依赖 ----------
def _bearer_token(request: Request) -> str | None:
    auth = request.headers.get("authorization", "")
    if auth.lower().startswith("bearer "):
        return auth[7:].strip()
    return None


def resolve_user_id(request: Request):
    """中间件鉴权用: 校验登录态, 有效返回 user_id(供日志关联), 无效返回 None。

    与 get_current_user 同口径(签名/过期/封禁/到期), 但不抛异常——
    调用方(全局中间件)自行决定放行或返回 401。
    """
    token = _bearer_token(request)
    uid = parse_token(token) if token else None
    if not uid:
        return None
    user = trace_store.get_user_by_id(uid)
    if not user or user["status"] != "active":
        return None
    if user.get("expire_date"):
        try:
            if datetime.date.fromisoformat(user["expire_date"]) < datetime.date.today():
                return None
        except ValueError:
            pass
    return uid


def get_current_user(request: Request) -> dict:
    """登录态校验: 有效 token + 用户存在且未封禁 + 未到期。失败抛 401"""
    token = _bearer_token(request)
    uid = parse_token(token) if token else None
    if not uid:
        raise HTTPException(status_code=401, detail="未登录或登录已过期, 请重新登录")
    user = trace_store.get_user_by_id(uid)
    if not user or user["status"] != "active":
        raise HTTPException(status_code=401, detail="账号不存在或已被封禁")
    if user.get("expire_date"):
        try:
            if datetime.date.fromisoformat(user["expire_date"]) < datetime.date.today():
                raise HTTPException(status_code=401, detail="账号已到期, 请联系管理员续期")
        except ValueError:
            pass
    return user


def require_admin(user: dict = Depends(get_current_user)) -> dict:
    """管理员校验; 非管理员抛 403"""
    if user["role"] != "admin":
        raise HTTPException(status_code=403, detail="需要管理员权限")
    return user
