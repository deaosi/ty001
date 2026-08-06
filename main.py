# -*- coding: utf-8 -*-
"""
探域数据看板 - 本地服务后端

提供:
  GET  /api/shops            - 店铺列表(来自本地 shops.json)
  GET  /api/overview         - 平台维度汇总指标
  GET  /api/shop/{id}        - 单店汇总指标 + 按日明细
  GET  /api/refresh          - 强制刷新全部店铺数据(异步)
  GET  /api/tasks            - 刷新任务状态
  POST /api/cookies          - 更新 Cookie 配置

数据源: https://agent.tanyuai.com/api/data-service/business/compass/*
"""
import asyncio
import datetime
import json
import os
import threading
import time
from pathlib import Path

import requests
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
CONFIG_FILE = BASE_DIR / "config.json"
SHOPS_FILE = BASE_DIR / "shops.json"

API_BASE = "https://agent.tanyuai.com/api/data-service/business/compass"

# 平台枚举(与探域接口一致): 0=淘宝 1=拼多多 2=京东 4=快手 5=抖音 7=天猫
PLATFORM_NAMES = {0: "淘宝", 1: "拼多多", 2: "京东", 4: "快手", 5: "抖音", 7: "天猫"}

# 抓取配置: 拼多多(1)/抖音(5)/京东(2)抓取; 淘宝(0)/天猫(7)保留接口但不抓取
FETCH_PLATFORMS = [1, 2, 5]
KEEP_PLATFORMS = [0, 7]  # 保留接口, 不抓取不统计

# 汇总指标定义(标题 / 格式化 / 排序权重)
METRICS = [
    {"key": "service_3m_response_rate", "title": "3分钟回复率", "format": "percent"},
    {"key": "ai_consult_response_accept_rate", "title": "采纳率", "format": "percent"},
    {"key": "ai_consult_response_rate", "title": "生成率", "format": "percent"},
]
METRIC_BY_KEY = {m["key"]: m for m in METRICS}

# 刷新状态
_refresh_state = {
    "running": False,
    "progress": {"done": 0, "total": 0, "current": ""},
    "last_run": None,
    "error": None,
}
_lock = threading.Lock()

app = FastAPI(title="探域数据看板")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------- 配置 ----------
def default_config():
    return {
        "cookies": {
            "tanyu-account-id": "2656113728446465571",
            "tanyu-agent-account": "fM_VS4GirTjMlPPJx_llv5kWStXKTMrRvW__",
            "tanyu-group-account": "le_FjY2F9WFwuBxC2_ISmD0LfcZkkuHaG9__",
            "tanyu-group-id": "1901419852011174006",
        },
        "groups": [],  # 已发现的集团列表(来自微信扫码登录 localStorage.groupList)
    }


def load_config():
    if CONFIG_FILE.exists():
        try:
            return json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
        except Exception as e:
            # 文件存在但解析失败: 绝不静默覆盖真实配置, 先备份到 .bak 再重建默认
            print(f"[config] config.json 解析失败({e}), 已备份为 config.json.bak")
            try:
                bak = CONFIG_FILE.with_name("config.json.bak")
                os.replace(CONFIG_FILE, bak)
            except Exception:
                pass
    cfg = default_config()
    save_config(cfg)
    return cfg


def save_config(cfg):
    # 原子写入: 先写临时文件再 os.replace, 读取方不会读到半截 JSON
    _atomic_write_text(CONFIG_FILE, json.dumps(cfg, ensure_ascii=False, indent=2))


# ---------- 数据访问 ----------
def get_headers():
    cfg = load_config()
    cookies = cfg.get("cookies", {})
    cookie_str = "; ".join(f"{k}={v}" for k, v in cookies.items())
    return {
        "Cookie": cookie_str,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36",
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://agent.tanyuai.com/",
        "Origin": "https://agent.tanyuai.com",
    }


def post_api(path, payload, timeout=20):
    """POST 请求探域接口, 返回解析后的 JSON; 带限速+风控检测"""
    _assert_no_risk()
    _rate_limit()
    resp = requests.post(API_BASE + path, json=payload, headers=get_headers(), timeout=timeout)
    data = resp.json()
    if _check_risk(resp, data):
        raise RuntimeError(f"风控/登录失效: {data.get('msg', resp.status_code)}")
    if data.get("code") != 0:
        raise RuntimeError(data.get("msg", "接口错误"))
    return data.get("data")


# ---------- 交互式请求快通道 ----------
# 看板页面加载/切换时间段的请求与后台抓取任务共用同一套全局限速。
# 若限速器队列已满(后台任务在跑), 这些交互请求会排队 30~60s, 前端体验极差。
# 方案: 交互请求不排队等待 —— 队列繁忙时立即返回 503(不发任何请求),
# 由前端展示"稍后重试"并在后台任务结束后自动补拉。
INTERACTIVE_CACHE_TTL = 10          # 快通道缓存秒数(交互场景重复查询零上游请求)
_interactive_cache = {}             # { (path, json_str): (ts, data) }


def _interactive_cache_get(path, payload):
    key = (path, json.dumps(payload, ensure_ascii=False, sort_keys=True))
    hit = _interactive_cache.get(key)
    if hit and time.time() - hit[0] < INTERACTIVE_CACHE_TTL:
        return hit[1]
    return None


def _interactive_cache_put(path, payload, data):
    global _interactive_cache
    key = (path, json.dumps(payload, ensure_ascii=False, sort_keys=True))
    _interactive_cache[key] = (time.time(), data)
    if len(_interactive_cache) > 64:  # 防内存无限增长
        cutoff = time.time() - INTERACTIVE_CACHE_TTL
        _interactive_cache = {k: v for k, v in _interactive_cache.items() if v[0] > cutoff}


def post_api_interactive(path, payload, timeout=20):
    """交互式请求: 不排队, 限速繁忙时立即返回 503; 10s 内同参命中内存缓存零请求"""
    cached = _interactive_cache_get(path, payload)
    if cached is not None:
        return cached
    _assert_no_risk()
    if not _rate_limit_available():
        raise BusyQueueError("后台抓取任务进行中, 请稍后重试")
    resp = requests.post(API_BASE + path, json=payload, headers=get_headers(), timeout=timeout)
    data = resp.json()
    if _check_risk(resp, data):
        raise RuntimeError(f"风控/登录失效: {data.get('msg', resp.status_code)}")
    if data.get("code") != 0:
        raise RuntimeError(data.get("msg", "接口错误"))
    _interactive_cache_put(path, payload, data)
    return data.get("data")


class BusyQueueError(RuntimeError):
    """限速队列繁忙(后台任务占用), 交互请求不排队直接返回"""


def load_shops(platform=None):
    """加载本地店铺列表, 合并平台名; 可按平台过滤"""
    if not SHOPS_FILE.exists():
        return []
    shops = json.loads(SHOPS_FILE.read_text(encoding="utf-8")).get("data", [])
    for s in shops:
        s["platformName"] = PLATFORM_NAMES.get(s.get("platform"), str(s.get("platform")))
    if platform is not None:
        shops = [s for s in shops if s.get("platform") == platform]
    return shops


def date_range(days=7, end=None):
    """生成近 N 天日期区间(截至昨天)"""
    import datetime

    end = end or (datetime.date.today() - datetime.timedelta(days=1))
    start = end - datetime.timedelta(days=days - 1)
    return start.isoformat(), end.isoformat()


def fetch_summary(payload):
    """拉取汇总指标, 返回 {key: {current, previous, comparePercent}}"""
    data = post_api("/summary", payload)
    items = data.get("items", []) if data else []
    return {it["key"]: it for it in items}


def fetch_summary_interactive(payload):
    """交互式汇总: 走快通道(不排队), 10s 内同参命中缓存零上游请求"""
    data = post_api_interactive("/summary", payload)
    items = data.get("items", []) if data else []
    return {it["key"]: it for it in items}


def fetch_section_table(payload):
    """拉取 section 明细表, 返回 {dates: [...], rows: [...]}"""
    data = post_api("/section/table", payload)
    if not data:
        return {"dates": [], "rows": []}
    return {"dates": data.get("dates", []), "rows": data.get("rows", [])}


def fetch_section_table_interactive(payload):
    """交互式 section 明细: 走快通道(不排队), 10s 内同参命中缓存零上游请求"""
    data = post_api_interactive("/section/table", payload)
    if not data:
        return {"dates": [], "rows": []}
    return {"dates": data.get("dates", []), "rows": data.get("rows", [])}


def cache_path(kind, target_id):
    """按类型+ID 定位缓存文件: data/{kind}/{id}.json"""
    d = DATA_DIR / kind
    d.mkdir(parents=True, exist_ok=True)
    return d / f"{target_id}.json"


TRACE_DAYS_CACHE_TTL = 60          # 内存里已解析的 trace_days 缓存秒数
_trace_days_cache = {}             # {shop_id: (ts, parsed_cache)}, 跨请求共享避免重复读盘+json 解析

# 按日缓存有效期(探域数据每日更新, 已抓取的天不能永久复用)
#   今天/昨天: 6 小时 TTL(当天数据随时可能更新, 短期窗口内也是新口径)
#   更早的天:  7 天 TTL(历史天数据已定型, 隔周刷新一次即可)
DAY_TTL = 6 * 3600          # 最近两天(含昨天)的有效期
HISTORY_DAY_TTL = 7 * 24 * 3600   # 更早历史天的有效期


def _day_cache_ttl(day_str):
    """按天返回缓存有效期秒数: 今天/昨天用短 TTL, 更早的历史用长 TTL"""
    try:
        d = datetime.date.fromisoformat(day_str)
        today = datetime.date.today()
        days_ago = (today - d).days
    except Exception:
        days_ago = 999
    return DAY_TTL if days_ago <= 1 else HISTORY_DAY_TTL


def _load_trace_days_cache(shop_id, force=False):
    """读取店铺按日缓存(带进程内共享缓存), force=True 时跳过内存缓存直接读盘"""
    _prune_trace_days_cache()
    if not force:
        hit = _trace_days_cache.get(shop_id)
        if hit and time.time() - hit[0] < TRACE_DAYS_CACHE_TTL:
            return hit[1]
    data = load_cache("trace_days", shop_id, max_age=HISTORY_DAY_TTL)
    _trace_days_cache[shop_id] = (time.time(), data)
    return data


def _cached_days_usable(cache, day_str):
    """判断某一天是否可直接复用: 在该天自己的有效期内则命中, 过期则视为缺失

    TTL 按天龄分级; 存量文件未按日记录抓取时间时, 回退到文件级 fetched_at。
    空天(0 条消息)只要在有效期内同样复用, 避免每次都重复抓取。
    """
    if not cache or not cache.get("days"):
        return False
    if day_str not in cache["days"]:
        return False
    fetched = (cache.get("day_fetched_at") or {}).get(day_str)
    if fetched is None:
        fetched = cache.get("fetched_at")
    if not fetched:
        return False
    return (time.time() - fetched) < _day_cache_ttl(day_str)


def _prune_trace_days_cache():
    """清理进程内共享缓存中超过 TTL 的条目(防无限增长)"""
    now = time.time()
    for shop_id, (ts, _) in list(_trace_days_cache.items()):
        if now - ts > TRACE_DAYS_CACHE_TTL:
            _trace_days_cache.pop(shop_id, None)


def load_cache(kind, target_id, max_age=21600):
    # 默认 TTL 6 小时: 平台/店铺汇总与明细是"自然日"数据, 一天内基本不变。
    # 之前的 30 分钟 TTL 导致切换平台时几乎必然全部缓存过期、重新抓取, 是卡顿主因。
    p = cache_path(kind, target_id)
    if not p.exists():
        return None
    if time.time() - p.stat().st_mtime > max_age:
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def save_cache(kind, target_id, data):
    p = cache_path(kind, target_id)
    _atomic_write_text(p, json.dumps(data, ensure_ascii=False, separators=(",", ":")))


def _atomic_write_text(path: Path, text: str):
    """原子写入: 先写临时文件再 os.replace, 崩溃/并发时不会留下半截文件"""
    tmp = path.with_name(path.name + ".tmp")
    try:
        tmp.write_text(text, encoding="utf-8")
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            try:
                tmp.unlink()
            except OSError:
                pass


# ---------- 批量刷新 ----------
def refresh_one(shop, start, end):
    """刷新单个店铺: summary + 3 个 section 明细, 返回汇总"""
    shop_id = shop["thirdShopId"]
    platform = shop.get("platform", 1)
    base = {
        "statType": "natural_day",
        "startDate": start,
        "endDate": end,
        "platform": platform,
        "dimension": "shop",
        "targetId": shop_id,
    }
    summary = fetch_summary(base)
    save_cache("summary", shop_id, {"fetched_at": time.time(), "data": summary})

    for section in ["operations", "service", "ai"]:
        payload = {**base, "section": section}
        try:
            table = fetch_section_table(payload)
            save_cache("table", f"{shop_id}__{section}", {"fetched_at": time.time(), "data": table})
        except Exception as e:
            print(f"[refresh] {shop['shopName']} section={section} 失败: {e}")
    return shop_id


def refresh_all_async(start, end):
    """后台线程: 遍历抓取平台(拼多多/抖音)的全部店铺刷新"""

    def worker():
        with _lock:
            if _refresh_state["running"]:
                return
            _refresh_state["running"] = True
            _refresh_state["error"] = None
        try:
            # 只抓取启用的平台
            shops = [s for s in load_shops() if s.get("platform") in FETCH_PLATFORMS]
            total = len(shops)
            _refresh_state["progress"] = {"done": 0, "total": total, "current": ""}
            for i, shop in enumerate(shops, 1):
                _refresh_state["progress"]["current"] = f"{shop['platformName']} · {shop['shopName']} ({i}/{total})"
                if i > 1:
                    sleep_trace_shop()
                try:
                    refresh_one(shop, start, end)
                except RiskTriggered as e:
                    # 风控/登录失效: 立即停止
                    print(f"[refresh] ⛔ 风控触发, 停止剩余 {total - i} 家店铺: {e}")
                    _refresh_state["error"] = f"风控/登录失效, 已停止: {e}"
                    break
                except Exception as e:
                    print(f"[refresh] {shop['shopName']} 失败: {e}")
                _refresh_state["progress"]["done"] = i
            _refresh_state["last_run"] = time.time()
        except Exception as e:
            _refresh_state["error"] = str(e)
        finally:
            _refresh_state["running"] = False

    threading.Thread(target=worker, daemon=True).start()


# ---------- 集团管理 ----------
GC_API = "https://agent.tanyuai.com/api/gc"

# accountType -> 平台/类型名(集团标签)
ACCOUNT_TYPE_NAMES = {3: "客服智能体"}


def get_current_group():
    """获取当前激活集团(来自 detail 接口)"""
    try:
        _assert_no_risk()
        _rate_limit()
        r = requests.get(f"{GC_API}/agent-personal/detail", headers=get_headers(), timeout=15)
        d = r.json()
        if _check_risk(r, d):
            return None
        if d.get("success") is False:
            return None
        g = (d.get("data") or {}).get("group") or {}
        return {"id": g.get("id"), "name": g.get("name"), "chatbotGroupId": g.get("chatbotGroupId")}
    except Exception:
        return None


def get_group_list():
    """已发现的集团列表(config.groups) + 当前集团"""
    cfg = load_config()
    groups = cfg.get("groups") or []
    cur = get_current_group()
    if cur and cur["id"]:
        # 当前集团不在列表时自动补上
        if not any(g.get("groupId") == cur["id"] for g in groups):
            groups = [{"groupId": cur["id"], "groupName": cur["name"],
                       "accountId": None, "accountType": None, "current": True}] + groups
            cfg["groups"] = groups
            save_config(cfg)
        for g in groups:
            g["current"] = g.get("groupId") == cur["id"]
    return {"groups": groups, "current": cur}


def switch_group(group_id):
    """切换到指定集团: 调 switch-group-by-wx 捕获 Set-Cookie 更新 config"""
    cfg = load_config()
    groups = cfg.get("groups") or []
    g = next((x for x in groups if x.get("groupId") == group_id), None)
    if not g:
        raise ValueError(f"集团不存在: {group_id}")
    if not g.get("accountId"):
        raise ValueError(f"集团「{g.get('groupName')}」缺少 accountId, 无法切换(请通过扫码登录重新发现集团)")
    _assert_no_risk()
    _rate_limit()
    resp = requests.post(
        f"{GC_API}/agent/auth/switch-group-by-wx",
        json={"accountId": g["accountId"], "accountType": g.get("accountType", 3)},
        headers=get_headers(), timeout=15,
    )
    d = resp.json()
    if _check_risk(resp, d):
        raise RuntimeError(f"风控/登录失效: {d.get('msg', resp.status_code)}")
    if d.get("success") is False:
        raise RuntimeError(d.get("msg", "切换集团失败"))
    # 捕获 Set-Cookie 并保存
    for name, value in resp.cookies.items():
        if name.startswith("tanyu-"):
            cfg["cookies"][name] = value
    save_config(cfg)
    # 同步店铺列表
    sync_shops_from_tanyu()
    return {"ok": True, "group": get_current_group(), "updated": True}


def _invalidate_audit_caches():
    """店铺列表变化后清空核算相关缓存(总览磁盘缓存 + 内存态), 防止跨集团串数据"""
    global _trace_state
    try:
        if TRACE_OVERVIEW_CACHE_FILE.exists():
            TRACE_OVERVIEW_CACHE_FILE.unlink()
    except Exception:
        pass
    with _lock:
        _trace_state["result"] = None
        _trace_state["start_date"] = None
        _trace_state["end_date"] = None
        _trace_state["platform"] = None
        _trace_state["last_run"] = None
        _trace_state["error"] = None
    # 进程内共享的 trace_days 解析缓存按 shop_id 存, 新集团店铺 ID 不冲突, 无需清空;
    # 但旧集团文件留在磁盘无害(不会被新店铺 ID 读到)。如需回收可删 trace_days 目录。


def sync_shops_from_tanyu():
    """从探域 brief 接口抓取当前集团的全部店铺, 写入 shops.json"""
    try:
        _assert_no_risk()
        _rate_limit()
        r = requests.get(f"{GC_API}/agent-personal/brief?searchValid=true", headers=get_headers(), timeout=20)
        d = r.json()
        if _check_risk(r, d):
            raise RuntimeError(f"风控/登录失效: {d.get('msg', r.status_code)}")
        if d.get("success") is False:
            raise RuntimeError(d.get("msg", "店铺列表获取失败"))
        data = d.get("data") or []
        if not isinstance(data, list):
            data = data.get("chatbotShops") or []
        shops = []
        for s in data:
            if not s.get("thirdShopId"):
                continue
            shops.append({
                "thirdShopId": s["thirdShopId"],
                "shopName": s.get("shopName") or "未知",
                "platform": s.get("platform", 0),
                "sellerId": s.get("sellerId", ""),
                "ifAgentReceipt": s.get("ifAgentReceipt", False),
            })
        if not shops:
            raise RuntimeError("接口返回空店铺列表")
        for s in shops:
            s["platformName"] = PLATFORM_NAMES.get(s.get("platform"), str(s.get("platform")))
        SHOPS_FILE.write_text(json.dumps({"data": shops}, ensure_ascii=False, indent=2), encoding="utf-8")
        # 店铺集合已变: 清掉核算缓存, 避免旧集团结果串到新集团
        _invalidate_audit_caches()
        return {"count": len(shops), "platforms": {p: sum(1 for s in shops if s["platform"] == p) for p in set(s["platform"] for s in shops)}}
    except Exception as e:
        raise RuntimeError(f"同步店铺失败: {e}")


# ---------- 风控保护 ----------
import random

# 请求节奏(秒): 用随机区间而非固定值, 接近人工操作习惯
RISK_PAGE_INTERVAL = (0.4, 1.2)    # 消息分页之间随机等待
RISK_SHOP_INTERVAL = (0.8, 2.5)    # 店铺之间随机等待
RISK_MAX_RPM = 30                  # 全局限速: 每分钟最多请求数
RISK_CACHE_HOURS = 24              # 核算缓存有效期(同时间段重复核算零请求)
RISK_KEYWORDS = ["登录", "过期", "验证", "风控", "频繁", "操作过频",
                 "forbidden", "unauthorized", "rate limit", "restrict"]

# 风控全局状态: 一旦触发, 所有抓取任务立即停止, 需重新登录后手动清除
_risk_state = {"triggered": False, "reason": "", "at": None, "last_code": None}
_request_times = []  # 滑动窗口请求时间戳


class RiskTriggered(Exception):
    """风控/登录失效信号, 触发后任务立即停止"""


def _set_risk(reason, code=None):
    with _lock:
        if not _risk_state["triggered"]:
            _risk_state["triggered"] = True
            _risk_state["reason"] = reason
            _risk_state["at"] = time.time()
            _risk_state["last_code"] = code
    print(f"[risk] ⚠️ 检测到风控/登录失效信号: {reason} — 已停止所有抓取任务, 请重新登录")


def _assert_no_risk():
    if _risk_state["triggered"]:
        raise RiskTriggered(_risk_state["reason"])


def _reset_risk():
    with _lock:
        _risk_state["triggered"] = False
        _risk_state["reason"] = ""
        _risk_state["at"] = None
        _risk_state["last_code"] = None
    _request_times.clear()


def _sleep_random(lo, hi):
    """随机间隔, 打散请求节奏"""
    time.sleep(random.uniform(lo, hi))


def _rate_limit():
    """滑动窗口限速: 超限则排队等待, 避免短时间大量请求"""
    global _request_times
    now = time.time()
    _request_times = [t for t in _request_times if now - t < 60]
    if len(_request_times) >= RISK_MAX_RPM:
        wait = 60 - (now - _request_times[0]) + random.uniform(0.3, 1.2)
        print(f"[rate] 已达限速(>={RISK_MAX_RPM}次/分), 等待 {wait:.1f}s")
        time.sleep(wait)
    _request_times.append(time.time())


def _rate_limit_available():
    """限速槽位是否可用(不占用): 供交互式请求判断是否需要立即返回"""
    now = time.time()
    return len([t for t in _request_times if now - t < 60]) < RISK_MAX_RPM


def _check_risk(resp, data=None):
    """检测响应中的风控/登录失效信号, 命中则全局止损"""
    if resp.status_code in (401, 403, 429):
        _set_risk(f"HTTP {resp.status_code}", resp.status_code)
        return True
    if not data:
        return False
    code, msg = data.get("code"), str(data.get("msg") or "")
    # 授权/会话失效类错误码
    if code in (3014, 4001, 4010, 5001):
        _set_risk(f"code={code} {msg[:60]}", code)
        return True
    for kw in RISK_KEYWORDS:
        if kw.lower() in msg.lower():
            _set_risk(f"{msg[:60]}", code)
            return True
    return False


def _assert_no_running_task():
    """任务互斥: 核算与数据刷新不能同时跑"""
    if _refresh_state.get("running"):
        raise RuntimeError("数据刷新任务进行中, 请稍后再试")
    if _trace_state.get("running"):
        raise RuntimeError("核算任务进行中, 请稍后再试")


# ---------- 核算(消息轨迹) ----------
TRACE_API = "https://agent.tanyuai.com/api/im/agent-trace/paginateV2"

# 发送状态: 1=自动发送 2=侧边栏点击发送 3=编辑后发送 (None=未发送)
SEND_TYPES = {1: "自动发送", 2: "侧边栏点击发送", 3: "编辑后发送"}
ADOPTED_SEND_TYPES = (1, 2, 3)

# 请求间隔(秒): 随机区间抖动, 打散节奏接近人工, 降低触发平台风控的概率
TRACE_PAGE_INTERVAL = (0.2, 0.6)   # 分页之间(大分页后页数极少, 间隔可收紧)
TRACE_SHOP_INTERVAL = (0.8, 2.5)   # 店铺之间
TRACE_PAGE_SIZE = 2000             # 单页消息数: 实测 paginateV2 支持到 5000+,
                                   # 大分页把页数从 N/100 降到 N/2000(20倍), 核算快一个数量级
TRACE_QUERY_CAP = 10000            # 接口单次核算区间最多返回 10000 条(实测硬上限),
                                   # 超出部分翻页也拿不到(results 为 None)


def sleep_trace_page():
    """分页间随机等待"""
    _sleep_random(*TRACE_PAGE_INTERVAL)


def sleep_trace_shop():
    """店铺间随机等待"""
    _sleep_random(*TRACE_SHOP_INTERVAL)


def fetch_trace_page(shop_id, begin, end, page_index, page_size=TRACE_PAGE_SIZE):
    """拉取一页消息轨迹; 带限速+风控检测. 返回 (total, results)

    total 为区间消息总数(用于判断是否超 1 万上限), results 为当前页列表
    """
    payload = {
        "thirdShopId": shop_id,
        "pageIndex": page_index,
        "pageSize": page_size,
        "beginTime": begin,
        "endTime": end,
    }
    _assert_no_risk()
    _rate_limit()
    resp = requests.post(TRACE_API, json=payload, headers=get_headers(), timeout=20)
    data = resp.json()
    if _check_risk(resp, data):
        raise RuntimeError(f"风控/登录失效: {data.get('msg', resp.status_code)}")
    if data.get("success") is False:
        raise RuntimeError(data.get("msg", "接口错误"))
    d = data.get("data") or {}
    return d.get("total") or 0, d.get("results") or []


def stat_trace(shop_id, begin, end, on_progress=None):
    """遍历指定店铺时间段内的全部消息轨迹, 统计发送状态分布

    返回: {total, counts, adopted, rate, byStaff, staffList, byType}
      total    - 消息总条数
      counts   - sendType 分布 {1: n, 2: n, 3: n, None: n}
      adopted  - 采纳条数(自动发送+侧边栏点击发送+编辑后发送)
      rate     - 核算采纳率
      byStaff  - 按人工客服(sellerAccount)维度统计
      staffList- 出现过的客服账号列表
    """
    results = _fetch_trace_range(shop_id, begin, end)
    total = len(results)

    counts = {1: 0, 2: 0, 3: 0, None: 0}
    by_staff = {}  # sellerAccount -> {"total": n, "adopted": n}
    by_type = {}
    daily = {}  # 按天聚合: date -> {"total": n, "adopted": n}
    for r in results:
        st = r.get("sendType")
        counts[st] = counts.get(st, 0) + 1
        staff = r.get("sellerAccount") or "未知"
        entry = by_staff.setdefault(staff, {"total": 0, "adopted": 0})
        entry["total"] += 1
        if st in ADOPTED_SEND_TYPES:
            entry["adopted"] += 1
        t = r.get("type") or "OTHER"
        by_type[t] = by_type.get(t, 0) + 1
        # 按天聚合(time 为毫秒时间戳)
        ts = r.get("time")
        if isinstance(ts, (int, float)):
            day = datetime.datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d")
            de = daily.setdefault(day, {"total": 0, "adopted": 0})
            de["total"] += 1
            if st in ADOPTED_SEND_TYPES:
                de["adopted"] += 1

    adopted = sum(counts.get(s, 0) for s in ADOPTED_SEND_TYPES)
    rate = (adopted / total * 100) if total else 0
    daily_list = [
        {"date": d, "total": v["total"], "adopted": v["adopted"],
         "rate": round(v["adopted"] / v["total"] * 100, 2) if v["total"] else 0}
        for d, v in sorted(daily.items())
    ]
    staff_list = sorted(
        (
            {"account": acct, "total": v["total"], "adopted": v["adopted"],
             "rate": (v["adopted"] / v["total"] * 100) if v["total"] else 0}
            for acct, v in by_staff.items()
        ),
        key=lambda x: -x["total"],
    )
    # 原始消息列表(最近的最前, 便于核对)
    raw_messages = [
        {
            "time": r.get("createTime") or r.get("createAt") or r.get("time") or "",
            "buyer": (r.get("buyerNick") or r.get("customerName") or r.get("userName") or ""),
            "sendType": r.get("sendType"),
            "content": (r.get("content") or r.get("replyContent") or r.get("question") or "")[:200],
            "staff": r.get("sellerAccount") or r.get("staffName") or "",
            "type": r.get("type") or "OTHER",
            "traceId": r.get("traceId") or r.get("id") or "",
        }
        for r in reversed(results)
    ]
    return {
        "total": total,
        "counts": counts,
        "adopted": adopted,
        "rate": round(rate, 2),
        "byStaff": staff_list,
        "byType": by_type,
        "daily": daily_list,
        "messages": raw_messages,
    }


# 聚合/原始消息仅用到这些字段, 抓取落盘时裁剪, 缓存体积可缩小 10 倍+
TRACE_KEEP_KEYS = (
    "time", "createTime", "createAt",
    "sendType", "sellerAccount", "staffName",
    "type", "buyerNick", "customerName", "userName",
    "traceId", "id",
)


def _trim_trace_msg(r):
    """把单条原始消息裁剪成下游聚合需要的字段"""
    m = {k: r.get(k) for k in TRACE_KEEP_KEYS if k in r}
    m["content"] = (r.get("content") or r.get("replyContent") or r.get("question") or "")[:200]
    return m


def _fetch_trace_range(shop_id, begin, end):
    """抓取指定时间区间内的全部消息轨迹

    接口单次核算区间最多返回 TRACE_QUERY_CAP 条(实测硬上限), 超出部分翻页也拿不到。
    若 total 触顶, 说明该区间消息数超上限, 自动按 4 小时细分为多个子区间,
    每个子区间都低于上限, 从而完整拿到全部消息(如 1.4 万条的一天需 6 个 4 小时片)。
    """
    total, first_batch = fetch_trace_page(shop_id, begin, end, 1)
    if total >= TRACE_QUERY_CAP:
        # 触顶: 该区间超过 1 万条, 递归细分以保证完整
        sub_results = []
        try:
            b = datetime.datetime.fromisoformat(begin)
            e = datetime.datetime.fromisoformat(end)
        except Exception:
            b = e = None
        if b and e and (e - b).total_seconds() > 4 * 3600:
            for start in _time_slices(b, e, 4):
                sleep_trace_page()
                sub_results.extend(
                    _fetch_trace_range(shop_id, start.strftime("%Y-%m-%d %H:%M:%S"),
                                       (start + datetime.timedelta(hours=4) - datetime.timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S"))
                )
            return sub_results
        return first_batch  # 已是 4 小时以内仍触顶: 接口极端限制, 接受截断
    # 未触顶: 正常翻页补齐
    results = first_batch
    page = 2
    while len(results) < total and len(results) < TRACE_QUERY_CAP:
        sleep_trace_page()
        try:
            _, batch = fetch_trace_page(shop_id, begin, end, page)
        except RiskTriggered:
            raise
        except Exception as e:
            print(f"[trace] {shop_id} {begin[:10]} page={page} 失败: {e}")
            break
        if not batch:
            break
        results.extend(batch)
        page += 1
    return results


def _time_slices(start, end, hours):
    """把 [start, end) 按 hours 小时切成若干子区间(返回每个子区间的起点)"""
    slices = []
    cur = start
    while cur < end:
        slices.append(cur)
        cur += datetime.timedelta(hours=hours)
    return slices


def _fetch_trace_day(shop_id, day_str):
    """抓取某一天的全部消息轨迹(按天遍历, 缓存可精确到天)

    返回该天所有消息的 results 列表; 失败返回 None(调用方决定是否跳过)
    """
    begin = f"{day_str} 00:00:00"
    end = f"{day_str} 23:59:59"
    return _fetch_trace_range(shop_id, begin, end)


def days_all_cached(shop_id, start, end):
    """判断区间内所有天是否已缓存且未过期(纯聚合, 可跳过限速停顿)"""
    cache = _load_trace_days_cache(shop_id)
    if not cache or not cache.get("days"):
        return False
    start_d = datetime.date.fromisoformat(start)
    end_d = datetime.date.fromisoformat(end)
    need = {(start_d + datetime.timedelta(days=i)).isoformat()
            for i in range((end_d - start_d).days + 1)}
    return all(_cached_days_usable(cache, ds) for ds in need)


def stat_trace_daily(shop_id, start, end, force=False):
    """按天遍历消息轨迹, 逐日缓存, 支持增量更新

    - 已缓存的天直接复用(零请求)
    - 未缓存的天按天抓取
    - force=True 时忽略缓存重新抓取(用于"重新抓取"按钮)
    - 返回聚合 stat(messages/daily 齐全, 供折线图/核算/原始消息)
    """
    # 按天拆分区间
    start_d = datetime.date.fromisoformat(start)
    end_d = datetime.date.fromisoformat(end)
    days = [start_d + datetime.timedelta(days=i) for i in range((end_d - start_d).days + 1)]
    day_strs = [d.isoformat() for d in days]

    cache = {} if force else (_load_trace_days_cache(shop_id) or {})
    cached_days = cache.get("days") or {}  # {day_str: [messages...]}
    day_fetched_at = cache.get("day_fetched_at") or {}  # {day_str: fetched_ts}
    total_results = []

    for ds in day_strs:
        if not force and _cached_days_usable(cache, ds):
            total_results.extend(cached_days[ds])
            continue
        try:
            res = [_trim_trace_msg(r) for r in _fetch_trace_day(shop_id, ds)]
        except RiskTriggered:
            raise
        except Exception as e:
            print(f"[trace] {shop_id} {ds} 抓取失败: {e}")
            continue
        cached_days[ds] = res
        day_fetched_at[ds] = time.time()
        total_results.extend(res)
        # 每天抓完立即落盘, 中途失败也不丢已抓数据
        save_cache("trace_days", shop_id, {
            "fetched_at": time.time(),
            "day_fetched_at": day_fetched_at,
            "days": cached_days,
        })
        _trace_days_cache[shop_id] = (time.time(), {
            "fetched_at": time.time(),
            "day_fetched_at": day_fetched_at,
            "days": cached_days,
        })
        print(f"[trace] {shop_id} {ds} 抓取完成 {len(res)} 条")

    # 聚合成与 stat_trace 相同的结构
    counts = {1: 0, 2: 0, 3: 0, None: 0}
    by_staff = {}
    by_type = {}
    daily = {}
    total = 0
    for r in total_results:
        st = r.get("sendType")
        counts[st] = counts.get(st, 0) + 1
        staff = r.get("sellerAccount") or "未知"
        entry = by_staff.setdefault(staff, {"total": 0, "adopted": 0})
        entry["total"] += 1
        if st in ADOPTED_SEND_TYPES:
            entry["adopted"] += 1
        t = r.get("type") or "OTHER"
        by_type[t] = by_type.get(t, 0) + 1
        ts = r.get("time")
        if isinstance(ts, (int, float)):
            day = datetime.datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d")
            de = daily.setdefault(day, {"total": 0, "adopted": 0})
            de["total"] += 1
            if st in ADOPTED_SEND_TYPES:
                de["adopted"] += 1
        total += 1

    adopted = sum(counts.get(s, 0) for s in ADOPTED_SEND_TYPES)
    rate = (adopted / total * 100) if total else 0
    daily_list = [
        {"date": d, "total": v["total"], "adopted": v["adopted"],
         "rate": round(v["adopted"] / v["total"] * 100, 2) if v["total"] else 0}
        for d, v in sorted(daily.items())
    ]
    staff_list = sorted(
        (
            {"account": acct, "total": v["total"], "adopted": v["adopted"],
             "rate": (v["adopted"] / v["total"] * 100) if v["total"] else 0}
            for acct, v in by_staff.items()
        ),
        key=lambda x: -x["total"],
    )
    raw_messages = [
        {
            "time": r.get("createTime") or r.get("createAt") or r.get("time") or "",
            "buyer": (r.get("buyerNick") or r.get("customerName") or r.get("userName") or ""),
            "sendType": r.get("sendType"),
            "content": (r.get("content") or r.get("replyContent") or r.get("question") or "")[:200],
            "staff": r.get("sellerAccount") or r.get("staffName") or "",
            "type": r.get("type") or "OTHER",
            "traceId": r.get("traceId") or r.get("id") or "",
        }
        for r in reversed(total_results)
    ]
    return {
        "total": total,
        "counts": counts,
        "adopted": adopted,
        "rate": round(rate, 2),
        "byStaff": staff_list,
        "byType": by_type,
        "daily": daily_list,
        "messages": raw_messages,
    }


# ---------- 接口 ----------
class CookieUpdate(BaseModel):
    cookies: dict


class GroupSwitch(BaseModel):
    group_id: str


@app.get("/api/health")
def health():
    return {"ok": True}


@app.get("/api/shops")
def shops_list(platform: int | None = None):
    return {"shops": load_shops(platform)}


@app.get("/api/platforms")
def platforms_info():
    """平台配置信息: 哪些平台抓取, 哪些保留接口"""
    return {
        "fetch": [{"id": p, "name": PLATFORM_NAMES.get(p, str(p))} for p in FETCH_PLATFORMS],
        "keep": [{"id": p, "name": PLATFORM_NAMES.get(p, str(p))} for p in KEEP_PLATFORMS],
        "all": [{"id": p, "name": PLATFORM_NAMES.get(p, str(p))} for p in sorted(PLATFORM_NAMES)],
    }


@app.get("/api/groups")
def groups_list():
    """集团列表 + 当前集团"""
    return get_group_list()


@app.post("/api/groups/switch")
def groups_switch(body: GroupSwitch):
    """切换到指定集团并同步店铺"""
    try:
        return switch_group(body.group_id)
    except ValueError as e:
        raise HTTPException(400, str(e))
    except RuntimeError as e:
        raise HTTPException(502, str(e))


@app.post("/api/shops/sync")
def shops_sync():
    """从探域接口同步当前集团的店铺列表"""
    try:
        return sync_shops_from_tanyu()
    except RuntimeError as e:
        raise HTTPException(502, str(e))


@app.get("/api/shops/cached-groups")
def shops_cached_groups():
    """返回 shops.json 里出现过哪些集团(按 groupId), 供比对"""
    return {"ok": True}


@app.get("/api/overview")
def overview(platform: int = 1, days: int = 7, start: str | None = None, end: str | None = None):
    """平台维度汇总, 默认近7天; 支持自定义时间段(start/end, YYYY-MM-DD)"""
    if platform not in PLATFORM_NAMES:
        raise HTTPException(400, f"不支持的平台: {platform}")
    start, end = date_range(days, end) if not start else (start, end or (datetime.date.today() - datetime.timedelta(days=1)).isoformat())
    payload = {
        "statType": "natural_day",
        "startDate": start,
        "endDate": end,
        "platform": platform,
        "dimension": "platform",
    }
    try:
        summary = fetch_summary_interactive(payload)
    except BusyQueueError as e:
        raise HTTPException(503, str(e))
    except RuntimeError as e:
        raise HTTPException(502, str(e))
    return {
        "startDate": start,
        "endDate": end,
        "platform": platform,
        "platformName": PLATFORM_NAMES.get(platform),
        "items": summary,
    }


@app.get("/api/shop/{shop_id}")
def shop_detail(shop_id: str, days: int = 7, start: str | None = None, end: str | None = None):
    """单店汇总 + 明细(带本地缓存); 支持自定义时间段"""
    shops = {s["thirdShopId"]: s for s in load_shops()}
    shop = shops.get(shop_id)
    if not shop:
        raise HTTPException(404, "店铺不存在")

    start, end = date_range(days, end) if not start else (start, end or (datetime.date.today() - datetime.timedelta(days=1)).isoformat())
    # 平台/店铺自然日数据一天内基本不变, 缓存 6 小时; 页面加载/切平台走交互快通道, 不排队
    cache = load_cache("summary", shop_id, max_age=21600)
    if cache and cache.get("data"):
        items = cache["data"]
        fetched_at = cache["fetched_at"]
    else:
        payload = {
            "statType": "natural_day",
            "startDate": start,
            "endDate": end,
            "platform": shop.get("platform", 1),
            "dimension": "shop",
            "targetId": shop_id,
        }
        try:
            items = fetch_summary_interactive(payload)
        except BusyQueueError as e:
            raise HTTPException(503, str(e))
        except RuntimeError as e:
            raise HTTPException(502, str(e))
        save_cache("summary", shop_id, {"fetched_at": time.time(), "data": items})
        fetched_at = time.time()

    tables = {}
    for section in ["operations", "service", "ai"]:
        c = load_cache("table", f"{shop_id}__{section}", max_age=21600)
        if c and c.get("data"):
            tables[section] = c["data"]
        else:
            payload = {
                "statType": "natural_day",
                "startDate": start,
                "endDate": end,
                "platform": shop.get("platform", 1),
                "dimension": "shop",
                "targetId": shop_id,
                "section": section,
            }
            try:
                tables[section] = fetch_section_table_interactive(payload)
            except (BusyQueueError, RuntimeError):
                tables[section] = {"dates": [], "rows": []}

    return {
        "shop": shop,
        "startDate": start,
        "endDate": end,
        "items": items,
        "fetchedAt": fetched_at,
        "tables": tables,
    }


@app.get("/api/refresh")
def refresh():
    """触发后台刷新全部店铺"""
    # 任务互斥: 核算进行中不允许同时刷新
    try:
        _assert_no_running_task()
    except RuntimeError as e:
        raise HTTPException(409, str(e))
    start, end = date_range(7)
    refresh_all_async(start, end)
    return {"ok": True, "message": "刷新任务已启动"}


@app.get("/api/trace/shop/{shop_id}")
def trace_shop(shop_id: str, days: int = 7, force: int = 0, start: str | None = None, end: str | None = None):
    """单店核算: 遍历消息轨迹统计核算采纳率(带按日缓存)

    start/end 可选: 自定义起止日期(YYYY-MM-DD), 不传则用近 N 天
    按日增量缓存: 已抓取的天零请求, 只补抓缺失的天
    """
    shops = {s["thirdShopId"]: s for s in load_shops()}
    shop = shops.get(shop_id)
    if not shop:
        raise HTTPException(404, "店铺不存在")
    start, end = date_range(days, end) if not start else (start, end or (datetime.date.today() - datetime.timedelta(days=1)).isoformat())
    # 新式按日缓存: 全部天都命中(且未过期)则零请求
    cache = _load_trace_days_cache(shop_id) if not force else None
    if cache and cache.get("days") and not force:
        start_d = datetime.date.fromisoformat(start)
        end_d = datetime.date.fromisoformat(end)
        need = {(start_d + datetime.timedelta(days=i)).isoformat()
                for i in range((end_d - start_d).days + 1)}
        if all(_cached_days_usable(cache, ds) for ds in need):
            stat = stat_trace_daily(shop_id, start, end)  # 全命中: 纯聚合零请求
            return {"shop": shop, "startDate": start, "endDate": end,
                    "fetchedAt": time.time(), "stat": stat}
    try:
        stat = stat_trace_daily(shop_id, start, end, force=bool(force))
    except RuntimeError as e:
        raise HTTPException(502, str(e))
    return {"shop": shop, "startDate": start, "endDate": end, "fetchedAt": time.time(), "stat": stat}


# 核算总览状态(异步任务)
TRACE_OVERVIEW_CACHE_FILE = DATA_DIR / "trace_overview_result.json"
_trace_state = {
    "running": False,
    "progress": {"done": 0, "total": 0, "current": ""},
    "result": None,  # 最近一次完成的结果
    "start_date": None,
    "end_date": None,
    "platform": None,
    "last_run": None,
    "error": None,
}


TRACE_OVERVIEW_CACHE_TTL = 6 * 3600   # 总览磁盘缓存有效期(探域数据每日更新)


def load_trace_overview_cache(start, end, platform=None):
    """从磁盘读核算总览结果(同时间段重复核算零请求, 且未过期)"""
    if not TRACE_OVERVIEW_CACHE_FILE.exists():
        return None
    try:
        c = json.loads(TRACE_OVERVIEW_CACHE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return None
    if c.get("startDate") == start and c.get("endDate") == end and c.get("platform") == platform:
        cached_at = c.get("fetched_at") or c.get("last_run")
        if not cached_at:
            # 旧格式缓存没有时间戳: 用文件 mtime 兜底, 避免永久缓存
            try:
                cached_at = TRACE_OVERVIEW_CACHE_FILE.stat().st_mtime
            except Exception:
                cached_at = None
        if cached_at and time.time() - cached_at > TRACE_OVERVIEW_CACHE_TTL:
            return None
        return c  # 保存的本身就是 result 字典(含 startDate/endDate/platform 字段)
    return None


def load_latest_trace_overview_cache():
    """读磁盘里最近一次核算结果(不分时间段)"""
    if not TRACE_OVERVIEW_CACHE_FILE.exists():
        return None
    try:
        c = json.loads(TRACE_OVERVIEW_CACHE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return None
    return c if "total" in c else None


def save_trace_overview_cache(result):
    try:
        _atomic_write_text(TRACE_OVERVIEW_CACHE_FILE,
                           json.dumps(result, ensure_ascii=False, separators=(",", ":")))
    except Exception:
        pass


@app.get("/api/trace/overview")
def trace_overview(days: int = 7, platform: int | None = None, force: int = 0,
                   start: str | None = None, end: str | None = None,
                   from_cache: int = 0):
    """核算总览(异步): 遍历全部抓取店铺统计核算采纳率

    支持自定义时间段(start/end, YYYY-MM-DD); 不传则用近 N 天。
    有缓存直接返回; 否则触发后台任务, 返回任务状态,
    前端轮询 /api/trace/overview/status 获取进度, 完成后取 result。
    from_cache=1: 只查内存+磁盘缓存, 未命中直接返回空(不触发任务)
    """
    global _trace_state
    # 任务互斥: 核算与数据刷新不能同时跑(避免叠加请求量)
    if not from_cache:
        try:
            _assert_no_running_task()
        except RuntimeError as e:
            raise HTTPException(409, str(e))
    start, end = date_range(days, end) if not start else (start, end or (datetime.date.today() - datetime.timedelta(days=1)).isoformat())
    # 内存缓存命中(需平台一致 + 未过期)
    if (not force and _trace_state.get("result")
            and _trace_state["start_date"] == start and _trace_state["end_date"] == end
            and _trace_state["platform"] == platform
            and _trace_state.get("last_run")
            and time.time() - _trace_state["last_run"] < TRACE_OVERVIEW_CACHE_TTL):
        return {"status": "done", "startDate": start, "endDate": end, "result": _trace_state["result"]}
    # 磁盘缓存命中(服务重启后同时间段重复核算零请求)
    disk = load_trace_overview_cache(start, end, platform) if not force else None
    if disk:
        _trace_state["result"] = disk
        _trace_state["start_date"] = start
        _trace_state["end_date"] = end
        _trace_state["platform"] = platform
        _trace_state["last_run"] = time.time()
        print(f"[trace] 命中磁盘缓存 {start}~{end}, 跳过抓取 (共{disk.get('total', 0)}条)")
        return {"status": "done", "startDate": start, "endDate": end, "result": disk}
    if from_cache:
        # 只读模式: 未命中缓存不触发任务; 有最近结果则带回展示
        latest = load_latest_trace_overview_cache()
        return {"status": "idle", "startDate": start, "endDate": end,
                "result": None, "latest": latest}

    with _lock:
        if _trace_state["running"]:
            return {"status": "running", "startDate": start, "endDate": end,
                    "progress": _trace_state["progress"]}
    _trace_state["running"] = True
    _trace_state["start_date"] = start
    _trace_state["end_date"] = end
    _trace_state["platform"] = platform
    _trace_state["error"] = None
    _trace_state["result"] = None
    _trace_state["progress"] = {"done": 0, "total": 0, "current": "准备中"}

    def worker():
        try:
            shops = [s for s in load_shops() if s.get("platform") in FETCH_PLATFORMS]
            if platform is not None:
                shops = [s for s in shops if s.get("platform") == platform]
            total = len(shops)
            begin, end_t = f"{start} 00:00:00", f"{end} 23:59:59"
            _trace_state["progress"] = {"done": 0, "total": total, "current": ""}
            shop_list = []
            agg_total = agg_adopted = 0
            staff_agg = {}
            for i, shop in enumerate(shops, 1):
                _trace_state["progress"]["current"] = f"{shop['platformName']} · {shop['shopName']} ({i}/{total})"
                # 该店在区间内天数全部已缓存 => 纯聚合零请求, 无需限速停顿
                if i > 1 and not days_all_cached(shop["thirdShopId"], start, end):
                    sleep_trace_shop()
                try:
                    stat = stat_trace_daily(shop["thirdShopId"], start, end)
                    shop_list.append(
                        {"shop": shop, "total": stat["total"], "adopted": stat["adopted"], "rate": stat["rate"]}
                    )
                    agg_total += stat["total"]
                    agg_adopted += stat["adopted"]
                    for st in stat.get("byStaff", []):
                        entry = staff_agg.setdefault(st["account"], {"total": 0, "adopted": 0})
                        entry["total"] += st["total"]
                        entry["adopted"] += st["adopted"]
                except RiskTriggered as e:
                    # 风控/登录失效: 立即停止, 不再请求剩余店铺
                    print(f"[trace] ⛔ 风控触发, 停止剩余 {total - i} 家店铺: {e}")
                    _trace_state["error"] = f"风控/登录失效, 已停止: {e}"
                    _trace_state["progress"]["current"] = f"已停止: {e}"
                    break
                except Exception as e:
                    print(f"[trace] {shop['shopName']} 失败: {e}")
                _trace_state["progress"]["done"] = i
            staff_list = sorted(
                (
                    {"account": a, "total": v["total"], "adopted": v["adopted"],
                     "rate": (v["adopted"] / v["total"] * 100) if v["total"] else 0}
                    for a, v in staff_agg.items()
                ),
                key=lambda x: -x["total"],
            )
            _trace_state["result"] = {
                "startDate": start,
                "endDate": end,
                "platform": platform,
                "fetched_at": time.time(),
                "total": agg_total,
                "adopted": agg_adopted,
                "rate": round(agg_adopted / agg_total * 100, 2) if agg_total else 0,
                "shopList": shop_list,
                "byStaff": staff_list,
            }
            _trace_state["last_run"] = time.time()
            save_trace_overview_cache(_trace_state["result"])
        except Exception as e:
            _trace_state["error"] = str(e)
        finally:
            _trace_state["running"] = False

    threading.Thread(target=worker, daemon=True).start()
    return {"status": "running", "startDate": start, "endDate": end,
            "progress": _trace_state["progress"]}


@app.get("/api/trace/overview/status")
def trace_overview_status():
    """核算总览任务状态/结果"""
    with _lock:
        return {
            "running": _trace_state["running"],
            "progress": _trace_state["progress"],
            "startDate": _trace_state["start_date"],
            "endDate": _trace_state["end_date"],
            "platform": _trace_state["platform"],
            "lastRun": _trace_state["last_run"],
            "error": _trace_state["error"],
            "result": _trace_state["result"],
        }


@app.get("/api/trace/messages/{shop_id}")
def trace_messages(shop_id: str, days: int = 7, force: int = 0,
                   start: str | None = None, end: str | None = None):
    """单店原始消息记录(用于与探域后台核对)

    返回逐条消息: 时间/买家/发送状态/人工客服/内容
    按日增量缓存: 已抓取的天零请求, 只补抓缺失的天
    """
    shops = {s["thirdShopId"]: s for s in load_shops()}
    shop = shops.get(shop_id)
    if not shop:
        raise HTTPException(404, "店铺不存在")
    start, end = date_range(days, end) if not start else (start, end or (datetime.date.today() - datetime.timedelta(days=1)).isoformat())
    cache = _load_trace_days_cache(shop_id) if not force else None
    if cache and cache.get("days") and not force:
        start_d = datetime.date.fromisoformat(start)
        end_d = datetime.date.fromisoformat(end)
        need = {(start_d + datetime.timedelta(days=i)).isoformat()
                for i in range((end_d - start_d).days + 1)}
        if all(_cached_days_usable(cache, ds) for ds in need):
            stat = stat_trace_daily(shop_id, start, end)
            return {"shop": shop, "startDate": start, "endDate": end,
                    "total": stat["total"], "daily": stat.get("daily", []),
                    "messages": stat.get("messages", [])}
    try:
        stat = stat_trace_daily(shop_id, start, end, force=bool(force))
    except RuntimeError as e:
        raise HTTPException(502, str(e))
    return {"shop": shop, "startDate": start, "endDate": end,
            "total": stat["total"], "daily": stat.get("daily", []),
            "messages": stat.get("messages", [])}


@app.get("/api/risk")
def risk_status():
    """风控状态: triggered/reason/at, 供前端展示"""
    with _lock:
        return {
            "triggered": _risk_state["triggered"],
            "reason": _risk_state["reason"],
            "at": _risk_state["at"],
            "lastCode": _risk_state["last_code"],
        }


@app.get("/api/tasks/refresh")
def task_status():
    with _lock:
        return dict(_refresh_state)


@app.post("/api/cookies")
def update_cookies(body: CookieUpdate):
    cfg = load_config()
    cfg["cookies"] = body.cookies
    save_config(cfg)
    return {"ok": True}


# ---------- 静态页面 ----------
# ---------- 浏览器登录获取 Cookie ----------
_login_state = {
    "running": False,
    "phase": "idle",  # idle / opening / waiting_login / got_cookie / failed
    "message": "",
    "started_at": None,
    "last_error": None,
}
_login_lock = threading.Lock()

# 需要捕获的 cookie 名
LOGIN_COOKIE_NAMES = [
    "tanyu-account-id",
    "tanyu-agent-account",
    "tanyu-group-account",
    "tanyu-group-id",
]


def _login_worker():
    """后台线程: 启动浏览器让用户登录, 检测到 cookie 后写入 config"""
    try:
        from playwright.sync_api import sync_playwright

        with _login_lock:
            _login_state["running"] = True
            _login_state["phase"] = "opening"
            _login_state["message"] = "正在打开浏览器…"
            _login_state["started_at"] = time.time()

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)
            ctx = browser.new_context()
            page = ctx.new_page()
            page.goto("https://agent.tanyuai.com/", wait_until="domcontentloaded", timeout=60000)
            with _login_lock:
                _login_state["phase"] = "waiting_login"
                _login_state["message"] = "请在浏览器中完成登录(推荐扫码登录以获取全部集团)…"

            # 轮询等待登录 cookie
            deadline = time.time() + 15 * 60  # 最多等15分钟
            while time.time() < deadline:
                page.wait_for_timeout(3000)
                cks = ctx.cookies()
                found = {c["name"]: c["value"] for c in cks if c["name"] in LOGIN_COOKIE_NAMES}
                if len(found) >= 3:  # 至少3个关键 cookie
                    cfg = load_config()
                    cfg["cookies"].update(found)
                    # 尝试读取扫码登录时写入 localStorage 的集团列表
                    try:
                        group_list = page.evaluate("() => localStorage.getItem('groupList')")
                        if group_list:
                            groups = json.loads(group_list)
                            if isinstance(groups, list) and groups:
                                cfg["groups"] = groups
                    except Exception:
                        pass
                    save_config(cfg)
                    # 新凭证已保存, 自动解除风控停止状态
                    _reset_risk()
                    group_note = ""
                    if cfg.get("groups"):
                        group_note = f", 发现 {len(cfg['groups'])} 个集团"
                    with _login_lock:
                        _login_state["phase"] = "got_cookie"
                        _login_state["message"] = (
                            "已获取 Cookie 并保存 ✅ " + ", ".join(found.keys()) + group_note
                        )
                    browser.close()
                    return
            with _login_lock:
                _login_state["phase"] = "failed"
                _login_state["message"] = "等待登录超时(15分钟), 未检测到 Cookie"
            browser.close()
    except Exception as e:
        with _login_lock:
            _login_state["phase"] = "failed"
            _login_state["message"] = f"登录流程出错: {e}"
    finally:
        with _login_lock:
            _login_state["running"] = False


@app.post("/api/login/start")
def login_start():
    """弹出浏览器窗口, 用户人工登录, 自动抓取 Cookie"""
    with _login_lock:
        if _login_state["running"]:
            return {"ok": False, "message": "登录流程已在运行中"}
        _login_state["running"] = True
        _login_state["phase"] = "opening"
        _login_state["message"] = "正在打开浏览器…"
    threading.Thread(target=_login_worker, daemon=True).start()
    return {"ok": True, "message": "浏览器已启动, 请完成登录"}


@app.get("/api/login/status")
def login_status():
    with _login_lock:
        return dict(_login_state)


@app.get("/api/login/close")
def login_close():
    """关闭浏览器窗口(可选)"""
    with _login_lock:
        _login_state["phase"] = "idle"
        _login_state["running"] = False
        _login_state["message"] = ""
    return {"ok": True}


@app.get("/")
def index():
    return FileResponse(BASE_DIR / "static" / "index.html")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    uvicorn.run(app, host="127.0.0.1", port=port)
