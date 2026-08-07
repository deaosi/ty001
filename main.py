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
import collections
import datetime
import json
import os
import sys
import threading
import time
from pathlib import Path

# Windows 控制台默认 GBK, 打印 emoji/生僻字符会抛 UnicodeEncodeError 打崩请求线程。
# 重配 stdout/stderr 为 UTF-8 并 errors=replace, 日志乱码可接受但绝不崩。
if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
if sys.stderr and hasattr(sys.stderr, "reconfigure"):
    try:
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

import requests
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel

import trace_store  # SQLite 消息轨迹存储层(30 天滚动窗口)

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
CONFIG_FILE = BASE_DIR / "config.json"
SHOPS_FILE = BASE_DIR / "shops.json"

API_BASE = "https://agent.tanyuai.com/api/data-service/business/compass"

# 平台枚举(与探域前端源码一致): 0=淘宝 1=拼多多 2=有赞 4=快手 5=抖店 7=京东 8=视频号 9=得物 10=1688
# 本账号只用到 1/5/7; 7=京东(集团"京东"的店铺, 非枚举字面上的天猫), 2=有赞无店铺
PLATFORM_NAMES = {0: "淘宝", 1: "拼多多", 2: "有赞", 4: "快手", 5: "抖音", 7: "京东"}

# 抓取配置: 拼多多(1)/京东(7, "京东"集团店铺)/抖音(5)抓取; 淘宝(0)/快手(4)保留接口但不抓取
# 注: 本账号店铺只涉及 1/5/7, 故 fetch 列表不含 2(有赞)等未用到平台
FETCH_PLATFORMS = [1, 5, 7]
KEEP_PLATFORMS = [0, 4]  # 保留接口, 不抓取不统计

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


# ---------- 进程内日志环形缓冲(供抽屉日志窗口展示) ----------
LOG_RING_MAX = 500
_log_ring = collections.deque(maxlen=LOG_RING_MAX)
_log_lock = threading.Lock()


def log_line(tag, msg):
    """写一条带时间戳的日志: 进环形缓冲(抽屉可查) + 打印到 stdout(server.log 追加)。

    绝不写入 cookie 值——所有日志内容只允许业务描述, 禁止任何凭据/密钥字段。
    """
    entry = {"ts": time.strftime("%Y-%m-%d %H:%M:%S"), "tag": tag, "msg": msg}
    try:
        with _log_lock:
            _log_ring.append(entry)
    except Exception:
        pass
    print(f"[{tag}] {msg}")


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
        # 夜间预抓多集团配置: 平台顺序(1拼多多 5抖音 7京东)、
        # 默认窗口天数(未在 prefetch_windows 列出的平台用此值)、
        # 各平台窗口覆盖: 抖音已抓满30天保留30天, PDD/JD 等其余平台抓近7天
        # prefetch_force_days: 预抓时窗口最近 N 天强制重抓(默认1=昨天),
        #   防 tanyu 回溯更新 sendType 造成的采纳口径漂移; 0=关闭
        "prefetch_platforms": [1, 5, 7],
        "prefetch_days": 7,
        "prefetch_windows": {5: 30},
        "prefetch_force_days": 1,
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
    """交互式请求: 不排队, 限速繁忙时立即返回 503; 10s 内同参命中内存缓存零请求

    缓存与首次返回保持同一形状: 都返回 data.data(内层业务数据)。
    注意: 绝不能把完整响应 {code,msg,data:{...}} 存进缓存——命中后返回完整响应,
    调用方 `.get("items")/.get("dates")` 会取不到内层字段, 表现为"间歇性空数据"。
    """
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
    inner = data.get("data") or {}
    _interactive_cache_put(path, payload, inner)
    return inner


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
    """原子写入: 唯一临时名(pid+线程+随机)写后再 os.replace, 并发/崩溃不互相踩踏"""
    import threading
    tmp = path.with_name(
        f".{path.name}.{os.getpid()}.{threading.get_ident()}.{random.randint(0, 2 ** 31 - 1)}.tmp"
    )
    try:
        tmp.write_text(text, encoding="utf-8")
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            try:
                tmp.unlink()
            except OSError:
                pass


# tanyu /summary 只支持三种统计口径, 全部忽略 startDate/endDate(已实测):
#   natural_day   = 值=昨天, 环比=前天
#   natural_week  = 值=本周, 环比=上周
#   natural_month = 值=本月, 环比=上月
# 店铺表现筛选因此按口径切换, 而非天数/自定义区间(那些对 summary 是空操作)。
SUMMARY_STAT_TYPES = ("natural_day", "natural_week", "natural_month")


def _valid_stat_type(stat_type):
    return stat_type in SUMMARY_STAT_TYPES


def _valid_date_iso(value):
    """校验 YYYY-MM-DD 格式, 合法返回原串, 非法返回 None"""
    try:
        import datetime as _dt
        _dt.date.fromisoformat(value)
        return value
    except (TypeError, ValueError):
        return None


def _stat_type_range(stat_type, ref=None):
    """统计口径对应的主值区间(与前端 statTypeRange 语义一致)。

    tanyu summary 三种口径忽略 startDate/endDate, 但明细表(section/table)
    对日期敏感; 未显式传 start/end 时按口径给默认区间, 使回显的
    startDate/endDate 不再是误导的 昨天~昨天:
      natural_day   = 昨天 ~ 昨天
      natural_week  = 本周一 ~ 今天
      natural_month = 本月1日 ~ 今天
    ref 为基准日(默认今天), 便于测试/回溯历史周期。
    """
    ref = ref or datetime.date.today()
    one_day = datetime.timedelta(days=1)
    if stat_type == "natural_week":
        start = ref - datetime.timedelta(days=(ref.weekday()))  # weekday(): 周一=0
        return start.isoformat(), ref.isoformat()
    if stat_type == "natural_month":
        start = ref.replace(day=1)
        return start.isoformat(), ref.isoformat()
    yest = ref - one_day
    return yest.isoformat(), yest.isoformat()


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
    # 缓存键带口径后缀(与 shop_detail 一致): 批量刷新只刷 natural_day,
    # 写 {shop_id}__natural_day 键, 避免与周/月口径缓存互相覆盖
    save_cache("summary", f"{shop_id}__natural_day", {"fetched_at": time.time(), "data": summary})

    for section in ["operations", "service", "ai"]:
        payload = {**base, "section": section}
        try:
            table = fetch_section_table(payload)
            # 明细表按日期区间缓存(键带起止日期): 与 shop_detail 的读键严格一致,
            # 避免批量刷新(近7天)与单店自然日(昨天)写读同一键串日期范围。
            save_cache("table", f"{shop_id}__natural_day__{section}__{start}__{end}",
                       {"fetched_at": time.time(), "data": table})
        except Exception as e:
            log_line("refresh", f"{shop['shopName']} section={section} 失败: {e}")
    return shop_id


def refresh_all_async(start, end, shops=None):
    """后台线程: 刷新给定店铺列表(默认当前激活集团的店铺, 限抓取平台)

    shops: 可选店铺列表(跨集团刷新时由 refresh_all_groups_async 逐集团传入),
           为空时用 load_shops()(当前激活集团)过滤抓取平台。
    """

    def worker():
        with _lock:
            if _refresh_state["running"]:
                return
            _refresh_state["running"] = True
            _refresh_state["error"] = None
        try:
            # 只抓取启用的平台
            if shops is None:
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
                    log_line("refresh", f"⛔ 风控触发, 停止剩余 {total - i} 家店铺: {e}")
                    _refresh_state["error"] = f"风控/登录失效, 已停止: {e}"
                    break
                except Exception as e:
                    log_line("refresh", f"{shop['shopName']} 失败: {e}")
                _refresh_state["progress"]["done"] = i
            _refresh_state["last_run"] = time.time()
        except Exception as e:
            _refresh_state["error"] = str(e)
        finally:
            _refresh_state["running"] = False

    threading.Thread(target=worker, daemon=True).start()


def refresh_all_groups_async(start, end):
    """后台线程: 按集团切换轮询刷新全部三个集团(pdd/jd/douyin)的店铺 summary

    与夜间预抓同模式: 逐集团 switch_group → sync_shops_from_tanyu →
    refresh_all_async(该集团店铺) → 下一集团。结束后恢复进入前的激活集团,
    让常驻看板继续服务原集团。风控/登录失效立即整体停止。
    """
    cfg = load_config()
    platform_order = cfg.get("prefetch_platforms") or [1, 5, 7]
    plat_rank = {p: i for i, p in enumerate(platform_order)}
    ordered = sorted(
        [g for g in cfg.get("groups", []) if g.get("groupId") and g.get("accountId")],
        key=lambda g: plat_rank.get(g.get("platform"), 99),
    )
    if not ordered:
        log_line("refresh", "⚠️ config.groups 为空或无可用集团, 仅刷新当前激活集团")
        refresh_all_async(start, end)
        return
    # 记录进入前的激活集团, 结束后切回
    orig_gid = cfg.get("cookies", {}).get("tanyu-group-id")

    def worker():
        try:
            for g in ordered:
                if _risk_state.get("triggered"):
                    log_line("refresh", "⛔ 风控/登录失效, 整体停止")
                    break
                try:
                    switch_group(g["groupId"])
                except RiskTriggered:
                    log_line("refresh", "⛔ 风控触发于切换集团, 整体停止")
                    _refresh_state["error"] = _risk_state.get("reason") or "风控/登录失效"
                    return
                except Exception as e:
                    log_line("refresh", f"⚠️ 集团「{g.get('groupName')}」切换失败, 跳过: {e}")
                    continue
                group_shops = [s for s in load_shops() if s.get("platform") in FETCH_PLATFORMS]
                if not group_shops:
                    log_line("refresh", f"集团「{g.get('groupName')}」无抓取平台店铺, 跳过")
                    continue
                # 用该集团店铺子集跑一次刷新(不带新线程, 直接在当前 worker 内循环)
                with _lock:
                    if _refresh_state["running"]:
                        log_line("refresh", "已有刷新任务进行中, 跳过")
                        return
                    _refresh_state["running"] = True
                    _refresh_state["error"] = None
                try:
                    total = len(group_shops)
                    _refresh_state["progress"] = {"done": 0, "total": total, "current": ""}
                    for i, shop in enumerate(group_shops, 1):
                        _refresh_state["progress"]["current"] = (
                            f"集团[{g.get('groupName')}] {shop['platformName']} · "
                            f"{shop['shopName']} ({i}/{total})")
                        if i > 1:
                            sleep_trace_shop()
                        try:
                            refresh_one(shop, start, end)
                        except RiskTriggered as e:
                            log_line("refresh", f"⛔ 风控触发于集团[{g.get('groupName')}], 停止: {e}")
                            _refresh_state["error"] = f"风控/登录失效, 已停止: {e}"
                            return
                        except Exception as e:
                            log_line("refresh", f"{shop['shopName']} 失败: {e}")
                        _refresh_state["progress"]["done"] = i
                    _refresh_state["last_run"] = time.time()
                finally:
                    _refresh_state["running"] = False
            log_line("refresh", "全部集团刷新完成")
        finally:
            # 恢复原激活集团(风控时不强行切换, 仅告警)
            if orig_gid and not _risk_state.get("triggered"):
                try:
                    if orig_gid != load_config().get("cookies", {}).get("tanyu-group-id"):
                        switch_group(orig_gid)
                        log_line("refresh", f"已恢复原激活集团 {orig_gid}")
                except Exception as e:
                    log_line("refresh", f"⚠️ 恢复原集团失败: {e}")
            elif _risk_state.get("triggered"):
                log_line("refresh", "⚠️ 风控触发, 未恢复原集团(需重新登录后手工切换)")

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
        # 给 current 对象补上平台字段(前端集团→平台联动不再需要从列表猜测)
        cur_g = next((g for g in groups if g.get("current")), None)
        if cur_g and "platform" in cur_g and cur["id"] == cur_g.get("groupId"):
            cur["platform"] = cur_g["platform"]
    return {"groups": groups, "current": cur}


def _parse_cookie_expiry_from_header(set_cookie_str, now_ts):
    """从单条 Set-Cookie 头串解析 cookie 到期日

    格式形如: 'tanyu-agent-account=xxx; Max-Age=2592000; Expires=Sun, 6 Sep 2026 11:27:42 +0800; ...'
    优先取 Max-Age(相对当前时刻), 回退 Expires(RFC1123)。返回 (cookie名, 'YYYY-MM-DD') 或 None。
    """
    if not set_cookie_str:
        return None
    parts = set_cookie_str.split(";")
    if not parts:
        return None
    first = parts[0].strip()
    if "=" not in first:
        return None
    name = first.split("=", 1)[0].strip()
    if not name.startswith("tanyu-"):
        return None
    max_age = None
    expires = None
    for p in parts[1:]:
        p = p.strip()
        if p.lower().startswith("max-age="):
            try:
                max_age = float(p.split("=", 1)[1].strip())
            except Exception:
                pass
        elif p.lower().startswith("expires="):
            expires = p.split("=", 1)[1].strip()
    when = None
    if max_age is not None:
        when = now_ts + max_age
    elif expires:
        # RFC1123 / 变体(含时区偏移, 如 'Sun, 6 Sep 2026 11:27:42 +0800')
        try:
            import email.utils
            parsed = email.utils.parsedate_to_datetime(expires)
            when = parsed.timestamp()
        except Exception:
            try:
                from datetime import datetime
                parsed = datetime.strptime(expires, "%a, %d %b %Y %H:%M:%S GMT")
                when = parsed.timestamp()
            except Exception:
                when = None
    if when is None:
        return None
    import datetime as _dt
    return name, _dt.date.fromtimestamp(when).isoformat()


def _capture_cookie_expiry(resp):
    """从 Set-Cookie 响应头解析各 cookie 到期日, 写入 config.cookie_expires(仅日期, 无 cookie 值)

    switch-group-by-wx 对 tanyu-agent-account 带 Max-Age=2592000(30天), tanyu-account-id/
    tanyu-group-id 是 SESSION(无 Max-Age/Expires)不记录。requests 的 cookie jar 会丢弃
    Max-Age/Expires 属性, 故直接解析原始 Set-Cookie 响应头。
    只存 'YYYY-MM-DD' 日期字符串, 绝不存 cookie 值/凭据。
    """
    try:
        cfg = load_config()
        expires_map = dict(cfg.get("cookie_expires") or {})
        changed = False
        now = time.time()
        # 可能有多条 Set-Cookie 头: 用底层 getlist 逐个解析
        try:
            headers_list = resp.raw.headers.getlist("Set-Cookie")
        except Exception:
            headers_list = [resp.headers.get("Set-Cookie", "")] if resp.headers.get("Set-Cookie") else []
        for sc in headers_list:
            parsed = _parse_cookie_expiry_from_header(sc, now)
            if parsed:
                name, day = parsed
                if expires_map.get(name) != day:
                    expires_map[name] = day
                    changed = True
        if changed:
            cfg["cookie_expires"] = expires_map
            save_config(cfg)
    except Exception as e:
        log_line("auth", f"cookie 到期时间解析失败(不影响使用): {e}")


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
    # 记录各 cookie 到期日(仅日期, 无 cookie 值)
    _capture_cookie_expiry(resp)
    # 同步店铺列表
    sync_shops_from_tanyu()
    log_line("group", f"已切换集团「{g.get('groupName')}」")
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
        _trace_state["partial_list"] = []
    # 注意: 不重置 paused/canceled。若 sync 已把运行中的核算置为 canceled=True,
    # 这里保留该标志, 让旧 worker 在下个店铺边界退出(取消是终态, 下次开始核算时复位);
    # paused 由 worker 的 finally 清掉。event.set() 唤醒阻塞在暂停等待中的 worker 使其退出。
    _trace_resume_evt.set()
    # 进程内共享的 trace_days 解析缓存按 shop_id 存, 新集团店铺 ID 不冲突, 无需清空;
    # 但旧集团文件留在磁盘无害(不会被新店铺 ID 读到)。如需回收可删 trace_days 目录。


def sync_shops_from_tanyu():
    """从探域 brief 接口抓取当前集团的全部店铺, 写入 shops.json"""
    # 集团/店铺集合将变化: 先取消进行中的核算(店铺数据即将失效, 旧 worker 结果会串组),
    # 避免旧 worker 继续抓取旧集团店铺。取消保留已完成店铺的部分结果供前端展示。
    with _lock:
        if _trace_state["running"]:
            _trace_state["canceled"] = True
            _trace_state["paused"] = False
    if _trace_state.get("canceled"):
        _trace_resume_evt.set()  # 若 worker 阻塞在暂停等待中, 唤醒使其退出
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
        # 店铺集合已变: 同步 SQLite 店铺维度表, 并清掉核算缓存, 避免旧集团结果串到新集团
        try:
            trace_store.upsert_shops(shops)
        except Exception as e:
            log_line("db", f"shops 同步失败: {e}")
        _invalidate_audit_caches()
        log_line("group", f"店铺同步完成: {len(shops)} 家")
        return {"count": len(shops), "platforms": {p: sum(1 for s in shops if s["platform"] == p) for p in set(s["platform"] for s in shops)}}
    except Exception as e:
        raise RuntimeError(f"同步店铺失败: {e}")


# ---------- 客服昵称同步 ----------
def _sync_staff_names_worker(start_day, end_day):
    """后台线程: 逐集团逐店抓 customer-service/table, 提取 serviceAccount → 昵称/店铺名

    时序与 refresh_all_groups_async 相同: 逐集团 switch_group → 该集团店铺 →
    逐店 POST customer-service/table(section=service) → 下一集团。结束后恢复进入前的
    激活集团。风控/登录失效立即整体停止。结果写 data/staff_names.json(无 cookie 值)。
    """
    cfg = load_config()
    platform_order = cfg.get("prefetch_platforms") or [1, 5, 7]
    plat_rank = {p: i for i, p in enumerate(platform_order)}
    ordered = sorted(
        [g for g in cfg.get("groups", []) if g.get("groupId") and g.get("accountId")],
        key=lambda g: plat_rank.get(g.get("platform"), 99),
    )
    if not ordered:
        log_line("staff", "⚠️ config.groups 为空, 无可同步的客服昵称")
        _staff_sync_state["error"] = "无可用集团"
        _staff_sync_state["running"] = False
        return
    orig_gid = cfg.get("cookies", {}).get("tanyu-group-id")
    name_map = {}
    total_plan = 0
    group_plan = []
    try:
        for g in ordered:
            if _risk_state.get("triggered"):
                log_line("staff", "⛔ 风控/登录失效, 客服昵称同步整体停止")
                _staff_sync_state["error"] = _risk_state.get("reason") or "风控/登录失效"
                return
            try:
                switch_group(g["groupId"])
            except RiskTriggered:
                log_line("staff", "⛔ 风控触发于切换集团, 整体停止")
                _staff_sync_state["error"] = _risk_state.get("reason") or "风控/登录失效"
                return
            except Exception as e:
                log_line("staff", f"⚠️ 集团「{g.get('groupName')}」切换失败, 跳过: {e}")
                continue
            # 该集团店铺(SQLite 表含全部集团店铺, 按集团平台过滤, 复用 _today_target_shops 修复逻辑)
            try:
                shops = [s for s in trace_store.get_shops() if s.get("platform") == g.get("platform")]
            except Exception:
                shops = []
            if not shops:
                log_line("staff", f"集团「{g.get('groupName')}」无抓取平台店铺, 跳过")
                continue
            group_plan.append((g, shops))
            total_plan += len(shops)
        if not group_plan:
            log_line("staff", "⚠️ 无任何集团店铺可同步")
            _staff_sync_state["error"] = "无店铺可同步"
            return
        _staff_sync_state["progress"] = {"done": 0, "total": total_plan, "current": ""}
        done = 0
        for g, shops in group_plan:
            if _risk_state.get("triggered"):
                log_line("staff", "⛔ 风控/登录失效, 同步停止")
                _staff_sync_state["error"] = _risk_state.get("reason") or "风控/登录失效"
                return
            for i, shop in enumerate(shops, 1):
                if _risk_state.get("triggered"):
                    log_line("staff", "⛔ 风控/登录失效, 同步停止")
                    _staff_sync_state["error"] = _risk_state.get("reason") or "风控/登录失效"
                    return
                _staff_sync_state["progress"]["current"] = (
                    f"集团[{g.get('groupName')}] {shop.get('platformName', '')} · "
                    f"{shop.get('shopName', '')} ({done + 1}/{total_plan})")
                if done > 0 or i > 1:
                    sleep_trace_shop()
                try:
                    rows = _fetch_staff_table_rows(shop, start_day, end_day)
                    plat = shop.get("platform")
                    for r in rows:
                        acct = r.get("serviceAccount")
                        # 跳过汇总行与无冒号的异常条目(客服账号必有 ':' 分隔,
                        # 无冒号的 'cs_<sellerId>' 是接口返回的非客服行, 永不匹配 byStaff)
                        if not acct or acct == "__SUMMARY__" or ":" not in acct:
                            continue
                        sv = r.get("serviceName") or ""
                        nick, shop_nm = _parse_staff_service_name(sv, plat)
                        name_map[acct] = {
                            "nick": nick,
                            "shopName": shop_nm,
                            "serviceName": sv,
                            "platform": plat,
                        }
                except RiskTriggered as e:
                    log_line("staff", f"⛔ 风控触发于 {shop.get('shopName', '')}: {e}")
                    _staff_sync_state["error"] = f"风控/登录失效, 已停止: {e}"
                    return
                except Exception as e:
                    log_line("staff", f"{shop.get('shopName', '')} 客服昵称抓取失败: {e}")
                done += 1
                _staff_sync_state["progress"]["done"] = done
        # 落盘(带抓取时间戳; 绝不写 cookie)
        _atomic_write_text(STAFF_NAMES_FILE, json.dumps(
            {"fetched_at": time.time(), "map": name_map}, ensure_ascii=False, indent=2))
        _staff_sync_state["last_run"] = time.time()
        log_line("staff", f"客服昵称同步完成: {len(name_map)} 个客服账号")
    except Exception as e:
        _staff_sync_state["error"] = str(e)
    finally:
        _staff_sync_state["running"] = False
        # 恢复原激活集团(风控时不强行切换)
        if orig_gid and not _risk_state.get("triggered"):
            try:
                if orig_gid != load_config().get("cookies", {}).get("tanyu-group-id"):
                    switch_group(orig_gid)
                    log_line("staff", f"已恢复原激活集团 {orig_gid}")
            except Exception as e:
                log_line("staff", f"⚠️ 恢复原集团失败: {e}")


def _fetch_staff_table_rows(shop, start_day, end_day):
    """拉单个店铺的客服表(section=service, 实际客服数据; operations 全 0 无数据)"""
    payload = {
        "startDate": start_day,
        "endDate": end_day,
        "platform": shop.get("platform"),
        "shopId": shop["thirdShopId"],
        "section": "service",
        "metricKeys": ["service_consult_cnt"],
    }
    data = post_api("/customer-service/table", payload)
    rows = (data or {}).get("rows") or []
    return rows


def _parse_staff_service_name(sv, platform):
    """从 serviceName 拆出客服昵称与店铺展示名

    拼多多(1): '易方配件专营店:哆啦A梦' -> (哆啦A梦, 易方配件专营店)
    抖音(5)/京东(7): '小罗' -> (小罗, '')  (serviceName 仅昵称)
    """
    if not sv:
        return "", ""
    if ":" in sv:
        a, b = sv.split(":", 1)
        if not b.strip().isdigit() and b.strip():
            # 冒号前是店铺展示名(拼多多), 冒号后是昵称
            return b.strip(), a.strip()
    return sv.strip(), ""


def sync_staff_names_from_tanyu():
    """触发客服昵称同步(异步后台线程); 互斥检查: 核算/刷新/今日进行中不可同时跑"""
    if _staff_sync_state.get("running"):
        raise RuntimeError("客服昵称同步任务进行中")
    try:
        _assert_no_running_task()
    except RuntimeError as e:
        raise RuntimeError(f"其他任务进行中, 请稍后再试: {e}")
    _staff_sync_state["running"] = True
    _staff_sync_state["error"] = None
    # 用"昨天~昨天"(客服表按天, 一天足矣; 该接口反映当前客服成员)
    end_day = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
    threading.Thread(target=_sync_staff_names_worker, args=(end_day, end_day), daemon=True).start()
    return {"ok": True, "message": "客服昵称同步已启动"}


@app.get("/api/staff/names")
def staff_names():
    """客服昵称映射: {fetched_at, map, count}(供客服池渲染昵称/店铺名)"""
    data = _load_staff_names()
    if not data:
        return {"fetched_at": None, "map": {}, "count": 0}
    name_map = data.get("map") or {}
    return {"fetched_at": data.get("fetched_at"), "map": name_map, "count": len(name_map)}


@app.get("/api/staff/shops")
def staff_shops(platform: int | None = None):
    """跨集团全量店铺(客服池店铺筛选下拉的数据源, 不依赖当前激活集团)

    与 /api/shops 不同: /api/shops 读 shops.json(当前激活集团), 客服池查询走
    SQLite 跨集团表(trace_store.get_shops), 下拉必须与该作用域一致。
    """
    try:
        shops = trace_store.get_shops()
    except Exception:
        shops = []
    for s in shops:
        s["platformName"] = PLATFORM_NAMES.get(s.get("platform"), str(s.get("platform")))
    if platform is not None:
        shops = [s for s in shops if s.get("platform") == platform]
    shops.sort(key=lambda s: (s.get("platformName") or "", s.get("shopName") or ""))
    return {"shops": shops}


@app.post("/api/staff/names/sync")
def staff_names_sync():
    """手动触发客服昵称同步(异步)"""
    try:
        r = sync_staff_names_from_tanyu()
    except RuntimeError as e:
        raise HTTPException(409, str(e))
    return r


@app.get("/api/staff/names/status")
def staff_names_status():
    """客服昵称同步任务状态"""
    with _lock:
        return dict(_staff_sync_state)


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


class RiskTriggered(RuntimeError):
    """风控/登录失效信号, 触发后任务立即停止; 继承 RuntimeError 使各端点
    except RuntimeError 能统一兜底(返回 502/503), 避免漏网 500"""


def _set_risk(reason, code=None):
    with _lock:
        if not _risk_state["triggered"]:
            _risk_state["triggered"] = True
            _risk_state["reason"] = reason
            _risk_state["at"] = time.time()
            _risk_state["last_code"] = code
    log_line("risk", f"⚠️ 检测到风控/登录失效信号: {reason} — 已停止所有抓取任务, 请重新登录")


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
    """任务互斥: 核算 / 数据刷新 / 今日抓取 不能同时跑"""
    if _refresh_state.get("running"):
        raise RuntimeError("数据刷新任务进行中, 请稍后再试")
    if _trace_state.get("running"):
        raise RuntimeError("核算任务进行中, 请稍后再试")
    if _today_state.get("running"):
        raise RuntimeError("今日数据抓取任务进行中, 请稍后再试")


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


# 客服昵称同步(人工客服账号池: 真实昵称 + 子账号)
# 数据源: customer-service/table 经营罗盘(实测确认)
#   拼多多(1): serviceAccount='cs_340410493:154573412' serviceName='易方配件专营店:哆啦A梦'(店铺名:昵称)
#   抖音(5)/京东(7): serviceAccount='48653908:小罗' serviceName='小罗'(仅昵称)
# 每店含 '__SUMMARY__' 汇总行(跳过); section 用 'service'(operations 全 0 无数据)
CUSTOMER_SERVICE_TABLE_API = API_BASE + "/customer-service/table"
STAFF_NAMES_FILE = DATA_DIR / "staff_names.json"
STAFF_NAMES_TTL = 6 * 3600  # 同步结果有效期(小时级刷新即可, 客服昵称低频变化)

# 客服昵称同步状态(异步任务, 独立于核算/刷新/今日)
_staff_sync_state = {
    "running": False,
    "progress": {"done": 0, "total": 0, "current": ""},
    "last_run": None,
    "error": None,
}


def _load_staff_names():
    """读本地客服昵称映射(不读 config cookie, 无敏感信息)"""
    if not STAFF_NAMES_FILE.exists():
        return None
    try:
        return json.loads(STAFF_NAMES_FILE.read_text(encoding="utf-8"))
    except Exception:
        return None


def _split_staff(acct):
    """拆解 sellerAccount 为账号与真实名称: '48653908:柯柯' -> 账号=48653908 名称=柯柯

    抖音(5)/京东(7) 冒号后是真实客服名; 拼多多(1) 'cs_340410493:154573412'
    冒号后是数字ID(接口不返回真实名), 此时无 name、账号显示完整串。
    拼多多账号形如 cs_<卖家ID>:<客服数字ID>, accountShort=cs_340410493, accountId=154573412。
    客服维度主键始终是完整 sellerAccount(同一个人名可对应多个账号)。
    """
    if not acct:
        return {"account": acct or "", "name": None, "accountShort": acct or "", "accountId": ""}
    if ":" in acct:
        a, b = acct.split(":", 1)
        if b.strip().isdigit():
            return {"account": acct, "name": None, "accountShort": a, "accountId": b.strip()}
        return {"account": acct, "name": b.strip(), "accountShort": a, "accountId": ""}
    return {"account": acct, "name": None, "accountShort": acct, "accountId": ""}


def _staff_list_from_agg(by_staff):
    """把 {account: {total, adopted}} 聚合成 byStaff 列表(含拆出的真实名称), 按 total 降序

    用本地客服昵称映射(staff_names.json)给拼多多客服补真实昵称/店铺名:
    拼多多 sellerAccount 是数字ID('cs_340410493:154573412'), 接口不返回名字,
    昵称来自 customer-service/table 的 serviceName('店铺名:客服昵称')。字段:
      nick     - 真实昵称(拼多多从映射取, 无映射则 None)
      shopName - 该客服所在店铺的展示名(拼多多从 serviceName 前缀取)
    抖音/京东 sellerAccount 已含昵称(_split_staff 的 name), 不受影响。
    """
    staff_names = _load_staff_names()
    name_map = (staff_names or {}).get("map") or {}
    out = []
    for acct, v in by_staff.items():
        meta = _split_staff(acct)
        rec = {
            "account": meta["account"],
            "name": meta["name"],
            "accountShort": meta["accountShort"],
            "total": v["total"],
            "adopted": v["adopted"],
            "rate": round(v["adopted"] / v["total"] * 100, 2) if v["total"] else 0,
        }
        # 拼接多多真实昵称/店铺名(映射键是完整 serviceAccount)
        n = name_map.get(acct)
        if n:
            rec["nick"] = n.get("nick") or rec["name"]
            if n.get("shopName"):
                rec["shopName"] = n["shopName"]
        out.append(rec)
    return sorted(out, key=lambda x: -x["total"])


def _staff_list_per_shop(by_shop, platform=None, shop_filter=None):
    """按 (店铺,客服) 组合列出客服(不去重/不跨店合并), 每项带店铺归属

    客服账号池"按店铺区分客服"的数据源: 同一客服账号在不同店铺各自成行。
    以 staff_names(在编客服)为权威, 合并消息统计:
      - 该店在编客服(含窗口内无消息)全部列出, total=消息统计(无则 0)
      - 消息中出现但不在 staff_names 的客服(新客服/已离职)也列出
    归属: 用 shops 表 seller_id 精确匹配 account 前缀(PDD 去 cs_), 而非
    serviceName 前缀(PDD 大量 serviceName 只有昵称、无店铺名前缀)。
    """
    shop_map = {s["thirdShopId"]: s for s in trace_store.get_shops()}
    # (seller_id, platform) → (shop_id, shop_name): 权威店铺归属表
    seller_shop = {}
    for sid, s in shop_map.items():
        if s.get("sellerId"):
            seller_shop.setdefault((str(s["sellerId"]), s.get("platform")),
                                   (sid, s.get("shopName") or sid))
    staff_names = _load_staff_names()
    name_map = (staff_names or {}).get("map") or {}

    def _acct_shop(acct, plat):
        pre = acct.split(":", 1)[0]
        if pre.startswith("cs_"):
            pre = pre[3:]
        return seller_shop.get((pre, plat))

    # 消息侧统计: shop_id -> {acct: {total, adopted}}
    msg_agg = {sid: dict(shop_agg) for sid, shop_agg in by_shop.items()}
    # 在编客服(权威): staff_names 按 seller_id 归属到店, 同时按平台/子集过滤
    roster = {}
    for acct, rec in name_map.items():
        plat = rec.get("platform")
        if platform is not None and plat != platform:
            continue
        hit = _acct_shop(acct, plat)
        if not hit:
            continue
        sid = hit[0]
        if shop_filter and sid not in shop_filter:
            continue
        roster.setdefault(sid, {})[acct] = rec
    combos = []
    for sid in set(msg_agg) | set(roster):
        shop = shop_map.get(sid, {})
        sp = shop.get("platform")
        if platform is not None and sp != platform:
            continue
        if shop_filter and sid not in shop_filter:
            continue
        for acct in set(msg_agg.get(sid, {})) | set(roster.get(sid, {})):
            meta = _split_staff(acct)
            v = msg_agg.get(sid, {}).get(acct, {"total": 0, "adopted": 0})
            rec = roster.get(sid, {}).get(acct) or {}
            combo = {
                "shopId": sid,
                "shopName": shop.get("shopName") or rec.get("shopName") or sid,
                "shopPlatform": sp,
                "platformName": PLATFORM_NAMES.get(sp, str(sp or "")),
                "account": acct,
                "name": meta["name"],
                "accountShort": meta["accountShort"],
                "accountId": meta["accountId"],
                "total": v.get("total", 0),
                "adopted": v.get("adopted", 0),
                "rate": round(v.get("adopted", 0) / v.get("total", 0) * 100, 2) if v.get("total") else 0,
            }
            if rec:
                combo["nick"] = rec.get("nick") or combo["name"]
                combo["serviceName"] = rec.get("serviceName")
            combos.append(combo)
    combos.sort(key=lambda c: (c.get("shopName") or "", -(c.get("total") or 0)))
    return combos


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
    staff_list = _staff_list_from_agg(by_staff)
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


def _shop_platform(shop_id):
    """查店铺平台: 先看当前 shops 列表, 找不到回退 shop_seller 缓存"""
    try:
        for s in load_shops():
            if s.get("thirdShopId") == shop_id:
                return s.get("platform", 0)
    except Exception:
        pass
    return 0


def _upsert_shop_day_db(shop_id, day_str, msgs):
    """抓取成功后把该店当天消息同步进 SQLite(平台从店铺列表取)"""
    platform = _shop_platform(shop_id)
    trace_store.upsert_shop_day(shop_id, platform, day_str, msgs)


def stat_trace_daily(shop_id, start, end, force=False, force_days=None):
    """按天遍历消息轨迹, 逐日缓存, 支持增量更新

    - 已缓存的天直接复用(零请求)
    - 未缓存的天按天抓取
    - force=True 时忽略缓存重新抓取全部天(用于"重新抓取"按钮)
    - force_days: 这些天即使缓存有效也强制重抓(如"昨天"防采纳口径漂移:
      tanyu 会对已抓消息回溯更新 sendType, 不重抓则昨天采纳数停在预抓时刻)
    - 返回聚合 stat(messages/daily 齐全, 供折线图/核算/原始消息)
    """
    force_days = force_days or set()
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
        if not force and ds not in force_days and _cached_days_usable(cache, ds):
            total_results.extend(cached_days[ds])
            continue
        try:
            res = [_trim_trace_msg(r) for r in _fetch_trace_day(shop_id, ds)]
        except RiskTriggered:
            raise
        except Exception as e:
            log_line("trace", f"{shop_id} {ds} 抓取失败: {e}")
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
        # 同步写入 SQLite(30 天滚动窗口), 供核算/展示纯本地聚合
        try:
            _upsert_shop_day_db(shop_id, ds, res)
        except Exception as e:
            log_line("db", f"{shop_id} {ds} 写入失败: {e}")
        _trace_days_cache[shop_id] = (time.time(), {
            "fetched_at": time.time(),
            "day_fetched_at": day_fetched_at,
            "days": cached_days,
        })
        log_line("trace", f"{shop_id} {ds} 抓取完成 {len(res)} 条")

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
    staff_list = _staff_list_from_agg(by_staff)
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


# ---------- 历史周(本地 SQLite 聚合) ----------
# tanyu /summary 忽略日期永远只给当前周(已实测), 历史周只能从本地 trace_daily
# 聚合(消息量/采纳数/采纳率)。纯本地零上游请求, 无需风控/限速。
# 历史周本地指标(前端据此渲染), 无 tanyu 专属指标(unavailable)。

def _week_anchor_range(anchor):
    """周一锚点 -> (start, end) 周区间"""
    ws = datetime.date.fromisoformat(anchor)
    return ws.isoformat(), (ws + datetime.timedelta(days=6)).isoformat()


def _history_week_summary(platform, week_anchor, shop_filter=None):
    """本地聚合指定历史周的平台/店铺子集汇总。

    返回 overview 同构 dict(source="history", items 含消息量/采纳数/采纳率,
    coverage 含缺天统计) 或 None(该周无任何本地数据)。
    shop_filter: 可选店铺子集(集合), 只聚合勾选店铺(账号池模式)。
    """
    ws, we = _week_anchor_range(week_anchor)
    today_str = datetime.date.today().isoformat()
    if ws <= today_str <= we:
        return None  # 当前周走 tanyu summary, 不落历史分支
    agg = trace_store.overview_aggregate(ws, we, platform, shop_filter)
    shop_list = agg["shop_list"]
    staff_agg = agg["staff_agg"]
    if not shop_list:
        return None
    total_msgs = sum(v["total"] for _, v in shop_list)
    total_adopted = sum(v["adopted"] for _, v in shop_list)
    # 缺天统计(仅该平台): 复用 week_coverage
    wc = trace_store.week_coverage(platform=platform)
    cov = next((w for w in wc if w["week_start"] == week_anchor), None)
    shop_total = dict(shop_list)
    items = {
        "history_msg_total": {"current": total_msgs, "previous": 0, "comparePercent": None, "label": "消息量"},
        "history_adopted_total": {"current": total_adopted, "previous": 0, "comparePercent": None, "label": "采纳数"},
        "history_accept_rate": {"current": (round(total_adopted * 100.0 / total_msgs, 2) if total_msgs else 0), "previous": 0, "comparePercent": None, "label": "采纳率"},
    }
    coverage = {
        "days": cov["days"] if cov else 7,
        "shops": len(shop_list),
        "missing": {sid: miss for sid, miss in (cov["missing"] if cov else {}).items() if sid in shop_total},
    }
    return {
        "statType": "natural_week",
        "startDate": ws,
        "endDate": we,
        "platform": platform,
        "platformName": PLATFORM_NAMES.get(platform),
        "source": "history",
        "items": items,
        "coverage": coverage,
    }


@app.get("/api/history/weeks")
def history_weeks(platform: int | None = None):
    """可回溯的历史周列表(基于本地 trace_daily 覆盖), 供历史周下拉。

    纯本地零上游请求; 返回按周降序的 [{start: 周一锚点, end, label, days}]。
    平台可选过滤。仅返回有数据的周(不含当前周——当前周走 tanyu summary)。
    """
    today = datetime.date.today()
    cur_ws = today - datetime.timedelta(days=today.weekday())
    weeks = trace_store.week_coverage(platform=platform)
    out = []
    for w in weeks:
        if w["week_start"] == cur_ws.isoformat():
            continue  # 当前周不在下拉(走 tanyu summary)
        if not w["shop_days"]:
            continue
        out.append({
            "start": w["week_start"],
            "end": w["week_end"],
            "days": w["days"],
            "shops": len(w["shop_days"]),
        })
    return {"weeks": out}


@app.get("/api/overview")
def overview(platform: int = 1, stat_type: str = "natural_day", start: str | None = None, end: str | None = None,
             week: str | None = None):
    """平台维度汇总, 按统计口径(自然日/自然周/自然月)。

    tanyu summary 固定忽略 startDate/endDate: 三种口径分别返回 昨天vs前天 /
    本周vs上周 / 本月vs上月。days 参数已废弃(曾误导为可调范围), 仅保留
    start/end 透传给 tanyu(无实际作用, 但保接口兼容)。

    week(可选, 仅 natural_week): 周一锚点 YYYY-MM-DD。传入历史周时改用本地
    SQLite 聚合(source="history", 消息量/采纳数/采纳率 + coverage), 因为 tanyu
    summary 忽略日期永远只给当前周(实测); 不传或传当前周走 tanyu summary。
    """
    if platform not in PLATFORM_NAMES:
        raise HTTPException(400, f"不支持的平台: {platform}")
    if not _valid_stat_type(stat_type):
        raise HTTPException(400, f"不支持的统计口径: {stat_type}")
    # 未显式传区间时按口径给默认值(周=本周一~今天, 月=本月1日~今天, 日=昨天)
    _start, _end = _stat_type_range(stat_type)
    start = _valid_date_iso(start) or _start
    end = _valid_date_iso(end) or _end
    # 历史周分支: 传了 week 且非当前周 => 本地 SQLite 聚合(零上游请求)
    if stat_type == "natural_week" and week and _valid_date_iso(week):
        hist = _history_week_summary(platform, week)
        if hist is not None:
            return hist
    payload = {
        "statType": stat_type,
        "startDate": start,
        "endDate": end,
        "platform": platform,
        "dimension": "platform",
    }
    try:
        summary = fetch_summary_interactive(payload)
    except RiskTriggered:
        raise HTTPException(503, "风控触发, 请先登录更新Cookie")
    except BusyQueueError as e:
        raise HTTPException(503, str(e))
    except RuntimeError as e:
        raise HTTPException(502, str(e))
    return {
        "startDate": start,
        "endDate": end,
        "statType": stat_type,
        "platform": platform,
        "platformName": PLATFORM_NAMES.get(platform),
        "items": summary,
    }


@app.get("/api/shop/{shop_id}")
def shop_detail(shop_id: str, stat_type: str = "natural_day", start: str | None = None, end: str | None = None,
                tables: int = 1, week: str | None = None):
    """单店汇总 + 明细(带本地缓存); 按统计口径(自然日/自然周/自然月)。

    tanyu summary 固定忽略 startDate/endDate(见 SUMMARY_STAT_TYPES 注释);
    明细表(section/table)对日期敏感, natural_week/natural_month 时按传入的
    周/月区间定位。days 参数已废弃, 保留 start/end 透传。
    tables=0: 只返回 summary(店铺列表用, 跳过明细表请求, 省上游配额)。

    week(可选, 仅 natural_week): 周一锚点。历史周时走本地 SQLite 聚合
    (source="history"), 返回该店该周消息量/采纳数/采纳率 + missing_days;
    tanyu 专属指标标记 unavailable。当前周不传/传当前周走 tanyu summary。
    """
    shops = {s["thirdShopId"]: s for s in load_shops()}
    shop = shops.get(shop_id)
    if not shop:
        raise HTTPException(404, "店铺不存在")
    if not _valid_stat_type(stat_type):
        raise HTTPException(400, f"不支持的统计口径: {stat_type}")

    # 未显式传区间时按口径给默认值(与 /api/overview 一致)
    _start, _end = _stat_type_range(stat_type)
    start = _valid_date_iso(start) or _start
    end = _valid_date_iso(end) or _end

    # 历史周分支: 本地 SQLite 聚合(零上游请求)
    if stat_type == "natural_week" and week and _valid_date_iso(week):
        hist = _history_week_shop(shop, week)
        if hist is not None:
            return hist
    # 平台/店铺自然日数据一天内基本不变, 缓存 6 小时; 页面加载/切平台走交互快通道, 不排队
    # 缓存键按口径拆维度: 三口径各用独立后缀键({id}__natural_day/week/month), 防批量刷新串写
    cache_key = f"{shop_id}__{stat_type}"
    cache = load_cache("summary", cache_key, max_age=21600)
    if cache and cache.get("data"):
        items = cache["data"]
        fetched_at = cache["fetched_at"]
    else:
        payload = {
            "statType": stat_type,
            "startDate": start,
            "endDate": end,
            "platform": shop.get("platform", 1),
            "dimension": "shop",
            "targetId": shop_id,
        }
        try:
            items = fetch_summary_interactive(payload)
        except RiskTriggered:
            raise HTTPException(503, "风控触发, 请先登录更新Cookie")
        except BusyQueueError as e:
            raise HTTPException(503, str(e))
        except RuntimeError as e:
            raise HTTPException(502, str(e))
        save_cache("summary", cache_key, {"fetched_at": time.time(), "data": items})
        fetched_at = time.time()

    tables_data = {}
    if tables:
        for section in ["operations", "service", "ai"]:
            # 明细表键带日期区间(与 refresh_one 批量刷新写键一致): 日期敏感的
            # /section/table 数据严格绑定其起止日期, 周/月明细不会与自然日串写。
            table_key = f"{shop_id}__{stat_type}__{section}__{start}__{end}"
            c = load_cache("table", table_key, max_age=21600)
            if c and c.get("data"):
                tables_data[section] = c["data"]
            else:
                payload = {
                    "statType": stat_type,
                    "startDate": start,
                    "endDate": end,
                    "platform": shop.get("platform", 1),
                    "dimension": "shop",
                    "targetId": shop_id,
                    "section": section,
                }
                try:
                    tables_data[section] = fetch_section_table_interactive(payload)
                    # 抓取成功即写 6h 缓存, 与读取键一致形成闭环(否则周/月每次必 miss)
                    save_cache("table", table_key,
                               {"fetched_at": time.time(), "data": tables_data[section]})
                except BusyQueueError as e:
                    raise HTTPException(503, str(e))
                except RiskTriggered:
                    raise HTTPException(503, "风控触发, 请先登录更新Cookie")
                except RuntimeError as e:
                    # 单个 section 上游失败: 记录日志, 返回空表(该区间可能确实无数据), 不打断其余 section
                    log_line("shop_detail", f"shop={shop_id} section={section} 失败: {e}")
                    tables_data[section] = {"dates": [], "rows": []}

    return {
        "shop": shop,
        "startDate": start,
        "endDate": end,
        "statType": stat_type,
        "items": items,
        "fetchedAt": fetched_at,
        "tables": tables_data,
    }


def _history_week_shop(shop, week_anchor):
    """本地聚合单个店铺的历史周数据(纯只读, 零上游请求)。

    返回与 shop_detail 同构的 dict(source="history"), 含该店周消息量/采纳数/
    采纳率 + missing_days(缺天列表); tanyu 专属指标标记 unavailable。
    该周该店无任何数据时返回 None(调用方走 tanyu summary 兜底, 行为不变)。
    """
    ws, we = _week_anchor_range(week_anchor)
    today_str = datetime.date.today().isoformat()
    if ws <= today_str <= we:
        return None  # 当前周走 tanyu summary
    shop_id = shop["thirdShopId"]
    platform = shop.get("platform", 1)
    daily = trace_store.query_daily(shop_id, ws, we)
    if not daily:
        return None
    total = sum(d["total"] for d in daily)
    adopted = sum(d["adopted"] for d in daily)
    have_days = {d["day"] for d in daily}
    # 缺天: 周区间应覆盖天数 vs 实际天数
    missing = []
    for i in range(7):
        ds = (datetime.date.fromisoformat(ws) + datetime.timedelta(days=i)).isoformat()
        if ds not in have_days:
            missing.append(ds)
    items = {
        "history_msg_total": {"current": total, "previous": 0, "comparePercent": None, "label": "消息量"},
        "history_adopted_total": {"current": adopted, "previous": 0, "comparePercent": None, "label": "采纳数"},
        "history_accept_rate": {"current": (round(adopted * 100.0 / total, 2) if total else 0), "previous": 0, "comparePercent": None, "label": "采纳率"},
    }
    # tanyu 专属指标历史不可用(前端显示"历史不可用")
    for key in ("service_3m_response_rate", "ai_consult_response_accept_rate", "ai_consult_response_rate"):
        items[key] = {"unavailable": True, "label": METRIC_BY_KEY[key]["title"]}
    return {
        "shop": shop,
        "startDate": ws,
        "endDate": we,
        "statType": "natural_week",
        "items": items,
        "fetchedAt": time.time(),
        "source": "history",
        "missing_days": missing,
        "coverage": {"days": 7, "have": len(have_days)},
    }


@app.get("/api/refresh")
def refresh():
    """触发后台刷新全部店铺数据(轮询三个集团, 恢复原激活集团)"""
    # 任务互斥: 核算进行中不允许同时刷新
    try:
        _assert_no_running_task()
    except RuntimeError as e:
        raise HTTPException(409, str(e))
    start, end = date_range(7)
    refresh_all_groups_async(start, end)
    return {"ok": True, "message": "刷新任务已启动(三集团轮询)"}


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
    today_str = datetime.date.today().isoformat()
    # 区间含今天: 历史(库) + 今天(实时) 合并, 保留实时数据
    if (not force and _use_sqlite_trace()
            and start <= today_str <= end):
        try:
            merged = _trace_shop_merged(shop_id, start, end)
            if merged and merged[0]:
                stat, has_today = merged
                return {"shop": shop, "startDate": start, "endDate": end,
                        "fetchedAt": time.time(), "stat": stat,
                        "live": has_today}
        except RiskTriggered:
            raise HTTPException(502, "风控/登录失效, 请重新登录后重试")
        except Exception as e:
            print(f"[trace] SQLite+实时 合并路径失败, 回退在线抓取: {e}")
    # SQLite 快路径: 数据库已覆盖该区间(纯历史) => 纯本地聚合(核算提速主因)
    if (not force and _use_sqlite_trace()
            and trace_store.db_window_covers(start, end)):
        try:
            stat = _trace_shop_from_db(shop_id, start, end)
            if stat:
                return {"shop": shop, "startDate": start, "endDate": end,
                        "fetchedAt": time.time(), "stat": stat}
        except Exception as e:
            print(f"[trace] SQLite 单店快路径失败, 回退在线抓取: {e}")
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
    "result": None,  # 最近一次完成的全量核算结果
    "subset_result": None,  # 最近一次店铺子集核算结果(不覆盖全量视图, 不落盘)
    "start_date": None,
    "end_date": None,
    "platform": None,
    "last_run": None,
    "error": None,
    "paused": False,       # 核算暂停标志(worker 在每店边界阻塞等待恢复)
    "canceled": False,     # 取消核算标志(worker 中断后续店铺)
    "partial_list": [],    # 运行中已完成店铺的统计(浅拷贝), 供前端边算边展示
    "shop_filter": None,   # 本次核算的店铺子集(账号池勾选), None=全部店铺
}
_trace_resume_evt = threading.Event()

# 今日实时抓取状态(独立于核算: 抓今天不写库, 供核算/人工客服板块「抓取今日数据」)
_today_state = {
    "running": False,
    "progress": {"done": 0, "total": 0, "current": ""},
    "result": None,
    "start_date": None,
    "end_date": None,
    "start_ts": "00:00",
    "end_ts": "",
    "platform": None,
    "shop_filter": None,
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


def _trace_shop_from_db(shop_id, start, end):
    """从 SQLite 聚合单店 stat(与 stat_trace_daily 输出结构完全一致)"""
    start_ms = int(datetime.datetime.fromisoformat(start).timestamp() * 1000)
    # end 23:59:59.999 转毫秒
    end_d = datetime.datetime.fromisoformat(end)
    end_ms = int((end_d + datetime.timedelta(days=1)).timestamp() * 1000) - 1
    rows = trace_store.query_shop_aggregate(shop_id, start, end, start_ms, end_ms)
    if not rows and not trace_store.query_daily(shop_id, start, end):
        return None
    # 复用 stat_trace_daily 的聚合逻辑(保证口径一致)
    return _aggregate_stat_rows(rows, start, end)


def _trace_shop_merged(shop_id, start, end):
    """历史天走 SQLite + 今天实时抓取, 合并出 [start, end] 的单店 stat

    口径: 完整历史天(含昨天)读库聚合(与探域逐日口径一致);
      "今天"未定型, 实时抓取当天消息(不走 6h 缓存), 只读不写库。
    返回 (stat_dict, has_today_flag); 实时抓取抛 RiskTriggered 时向上传播。
    """
    today_str = datetime.date.today().isoformat()
    yesterday_str = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
    has_today = start <= today_str <= end
    # 历史部分: start .. min(end, 昨天)(ISO 日期字符串字典序可比较)
    hist_end = end if end < today_str else yesterday_str
    rows = []
    if start <= hist_end:
        rows = trace_store.query_shop_aggregate(
            shop_id, start, hist_end,
            int(datetime.datetime.fromisoformat(start).timestamp() * 1000),
            int((datetime.datetime.fromisoformat(hist_end) + datetime.timedelta(days=1)).timestamp() * 1000) - 1,
        )
    # 今天实时(未定型日不进库, 只读合并)
    live = []
    if has_today:
        live_res = _fetch_trace_day(shop_id, today_str)
        if live_res is None:
            live_res = []  # 今天暂无数据/请求失败, 历史部分照常返回
        live = [_trim_trace_msg(m) for m in live_res]
        rows.extend(live)
    # 区间含今天时恒返回 stat(即使全空), 避免回退到会写库的 stat_trace_daily;
    # 纯历史且无数据才返回 None(由调用方走在线抓取兜底)
    if not has_today and not rows and not live:
        return None, False
    return _aggregate_stat_rows(rows, start, end), has_today


def _aggregate_stat_rows(results, start, end):
    """按 stat_trace_daily 完全相同的口径聚合(独立函数, 供 JSON/SQLite 两路共用)"""
    counts = {1: 0, 2: 0, 3: 0, None: 0}
    by_staff = {}
    by_type = {}
    daily = {}
    total = 0
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
    staff_list = _staff_list_from_agg(by_staff)
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


def _trace_overview_from_db(start, end, platform=None, shop_filter=None):
    """从 SQLite trace_daily 聚合核算总览(与在线 worker 输出结构完全一致)

    shop_filter: 可选店铺子集(集合), 只聚合勾选的店铺。
    店铺池 = 跨集团店铺表(全部店铺)按 platform/shop_filter 过滤后的全集:
    零消息店铺(total=0)也保留在 shopList 里, 与在线 worker 口径一致。
    """
    agg = trace_store.overview_aggregate(start, end, platform, shop_filter)
    # 用跨集团店铺表(全部 118 家)解析店铺元数据并枚举店铺全集, 而非当前激活
    # 集团的 shops.json(否则非当前集团店铺 platform 为 None、无法展示平台标签)
    shop_map = {s["thirdShopId"]: s for s in trace_store.get_shops()}
    scope = [s for s in shop_map.values() if s.get("platform") in FETCH_PLATFORMS]
    if platform is not None:
        scope = [s for s in scope if s.get("platform") == platform]
    if shop_filter:
        scope = [s for s in scope if s["thirdShopId"] in shop_filter]
    if not scope:
        return None
    data = {sid: v for sid, v in agg["shop_list"]}
    shop_list = []
    agg_total = agg_adopted = 0
    for shop in scope:
        v = data.get(shop["thirdShopId"], {"total": 0, "adopted": 0})
        shop_list.append({"shop": shop, "total": v["total"],
                          "adopted": v["adopted"],
                          "rate": round(v["adopted"] / v["total"] * 100, 2) if v["total"] else 0,
                          "startDate": start, "endDate": end})
        agg_total += v["total"]
        agg_adopted += v["adopted"]
    # 客服按 (店铺,客服) 组合区分(不去重), 用同一区间/店铺集的按店聚合
    per_shop = trace_store.staff_aggregate_per_shop(start, end, platform, shop_filter)
    staff_list = _staff_list_per_shop(per_shop["by_shop"], platform, shop_filter)
    return {
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


@app.get("/api/trace/overview")
def trace_overview(days: int = 7, platform: int | None = None, force: int = 0,
                   start: str | None = None, end: str | None = None,
                   from_cache: int = 0, shop_ids: str | None = None):
    """核算总览(异步): 遍历抓取店铺统计核算采纳率

    支持自定义时间段(start/end, YYYY-MM-DD); 不传则用近 N 天。
    platform: 平台筛选(只核算该平台店铺)。
    shop_ids: 逗号分隔的店铺 ID 子集(账号池勾选后只核算勾选店铺)。
    has_shop_filter: 勾选了店铺子集时强制走在线路径(不命中全平台缓存)。
    有缓存直接返回; 否则触发后台任务, 返回任务状态,
    前端轮询 /api/trace/overview/status 获取进度, 完成后取 result。
    from_cache=1: 只查内存+磁盘缓存, 未命中直接返回空(不触发任务)
    """
    global _trace_state
    # 店铺子集: 解析 shop_ids 参数(逗号分隔)
    shop_filter = None
    if shop_ids and shop_ids.strip():
        shop_filter = {s for s in shop_ids.split(",") if s.strip()}
    # 任务互斥: 核算与数据刷新不能同时跑(避免叠加请求量)
    if not from_cache:
        try:
            _assert_no_running_task()
        except RuntimeError as e:
            raise HTTPException(409, str(e))
    start, end = date_range(days, end) if not start else (start, end or (datetime.date.today() - datetime.timedelta(days=1)).isoformat())
    # 店铺子集核算不复用全量缓存(勾选不同店铺结果不同)
    no_subset_cache = bool(shop_filter)
    # 内存缓存命中(需平台一致 + 未过期 + 结果自身时间段与请求一致, 防止跨范围误命中)
    mem_result = _trace_state.get("result")
    if (not force and not no_subset_cache and mem_result
            and _trace_state["start_date"] == start and _trace_state["end_date"] == end
            and _trace_state["platform"] == platform
            and mem_result.get("startDate") == start and mem_result.get("endDate") == end
            and _trace_state.get("last_run")
            and time.time() - _trace_state["last_run"] < TRACE_OVERVIEW_CACHE_TTL):
        return {"status": "done", "startDate": start, "endDate": end, "result": mem_result}
    # 磁盘缓存命中(服务重启后同时间段重复核算零请求)
    disk = load_trace_overview_cache(start, end, platform) if not force and not no_subset_cache else None
    if disk:
        _trace_state["result"] = disk
        _trace_state["start_date"] = start
        _trace_state["end_date"] = end
        _trace_state["platform"] = platform
        _trace_state["last_run"] = time.time()
        print(f"[trace] 命中磁盘缓存 {start}~{end}, 跳过抓取 (共{disk.get('total', 0)}条)")
        return {"status": "done", "startDate": start, "endDate": end, "result": disk}
    # SQLite 快路径: 数据库已覆盖该区间 => 纯本地聚合, 零上游请求(核算提速主因)
    # 店铺子集/跨平台核算同样走 DB 快路径(预抓已把三集团数据都入库, 无需切集团)
    if (not force and _use_sqlite_trace()
            and trace_store.db_window_covers(start, end)):
        try:
            result = _trace_overview_from_db(start, end, platform, shop_filter)
            if result:
                if shop_filter:
                    # 店铺子集结果单独存, 不覆盖全量核算视图
                    _trace_state["subset_result"] = result
                else:
                    _trace_state["result"] = result
                _trace_state["start_date"] = start
                _trace_state["end_date"] = end
                _trace_state["platform"] = platform
                _trace_state["shop_filter"] = shop_filter
                _trace_state["last_run"] = time.time()
                print(f"[trace] SQLite 快路径 {start}~{end} (共{result['total']}条)")
                return {"status": "done", "startDate": start, "endDate": end, "result": result}
        except Exception as e:
            print(f"[trace] SQLite 快路径失败, 回退在线抓取: {e}")
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
    _trace_state["shop_filter"] = shop_filter
    _trace_state["error"] = None
    _trace_state["paused"] = False
    _trace_state["canceled"] = False
    _trace_state["partial_list"] = []
    # 新一轮核算开始: 内存中的上一次完成结果作废(取消/中断后不能再把旧结果当本次结果,
    # 且避免跨范围误命中); 磁盘缓存仅当同范围时才删除(不同范围旧结果保留供切换后加载)。
    _trace_state["result"] = None
    _trace_state["subset_result"] = None
    _trace_state["last_run"] = None
    _trace_state["progress"] = {"done": 0, "total": 0, "current": "准备中"}
    if not no_subset_cache and load_trace_overview_cache(start, end, platform):
        try:
            if TRACE_OVERVIEW_CACHE_FILE.exists():
                TRACE_OVERVIEW_CACHE_FILE.unlink()
        except Exception:
            pass

    def worker():
        try:
            shops = [s for s in load_shops() if s.get("platform") in FETCH_PLATFORMS]
            if platform is not None:
                shops = [s for s in shops if s.get("platform") == platform]
            if shop_filter is not None:
                # 店铺子集: 只核算勾选的店铺(账号池模式)
                shops = [s for s in shops if s["thirdShopId"] in shop_filter]
            total = len(shops)
            begin, end_t = f"{start} 00:00:00", f"{end} 23:59:59"
            today_str = datetime.date.today().isoformat()
            # 区间含今天 => 单店走"历史库 + 今天实时"合并(今天未定型不进库)
            range_has_today = start <= today_str <= end
            _trace_state["progress"] = {"done": 0, "total": total, "current": ""}
            shop_list = []
            agg_total = agg_adopted = 0
            staff_shop_agg = {}
            for i, shop in enumerate(shops, 1):
                # 暂停/取消检查(在店铺边界, 请求间隙)
                while _trace_state["paused"] and not _trace_state["canceled"]:
                    _trace_state["progress"]["current"] = "已暂停, 等待恢复…"
                    _trace_resume_evt.wait(timeout=1.0)
                    _trace_resume_evt.clear()  # 事件用完即清, 防止残留置位导致 wait() 忙等
                if _trace_state["canceled"]:
                    log_line("trace", f"⏹ 已取消, 已核算 {i - 1}/{total} 家")
                    break
                _trace_state["progress"]["current"] = f"{shop['platformName']} · {shop['shopName']} ({i}/{total})"
                # 该店在区间内天数全部已缓存 => 纯聚合零请求, 无需限速停顿
                if i > 1 and not days_all_cached(shop["thirdShopId"], start, end):
                    sleep_trace_shop()
                try:
                    if range_has_today:
                        # 含今天: 历史(库)+今天(实时) 合并, 不写库
                        merged = _trace_shop_merged(shop["thirdShopId"], start, end)
                        if not merged or not merged[0]:
                            # 零消息店铺也保留在池中(total=0), 展示"无消息"
                            shop_list.append(
                                {"shop": shop, "total": 0, "adopted": 0, "rate": 0,
                                 "startDate": start, "endDate": end}
                            )
                            _trace_state["progress"]["done"] = i
                            continue
                        stat = merged[0]
                    else:
                        stat = stat_trace_daily(shop["thirdShopId"], start, end)
                    shop_list.append(
                        {"shop": shop, "total": stat["total"], "adopted": stat["adopted"],
                         "rate": stat["rate"],
                         "startDate": start, "endDate": end}
                    )
                    agg_total += stat["total"]
                    agg_adopted += stat["adopted"]
                    for st in stat.get("byStaff", []):
                        shop_agg = staff_shop_agg.setdefault(shop["thirdShopId"], {})
                        entry = shop_agg.setdefault(st["account"], {"total": 0, "adopted": 0})
                        entry["total"] += st["total"]
                        entry["adopted"] += st["adopted"]
                except RiskTriggered as e:
                    # 风控/登录失效: 立即停止, 不再请求剩余店铺
                    log_line("trace", f"⛔ 风控触发, 停止剩余 {total - i} 家店铺: {e}")
                    _trace_state["error"] = f"风控/登录失效, 已停止: {e}"
                    _trace_state["progress"]["current"] = f"已停止: {e}"
                    break
                except Exception as e:
                    log_line("trace", f"{shop['shopName']} 失败: {e}")
                _trace_state["progress"]["done"] = i
                # 边算边展示: 每完成一店就发布部分结果, 前端实时渲染
                with _lock:
                    _trace_state["partial_list"] = list(shop_list)
            # 被取消/风控中断时: 不写磁盘缓存, 不覆盖 result(保持部分结果可查)
            # 仅风控(非取消)允许 partial_list 作为部分结果供前端展示, 但绝不落盘
            if _trace_state["canceled"] or _trace_state["error"]:
                log_line("trace", f"中断不写缓存: canceled={_trace_state['canceled']} error={_trace_state['error']}")
                return
            staff_list = _staff_list_per_shop(staff_shop_agg, platform, shop_filter)
            completed = {
                "startDate": start,
                "endDate": end,
                "platform": platform,
                "fetched_at": time.time(),
                "total": agg_total,
                "adopted": agg_adopted,
                "rate": round(agg_adopted / agg_total * 100, 2) if agg_total else 0,
                "shopList": shop_list,
                "byStaff": staff_list,
                # 区间含今天时标记实时(前端显示"实时"徽标)
                "live": start <= datetime.date.today().isoformat() <= end,
            }
            if shop_filter:
                # 店铺子集结果单独存, 不覆盖全量核算视图, 也不落盘
                _trace_state["subset_result"] = completed
            else:
                _trace_state["result"] = completed
                # 全量核算完成: 清掉旧子集结果, 避免 status 接口残留暴露陈旧子集数据
                _trace_state["subset_result"] = None
            _trace_state["last_run"] = time.time()
            # 店铺子集核算结果不落盘: 磁盘缓存按(时间段,平台)为键、不分店铺子集,
            # 写入子集结果会让后续同时间段的全量查询命中错误的子集数据
            if not shop_filter:
                save_trace_overview_cache(completed)
        except Exception as e:
            _trace_state["error"] = str(e)
        finally:
            _trace_state["running"] = False
            # 运行已结束: 清掉暂停标记, 避免下次查询/再核算时遗留 paused=True
            _trace_state["paused"] = False
            _trace_resume_evt.clear()  # 清事件, 避免后续暂停 wait() 立即返回造成忙等

    threading.Thread(target=worker, daemon=True).start()
    return {"status": "running", "startDate": start, "endDate": end,
            "progress": _trace_state["progress"]}


@app.get("/api/trace/staff")
def trace_staff(days: int = 7, start: str | None = None, end: str | None = None,
                platform: int | None = None, shop_ids: str | None = None):
    """客服账号池独立时间筛选: 返回该时间段内各客服的总消息/采纳条数/采纳率

    从 SQLite trace_daily 聚合(跨集团全量), 与核算口径一致(adopted=send_type IN 1,2,3)。
    任意平台/店铺子集/时间段均可查询, 不依赖当前激活集团。
    客服**按 (店铺,客服) 组合区分**(不去重/不跨店合并): 同一客服账号在不同店铺
    各自成行, 并补入该店在编但窗口内无消息的客服(来自 staff_names)。
    DB 未覆盖该区间时返回空 byStaff(前端提示数据未抓取)。
    """
    start, end = date_range(days, end) if not start else (start, end or (datetime.date.today() - datetime.timedelta(days=1)).isoformat())
    shop_filter = {s for s in shop_ids.split(",") if s.strip()} if shop_ids else None
    result = {"startDate": start, "endDate": end, "platform": platform,
              "total": 0, "adopted": 0, "rate": 0, "byStaff": []}
    if trace_store.db_window_covers(start, end):
        per_shop = trace_store.staff_aggregate_per_shop(start, end, platform, shop_filter)
        staff_list = _staff_list_per_shop(per_shop["by_shop"], platform, shop_filter)
        result["byStaff"] = staff_list
        result["total"] = sum(s["total"] for s in staff_list)
        result["adopted"] = sum(s["adopted"] for s in staff_list)
        result["rate"] = round(result["adopted"] / result["total"] * 100, 2) if result["total"] else 0
    return result


# ---------- 今日实时抓取(核算/人工客服「抓取今日数据」) ----------
def _parse_hhmm(ts, default="00:00"):
    """解析 HH:MM, 非法返回 default"""
    try:
        h, m = str(ts).strip().split(":")
        return f"{int(h):02d}:{int(m):02d}"
    except Exception:
        return default


def _today_target_shops(platform=None, shop_filter=None):
    """按平台/店铺子集解析目标店铺集合(跨全部 config.groups)

    返回 [(groupId, group, [shops...]), ...]: 每个集团一组店铺, 便于 worker 逐集团切换抓取。
    platform 为 None 时含全部抓取平台集团; shop_filter 只保留集合内的店铺。
    """
    cfg = load_config()
    platform_order = cfg.get("prefetch_platforms") or [1, 5, 7]
    plat_rank = {p: i for i, p in enumerate(platform_order)}
    ordered = sorted(
        [g for g in cfg.get("groups", []) if g.get("groupId") and g.get("accountId")],
        key=lambda g: plat_rank.get(g.get("platform"), 99),
    )
    out = []
    for g in ordered:
        if platform is not None and g.get("platform") != platform:
            continue
        # 每家集团切过去后 load_shops 才读对(switch_group 内部会 sync_shops_from_tanyu),
        # 这里用 SQLite shops 表(全部集团店铺已入库)解析, 不依赖当前激活集团
        import trace_store as _ts
        try:
            shops = _ts.get_shops()
        except Exception:
            shops = []
        if not shops:
            continue
        # SQLite shops 表含全部集团店铺; 每家集团只取本平台店铺, 否则 scoped
        # shop_filter 会被错误地塞进每个集团重复抓取(单店被抓 3 次的 bug)
        shops = [s for s in shops if s.get("platform") == g.get("platform")]
        if shop_filter is not None:
            shops = [s for s in shops if s["thirdShopId"] in shop_filter]
        if shops:
            out.append((g["groupId"], g, shops))
    return out


@app.post("/api/trace/today")
def trace_today(platform: int | None = None, shop_ids: str | None = None,
                start_ts: str = "00:00", end_ts: str | None = None):
    """抓取今日数据(实时, 不写库): 按集团切换轮询, 支持平台/店铺子集筛选

    - start_ts/end_ts: 今天内的起止时刻(HH:MM), 默认全天 00:00 ~ 当前时刻
    - shop_ids: 逗号分隔店铺子集, 空=全部店铺(含平台筛选)
    - 逐集团 switch_group → 抓今天 → 恢复原集团; 风控立即停止
    """
    try:
        _assert_no_running_task()
    except RuntimeError as e:
        raise HTTPException(409, str(e))
    today_str = datetime.date.today().isoformat()
    start_ts = _parse_hhmm(start_ts, "00:00")
    end_ts = _parse_hhmm(end_ts, datetime.datetime.now().strftime("%H:%M")) if end_ts else datetime.datetime.now().strftime("%H:%M")
    if end_ts < start_ts:
        raise HTTPException(400, "结束时刻不能早于开始时刻")
    shop_filter = {s for s in shop_ids.split(",") if s.strip()} if shop_ids else None
    groups_target = _today_target_shops(platform, shop_filter)
    if not groups_target:
        return {"status": "done", "startDate": today_str, "endDate": today_str,
                "startTs": start_ts, "endTs": end_ts,
                "result": {"startDate": today_str, "endDate": today_str,
                           "startTs": start_ts, "endTs": end_ts, "platform": platform,
                           "total": 0, "adopted": 0, "rate": 0, "byStaff": [],
                           "shopList": [], "live": True}}
    with _lock:
        _today_state["running"] = True
        _today_state["start_date"] = today_str
        _today_state["end_date"] = today_str
        _today_state["start_ts"] = start_ts
        _today_state["end_ts"] = end_ts
        _today_state["platform"] = platform
        _today_state["shop_filter"] = shop_filter
        _today_state["result"] = None
        _today_state["last_run"] = None
        _today_state["error"] = None
        _today_state["progress"] = {"done": 0, "total": 0, "current": "准备中"}
    total_plan = sum(len(shops) for _, _, shops in groups_target)
    _today_state["progress"]["total"] = total_plan
    orig_gid = load_config().get("cookies", {}).get("tanyu-group-id")

    def worker():
        try:
            shop_list = []
            staff_shop_agg = {}
            agg_total = agg_adopted = 0
            done = 0
            for gid, g, shops in groups_target:
                if _risk_state.get("triggered"):
                    log_line("today", "⛔ 风控/登录失效, 整体停止")
                    _today_state["error"] = _risk_state.get("reason") or "风控/登录失效"
                    return
                try:
                    switch_group(gid)
                except RiskTriggered:
                    log_line("today", "⛔ 风控触发于切换集团, 整体停止")
                    _today_state["error"] = _risk_state.get("reason") or "风控/登录失效"
                    return
                except Exception as e:
                    log_line("today", f"⚠️ 集团「{g.get('groupName')}」切换失败, 跳过: {e}")
                    continue
                for i, shop in enumerate(shops, 1):
                    if _risk_state.get("triggered"):
                        log_line("today", "⛔ 风控/登录失效, 停止")
                        _today_state["error"] = _risk_state.get("reason") or "风控/登录失效"
                        return
                    _today_state["progress"]["current"] = (
                        f"集团[{g.get('groupName')}] {shop.get('platformName', '')} · "
                        f"{shop.get('shopName', '')} ({done + i}/{total_plan})")
                    if done > 0 or i > 1:
                        sleep_trace_shop()
                    try:
                        res = _fetch_trace_range(
                            shop["thirdShopId"],
                            f"{today_str} {start_ts}:00",
                            f"{today_str} {end_ts}:59",
                        )
                        msgs = [_trim_trace_msg(m) for m in (res or [])]
                        stat = _aggregate_stat_rows(msgs, today_str, today_str)
                        shop_list.append(
                            {"shop": shop, "total": stat["total"], "adopted": stat["adopted"],
                             "rate": stat["rate"], "startDate": today_str, "endDate": today_str}
                        )
                        agg_total += stat["total"]
                        agg_adopted += stat["adopted"]
                        for st in stat.get("byStaff", []):
                            shop_agg = staff_shop_agg.setdefault(shop["thirdShopId"], {})
                            entry = shop_agg.setdefault(st["account"], {"total": 0, "adopted": 0})
                            entry["total"] += st["total"]
                            entry["adopted"] += st["adopted"]
                        if stat["total"]:
                            log_line("today", f"{shop.get('shopName', '')} 今日 {stat['total']} 条 / 采纳 {stat['adopted']}")
                    except RiskTriggered as e:
                        log_line("today", f"⛔ 风控触发, 停止: {e}")
                        _today_state["error"] = f"风控/登录失效, 已停止: {e}"
                        return
                    except Exception as e:
                        log_line("today", f"{shop.get('shopName', '')} 今日抓取失败: {e}")
                    done += 1
                    _today_state["progress"]["done"] = done
                    with _lock:
                        _today_state["result"] = {
                            "startDate": today_str, "endDate": today_str,
                            "startTs": start_ts, "endTs": end_ts, "platform": platform,
                            "total": agg_total, "adopted": agg_adopted,
                            "rate": round(agg_adopted / agg_total * 100, 2) if agg_total else 0,
                            "shopList": list(shop_list),
                            "byStaff": _staff_list_per_shop(staff_shop_agg, platform, shop_filter),
                            "live": True,
                        }
            _today_state["last_run"] = time.time()
            log_line("today", f"今日抓取完成: {agg_total} 条 / 采纳 {agg_adopted} / {total_plan} 家店铺")
        except Exception as e:
            _today_state["error"] = str(e)
        finally:
            _today_state["running"] = False
            # 恢复原激活集团(风控时不强行切换)
            if orig_gid and not _risk_state.get("triggered"):
                try:
                    if orig_gid != load_config().get("cookies", {}).get("tanyu-group-id"):
                        switch_group(orig_gid)
                        log_line("today", f"已恢复原激活集团 {orig_gid}")
                except Exception as e:
                    log_line("today", f"⚠️ 恢复原集团失败: {e}")

    threading.Thread(target=worker, daemon=True).start()
    return {"status": "running", "startDate": today_str, "endDate": today_str,
            "startTs": start_ts, "endTs": end_ts, "progress": _today_state["progress"]}


@app.get("/api/trace/today/status")
def trace_today_status():
    """今日实时抓取任务状态/结果"""
    with _lock:
        return dict(_today_state)


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
            "taskShopFilter": _trace_state["shop_filter"],
            "error": _trace_state["error"],
            "result": _trace_state["result"],
            "subsetResult": _trace_state["subset_result"],
            "paused": _trace_state["paused"],
            "canceled": _trace_state["canceled"],
            "shopFilter": _trace_state["shop_filter"],
            "shopList": _trace_state["partial_list"],  # 运行中已完成店铺(边算边展示)
        }


@app.post("/api/trace/overview/pause")
def trace_overview_pause():
    """暂停核算: 置暂停标志, worker 在当前店铺完成后阻塞, 不再请求剩余店铺"""
    with _lock:
        if not _trace_state["running"]:
            return {"ok": False, "reason": "无进行中的核算任务"}
        if _trace_state["canceled"]:
            # 已取消的运行不响应暂停(worker 即将退出); 避免遗留 paused=True 误导前端
            return {"ok": False, "reason": "核算已取消"}
        _trace_state["paused"] = True
        _trace_state["progress"]["current"] = "已暂停, 等待恢复…"
    print("[trace] paused")
    return {"ok": True, "paused": True}


@app.post("/api/trace/overview/resume")
def trace_overview_resume():
    """恢复核算: 清除暂停标志并唤醒 worker"""
    with _lock:
        if not _trace_state["running"]:
            return {"ok": False, "reason": "无进行中的核算任务"}
        _trace_state["paused"] = False
    _trace_resume_evt.set()  # 唤醒阻塞中的 worker
    print("[trace] resumed")
    return {"ok": True, "paused": False}


@app.post("/api/trace/overview/cancel")
def trace_overview_cancel():
    """取消核算: 停止后续店铺, 保留已完成店铺的部分结果(不写磁盘缓存)"""
    with _lock:
        if not _trace_state["running"]:
            return {"ok": False, "reason": "无进行中的核算任务"}
        _trace_state["canceled"] = True
        _trace_state["paused"] = False
    _trace_resume_evt.set()  # 若 worker 正阻塞在暂停等待中, 唤醒使其退出
    print("[trace] canceled")
    return {"ok": True, "canceled": True}


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
    today_str = datetime.date.today().isoformat()
    # 区间含今天: 历史(库) + 今天(实时) 合并
    if (not force and _use_sqlite_trace()
            and start <= today_str <= end):
        try:
            merged = _trace_shop_merged(shop_id, start, end)
            if merged and merged[0]:
                stat, has_today = merged
                return {"shop": shop, "startDate": start, "endDate": end,
                        "total": stat["total"], "daily": stat.get("daily", []),
                        "messages": stat.get("messages", []), "live": has_today}
        except RiskTriggered:
            raise HTTPException(502, "风控/登录失效, 请重新登录后重试")
        except Exception as e:
            print(f"[trace] 原始消息 合并路径失败, 回退在线抓取: {e}")
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


@app.get("/api/logs")
def logs_list(tag: str | None = None, limit: int = 200):
    """抽屉日志窗口: 返回进程内环形缓冲的最近日志(不含任何 cookie/凭据)"""
    limit = max(1, min(int(limit), 500))
    with _log_lock:
        entries = list(_log_ring)
    if tag and tag.strip():
        wanted = {t for t in tag.split(",") if t.strip()}
        entries = [e for e in entries if e["tag"] in wanted]
    return {"logs": entries[-limit:], "total": len(entries), "limit": limit, "tag": tag or ""}


@app.post("/api/cookies")
def update_cookies(body: CookieUpdate):
    cfg = load_config()
    cfg["cookies"] = body.cookies
    save_config(cfg)
    return {"ok": True}


@app.get("/api/auth/cookie-expiry")
def auth_cookie_expiry():
    """各登录 cookie 的到期日 + 最近到期剩余天数(仅日期, 无 cookie 值)"""
    cfg = load_config()
    expires_map = dict(cfg.get("cookie_expires") or {})
    today = datetime.date.today()
    days_left = None
    for day in expires_map.values():
        try:
            d = datetime.date.fromisoformat(day)
        except Exception:
            continue
        left = (d - today).days
        if days_left is None or left < days_left:
            days_left = left
    return {"expires": expires_map, "daysLeft": days_left}


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
                    # 记录各 cookie 到期日(playwright expires 是 epoch 秒; -1=SESSION 不记录)
                    try:
                        expires_map = dict(cfg.get("cookie_expires") or {})
                        for c in cks:
                            if c["name"] in LOGIN_COOKIE_NAMES and c.get("expires", -1) not in (-1, None):
                                day = datetime.datetime.fromtimestamp(c["expires"]).strftime("%Y-%m-%d")
                                expires_map[c["name"]] = day
                        cfg["cookie_expires"] = expires_map
                    except Exception:
                        pass
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


# ---------- SQLite 预抓 / 回填 ----------
_last_prefetch_duration = None  # 最近一次夜间预抓耗时(秒), 供 /api/db/status 展示


def _use_sqlite_trace():
    """config.json 开关: use_sqlite_trace(默认 true)"""
    try:
        cfg = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
        return cfg.get("use_sqlite_trace", True)
    except Exception:
        return True


def backfill_trace_db():
    """一次性回填: 把现有 data/trace_days 逐日 JSON 全量投影进 SQLite(幂等可重跑)"""
    import glob

    trace_store.init_db()
    shops = load_shops()
    trace_store.upsert_shops(shops)
    shop_map = {s.get("thirdShopId"): s.get("platform", 0) for s in shops}
    files = sorted(glob.glob(str(DATA_DIR / "trace_days" / "*.json")))
    total_rows = total_shops = 0
    t0 = time.time()
    for i, f in enumerate(files, 1):
        shop_id = Path(f).stem
        try:
            cache = json.loads(Path(f).read_text(encoding="utf-8"))
        except Exception as e:
            print(f"[backfill] 读取失败 {f}: {e}")
            continue
        days = cache.get("days") or {}
        platform = shop_map.get(shop_id, 0)
        for day_str, msgs in days.items():
            try:
                trace_store.upsert_shop_day(shop_id, platform, day_str, msgs)
            except Exception as e:
                print(f"[backfill] {shop_id} {day_str} 失败: {e}")
        n = sum(len(v) for v in days.values())
        total_rows += n
        total_shops += 1
        if i % 20 == 0 or i == len(files):
            print(f"[backfill] {i}/{len(files)} 家, 已入库 {total_rows} 条, "
                  f"{time.time() - t0:.0f}s")
    print(f"[backfill] 完成: {total_shops} 家 / {total_rows} 条 / {time.time() - t0:.0f}s")


def _prefetch_group(gid, start, end, force_days=None):
    """切换集团→同步店铺→抓该集团全部店铺 trace, 返回 (店铺总数, 失败数)

    依赖 switch_group 内部自动 sync_shops_from_tanyu(写 shops.json + SQLite 店铺表),
    切换后 load_shops 即读当前集团店铺。任一店铺 RiskTriggered 向上传播(整个预抓停止)。
    force_days: 窗口内这些天强制重抓(默认最近一天=昨天, 防 tanyu 回溯更新 sendType
    造成的采纳口径漂移), 由 config.prefetch_force_days 控制(0=关闭)。
    """
    cfg = load_config()
    g = next((x for x in cfg.get("groups", []) if x.get("groupId") == gid), None)
    if not g:
        print(f"[prefetch] ⚠️ 集团 {gid} 不在 config.groups 中, 跳过")
        return 0, 0
    if not g.get("accountId"):
        print(f"[prefetch] ⚠️ 集团「{g.get('groupName')}」缺少 accountId, 无法切换, 跳过")
        return 0, 0
    switch_group(gid)  # 内部会 _assert_no_risk + _rate_limit + sync_shops_from_tanyu
    shops = load_shops()
    print(f"[prefetch] 集团「{g.get('groupName')}」({gid}) 店铺 {len(shops)} 家, "
          f"窗口 {start} ~ {end}" + (f", 强制重抓最近 {len(force_days)} 天" if force_days else ""))
    failed = []
    for i, shop in enumerate(shops, 1):
        if _risk_state.get("triggered"):
            print(f"[prefetch] ⛔ 风控/登录失效, 集团内停止于 {i - 1}/{len(shops)} 家")
            raise RiskTriggered(_risk_state.get("reason") or "风控触发")
        sid = shop["thirdShopId"]
        try:
            stat = stat_trace_daily(sid, start, end, force_days=force_days)
            print(f"[prefetch]   {i}/{len(shops)} {shop.get('platformName','')}·{shop.get('shopName','')} "
                  f"total={stat['total']} adopted={stat['adopted']}")
        except RiskTriggered:
            print(f"[prefetch] ⛔ 风控触发于 {shop.get('shopName','')}")
            raise
        except Exception as e:
            print(f"[prefetch]   {shop.get('shopName','')} 失败: {e}")
            failed.append(sid)
    return len(shops), len(failed)


def repair_platform_attribution():
    """按 SQLite 店铺表把历史 platform=0 的消息纠正为正确平台(按 shop_id 映射)

    早期全量回填时无平台信息, 359K 条消息 platform=0。三集团同步后 shops 表已含
    各集团店铺的正确平台, 这里做一次兜底修复: 逐店 UPDATE 该店仍为 0 的行,
    并重建受影响店的按日聚合。shop_id 不在 shops 表的孤儿消息保持原样。
    """
    import sqlite3
    try:
        c = sqlite3.connect(trace_store.db_path())
        rows = c.execute(
            "SELECT third_shop_id, platform FROM shops WHERE third_shop_id IN "
            "(SELECT DISTINCT third_shop_id FROM messages WHERE platform = 0)"
        ).fetchall()
        c.close()
    except Exception as e:
        print(f"[repair] 读取映射失败: {e}")
        return 0
    if not rows:
        return 0
    fixed = 0
    for sid, platform in rows:
        try:
            import trace_store as _ts
            _ts.repair_shop_platform(sid, platform)
            fixed += 1
        except Exception as e:
            print(f"[repair] {sid} 修复失败: {e}")
    print(f"[repair] 平台归属修复完成: {fixed} 家店铺(shop 表映射)已归位")
    return fixed


def prefetch_trace_window(days=None):
    """夜间预抓: 轮询拼多多→京东→抖音三集团, 按集团窗口抓缺失/过期天, 并滚动裁剪

    每天 00:00 由计划任务 TanyuDashboardTracePrefetch 触发:
      零点一过, 昨天才真正定型, 这里把"昨天"整体抓取入库(当天数据当天实时展示,
      不进库); 完成后 prune_window 按整日边界裁剪。
    trace API 以激活集团(cookie tanyu-group-id)为作用域, 所以多平台预抓逐集团:
      switch_group → sync_shops_from_tanyu → stat_trace_daily(该集团店铺) → 下一个。
    窗口: 已抓满 30 天的平台(抖音)保留 30 天; 其余平台(拼多多/京东)抓近 7 天。
      集团窗口取 config.prefetch_windows{platform: days}, 未配置的集团用
      config.prefetch_days(默认 7)。已抓的天命中缓存零请求(增量)。
    强制重抓: config.prefetch_force_days(默认 1)指定窗口最近 N 天即使缓存有效也
      强制重抓(默认最近一天=昨天)。tanyu 会对已抓消息回溯更新 sendType(草稿→已发送),
      不重抓则昨天采纳数停在预抓时刻、看板采纳率与 tanyu 后台不一致。
    结束后恢复原激活集团, 让常驻看板继续服务原集团。风控/登录失效立即整体停止。
    """
    t_all = time.time()
    trace_store.init_db()
    cfg = load_config()
    days = days or int(cfg.get("prefetch_days", 7))
    # prefetch_windows 的 key 可能是 JSON 字符串("5")或数字(5), 统一归一化为 int
    window_map = {int(k): int(v) for k, v in (cfg.get("prefetch_windows") or {}).items()}
    force_days = int(cfg.get("prefetch_force_days", 1) or 0)
    platform_order = cfg.get("prefetch_platforms") or [1, 5, 7]
    groups = cfg.get("groups") or []
    # 按平台排序优先级(1 拼多多, 5 抖音, 7 京东), 未标注平台的集团放最后
    plat_rank = {p: i for i, p in enumerate(platform_order)}
    ordered = sorted(
        [g for g in groups if g.get("groupId")],
        key=lambda g: plat_rank.get(g.get("platform"), 99),
    )
    if not ordered:
        print("[prefetch] ⚠️ config.groups 为空, 无可预抓集团")
        return
    # 记录进入前的激活集团, 预抓结束后切回
    orig_gid = cfg.get("cookies", {}).get("tanyu-group-id")
    kept_days = days  # 最终裁剪窗口天数(取各集团窗口最大值)
    try:
        g_total = g_failed = 0
        for g in ordered:
            if _risk_state.get("triggered"):
                print("[prefetch] ⛔ 风控/登录失效, 整体停止")
                raise RiskTriggered(_risk_state.get("reason") or "风控触发")
            # 集团窗口: 优先按平台映射(PDD/JD=7 天), 未映射的集团用默认 days
            gid = g["groupId"]
            g_days = int(window_map.get(g.get("platform"), days))
            kept_days = max(kept_days, g_days)
            start, end = date_range(g_days)
            # 强制重抓"昨天"等最近 N 天(防 tanyu 回溯更新 sendType 造成的采纳漂移)
            fset = set()
            if force_days > 0:
                from datetime import timedelta as _td
                end_d = datetime.date.fromisoformat(end)
                fset = {(end_d - _td(days=i)).isoformat() for i in range(min(force_days, g_days))}
            print(f"[prefetch] ===== 集团「{g.get('groupName')}」窗口 {g_days} 天 "
                  f"({start} ~ {end}) =====" + (f" 强制重抓: {sorted(fset)}" if fset else ""))
            try:
                n, f = _prefetch_group(gid, start, end, force_days=fset)
                g_total += n
                g_failed += f
            except RiskTriggered:
                print("[prefetch] ⛔ 风控触发, 整体停止")
                raise
        print(f"[prefetch] 全部集团完成: {g_total} 家店铺, {g_failed} 家失败, "
              f"耗时 {time.time() - t_all:.0f}s")
    except RiskTriggered:
        # 风控触发: 以已抓数据为准(kept_days 保持已处理集团的最大窗口)
        print("[prefetch] ⛔ 风控触发, 停止轮询")
    finally:
        # 兜底修复历史 platform=0 消息(依赖 shops 表, 各集团已同步)
        try:
            repair_platform_attribution()
        except Exception as e:
            print(f"[repair] 兜底修复异常: {e}")
        # 裁剪窗口到已抓数据的最大集团窗口(抖音 30 天, PDD/JD 7 天)
        try:
            deleted = trace_store.prune_window(keep_days=kept_days)
            print(f"[prefetch] 裁剪完成(保留 {kept_days} 天): 删除 {deleted} 条过期消息")
        except Exception as e:
            print(f"[prefetch] 裁剪失败: {e}")
        # 恢复原激活集团(常驻看板继续服务原集团); 风控时不强行切换, 仅告警
        if orig_gid and not _risk_state.get("triggered"):
            try:
                if orig_gid != load_config().get("cookies", {}).get("tanyu-group-id"):
                    switch_group(orig_gid)
                    print(f"[prefetch] 已恢复原激活集团 {orig_gid}")
            except Exception as e:
                print(f"[prefetch] ⚠️ 恢复原集团失败: {e}")
        elif _risk_state.get("triggered"):
            print("[prefetch] ⚠️ 风控触发, 未恢复原集团(需重新登录后手工切换)")
    _last_prefetch_duration = time.time() - t_all
    print(f"[prefetch] 完成: 窗口 {kept_days} 天, 总耗时 {_last_prefetch_duration:.0f}s")


@app.get("/api/db/status")
def db_status():
    """SQLite 存储状态(供前端/运维确认预抓进度)"""
    import sqlite3

    info = {"enabled": _use_sqlite_trace(), "path": trace_store.db_path()}
    try:
        c = sqlite3.connect(trace_store.db_path())
        info["messages"] = c.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
        info["shops"] = c.execute("SELECT COUNT(DISTINCT third_shop_id) FROM messages").fetchone()[0]
        info["daily"] = c.execute("SELECT COUNT(*) FROM trace_daily").fetchone()[0]
        info["platform0_count"] = c.execute(
            "SELECT COUNT(*) FROM messages WHERE platform = 0"
        ).fetchone()[0]
        lo, hi = c.execute("SELECT MIN(day), MAX(day) FROM trace_daily").fetchone()
        info["window"] = [lo, hi]
        c.close()
    except Exception as e:
        info["error"] = str(e)
    # 预抓配置与最近一次耗时
    try:
        cfg = load_config()
        info["prefetch_days"] = cfg.get("prefetch_days", 7)
        info["prefetch_platforms"] = cfg.get("prefetch_platforms", [1, 5, 7])
        info["prefetch_windows"] = cfg.get("prefetch_windows", {5: 30})
        info["prefetch_force_days"] = cfg.get("prefetch_force_days", 1)
    except Exception:
        pass
    info["prefetch_duration"] = _last_prefetch_duration
    return info


if __name__ == "__main__":
    if "--backfill" in sys.argv:
        backfill_trace_db()
        sys.exit(0)
    if "--prefetch" in sys.argv:
        prefetch_trace_window()
        sys.exit(0)
    # 常驻服务启动时初始化 SQLite 表结构
    try:
        trace_store.init_db()
    except Exception as e:
        print(f"[db] 初始化失败: {e}")
    port = int(os.environ.get("PORT", "8080"))
    uvicorn.run(app, host="127.0.0.1", port=port)
