# -*- coding: utf-8 -*-
"""钉钉机器人接入模块

两种接入方式(互不依赖, 各自独立启用):

方式 A - 自定义机器人 Webhook 推送(主动播报):
  钉钉群「添加机器人 → 自定义」得到一个 webhook URL(含 access_token)。
  本模块用 requests POST 一条 markdown 到该 webhook, 把看板平台概览
  数据推送进群。数据复用 /api/overview 同源聚合(_overview_cards)。

方式 B - Stream 模式长连接交互(群内 @机器人 查询):
  钉钉开放平台「企业内部应用」凭据 AppKey/AppSecret 起一条常驻长连接,
  钉钉经此连接推送群消息, 机器人在群内 @回复实时数据。
  无需公网回调地址、无需内网穿透, 本机 pythonw 常驻即可。

配置: 独立文件 dingtalk_config.json(已被 .gitignore 排除, 含凭据不上传):

  {
    "webhook": { "url": "https://oapi.dingtalk.com/robot/send?access_token=...",
                 "keyword": "可选安全设置关键词(机器人要求时填)" },
    "stream":  { "client_id": "dingxxx", "client_secret": "..." }
  }

安全: 本模块只读写 dingtalk_config.json, 绝不触碰 config.json 的 cookie;
      Stream 凭据/Webhook 值只在本文件内部使用, 不上日志不落 git。
"""
from __future__ import annotations

import datetime
import json
import threading
from pathlib import Path

import requests

# Stream 模式 SDK(未安装时方式B禁用, 不影响方式A); 运行时按需 import 避免启动硬依赖
import dingtalk_stream

# 敏感配置独立存放(已被 .gitignore 排除), 与抓取 cookie 的 config.json 分离
CONFIG = Path(__file__).resolve().parent / "dingtalk_config.json"

# 平台名(与 main.PLATFORM_NAMES 同源, 独立维护避免循环 import)
PLATFORM_NAMES = {
    0: "淘宝", 1: "拼多多", 2: "有赞", 4: "快手",
    5: "抖音", 7: "京东", 10: "天猫1", 11: "天猫2",
}

# Stream 长连接线程(单例, 由 ensure_stream() 启动)
_stream_thread = None
_stream_state = {"running": False, "error": None, "conn_at": None}
_stop_flag = threading.Event()


# ---------- 配置 ----------

def load_config():
    """读钉钉配置; 缺失/解析失败返回空 dict(不自动创建, 由管理端点建)"""
    try:
        if CONFIG.exists():
            return json.loads(CONFIG.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}


def save_config(cfg):
    """原子写 dingtalk_config.json"""
    tmp = CONFIG.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
    import os
    os.replace(tmp, CONFIG)


# ---------- 方式 A: 自定义机器人 Webhook 推送 ----------

def _is_configured() -> bool:
    cfg = load_config()
    return bool((cfg.get("webhook") or {}).get("url"))


def _sign_webhook_url(url, secret):
    """钉钉加签: 按官方算法给 webhook URL 附加 timestamp+sign

    开启"加签"安全设置的机器人要求每次请求带签名, 否则 errcode 310000
    ("机器人发送签名不匹配")。算法:
      string_to_sign = f"{timestamp_ms}\\n{secret}"(secret 是 SEC 开头的串)
      sign = urlencode(base64(HMAC-SHA256(string_to_sign, key=secret)))
    """
    import base64
    import hashlib
    import hmac
    import time as _time
    import urllib.parse
    timestamp = str(round(_time.time() * 1000))
    string_to_sign = f"{timestamp}\n{secret}"
    hmac_code = hmac.new(secret.encode("utf-8"), string_to_sign.encode("utf-8"),
                         digestmod=hashlib.sha256).digest()
    sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}timestamp={timestamp}&sign={sign}"


def _push_markdown(title, text):
    """推一条 markdown 到群 webhook; 返回 (ok, err_msg)。

    钉钉自定义机器人安全设置:
      - "自定义关键词": 标题必须含该关键词, 否则 errcode 310000。
        webhook.keyword(可选配置): 标题不含时自动拼接, 保证换机器人也能推送。
      - "加签": 需要 webhook.secret(SEC 开头)按官方算法附加 timestamp+sign。
      - "IP 白名单": 需把本机出网 IP 加白(代码侧无动作)。
    URL 本身可带自定义鉴权 access_token。
    """
    cfg = load_config()
    web = cfg.get("webhook") or {}
    url = web.get("url")
    if not url:
        return False, "未配置钉钉 webhook(dingtalk_config.json 的 webhook.url)"
    keyword = (web.get("keyword") or "").strip()
    if keyword and keyword not in title:
        title = f"{title} · {keyword}"
    secret = (web.get("secret") or "").strip()
    if secret:
        url = _sign_webhook_url(url, secret)
    payload = {"msgtype": "markdown",
               "markdown": {"title": title, "text": text}}
    try:
        r = requests.post(url, json=payload, timeout=15)
        data = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
    except Exception as e:
        return False, f"请求钉钉失败: {e}"
    if data.get("errcode") == 0:
        return True, ""
    # errcode 310000 = 关键词不匹配/签名不匹配, 311000 = URL非法 等
    return False, f"钉钉拒绝: errcode={data.get('errcode')} {data.get('errmsg', '')}".strip()


def push_alert_to_group(title, text):
    """推一条告警消息到群(夜间抓取失败/风控/登录失效提醒, 方式A)

    与 _push_markdown 同路径(同 webhook/加签/关键词), 标题带「告警」便于
    群内与数据播报区分。返回 (ok, err_msg)。
    """
    ok, err = _push_markdown(f"🚨 {title}", text)
    return {"ok": ok, "error": err}


def push_overview_to_group(platform: int | None = None, stat_type: str = "natural_week",
                           week: str | None = None) -> dict:
    """组装看板平台概览为 markdown 并推送到群(方式A)。

    数据面复用看板自身聚合: 直接调 _overview_cards 同源函数。
    platform=None 推全部已启用平台。
    """
    cards_text, detail = _overview_cards(platform, stat_type, week)
    # 正文顶部加一级大标题「本周数据快报」+ 本期起止区间 + 抓取时间 + 数据截止
    # (钉钉 markdown 最高 # 一级; 标题固定按"本周"(natural_week)口径;
    #  抓取时间=推送生成时刻; 数据截止=本周起点0点~当前时刻,
    #  例 本周=08-03~08-09, 截止=08-03 00:00 ~ 08-08 现在)
    import main as M
    ws, we = M._stat_type_range("natural_week")
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    heading = (f"# 📊 本周数据快报 `{ws} ~ {we}`\n"
               f"> 数据抓取时间: **{ts}**\n"
               f"> 数据截止: **{ws} 00:00 ~ {ts}**\n\n")
    title = f"📊 探域看板播报 {datetime.date.today().isoformat()}"
    ok, err = _push_markdown(title, heading + cards_text)
    return {"ok": ok, "error": err, "platforms": detail}


def _overview_cards(platform: int | None, stat_type: str, week: str | None):
    """聚合平台概览为 markdown 文本(与 /api/overview 同口径: 日=昨天vs前天 等)。

    复用 main.overview 的逻辑: 为避免循环 import, 运行时才 import main。
    platform: 单个平台号; 为 "all"/None 时遍历全部已启用平台。
    返回 (markdown 文本, [{platform, name, start_date, end_date, cards}]).
    """
    import main as M
    lines = []
    detail = []
    # 默认全推顺序: 天猫1/2 优先(导入数据稳定), 再抓取平台
    if platform in (None, "all"):
        plats = M.IMPORT_PLATFORMS + M.FETCH_PLATFORMS
    else:
        plats = [int(platform)]
    for p in plats:
        try:
            # 导入平台(天猫1/2)传 cap_to_data_end=True: 表滞后(未导入昨天)时数值
            # 聚合截止到表末天, 与 data_end 显示区间对齐(否则"显示 06-01~08-06
            # 但消息量按昨天聚合=0"自相矛盾)。抓取平台不受影响。
            if p in M.IMPORT_PLATFORMS:
                d = M.overview(platform=p, stat_type=stat_type, week=week,
                               cap_to_data_end=True)
            else:
                d = M.overview(platform=p, stat_type=stat_type, week=week)
        except Exception as e:
            lines.append(f"### {PLATFORM_NAMES.get(p, p)}: 取数失败 {e}")
            detail.append({"platform": p, "name": PLATFORM_NAMES.get(p, str(p)), "error": str(e)})
            continue
        items = d.get("items", {})
        # 抓取平台 tanyu summary 返回的是 tanyu 原生 key(service_consult_cnt=咨询量 /
        # ai_consult_response_accept_rate=采纳率), 没有 history_msg_total/adopted_total。
        # 推送统一用本地 DB 聚合(消息量/采纳数/采纳率, 与天猫1/2 同构), 保证三卡都有值。
        # 生成率例外: tanyu 在线时其 ai_consult_response_rate 就是生成率(0-100 与本地
        # 同标度), 本地聚合对抓取平台恒 None。先捕获, 替换后覆盖回 history_gen_rate。
        tanyu_gen = (items.get("ai_consult_response_rate") or {}).get("current")
        if not items.get("history_msg_total") and not items.get("history_adopted_total"):
            d = M._import_overview_summary(p, stat_type)
            items = d.get("items", {})
            if tanyu_gen is not None:
                items["history_gen_rate"] = {"current": tanyu_gen, "previous": None,
                                             "comparePercent": None, "label": "生成率"}
                d["source"] = "tanyu在线+本地聚合"
        name = d.get("platformName") or PLATFORM_NAMES.get(p, str(p))
        source = d.get("source", "")
        sdate = d.get("startDate")
        edate = d.get("endDate")
        # 导入平台(天猫1/2): 数据截止按导入表格实际范围(trace_daily data_end),
        # 不是自然日口径默认区间(周=本周一~今天 会显示未导入的未来天)。
        # _import_overview_summary 返回 data_start/data_end; 有则优先用。
        dstart = d.get("data_start")
        dend = d.get("data_end")
        if p in M.IMPORT_PLATFORMS and dend:
            sdate, edate = dstart, dend
        elif p in M.IMPORT_PLATFORMS:
            # 导入平台但无数据(data_start/end 均 None, 如天猫2 尚未导入任何表格):
            # 不显示自然日默认区间(周=本周一~周日 会含未来周日, 误导"数据到未来")。
            sdate = edate = None
        cur = items.get("history_msg_total", {}).get("current")
        adp = items.get("history_adopted_total", {}).get("current")
        acc = items.get("history_accept_rate", {}).get("current")
        gen = items.get("history_gen_rate", {}).get("current")
        # 日期区间随平台展示(概览口径: 日=昨天 周=本周 月=本月; 历史周=实际区间)
        date_str = f"{sdate} ~ {edate}" if sdate and edate else ""
        line = f"### {name} `{date_str}`\n" if date_str else f"### {name}\n"
        line += f"- 消息量: **{cur:,}**\n" if cur is not None else "- 消息量: —\n"
        line += f"- 采纳数: **{adp:,}**\n" if adp is not None else "- 采纳数: —\n"
        line += f"- 采纳率: **{acc}%**\n" if acc is not None else "- 采纳率: —\n"
        if gen is not None:
            line += f"- 生成率: **{gen}%**\n"
        if source:
            line += f"> 数据源: {source}"
        lines.append(line)
        detail.append({"platform": p, "name": name, "start_date": sdate, "end_date": edate,
                       "source": source,
                       "msg_total": cur, "adopted": adp, "accept_rate": acc, "gen_rate": gen})
    return "\n\n".join(lines), detail


# ---------- 方式 B: Stream 长连接交互 ----------

def ensure_stream():
    """按配置启动 Stream 长连接线程(方式B); 未配置/已运行则返回状态。

    返回 {"enabled": bool, "running": bool, "error": str|None, "conn_at": str|None}。
    """
    global _stream_thread
    cfg = load_config()
    st = cfg.get("stream") or {}
    if not (st.get("client_id") and st.get("client_secret")):
        return {"enabled": False, "running": False, "error": "未配置 stream 凭据(dingtalk_config.json 的 stream.client_id/secret)",
                "conn_at": _stream_state.get("conn_at")}
    if _stream_thread and _stream_thread.is_alive():
        return {"enabled": True, "running": True, "error": None,
                "conn_at": _stream_state.get("conn_at")}
    _stream_state["error"] = None
    _stream_thread = threading.Thread(target=_stream_run, daemon=True, name="dingtalk-stream")
    _stream_thread.start()
    return {"enabled": True, "running": True, "error": None,
            "conn_at": _stream_state.get("conn_at")}


def _stream_run():
    """后台线程: 自管 Stream 连接循环, 可被 stop_stream() 真正中断。

    不复用 client.start_forever(SDK 内置 while True 只认 KeyboardInterrupt,
    无法外部停止)。改为: 每轮 open_connection 换 endpoint/ticket → 起
    websocket 收消息 → route_message 分发给回调 → 断线后检查 _stop_flag
    决定退出或重连。ping 保活由 SDK keepalive(60s)负责。
    """
    import asyncio
    import dingtalk_stream as DS

    cfg = load_config()
    st = cfg.get("stream") or {}
    cred = DS.Credential(st.get("client_id"), st.get("client_secret"))
    client = DS.DingTalkStreamClient(cred)
    client.register_callback_handler(
        DS.ChatbotMessage.TOPIC,
        _ChatbotHandler(),
    )
    _stream_state["running"] = True
    _stream_state["conn_at"] = None
    _stream_state["error"] = None

    def _loop():
        conn = client.open_connection()
        if not conn:
            return None
        return conn

    while not _stop_flag.is_set():
        try:
            conn = _loop()
            if not conn:
                _stream_state["error"] = "连接钉钉失败(open_connection 返回空)"
                import time as _time
                _time.sleep(5)
                continue
            endpoint = conn.get("endpoint")
            ticket = conn.get("ticket")
            if not endpoint:
                _stream_state["error"] = "open_connection 无 endpoint"
                import time as _time
                _time.sleep(5)
                continue
            uri = f"{endpoint}?ticket={__import__('urllib.parse', fromlist=['quote_plus']).quote_plus(ticket)}"
            _stream_state["conn_at"] = datetime.datetime.now().isoformat(timespec="seconds")
            _stream_state["error"] = None
            import asyncio as _aio
            with _aio.new_event_loop() as loop:
                loop.run_until_complete(_ws_session(loop, client, uri))
        except _stop:
            break
        except Exception as e:
            _stream_state["error"] = str(e)
            import time as _time
            _time.sleep(3)


class _stop(Exception):
    pass


async def _ws_session(loop, client, uri):
    """单条连接会话: 收消息 → 分发; 连接关闭/stop 时返回"""
    import websockets
    async with websockets.connect(uri) as ws:
        client.websocket = ws
        # 每 60s ping 保活, 断线抛出后跳出
        try:
            async for raw in ws:
                if _stop_flag.is_set():
                    return
                import json as _json
                msg = _json.loads(raw)
                loop.create_task(client.background_task(msg))
        except websockets.ConnectionClosed:
            return
        finally:
            client.websocket = None


def stop_stream():
    """停止 Stream 长连接(供管理端点/退出时调用)"""
    _stop_flag.set()
    global _stream_thread
    if _stream_thread:
        _stream_thread.join(timeout=10)
    _stream_state["running"] = False


class _ChatbotHandler(dingtalk_stream.ChatbotHandler):
    """Stream 机器人消息处理器: 解析 @机器人 文本 → 调看板 → markdown 回复

    SDK 回调协议: 必须继承 ChatbotHandler(父类已实现 pre_start/raw_process),
    重写 process(基类 process 为 async, raw_process 会 await 并回 ack)。
    reply_markdown 是同步方法, 内部直接 requests.post 到消息的 sessionWebhook。
    消息经钉钉推送到长连接, 由本类处理; 支持指令:
      概览: 拼多多 / 抖音 / 京东 / 天猫1 / 天猫2 / 全部
      口径: 昨天 / 本周 / 上周 / 上月 / 本月 / 7月13日 等
      例:  @机器人  拼多多 昨天
    """

    def __init__(self):
        super().__init__()

    async def process(self, message):
        import dingtalk_stream
        msg = dingtalk_stream.ChatbotMessage.from_dict(message.data)
        # 仅处理文本消息
        if msg.message_type != "text" or not msg.text:
            return dingtalk_stream.AckMessage.STATUS_OK, "ignore"
        text = (msg.text.content or "").strip()
        # 去掉 @机器人 的昵称片段
        import re
        for tok in re.findall(r"@[^\s]+", text):
            text = text.replace(tok, "").strip()
        text = text.strip()
        if not text:
            return dingtalk_stream.AckMessage.STATUS_OK, "empty"
        # 命令分派
        cmd = text.lower()
        if cmd in ("帮助", "help", "?"):
            reply = _help_text()
        elif "概览" in cmd or "数据" in cmd or any(p in cmd for p in ("拼多多", "抖音", "京东", "天猫1", "天猫2", "淘宝", "快手", "有赞")):
            reply = _cmd_overview(text)
        else:
            reply = "试试: @机器人 拼多多 昨天(或 帮助)"
        self.reply_markdown("探域看板", reply, msg)
        return dingtalk_stream.AckMessage.STATUS_OK, "ok"


def _help_text():
    return ("**探域看板钉钉机器人**\n\n"
            "支持指令(在群里 @机器人):\n"
            "- `拼多多 昨天` — 某平台指定口径概览\n"
            "- `全部 本周` — 全部平台\n"
            "- `帮助` — 本说明\n\n"
            "口径: 昨天/前天/本周/上周/本月/上月/7月13日")


def _cmd_overview(text):
    """把「平台+口径」指令解析成 _overview_cards 并返回 markdown"""
    plat = _parse_platform(text)
    stat = _parse_stat_type(text)
    if plat is None and not any(p in text for p in ("全部", "所有")):
        return "未识别平台。试试: `拼多多 昨天` / `天猫1 本周`"
    txt, detail = _overview_cards(plat, stat, week=None)
    return txt or "暂无数据"


_PLATFORM_ALIASES = {
    "拼多多": 1, "pdd": 1,
    "抖音": 5, "dy": 5, "douyin": 5,
    "京东": 7, "jd": 7,
    "天猫1": 10, "天猫一": 10, "tmall1": 10,
    "天猫2": 11, "天猫二": 11, "tmall2": 11,
    "淘宝": 0,
    "有赞": 2,
    "快手": 4,
}


def _parse_platform(text):
    for k, v in _PLATFORM_ALIASES.items():
        if k in text:
            return v
    return None


def _parse_stat_type(text):
    if "上月" in text:
        return "natural_month"
    if "本月" in text or "这月" in text or "当月" in text:
        return "natural_month"
    if "上周" in text:
        return "natural_week"
    if "本周" in text or "这周" in text:
        return "natural_week"
    if "昨天" in text or "前天" in text or "今日" in text or "今天" in text:
        return "natural_day"
    return "natural_week"
