# -*- coding: utf-8 -*-
"""受控线上数据对齐验证: tanyu 原始响应 vs 看板 API(三档口径, 只读)

对当前激活集团(抖音=5)的样本店, 分别:
  - tanyu 直连 /summary (natural_day/week/month) 拿原始值(预算内, 风控保护)
  - 本地 /api/overview 与 /api/shop/{id} 拿看板展示值(走缓存, 不耗上游配额)
对比 3 个指标(current/previous), 判定看板与 tanyu 是否对齐。
"""
import sys, json, time, random
sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, "D:/test/test01")
import main as M
import requests as _rq

LOCAL = "http://127.0.0.1:8080"
MAX_REQS = int(sys.argv[1]) if len(sys.argv) > 1 else 6  # tanyu 直连预算
_sent = 0
KEYS = ["service_3m_response_rate", "ai_consult_response_accept_rate", "ai_consult_response_rate"]
META = {"service_3m_response_rate": "3m回复率", "ai_consult_response_accept_rate": "采纳率", "ai_consult_response_rate": "生成率"}


def tanyu_summary(stat_type, start, end, dimension="shop"):
    """直连 tanyu, 返回 {key: {current, previous}} 或 None"""
    global _sent
    if _sent >= MAX_REQS:
        return None
    _sent += 1
    time.sleep(random.uniform(0.6, 1.3))
    payload = {
        "statType": stat_type, "startDate": start, "endDate": end,
        "platform": PF, "dimension": dimension,
    }
    if dimension == "shop":
        payload["targetId"] = SID
    try:
        resp = M.requests.post(M.API_BASE + "/summary", json=payload, headers=M.get_headers(), timeout=20)
    except Exception as e:
        print(f"  [tanyu] 请求异常: {e}"); return None
    if resp.status_code in (401, 403, 429):
        print(f"  [tanyu] ⛔ HTTP {resp.status_code} 风控/登录失效"); return None
    try:
        data = resp.json()
    except Exception:
        return None
    if M._check_risk(resp, data):
        print("  [tanyu] ⛔ 风控关键词, 停止"); return None
    if data.get("code") != 0:
        print(f"  [tanyu] ❌ code={data.get('code')} msg={data.get('msg')}"); return None
    items = (data.get("data") or {}).get("items") or []
    return {it["key"]: {"current": it.get("current"), "previous": it.get("previous")} for it in items}


def local_get(path):
    try:
        r = _rq.get(LOCAL + path, timeout=25)
        if r.status_code == 503:
            return {"_busy": True}
        return r.json()
    except Exception as e:
        return {"_error": str(e)}


def cmp(label, ta, lo):
    """对比 tanyu 直连 vs 本地, 输出差异"""
    if ta is None:
        print(f"  [对齐] {label}: tanyu 直连未取到(预算/风控), 跳过"); return "skip"
    if not lo or lo.get("_busy"):
        print(f"  [对齐] {label}: 本地 API 503/未取到"); return "busy"
    lo_items = lo.get("items") or {}
    all_ok = True
    for k in KEYS:
        t, l = (ta.get(k) or {}), (lo_items.get(k) or {})
        tc, tp = t.get("current"), t.get("previous")
        lc, lp = l.get("current"), l.get("previous")
        match = (tc == lc) and (tp == lp)
        if not match:
            all_ok = False
        flag = "✅" if match else "❌"
        print(f"  [对齐] {label} {META[k]}: tanyu={tc}/{tp} 看板={lc}/{lp} {flag}")
    return "ok" if all_ok else "mismatch"


if __name__ == "__main__":
    cfg = M.load_config()
    gid = cfg["cookies"].get("tanyu-group-id")
    cur = [g for g in cfg.get("groups", []) if g.get("groupId") == gid]
    PF = cur[0].get("platform") if cur else None
    shops = {s["thirdShopId"]: s for s in M.load_shops()}
    cs = [s for s in shops.values() if s.get("platform") == PF]
    if not cs:
        print("无当前平台店铺"); sys.exit(1)
    SID = cs[0]["thirdShopId"]
    pf_name = M.PLATFORM_NAMES.get(PF, str(PF))
    print(f"=== 数据对齐验证: 平台={PF}({pf_name}) 样本店={shops[SID]['shopName']} ({SID}) tanyu预算={MAX_REQS} ===\n")

    import datetime
    today = datetime.date.today()
    yest = (today - datetime.timedelta(days=1)).isoformat()
    monday = today - datetime.timedelta(days=today.weekday())
    ms = today.replace(day=1).isoformat()

    print("[1] 平台概览 /api/overview 对齐(platform 维度, 验证作用域正确)")
    for st, label, s, e in [
        ("natural_day", "自然日", yest, yest),
        ("natural_week", "自然周", monday.isoformat(), today.isoformat()),
        ("natural_month", "自然月", ms, today.isoformat()),
    ]:
        ta = tanyu_summary(st, s, e, dimension="platform")
        lo = local_get(f"/api/overview?platform={PF}&stat_type={st}&start={s}&end={e}")
        cmp(f"overview/{st}", ta, lo)
        if _sent >= MAX_REQS:
            print(f"  ⛔ tanyu 预算已尽({MAX_REQS}), 停止后续直连"); break

    print("\n[2] 单店 /api/shop/{id} 对齐(shop 维度)")
    for st, label, s, e in [
        ("natural_day", "自然日", yest, yest),
        ("natural_week", "自然周", monday.isoformat(), today.isoformat()),
        ("natural_month", "自然月", ms, today.isoformat()),
    ]:
        ta = tanyu_summary(st, s, e, dimension="shop")
        lo = local_get(f"/api/shop/{SID}?stat_type={st}&start={s}&end={e}&tables=0")
        cmp(f"shop/{st}", ta, lo)
        if _sent >= MAX_REQS:
            print(f"  ⛔ tanyu 预算已尽({MAX_REQS}), 停止后续直连"); break

    print(f"\n=== 完成, tanyu 直连 {_sent}/{MAX_REQS} 次 ===")
