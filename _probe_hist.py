# -*- coding: utf-8 -*-
"""一次性只读探针: tanyu /summary 对历史日期是否返回历史周数据(受控, 预算2次, 风控保护)"""
import sys, json, time, random
sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, "D:/test/test01")
import main as M

def probe(stat_type, start, end, dimension="shop", target_id=None):
    M._assert_no_risk(); M._rate_limit()
    payload = {"statType": stat_type, "startDate": start, "endDate": end,
               "platform": PF, "dimension": dimension}
    if target_id: payload["targetId"] = target_id
    resp = M.requests.post(M.API_BASE + "/summary", json=payload, headers=M.get_headers(), timeout=20)
    try: data = resp.json()
    except Exception: data = {}
    if M._check_risk(resp, data):
        print("  [tanyu] ⛔ 风控, 停止"); return None
    if data.get("code") != 0:
        print(f"  [tanyu] ❌ code={data.get('code')} msg={data.get('msg')}"); return None
    inner = data.get("data") or {}
    items = inner.get("items") or []
    # 只打印指标 key + current/previous + 所有非指标字段(不含 cookie)
    print("  [tanyu] 原始 items 结构:")
    if items:
        k0 = items[0]
        for k, v in k0.items():
            if isinstance(v, dict): print(f"    {k}: keys={list(v.keys())}")
            else: print(f"    {k}: {repr(v)[:60]}")
    else:
        print("    (empty items)")
    return {it["key"]: it for it in items}

cfg = M.load_config()
gid = cfg["cookies"].get("tanyu-group-id")
cur = [g for g in cfg.get("groups", []) if g.get("groupId") == gid]
PF = cur[0].get("platform") if cur else None
shops = {s["thirdShopId"]: s for s in M.load_shops()}
SID = next((s for s in shops.values() if s.get("platform") == PF), None)
if not SID:
    print("无当前平台店铺"); sys.exit(1)
SID = SID["thirdShopId"]
import datetime
today = datetime.date.today()
# 上周一~上周日
last_mon = today - datetime.timedelta(days=today.weekday() + 7)
last_sun = last_mon + datetime.timedelta(days=6)
print(f"=== 平台={PF} 店={shops[SID]['shopName']} 探针: 上周={last_mon}~{last_sun} ===")
print(f"[probe1] natural_week 传上周日期 {last_mon}~{last_sun} (看是否返回历史周)")
probe("natural_week", last_mon.isoformat(), last_sun.isoformat(), target_id=SID)
time.sleep(random.uniform(1.2, 2.0))
print(f"[probe2] natural_week 传本周一~今天(对照)")
monday = today - datetime.timedelta(days=today.weekday())
probe("natural_week", monday.isoformat(), today.isoformat(), target_id=SID)
print("=== 完成 ===")
