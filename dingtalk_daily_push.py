# -*- coding: utf-8 -*-
"""每日钉钉推送脚本(方式A webhook 机器人)

用法:
  python dingtalk_daily_push.py            # 推全部平台(natural_week)
  python dingtalk_daily_push.py --platform 5   # 只推指定平台

由 Windows 计划任务在每天早上 10:00 调用(pythonw 无窗口运行)。
实现: 调用本机 8080 常驻看板服务的 /api/dingtalk/push, 让常驻进程完成
数据聚合+推送(与 /api/dingtalk/preview、浏览器看板同源), 本脚本只做 HTTP
触发并记录结果, 不重复 import 业务模块。

为何不直接在这里 import main / dingtalk_bot:
  - 常驻 8080 已加载代码, 推送逻辑(webhook 实时读盘/模板)与看板一致;
  - 独立进程 import 会各起一套模块状态, 与常驻进程的缓存/限速/风控状态脱节。
"""
import json
import sys
import time
import urllib.request
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
LOG = BASE_DIR / "dingtalk_push.log"
BASE_URL = "http://127.0.0.1:8080"


def log_line(msg: str):
    with open(LOG, "a", encoding="utf-8") as f:
        f.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}\n")


def http_get(path: str, timeout: int = 60):
    with urllib.request.urlopen(BASE_URL + path, timeout=timeout) as r:
        return r.read().decode("utf-8", "replace")


def main():
    platform = "all"
    args = sys.argv[1:]
    for i, a in enumerate(args):
        if a == "--platform" and i + 1 < len(args):
            platform = args[i + 1]
    # 常驻服务不可达时(重启窗口/未启动)重试 3 次
    for attempt in range(1, 4):
        try:
            raw = http_get(f"/api/dingtalk/push?platform={platform}")
            data = json.loads(raw)
            break
        except Exception as e:
            log_line(f"推送失败(第{attempt}次): {e}")
            if attempt == 3:
                log_line("推送失败: 常驻 8080 服务不可达(已重试3次)")
                return
            time.sleep(10)
    ok = bool(data.get("ok"))
    err = data.get("error") or ""
    plats = data.get("platforms") or []
    n = len(plats)
    log_line(f"推送 {'成功' if ok else '失败'}: platform={platform} 平台数={n} error={err}")
    if ok:
        # 记录各平台生成率摘要, 便于事后核对
        summary = "; ".join(
            f"{p.get('name')}:{p.get('gen_rate')}" for p in plats if p.get("name")
        )
        log_line(f"  详情: {summary}")


if __name__ == "__main__":
    main()
