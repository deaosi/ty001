# -*- coding: utf-8 -*-
"""夜间抓取守夜人: 每晚增量抓"昨天", 满 35 天连续无缺口才滚动裁剪

背景: 数据窗口上限 35 天, 但不一次性抓满 35 天(太慢)。每晚只增量抓取
"昨天"入库, 窗口内的历史天靠缓存复用(零请求)。只有从昨天往前连续 35 天
每店每天都已抓过(缓存级判定), 才进入"抓新一天 + 裁最老一天"的滚动模式;
否则只积累、绝不裁剪(不满不裁)。

覆盖判定用"缓存文件存在性"(data/trace_days/{店}.json 的 days 键), 因为
0 消息的天在 SQLite trace_daily 里没有行(空店), 用 SQLite 会把空店误判成
缺口, 永远不裁。缓存文件里抓过就记录, 无论当天 0 条还是 N 条消息。

由计划任务 TanyuNightlyFetch 每天 00:05 触发(StartWhenAvailable=True)。
独立进程, 不依赖 8080 常驻。全部输出写 data/nightly_fetch.log。

用法:
  python nightly_fetch.py                # 正常夜间抓取
  python nightly_fetch.py --dry-run      # 只打印覆盖/裁剪判定, 不实际抓取
"""
from __future__ import annotations

import datetime
import json
import os
import sys
import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
TRACE_DAYS_DIR = DATA_DIR / "trace_days"
LOG_FILE = DATA_DIR / "nightly_fetch.log"
LOCK_FILE = DATA_DIR / "nightly_fetch.lock"

# 窗口上限天数(与 config.prefetch_windows 保持一致)
WINDOW_DAYS = 35

# 抓取平台(与 config.prefetch_platforms 一致)
FETCH_PLATFORMS = (1, 5, 7)

# 残留锁兜底: 锁文件超过该秒数视为残留(崩溃/句柄未释放), 无视 PID 强制获取。
# 6h 远超单次运行时长(正常 10~40 分钟), 不会误伤真正的并发防护。
LOCK_MAX_AGE = 6 * 3600


def log(msg: str):
    """带时间戳追加写日志; 同时打 stdout(计划任务重定向到同一文件时无副作用)"""
    line = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass
    print(line, flush=True)


def _alert(title: str, text: str):
    """向钉钉群推告警(方式A webhook); 未配置 webhook 时静默降级, 不阻塞主流程"""
    try:
        import dingtalk_bot
        if not dingtalk_bot._is_configured():
            log("⚠️ 钉钉 webhook 未配置, 跳过告警推送")
            return
        ok, err = dingtalk_bot.push_alert_to_group(title, text)
        log(f"钉钉告警推送 {'成功' if ok else '失败: ' + err}")
    except Exception as e:
        log(f"⚠️ 钉钉告警推送异常(忽略): {e}")


def _lock():
    """拿幂等锁: 已有别的夜间抓取在跑(文件存在且进程活)则返回 None

    残留锁兜底: 锁文件写入超过 LOCK_MAX_AGE 秒(6h, 远超单次运行时长)时无视
    PID 直接强制获取——覆盖崩溃后句柄未释放 / PID 被复用导致 os.kill 误判存活
    而跳过当夜的窗口。正常存活进程的锁必然比 6h 年轻, 不会误伤并发防护。
    """
    try:
        if LOCK_FILE.exists():
            # 残留锁超时兜底: 太老的锁直接视为过期(即使 PID 探测显示"存活")
            try:
                age = time.time() - LOCK_FILE.stat().st_mtime
                if age > LOCK_MAX_AGE:
                    log(f"⚠️ 锁文件已超过 {int(age)}s 未更新, 视为残留(即使 PID 显示存活), 强制获取")
                else:
                    pid = LOCK_FILE.read_text(encoding="utf-8").strip()
                    if pid:
                        try:
                            os.kill(int(pid), 0)  # 0 = 仅探测存活
                            return None
                        except OSError:
                            pass  # 进程已死, 锁过期
            except Exception:
                pass
        LOCK_FILE.write_text(str(os.getpid()), encoding="utf-8")
        return True
    except Exception as e:
        log(f"⚠️ 取锁失败(不阻塞, 继续): {e}")
        return True


def _release():
    try:
        if LOCK_FILE.exists():
            LOCK_FILE.unlink()
    except Exception:
        pass


def _shops_by_platform():
    """跨集团全量店铺, 按平台分组(SQLite 优先, shops.json 兜底)"""
    import main as M
    out = {p: [] for p in FETCH_PLATFORMS}
    for s in M.load_all_shops():
        p = s.get("platform", 0)
        if p in out:
            out[p].append(s)
    return out


def _cache_days(shop_id: str) -> set:
    """某店缓存文件里记录过的所有天(无论 0 条还是 N 条消息)"""
    f = TRACE_DAYS_DIR / f"{shop_id}.json"
    if not f.exists():
        return set()
    try:
        data = json.loads(f.read_text(encoding="utf-8"))
        return set((data.get("days") or {}).keys())
    except Exception:
        return set()


def _window_days(yesterday: datetime.date) -> list:
    """窗口天数列表: 昨天往前 WINDOW_DAYS 天(含), 升序"""
    return [(yesterday - datetime.timedelta(days=i)).isoformat()
            for i in range(WINDOW_DAYS - 1, -1, -1)]


def coverage_report(yesterday: datetime.date | None = None):
    """缓存级覆盖报告: 返回 {gap_shops, total_shops, missing_cells, oldest_cached, total_days}

    判定标准: 窗口内每店每天在 trace_days 缓存里都有记录=已抓。
    missing_cells=满窗格数-已抓格数; 缺口店 = 有任何一天缺的店。
    """
    shops_by_p = _shops_by_platform()
    yd = yesterday or (datetime.date.today() - datetime.timedelta(days=1))
    days = _window_days(yd)
    day_set = set(days)
    total_shops = sum(len(v) for v in shops_by_p.values())
    total_cells = total_shops * len(days)
    fetched_cells = 0
    missing_cells = 0
    gap_shops = []
    oldest_cached = None  # 所有店缓存文件的最早 mtime(判断窗口实际积到多早)
    for p, slist in shops_by_p.items():
        for s in slist:
            sid = s.get("thirdShopId")
            have = _cache_days(sid)
            miss = day_set - have
            # 只统计窗口内缺失(缓存里可能还有窗口外的旧天, 不影响缺口判定)
            miss_inside = {d for d in miss if d in day_set}
            fetched_cells += len(day_set & have)
            missing_cells += len(miss_inside)
            if miss_inside:
                gap_shops.append({"shop": s.get("shopName"), "platform": p,
                                  "missing": sorted(miss_inside)})
            f = TRACE_DAYS_DIR / f"{sid}.json"
            if f.exists():
                mt = f.stat().st_mtime
                if oldest_cached is None or mt < oldest_cached:
                    oldest_cached = mt
    return {
        "gap_shops": gap_shops,
        "total_shops": total_shops,
        "missing_cells": missing_cells,
        "total_cells": total_cells,
        "oldest_cached_ts": oldest_cached,
        "window_days": len(days),
        "days_range": (days[0], days[-1]),
    }


def window_full(report) -> bool:
    """窗口是否满(从昨天连续 WINDOW_DAYS 天每店每天都已抓过)"""
    return report["missing_cells"] == 0


def _fetch():
    """调 prefetch_trace_window(prune=False), 只增量抓, 不裁剪"""
    import main as M
    M.prefetch_trace_window(days=WINDOW_DAYS, prune=False)


def _prune():
    import trace_store
    deleted = trace_store.prune_window(keep_days=WINDOW_DAYS)
    log(f"滚动裁剪: 保留最近 {WINDOW_DAYS} 天, 删除 {deleted} 条过期消息")


def main():
    dry_run = "--dry-run" in sys.argv
    if not dry_run:
        lock = _lock()
        if lock is None:
            log("⏭️  已有夜间抓取在跑, 跳过本次")
            return
    outcome = None   # 告警场景: None=正常 / risk=登录失效·风控 / exception=异常 / gap=未满窗
    today = datetime.date.today().isoformat()
    post = None
    exc = None
    try:
        import main as M  # 顶部 import 会拉 uvicorn 等重依赖, 这里才 import
        today = datetime.date.today()
        yesterday = today - datetime.timedelta(days=1)
        t0 = time.time()
        log(f"=== 夜间抓取开始 today={today} 目标昨天={yesterday} (dry_run={dry_run}) ===")

        # 1) 抓取前先看覆盖(记录进入前缺口, 用于判断昨晚是否漏抓)
        pre = coverage_report(yesterday)
        if pre["missing_cells"] > 0:
            log(f"进入前覆盖: 缺 {pre['missing_cells']}/{pre['total_cells']} 格, "
                f"缺口店 {len(pre['gap_shops'])} 家(含未补齐历史缺口; 若比昨晚多出 1+ 店×整窗, 可能昨晚漏抓)")

        # 2) 增量抓取昨天(prune=False 不裁剪)
        if not dry_run:
            _fetch()
            log(f"抓取完成, 耗时 {time.time() - t0:.0f}s")

        # 3) 抓取后重新算覆盖
        post = coverage_report(yesterday)
        full = window_full(post)
        log(f"覆盖: 总店 {post['total_shops']} 窗口 {post['window_days']} 天 "
            f"({post['days_range'][0]} ~ {post['days_range'][1]}) "
            f"缺格 {post['missing_cells']}/{post['total_cells']} "
            f"缺口店 {len(post['gap_shops'])} 家"
            + (f" → 窗口已满 {WINDOW_DAYS} 天" if full else f" → 未满, 缺 {post['missing_cells']} 格, 跳过裁剪"))
        if post["gap_shops"]:
            for g in post["gap_shops"][:10]:
                log(f"  缺口店: {g['shop']}(平台{g['platform']}) 缺 {len(g['missing'])} 天")
            if len(post["gap_shops"]) > 10:
                log(f"  ...共 {len(post['gap_shops'])} 家缺口店")

        # 4) 满窗才滚动裁剪; 不满只积累
        if full and not dry_run:
            _prune()
        elif full:
            log("(dry-run) 窗口已满, 将进入滚动裁剪")

        log(f"=== 夜间抓取完成 耗时 {time.time() - t0:.0f}s ===" if not dry_run else
            "=== dry-run 完成(未实际抓取/裁剪) ===")

        # 5) 成功但窗口未满: 可能有历史缺口/某店缺天, 群里提醒(让用户知道需要手动补)
        if not dry_run and not full:
            outcome = "gap"
    except Exception as e:
        exc = e
        # 风控/登录失效(内核抛 RiskTriggered): 用户在群里被@踢出/换密码后, 夜间抓取
        # 必然失败, 主动推送让用户扫码续期; 其他异常也一并推送(不再静默)。
        risk = bool(M._risk_state.get("triggered")) if "M" in dir() else False
        outcome = "risk" if risk else "exception"
        log(f"夜间抓取失败: {type(e).__name__}: {e}")
    finally:
        if not dry_run:
            _release()
        # 告警: 只在真实运行(非 dry-run)发, 避免手动测试时打扰群里
        if outcome and not dry_run:
            if outcome == "gap" and post is not None:
                _alert("夜间抓取未满窗",
                       f"**{today} 夜间抓取完成但窗口未满**: 缺格 {post['missing_cells']}/"
                       f"{post['total_cells']} 格, 缺口店 {len(post['gap_shops'])} 家。\n"
                       f"可能原因: cookie 过期 / 登录失效 / 风控, 或部分店铺抓取失败。\n"
                       f"请到看板「📅 补抓数据」手动补抓, 或检查日志 data/nightly_fetch.log。")
            elif outcome == "risk":
                _alert("夜间抓取失败: 登录失效/风控",
                       f"**{today} 夜间抓取因登录失效或风控停止**。\n"
                       f"请到探域后台确认账号状态(可能被踢出/密码被改), 必要时重新登录并扫码续期 cookie。\n"
                       f"日志: data/nightly_fetch.log")
            else:
                _alert("夜间抓取异常",
                       f"**{today} 夜间抓取异常终止**: {type(exc).__name__}: {exc}\n"
                       f"请检查 data/nightly_fetch.log。")


if __name__ == "__main__":
    main()
