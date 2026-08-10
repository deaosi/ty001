# -*- coding: utf-8 -*-
"""核算近7天 vs tanyu 后台 对比验证脚本

用法: python _verify_7d_diff.py <before.json> <after.json>
before/after 各为 {"日期": {"p1": (total, adopted), ...}} 的 JSON。
输出: 各平台近7天 adopted 增量(重抓后 - 重抓前) = tanyu 回溯更新补上的量。
"""
import json
import sys


def load(p):
    with open(p, encoding="utf-8") as f:
        return json.load(f)


def main():
    before, after = load(sys.argv[1]), load(sys.argv[2])
    days = sorted(after.keys())
    print(f"{'日期':<12} {'平台':<6} {'before(total,adopted)':<28} {'after':<28} {'adopted增':<8}")
    for d in days:
        b, a = before.get(d, {}), after.get(d, {})
        for plat in sorted(set(b) | set(a)):
            bt, ba = b.get(plat, (0, 0))
            at, aa = a.get(plat, (0, 0))
            print(f"{d:<12} {plat:<6} ({bt},{ba}){'':<14} ({at},{aa}){'':<14} {aa - ba:<8}")
    # 合计
    print("\n=== 合计 ===")
    for plat in sorted(set().union(*[set(before.get(d, {})) for d in days],
                                  *[set(after.get(d, {})) for d in days])):
        btot = sum(before.get(d, {}).get(plat, (0, 0))[1] for d in days)
        atot = sum(after.get(d, {}).get(plat, (0, 0))[1] for d in days)
        print(f"平台 {plat}: adopted {btot} -> {atot} (增 {atot - btot})")


if __name__ == "__main__":
    main()
