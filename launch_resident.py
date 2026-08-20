# -*- coding: utf-8 -*-
"""
探域数据看板 - 常驻服务启动器

职责:
  1. 端口探测: 目标端口已被监听则跳过 (幂等, 双开不会绑两遍)
  2. 用无窗口解释器(pythonw.exe 优先, 退化为当前解释器)启动 main.py, 日志追加写入 server.log

由以下方式调用:
  - start_resident_service.bat (开机自启 / 手动双击)
  - main.py 的 /api/admin/server/restart 端点(浏览器一键重启, detached 起本脚本)

跨平台: 解释器不再写死 C:\\Python314\\pythonw.exe —— 优先取 sys.executable
同目录下的 pythonw.exe(Windows 无窗口), 找不到就直接用 sys.executable
(Windows 下是 python.exe 会弹控制台, 但至少能跑; Linux/macOS 无 pythonw 概念)。
2026-08-20: 原来写死路径, 换机器/换 Python 版本就启动失败。
"""
import socket
import subprocess
import sys
import time
from pathlib import Path

BASE = Path(__file__).resolve().parent
PORT = int(sys.argv[1]) if len(sys.argv) > 1 else int(__import__("os").environ.get("PORT", "8080"))
LOG = BASE / "server.log"


def log(msg: str):
    with open(LOG, "a", encoding="utf-8") as f:
        f.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}\n")


def port_free(port: int) -> bool:
    s = socket.socket()
    try:
        s.bind(("127.0.0.1", port))
        return True
    except OSError:
        return False
    finally:
        s.close()


def wait_port_free(port: int, timeout: int = 15) -> bool:
    """等端口空闲(重启场景: 旧进程正在退出, 端口还被占)。

    直接 port_free 一次就放弃会踩坑: 浏览器一键重启时, 后端先 detached 起本脚本、
    再退出旧进程。本脚本启动时旧进程还活着 → 端口被占 → 直接 skip → 服务永远起不来。
    所以这里轮询等待, 给旧进程留出退出时间。2026-08-20
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        if port_free(port):
            return True
        time.sleep(0.5)
    return False


def find_interpreter():
    """优先 pythonw.exe(无窗口), 退化为 sys.executable。"""
    candidate = Path(sys.executable).with_name("pythonw.exe")
    if candidate.exists():
        return str(candidate)
    return sys.executable


def main():
    interpreter = find_interpreter()
    # 重启场景下旧进程正在退出, 端口可能还被占: 最多等 15 秒。
    # 端口空闲 → 立即启动; 一直被占(真有人占用/旧进程卡死) → 放弃并提示。
    if not wait_port_free(PORT, timeout=15):
        log(f"⚠️  端口 {PORT} 在 15 秒内一直被占用, 放弃启动(可能已有别的实例在跑)")
        print(f"⚠️  端口 {PORT} 被占用, 请检查是否已有服务在运行", flush=True)
        return
    log(f"launching resident service on port {PORT} via {interpreter} (windowless)")
    with open(LOG, "a", encoding="utf-8") as logf:
        subprocess.Popen(
            [interpreter, str(BASE / "main.py")],
            cwd=str(BASE),
            stdout=logf,
            stderr=subprocess.STDOUT,
            env={**__import__("os").environ, "PORT": str(PORT)},
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
    log("resident service launched")


if __name__ == "__main__":
    main()