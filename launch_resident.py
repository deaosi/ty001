# -*- coding: utf-8 -*-
"""
探域数据看板 - 常驻服务启动器
由 start_resident_service.bat 调用 (python.exe _launch_resident.py)

职责:
  1. 端口探测: 8080 已被监听则跳过 (幂等)
  2. 用 pythonw 无窗口启动 main.py, 日志追加写入 server.log
"""
import socket
import subprocess
import sys
import time
from pathlib import Path

BASE = Path(__file__).resolve().parent
PORT = 8080
PYW = r"C:\Python314\pythonw.exe"
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


def main():
    if not port_free(PORT):
        log(f"resident service already running on port {PORT}, skip")
        return
    log(f"launching resident service on port {PORT} (windowless)")
    with open(LOG, "a", encoding="utf-8") as logf:
        subprocess.Popen(
            [PYW, str(BASE / "main.py")],
            cwd=str(BASE),
            stdout=logf,
            stderr=subprocess.STDOUT,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
    log("resident service launched")


if __name__ == "__main__":
    main()
