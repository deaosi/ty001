# -*- coding: utf-8 -*-
"""
探域数据看板 - 本地服务启动控制面板

功能:
  1. 启动/停止本地服务(默认 127.0.0.1:8080)
  2. 显示服务运行状态(端口、PID、日志)
  3. 一键打开浏览器访问看板
  4. Cookie 配置管理(查看/更新)
  5. 实时查看服务日志

用法:
  python control_panel.py        # 启动控制面板(交互式菜单)
  python control_panel.py --start  # 直接启动服务
"""
import argparse
import json
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

# Windows 控制台 GBK 编码不支持 emoji, 统一用 UTF-8 输出
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

BASE_DIR = Path(__file__).resolve().parent
CONFIG_FILE = BASE_DIR / "config.json"
LOG_FILE = BASE_DIR / "server.log"
MAIN_FILE = BASE_DIR / "main.py"

DEFAULT_PORT = 8080
DEFAULT_COOKIES = {
    "tanyu-account-id": "2656113728446465571",
    "tanyu-agent-account": "fM_VS4GirTjMlPPJx_llv5kWStXKTMrRvW__",
    "tanyu-group-account": "le_FjY2F9WFwuBxC2_ISmD0LfcZkkuHaG9__",
    "tanyu-group-id": "1901419852011174006",
}

_service = {"process": None, "port": DEFAULT_PORT}


# ---------- 工具函数 ----------
def load_config():
    if CONFIG_FILE.exists():
        try:
            return json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    cfg = {"cookies": dict(DEFAULT_COOKIES)}
    save_config(cfg)
    return cfg


def save_config(cfg):
    CONFIG_FILE.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")


def is_port_in_use(port):
    """检查端口是否被占用"""
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(("127.0.0.1", port))
            return False
        except OSError:
            return True


def find_python():
    """找到当前 Python 解释器"""
    return sys.executable


# ---------- 服务控制 ----------
def start_service(port=DEFAULT_PORT):
    """启动本地服务(后台运行)"""
    if _service["process"] and _service["process"].poll() is None:
        print(f"⚠️  服务已在运行 (PID {_service['process'].pid})")
        return False
    if is_port_in_use(port):
        print(f"⚠️  端口 {port} 已被占用, 请先停止其他程序或使用其他端口")
        return False

    python = find_python()
    with open(LOG_FILE, "w", encoding="utf-8") as logf:
        proc = subprocess.Popen(
            [python, str(MAIN_FILE)],
            cwd=str(BASE_DIR),
            stdout=logf,
            stderr=subprocess.STDOUT,
            env={**os.environ, "PORT": str(port)},
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
    _service["process"] = proc
    _service["port"] = port

    # 等待服务就绪
    import urllib.request

    for _ in range(20):
        time.sleep(0.5)
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/api/health", timeout=2) as r:
                if r.status == 200:
                    print(f"✅ 服务已启动: http://127.0.0.1:{port} (PID {proc.pid})")
                    return True
        except Exception:
            continue
    print(f"⚠️  服务启动超时, 请查看日志: {LOG_FILE}")
    return False


def stop_service():
    """停止本地服务"""
    if _service["process"] and _service["process"].poll() is None:
        proc = _service["process"]
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
        _service["process"] = None
        print("✅ 服务已停止")
        return True
    # 尝试按端口找进程
    port = _service["port"]
    if is_port_in_use(port):
        pid = find_pid_by_port(port)
        if pid:
            kill_pid(pid)
            print(f"✅ 服务已停止 (PID {pid})")
            return True
    print("ℹ️  服务未在运行")
    return False


def find_pid_by_port(port):
    """通过端口查 PID(跨平台)"""
    if os.name == "nt":
        try:
            out = subprocess.check_output(
                ["powershell", "-Command",
                 f"(Get-NetTCPConnection -LocalPort {port} -State Listen).OwningProcess"],
                text=True, timeout=10,
            ).strip()
            return int(out.splitlines()[0]) if out else None
        except Exception:
            return None
    else:
        try:
            out = subprocess.check_output(
                ["lsof", "-ti", f"tcp:{port}"], text=True, timeout=10
            ).strip()
            return int(out.splitlines()[0]) if out else None
        except Exception:
            return None


def kill_pid(pid):
    if os.name == "nt":
        subprocess.run(["powershell", "-Command", f"Stop-Process -Id {pid} -Force"],
                       check=False, timeout=10)
    else:
        os.kill(pid, signal.SIGTERM)


def is_running():
    """检查服务是否在运行(通过端口探测)"""
    port = _service["port"]
    import urllib.request

    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/api/health", timeout=2) as r:
            return r.status == 200
    except Exception:
        return False


# ---------- Cookie 管理 ----------
def show_cookies():
    cfg = load_config()
    cookies = cfg.get("cookies", {})
    print("\n📋 当前 Cookie 配置:")
    for k, v in cookies.items():
        masked = v[:6] + "*" * 8 + v[-4:] if len(v) > 14 else v
        print(f"  {k}: {masked}")
    return cookies


def update_cookies():
    print("\n📝 更新 Cookie(输入键值对, 每行一个, 格式: 键=值, 输入 q 结束)")
    cookies = load_config().get("cookies", {})
    while True:
        line = input("  键=值 (q 结束): ").strip()
        if line.lower() == "q":
            break
        if "=" in line:
            k, v = line.split("=", 1)
            cookies[k.strip()] = v.strip()
            print(f"  ✅ 已设置 {k.strip()}")
        else:
            print("  ❌ 格式错误, 请用 键=值 格式")
    cfg = load_config()
    cfg["cookies"] = cookies
    save_config(cfg)
    print("✅ Cookie 已保存")


# ---------- 浏览器登录 ----------
def browser_login():
    """弹出浏览器窗口, 用户登录后自动抓取 Cookie"""
    if not is_running():
        print("⚠️  服务未运行, 请先启动服务 (选项 1)")
        return
    import urllib.request

    port = _service["port"]
    try:
        req = urllib.request.Request(f"http://127.0.0.1:{port}/api/login/start", method="POST")
        with urllib.request.urlopen(req, timeout=10) as r:
            resp = json.loads(r.read().decode("utf-8"))
        print(f"🚀 {resp.get('message', '浏览器已启动')}")
        print("   请在浏览器中完成登录, 登录成功后将自动抓取 Cookie 并保存…")
        # 轮询状态
        last_phase = ""
        while True:
            time.sleep(3)
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{port}/api/login/status", timeout=5) as r:
                    st = json.loads(r.read().decode("utf-8"))
            except Exception:
                print("⚠️  无法查询登录状态(服务可能已停止)")
                return
            phase, msg = st.get("phase"), st.get("message", "")
            if phase != last_phase:
                print(f"  [{phase}] {msg}")
                last_phase = phase
            if phase == "got_cookie":
                print("✅ 登录成功, Cookie 已自动保存!")
                return
            if phase == "failed":
                print("❌ 登录失败: " + (st.get("message") or "未知错误"))
                return
    except Exception as e:
        print(f"❌ 启动登录失败: {e}")


# ---------- 日志 ----------
def show_log(lines=30):
    if not LOG_FILE.exists():
        print("ℹ️  暂无日志")
        return
    logs = LOG_FILE.read_text(encoding="utf-8", errors="ignore").splitlines()
    print(f"\n📜 最近 {min(lines, len(logs))} 条日志:")
    for line in logs[-lines:]:
        print("  " + line)


# ---------- 主菜单 ----------
def open_browser(port=DEFAULT_PORT):
    import webbrowser

    webbrowser.open(f"http://127.0.0.1:{port}")
    print(f"🌐 已在浏览器打开 http://127.0.0.1:{port}")


def main_menu():
    print("=" * 50)
    print("  📊 探域数据看板 - 启动控制面板")
    print("=" * 50)
    while True:
        running = is_running()
        status = "🟢 运行中" if running else "🔴 已停止"
        print(f"\n服务状态: {status}  (端口 {_service['port']})")
        print("  [1] 🚀 启动服务")
        print("  [2] ⏹ 停止服务")
        print("  [3] 🌐 打开看板页面")
        print("  [4] 📋 查看 Cookie 配置")
        print("  [5] 📝 更新 Cookie")
        print("  [6] 🔑 浏览器登录获取 Cookie")
        print("  [7] 📜 查看服务日志")
        print("  [0] 🚪 退出")
        choice = input("\n请选择: ").strip()
        if choice == "1":
            start_service(_service["port"])
        elif choice == "2":
            stop_service()
        elif choice == "3":
            open_browser(_service["port"])
        elif choice == "4":
            show_cookies()
        elif choice == "5":
            update_cookies()
        elif choice == "6":
            browser_login()
        elif choice == "7":
            show_log()
        elif choice == "0":
            # 退出前询问是否停止服务
            if is_running():
                ans = input("服务正在运行, 退出前要停止吗? (y/n): ").strip().lower()
                if ans == "y":
                    stop_service()
            print("再见!")
            break
        else:
            print("❌ 无效选择")


def main():
    parser = argparse.ArgumentParser(description="探域数据看板控制面板")
    parser.add_argument("--start", action="store_true", help="直接启动服务")
    parser.add_argument("--stop", action="store_true", help="直接停止服务")
    parser.add_argument("--status", action="store_true", help="查看服务状态")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="服务端口")
    parser.add_argument("--open", action="store_true", help="打开浏览器访问看板")
    args = parser.parse_args()

    _service["port"] = args.port

    if args.start:
        start_service(args.port)
    elif args.stop:
        stop_service()
    elif args.status:
        running = is_running()
        print(f"服务状态: {'🟢 运行中' if running else '🔴 已停止'}")
        if running:
            pid = find_pid_by_port(args.port)
            print(f"地址: http://127.0.0.1:{args.port}  PID: {pid}")
    elif args.open:
        open_browser(args.port)
    else:
        main_menu()


if __name__ == "__main__":
    main()
