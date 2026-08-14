# -*- coding: utf-8 -*-
"""一次性截图脚本: 注册临时演示账号 → 截图登录页 + 后台(供前端效果确认)"""
import json
import os
import time
import uuid

import requests
from playwright.sync_api import sync_playwright

BASE = "http://127.0.0.1:8080"
OUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "frontend_shots")
os.makedirs(OUT_DIR, exist_ok=True)

user = f"demo_{uuid.uuid4().hex[:8]}@local"
pw = "demo123456"

# 1) 注册 + 登录(拿 token)
r = requests.post(BASE + "/api/auth/register", json={"username": user, "password": pw}, timeout=15)
print("register:", r.status_code, r.json() if r.headers.get("content-type", "").startswith("application/json") else r.text[:120])
if r.status_code >= 400:
    # 可能已存在, 直接登录
    pass
lr = requests.post(BASE + "/api/auth/login", json={"username": user, "password": pw}, timeout=15)
data = lr.json()
print("login:", lr.status_code)
if lr.status_code != 200:
    raise SystemExit("login failed: " + str(data))
token = data["token"]

with sync_playwright() as p:
    browser = p.chromium.launch()
    ctx = browser.new_context(viewport={"width": 1440, "height": 900}, device_scale_factor=1.25)
    page = ctx.new_page()

    # 登录页截图(深色)
    page.goto(BASE + "/login", wait_until="networkidle", timeout=30000)
    page.wait_for_timeout(1500)
    page.screenshot(path=os.path.join(OUT_DIR, "login_dark.png"))

    # 后台截图: 注入登录态后再进入
    page.add_init_script(
        "localStorage.setItem('tanyu_token', %r);"
        "localStorage.setItem('tanyu_user', %r);"
        "localStorage.setItem('tanyu_client_name', %r);"
        "localStorage.setItem('tanyu_client_id', %r);"
        % (token, json.dumps(data["user"], ensure_ascii=False), user, "shot-" + uuid.uuid4().hex[:8])
    )
    page.goto(BASE + "/app", wait_until="networkidle", timeout=30000)
    page.wait_for_timeout(5000)  # 等 Dashboard 数据加载
    page.screenshot(path=os.path.join(OUT_DIR, "app_dark_top.png"))
    page.screenshot(path=os.path.join(OUT_DIR, "app_dark_full.png"), full_page=True)

    # 切到浅色再来一张
    page.evaluate("document.documentElement.dataset.bsTheme='light'")
    page.wait_for_timeout(2500)
    page.screenshot(path=os.path.join(OUT_DIR, "app_light_full.png"), full_page=True)

    browser.close()

print("shots saved to", OUT_DIR)
print("demo account:", user, "/", pw, "(可在系统设置-用户管理中封禁)")
