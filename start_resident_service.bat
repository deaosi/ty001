@echo off
rem Resident service launcher for Tanyu dashboard.
rem ASCII only - cmd.exe parses this file in the OEM codepage, and
rem non-ASCII bytes in a UTF-8 file break command parsing on GBK locales.
rem Idempotent: the python launcher skips startup if port is already in use.
rem All real work (port probe + windowless launch) lives in launch_resident.py
rem to avoid cmd `start` quoting pitfalls.
rem
rem 跨平台: 不再写死 C:\Python314\python.exe —— 优先 pythonw.exe(无窗口),
rem 找不到退化为 python.exe。换机器/换 Python 版本不用改这个文件。2026-08-20
setlocal
cd /d "%~dp0"
where pythonw.exe >nul 2>&1
if %errorlevel% == 0 (
    pythonw.exe "launch_resident.py"
) else (
    python "launch_resident.py"
)
exit /b 0