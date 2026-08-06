@echo off
rem Resident service launcher for Tanyu dashboard.
rem ASCII only - cmd.exe parses this file in the OEM codepage, and
rem non-ASCII bytes in a UTF-8 file break command parsing on GBK locales.
rem Idempotent: the python launcher skips startup if port is already in use.
rem All real work (port probe + windowless launch) lives in launch_resident.py
rem to avoid cmd `start` quoting pitfalls.
setlocal
cd /d "%~dp0"
"C:\Python314\python.exe" "launch_resident.py"
exit /b 0
