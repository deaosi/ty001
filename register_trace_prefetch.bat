@echo off
rem Register the nightly trace-prefetch scheduled task for Tanyu dashboard.
rem ASCII only - cmd.exe parses this file in the OEM codepage.
rem One-time setup: run this once to create TanyuDashboardTracePrefetch.
rem Daily at 06:02 (off :00/:30) fetches the previous day for all shops,
rem then prunes the 30-day rolling window.
setlocal

schtasks /Create /TN TanyuDashboardTracePrefetch /F ^
  /TR "\"C:\Python314\python.exe\" \"D:\test\test01\main.py\" --prefetch" ^
  /SC DAILY /ST 06:02 /RL LIMITED

if errorlevel 1 (
    echo [%date% %time%] FAILED to register prefetch task >> server.log
    exit /b 1
)
echo [%date% %time%] prefetch task registered (daily 06:02) >> server.log
exit /b 0
