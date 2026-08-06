@echo off
rem Register the nightly trace-prefetch scheduled task for Tanyu dashboard.
rem ASCII only - cmd.exe parses this file in the OEM codepage.
rem One-time setup: run this once to create TanyuDashboardTracePrefetch.
rem Daily at 00:00 (midnight): the previous day is complete at 24:00, so the
rem prefetch fetches the fully-finished day and prunes the 30-day window.
setlocal

"C:\Windows\System32\schtasks.exe" /Create /TN TanyuDashboardTracePrefetch /F ^
  /TR "\"C:\Python314\python.exe\" \"D:\test\test01\main.py\" --prefetch" ^
  /SC DAILY /ST 00:00 /RL LIMITED

if errorlevel 1 (
    echo [%date% %time%] FAILED to register prefetch task >> server.log
    exit /b 1
)
echo [%date% %time%] prefetch task registered (daily 00:00) >> server.log
exit /b 0
