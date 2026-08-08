@echo off
rem Register the nightly-fetch scheduled task for Tanyu dashboard.
rem ASCII only - cmd.exe parses this file in the OEM codepage.
rem One-time setup: run once to create TanyuNightlyFetch and remove the old bare prefetch.
rem Daily at 00:05: prefetch fetches the fully-finished "yesterday" (incremental),
rem and only prunes the 35-day window when it is full (see nightly_fetch.py).

rem --- Remove the old bare prefetch task (replaced by nightly_fetch.py wrapper) ---
"C:\Windows\System32\schtasks.exe" /Delete /TN TanyuDashboardTracePrefetch /F >nul 2>&1

rem --- Register TanyuNightlyFetch: nightly_fetch.py, daily 00:05, start when available ---
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$a = New-ScheduledTaskAction -Execute 'C:\Python314\python.exe' -Argument 'D:\test\test01\nightly_fetch.py'; " ^
  "$t = New-ScheduledTaskTrigger -Daily -At 00:05; " ^
  "$s = New-ScheduledTaskSettingsSet -StartWhenAvailable -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Hours 6); " ^
  "Register-ScheduledTask -TaskName 'TanyuNightlyFetch' -Action $a -Trigger $t -Settings $s -Force" ^
  >> server.log 2>&1

if errorlevel 1 (
    echo [%date% %time%] FAILED to register TanyuNightlyFetch >> server.log
    exit /b 1
)
echo [%date% %time%] TanyuNightlyFetch registered (daily 00:05, StartWhenAvailable) >> server.log
exit /b 0
