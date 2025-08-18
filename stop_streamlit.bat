@echo off
echo ========================================
echo    Stopping Streamlit Application
echo ========================================
echo.

REM Find and kill Streamlit processes
echo Looking for Streamlit processes...

REM Kill processes on port 8501
for /f "tokens=5" %%a in ('netstat -aon ^| findstr :8501') do (
    echo Found process on port 8501: %%a
    taskkill /PID %%a /F >nul 2>&1
    if errorlevel 1 (
        echo Process %%a was not running or could not be stopped
    ) else (
        echo Successfully stopped process %%a
    )
)

REM Kill any remaining streamlit processes
taskkill /IM "streamlit.exe" /F >nul 2>&1
if errorlevel 1 (
    echo No Streamlit processes found running
) else (
    echo Successfully stopped all Streamlit processes
)

echo.
echo Streamlit application stopped.
echo.
pause
