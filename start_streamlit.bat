@echo off
echo ========================================
echo    Kimball Stage1 Streamlit App
echo ========================================
echo.
echo Starting Streamlit application...
echo.
echo The app will be available at:
echo http://localhost:8501
echo.
echo Press Ctrl+C to stop the application
echo.
echo ========================================

REM Check if Python is available
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH
    echo Please install Python and try again
    pause
    exit /b 1
)

REM Check if required packages are installed
echo Checking dependencies...
python -c "import streamlit, pandas, pymongo, duckdb" >nul 2>&1
if errorlevel 1 (
    echo ERROR: Required packages are missing
    echo Please run: pip install -r requirements.txt
    pause
    exit /b 1
)

REM Start Streamlit
echo Starting Streamlit...
streamlit run "01_Find_Collections.py" --server.port 8501 --server.address localhost

echo.
echo Streamlit application stopped.
pause
