@echo off
setlocal EnableExtensions EnableDelayedExpansion

rem uv-run.bat
rem Runs the main Retail BI pipeline with uv from the script directory
rem Always waits 15 seconds before closing

set "EXIT_CODE=0"

pushd "%~dp0" >nul 2>nul

echo.
echo uv-run.bat starting...
echo.
echo Checking prerequisites...

where uv >nul 2>nul
if errorlevel 1 (
    echo uv is not found in PATH. Please run uv-install.bat first.
    set "EXIT_CODE=1"
    goto :finalize
)

if not exist "pyproject.toml" (
    echo pyproject.toml not found in this directory:
    cd
    set "EXIT_CODE=1"
    goto :finalize
)

if not exist ".venv\" (
    echo Virtual environment .venv was not found.
    echo Run uv-make.bat first, then retry.
    set "EXIT_CODE=1"
    goto :finalize
)

echo.
echo Running Retail BI pipeline...
uv run python -m src.pipeline
if errorlevel 1 (
    echo.
    echo Pipeline execution failed.
    set "EXIT_CODE=1"
    goto :finalize
)

echo.
echo Pipeline execution completed successfully.

:finalize
echo.
echo This window will close automatically in 15 seconds.
echo Press Ctrl+C to abort the countdown if you need more time.
timeout /t 15 >nul

popd >nul 2>nul
exit /b %EXIT_CODE%
