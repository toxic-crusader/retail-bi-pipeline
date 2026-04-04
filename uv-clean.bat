@echo off
setlocal EnableExtensions EnableDelayedExpansion

rem uv-clean.bat
rem Non-interactive cleanup for uv-based dev projects on Windows
rem Removes virtualenv, caches, build artifacts, and uv.lock
rem Always waits 15 seconds before closing

set "EXIT_CODE=0"

pushd "%~dp0" >nul 2>nul

echo.
echo uv-clean.bat starting...

if not exist "pyproject.toml" (
    echo pyproject.toml not found in this directory:
    cd
    echo Nothing to do for this folder.
    goto :finalize
)

set "TO_DELETE_DIRS=.venv __pycache__ .pytest_cache .ruff_cache .mypy_cache .tox .nox build dist .coverage_html"
set "TO_DELETE_FILES=.coverage coverage.xml uv.lock"

set "REMOVED=0"

echo.
echo Removing directories...
for %%D in (%TO_DELETE_DIRS%) do (
    if exist "%%D" (
        echo   removing %%D
        rmdir /s /q "%%D" 2>nul
        if not exist "%%D" (
            echo   removed %%D
            set /a REMOVED+=1
        ) else (
            echo   failed to remove %%D
            set "EXIT_CODE=1"
        )
    )
)

echo.
echo Removing files...
for %%F in (%TO_DELETE_FILES%) do (
    if exist "%%F" (
        echo   removing %%F
        del /f /q "%%F" 2>nul
        if not exist "%%F" (
            echo   removed %%F
            set /a REMOVED+=1
        ) else (
            echo   failed to remove %%F
            set "EXIT_CODE=1"
        )
    )
)

if %REMOVED% EQU 0 (
    echo.
    echo Nothing to remove. Project already clean.
)

echo.
echo Cleanup complete. Project reset to a clean state.

:finalize
echo.
echo This window will close automatically in 15 seconds.
echo Press Ctrl+C to abort the countdown if you need more time.
timeout /t 15 >nul

popd >nul 2>nul
exit /b %EXIT_CODE%
