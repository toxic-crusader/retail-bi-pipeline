@echo off
setlocal EnableExtensions EnableDelayedExpansion

rem uv-make.bat
rem Idempotent setup for a dev environment using uv on Windows
rem First run: create .venv and install runtime plus dev dependencies
rem Next runs: update lock and upgrade installed packages within constraints
rem The window always waits 15 seconds before closing

set "EXIT_CODE=0"

rem Work in script directory so it can be run from anywhere
pushd "%~dp0" >nul 2>nul

echo.
echo Checking prerequisites...
where uv >nul 2>nul
if errorlevel 1 (
    echo uv is not found in PATH. Please install uv first and retry.
    set "EXIT_CODE=1"
    goto :finalize
)

if not exist "pyproject.toml" (
    echo pyproject.toml not found in the current directory:
    cd
    set "EXIT_CODE=1"
    goto :finalize
)

set "VENV_DIR=.venv"
set "ACTIVATE=%VENV_DIR%\Scripts\activate.bat"
set "VENV_PYTHON=%VENV_DIR%\Scripts\python.exe"
set "PYTHON_VERSION=3.12"

if not exist "%VENV_DIR%\" (
    echo.
    echo Creating virtual environment with uv using Python %PYTHON_VERSION%...
    uv venv --python %PYTHON_VERSION% "%VENV_DIR%"
    if errorlevel 1 (
        echo Failed to create the virtual environment.
        set "EXIT_CODE=1"
        goto :finalize
    )
) else (
    echo.
    echo Virtual environment already exists at %VENV_DIR%
)

if exist "%ACTIVATE%" (
    echo.
    echo Activating virtual environment...
    call "%ACTIVATE%"
) else (
    echo.
    echo Activation script not found at %ACTIVATE%
    set "EXIT_CODE=1"
    goto :finalize
)

echo.
echo Using uv:
uv --version
echo Using Python:
if exist "%VENV_PYTHON%" (
    "%VENV_PYTHON%" --version
) else (
    python --version
)

for /f "usebackq tokens=2" %%V in (`"%VENV_PYTHON%" --version`) do set "CURRENT_PYTHON_VERSION=%%V"
if /I not "%CURRENT_PYTHON_VERSION:~0,4%"=="%PYTHON_VERSION%" (
    echo.
    echo The existing virtual environment uses Python %CURRENT_PYTHON_VERSION%.
    echo This project is pinned to Python %PYTHON_VERSION% for stable VS Code Jupyter kernels.
    echo Remove .venv and run uv-make.bat again to recreate it with Python %PYTHON_VERSION%.
    set "EXIT_CODE=1"
    goto :finalize
)

if exist "%VENV_DIR%\" (
    if exist "uv.lock" (
        echo.
        echo Updating lock file to the newest allowed versions...
        uv lock --upgrade
        if errorlevel 1 (
            echo uv lock reported an error.
            set "EXIT_CODE=1"
            goto :finalize
        )
    )
)

echo.
echo Synchronizing environment with dev dependencies and upgrades...
uv sync --group dev --upgrade
if errorlevel 1 (
    echo uv sync reported an error.
    set "EXIT_CODE=1"
    goto :finalize
)

echo.
echo Verifying environment health...
uv pip check
if errorlevel 1 (
    echo Dependency issues detected by pip check.
    echo Review output above and adjust constraints if needed.
    set "EXIT_CODE=1"
    goto :finalize
)

echo.
echo All done. The environment is active in this window.
echo You can now run:  python -c "import sys; print(sys.executable)"

:finalize
echo.
echo This window will close automatically in 15 seconds.
echo Press Ctrl+C to abort the countdown if you need more time.
timeout /t 15 >nul

popd >nul 2>nul
exit /b %EXIT_CODE%
