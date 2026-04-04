@echo off
setlocal EnableExtensions EnableDelayedExpansion

rem uv-install.bat
rem Official per-user installation of uv on Windows using the PowerShell installer
rem No admin rights are requested
rem The script always waits 15 seconds before closing so the user can read the output

set "EXIT_CODE=0"

echo.
echo Installing uv for the current user using the official PowerShell installer...
echo.

rem Check PowerShell availability
where powershell >nul 2>nul
if errorlevel 1 (
    echo PowerShell not found in PATH. Please install or enable PowerShell and retry.
    set "EXIT_CODE=1"
    goto :finalize
)

rem Run the official installer script from astral.sh
rem Do not escape the pipe in the -Command string
powershell -NoProfile -NoLogo -NonInteractive -ExecutionPolicy Bypass -Command "try { irm https://astral.sh/uv/install.ps1 | iex } catch { Write-Error $_; $host.SetShouldExit(1) }"
if errorlevel 1 (
    echo.
    echo The installer reported an error. Check your network or proxy and try again.
    set "EXIT_CODE=1"
    goto :finalize
)

rem Expected install dir for per-user installation
set "UV_DIR=%USERPROFILE%\.local\bin"
set "UV_EXE=%UV_DIR%\uv.exe"

rem Make uv available in this cmd session without reopening the terminal
if exist "%UV_EXE%" (
    set "PATH=%UV_DIR%;%PATH%"
)

echo.
echo Verifying installation...
where uv >nul 2>nul
if errorlevel 1 (
    echo uv is not visible in this session PATH yet.
    echo Open a new PowerShell window and run: uv --version
    goto :finalize
)

for /f "usebackq tokens=*" %%V in (`uv --version`) do (
    echo %%V
)

echo.
echo uv installation completed successfully.

:finalize
echo.
echo This window will close automatically in 15 seconds.
echo Press Ctrl+C to abort the countdown if you need more time.
timeout /t 15 >nul
exit /b %EXIT_CODE%
