@echo off
REM One-command setup for Windows.
setlocal enabledelayedexpansion

set VENV_DIR=env

echo ==^> Creating virtual environment in .\%VENV_DIR%
python -m venv %VENV_DIR%
if errorlevel 1 goto :error

call %VENV_DIR%\Scripts\activate.bat

echo ==^> Upgrading pip and installing dependencies
python -m pip install --upgrade pip >nul
pip install -r requirements.txt
if errorlevel 1 goto :error

echo ==^> Registering Jupyter kernel
python -m ipykernel install --user --name local-data-env --display-name "Local Data Env" >nul 2>&1

echo ==^> Validating installation
python -m scripts.smoke_test
if errorlevel 1 goto :error

echo.
echo Setup complete. Activate with:  %VENV_DIR%\Scripts\activate.bat
echo Then run:                       python -m etl.cli run-etl
goto :eof

:error
echo Setup failed. See messages above.
exit /b 1
