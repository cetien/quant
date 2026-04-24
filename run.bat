@echo off
setlocal

cd /d D:\Trabajo\ai\quant

set "APP_URL=http://localhost:8501"
set "CHROME_EXE=%ProgramFiles%\Google\Chrome\Application\chrome.exe"
if exist "%ProgramFiles(x86)%\Google\Chrome\Application\chrome.exe" (
    set "CHROME_EXE=%ProgramFiles(x86)%\Google\Chrome\Application\chrome.exe"
)

start "Quant Platform Server" cmd /k "cd /d D:\Trabajo\ai\quant && call .venv312\Scripts\activate.bat && streamlit run ui/app.py --server.headless true"

timeout /t 3 /nobreak >nul

if exist "%CHROME_EXE%" (
    start "Quant Platform" "%CHROME_EXE%" --app="%APP_URL%"
) else (
    start "Quant Platform" "%APP_URL%"
)

endlocal
