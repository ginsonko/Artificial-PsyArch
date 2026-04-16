@echo off
chcp 65001 >nul
setlocal
cd /d "%~dp0"
echo ======================================
echo          启动 Observatory
echo ======================================
echo.
echo 当前运行目录：%cd%
echo.
set "PY=python"
if exist ".venv\\Scripts\\python.exe" (
  set "PY=.venv\\Scripts\\python.exe"
) else (
  where python >nul 2>nul || (
    where py >nul 2>nul && set "PY=py -3"
  )
)
echo 正在执行命令：%PY% -m observatory
echo.
:: 执行核心命令
%PY% -m observatory
echo.
echo ======================================
echo          运行结束
echo ======================================
pause
endlocal
