@echo off
chcp 65001 >nul
setlocal enabledelayedexpansion
cd /d "%~dp0"

echo ======================================
echo     AP 原型依赖自检与安装 (Windows)
echo ======================================
echo.
echo 当前目录: %cd%
echo.

:: 1) Locate Python (prefer py -3, fallback to python)
set "PY="
where py >nul 2>nul
if %errorlevel%==0 (
  set "PY=py -3"
) else (
  where python >nul 2>nul
  if %errorlevel%==0 (
    set "PY=python"
  )
)

if "%PY%"=="" (
  echo [ERROR] 未检测到 Python.
  echo 解决方案:
  echo 1. 安装 Python 3.10+ (推荐 3.11)
  echo 2. 安装时勾选 "Add Python to PATH"
  echo 3. 重新打开本窗口再试一次
  echo.
  pause
  exit /b 1
)

echo 使用 Python: %PY%
%PY% --version
if %errorlevel% neq 0 (
  echo [ERROR] Python 命令不可用: %PY%
  echo.
  pause
  exit /b 1
)

echo.
:: 2) Create venv
if not exist ".venv\\Scripts\\python.exe" (
  echo 正在创建虚拟环境: .venv
  %PY% -m venv .venv
  if %errorlevel% neq 0 (
    echo [ERROR] 创建虚拟环境失败。
    echo 你可以尝试手动运行: %PY% -m venv .venv
    echo.
    pause
    exit /b 1
  )
) else (
  echo 已检测到虚拟环境: .venv
)

echo.
:: 3) Activate venv
call ".venv\\Scripts\\activate.bat"
if %errorlevel% neq 0 (
  echo [ERROR] 激活虚拟环境失败。
  echo.
  pause
  exit /b 1
)

echo.
:: 4) Install deps
echo 正在升级 pip...
python -m pip install -U pip

echo.
echo 正在安装依赖: requirements.txt
python -m pip install -r requirements.txt
if %errorlevel% neq 0 (
  echo.
  echo [ERROR] 依赖安装失败。
  echo 你可以尝试:
  echo 1. 检查网络代理/防火墙
  echo 2. 在命令行里重试: python -m pip install -r requirements.txt
  echo.
  pause
  exit /b 1
)

echo.
echo 依赖自检:
python -c "import yaml; print('PyYAML OK')" >nul 2>nul
if %errorlevel%==0 (
  echo - PyYAML: OK
) else (
  echo - PyYAML: NOT FOUND (可选, 但建议安装)
)

python -c "import jieba; print('jieba OK')" >nul 2>nul
if %errorlevel%==0 (
  echo - jieba: OK
) else (
  echo - jieba: NOT FOUND (系统会自动关闭 jieba 分词并回退到字符切分, 不影响运行)
)

echo.
echo ======================================
echo 完成. 下一步:
echo 1. 双击 "快速启动观测台.bat"
echo 2. 浏览器打开后输入短文本试跑一轮
echo ======================================
echo.
pause
endlocal

