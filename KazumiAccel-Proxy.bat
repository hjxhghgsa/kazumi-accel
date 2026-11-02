@echo off
REM 启动 KazumiAccel 本地加速代理（火火）
REM 可选：把 http://127.0.0.1:8899 填进 Kazumi 设置里的「全局代理」
cd /d "%~dp0"
node "%~dp0server.js"
pause
