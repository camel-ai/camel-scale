@echo off
setlocal

REM Read paths from the configuration file
for /F "tokens=1,* delims==" %%i in (server_home_path.txt) do (
    if "%%i"=="ZOOKEEPER_HOME" set ZOOKEEPER_HOME=%%j
)

REM Check if ZOOKEEPER_HOME is set properly
if "%ZOOKEEPER_HOME%"=="" (
    echo ZOOKEEPER_HOME is not set. Please check the server_home_path file.
    exit /b 1
)

REM Check if the ZooKeeper home directory exists
if not exist "%ZOOKEEPER_HOME%" (
    echo ZooKeeper directory %ZOOKEEPER_HOME% does not exist. Please check the server_home_path file.
    exit /b 1
)

REM Start the ZooKeeper server with configuration file
echo Starting ZooKeeper...
call "%ZOOKEEPER_HOME%\bin\zkServer.cmd"

endlocal
