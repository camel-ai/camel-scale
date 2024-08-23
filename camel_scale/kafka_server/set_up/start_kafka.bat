@echo off
setlocal

REM Read paths from the configuration file
for /F "tokens=1,* delims==" %%i in (server_home_path.txt) do (
    if "%%i"=="KAFKA_HOME" set KAFKA_HOME=%%j
)

REM Check if KAFKA_HOME is set properly
if "%KAFKA_HOME%"=="" (
    echo KAFKA_HOME is not set. Please check the server_home_path file.
    exit /b 1
)

REM Check if the Kafka home directory exists
if not exist "%KAFKA_HOME%" (
    echo Kafka directory %KAFKA_HOME% does not exist. Please check the server_home_path file.
    exit /b 1
)

REM Start the Kafka server with configuration file
echo Starting Kafka...
call "%KAFKA_HOME%\bin\windows\kafka-server-start.bat" "%KAFKA_HOME%\config\server.properties"

endlocal
