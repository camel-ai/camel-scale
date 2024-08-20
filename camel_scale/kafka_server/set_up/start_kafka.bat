@echo off
setlocal enabledelayedexpansion

:: Load configuration
for /f "tokens=1,2 delims==" %%a in (kafka_config.txt) do (
    set %%a=%%b
)

:: Set Kafka and Zookeeper configuration paths
set "ZOOKEEPER_CONFIG=%KAFKA_HOME%\config\zookeeper.properties"
set "KAFKA_CONFIG=%KAFKA_HOME%\config\server.properties"

:: Start Zookeeper
echo Starting Zookeeper...
start "Zookeeper" cmd /c ""%KAFKA_HOME%\bin\windows\zookeeper-server-start.bat" "%ZOOKEEPER_CONFIG%""

:: Wait for Zookeeper to start
timeout /t 10 /nobreak > nul

:: Start Kafka
echo Starting Kafka...
start "Kafka" cmd /c ""%KAFKA_HOME%\bin\windows\kafka-server-start.bat" "%KAFKA_CONFIG%""

:: Wait for Kafka to start
timeout /t 10 /nobreak > nul

echo Kafka and Zookeeper have been started.

endlocal