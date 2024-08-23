@echo off
setlocal

REM Start ZooKeeper in a new terminal window
start "ZooKeeper" cmd /c start_zookeeper.bat

REM Pause for a few seconds to ensure ZooKeeper starts completely
timeout /t 10

REM Start Kafka in a new terminal window
start "Kafka" cmd /c start_kafka.bat

echo Kafka and ZooKeeper have been started.
endlocal
