@echo off

set CURRENT_SCRIPT=%~f0
set APP_HOME=%~dp0

set APP_HOME=C:\iceberg\app

java -Djava.util.logging.config.file=%APP_HOME%\config\logging.properties -cp %APP_HOME%\resources/;%APP_HOME%\lib\* org.hilel14.iceberg.cli.Backup %*
