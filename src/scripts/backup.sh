#!/bin/sh

APP_HOME=`dirname $0`

java \
-Djava.util.logging.config.file=$APP_HOME/config/logging.properties \
-cp $APP_HOME/resources/:$APP_HOME/lib/* \
org.hilel14.iceberg.Backup "$@"
