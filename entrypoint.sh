#!/bin/sh
ls -l /ai
# Start the Java application
exec java -Djava.security.egd=file:/dev/./urandom -jar /ai/mcpdemo.jar