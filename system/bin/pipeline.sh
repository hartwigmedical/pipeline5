#!/bin/bash

/usr/bin/java -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xloggc:logs/gc.log -XX:+UseG1GC -jar /usr/share/pipeline2/system.jar
status=$?
if [ $status -ne 0 ]; then
  echo "Failed to start pipeline: $status"
  exit $status
fi