#!/bin/bash

/usr/bin/java ${JAVA_OPTS} \
    -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xloggc:logs/gc.log -XX:+UseG1GC \
     -XX:+PrintFlagsFinal -jar /usr/share/pipeline2/system.jar
status=$?
if [ $status -ne 0 ]; then
  echo "Failed to start pipeline: $status"
  exit $status
fi