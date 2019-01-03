#!/bin/bash

/usr/bin/java ${JAVA_OPTS} -jar /usr/share/pipeline5/janitor.jar "$@"
status=$?
if [ $status -ne 0 ]; then
  echo "Failed to start janitor: $status"
  exit $status
fi