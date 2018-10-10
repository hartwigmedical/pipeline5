#!/bin/bash

/usr/bin/java ${JAVA_OPTS} -jar /usr/share/pipeline5/bootstrap.jar "$@"
status=$?
if [ $status -ne 0 ]; then
  echo "Failed to start bootstrap: $status"
  exit $status
fi