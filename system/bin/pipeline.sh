#!/bin/bash

/usr/bin/java -Xmx64G -jar /usr/share/pipeline2/system.jar
status=$?
if [ $status -ne 0 ]; then
  echo "Failed to start pipeline: $status"
  exit $status
fi