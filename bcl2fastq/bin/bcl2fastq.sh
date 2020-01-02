#!/bin/bash

/usr/bin/java ${JAVA_OPTS} -jar /usr/share/bcl2fastq/bcl2fastq.jar "$@"
status=$?
if [ ${status} -ne 0 ]; then
  echo "Failed to start bcl2fastq: $status"
  exit ${status}
fi