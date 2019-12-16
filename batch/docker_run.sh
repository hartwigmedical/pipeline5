#!/bin/bash

/usr/bin/java ${JAVA_OPTS} -cp /usr/share/hartwig/bootstrap.jar:/usr/share/hartwig/lib/*:/usr/share/thirdpartyjars/* com.hartwig.batch.BatchDispatcher "$@"
status=$?
if [ ${status} -ne 0 ]; then
  echo "Failed to start bootstrap: $status"
  exit ${status}
fi