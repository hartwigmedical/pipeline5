#!/usr/bin/env bash

set -e

tool=$1
version=$2
full=${tool}-${version}
local_jar="${full}.jar"
if [ -n "$3" ]; then
  local_jar="$3"
else
  wget "https://github.com/hartwigmedical/hmftools/releases/download/${tool}-v${version}/${full}.jar"
fi
gsutil cp $local_jar "gs://common-tools/${tool}/${version}/${tool}.jar"
if [ -z "$3" ]; then
  rm $local_jar
fi

./images/create_public_image.sh -t

mvn versions:set -DnewVersion=${full}
mvn clean install -DskipTests -Dcontainer-registry=europe-west4-docker.pkg.dev/hmf-build/hmf-docker/pipeline5
mvn versions:set -DnewVersion=local-SNAPSHOT