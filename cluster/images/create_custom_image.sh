#!/usr/bin/env bash

set -e
TOOLS_BUCKET="common-tools"
PROJECT="hmf-pipeline-development"
if [ -n "$1" ]
  then
    PROJECT=$1
fi
if [ -n "$2" ]
  then
    TOOLS_BUCKET=$2
fi

script="$(mktemp)"
touch "${script}"
"$(dirname "$0")/generate_imaging_script.sh" $PROJECT > ${script}
echo "Executing generated script ${script}"
chmod +x ${script}
${script} $TOOLS_BUCKET
rm ${script}

