#!/usr/bin/env bash

set -e

script="$(mktemp)"
touch "${script}"
"$(dirname "$0")/generate_imaging_script.sh" > ${script}
echo "Executing generated script ${script}"
chmod +x ${script}
${script}
rm ${script}

