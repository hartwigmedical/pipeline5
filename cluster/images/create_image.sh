#!/usr/bin/env bash

set -e
script="$(mktemp)"
touch "${script}"
/usr/bin/env bash "$(dirname "$0")/generate_imaging_script.sh" "$@"> ${script}
echo "Executing generated script ${script}. It will not be cleaned up opon failure."
chmod +x ${script}
${script} 
rm ${script}
