#!/usr/bin/env bash

set -e

function do_curl() {
    # New versions (>7.52.1) of `curl` work properly with `--oauth2-bearer` instead of the explicit setting of a header
    # At time of writing the imager VM was using this older version without prospect of upgrade
    token="$(gcloud auth print-access-token)"
    curl -H "authorization: Bearer ${token}" "$@"
}

AR_URL="https://europe-west4-maven.pkg.dev/hmf-build/hmf-maven/com/hartwig"
TOOL_DIR="/opt/tools"

[[ $# -ne 2 ]] && echo "USAGE: $0 [tool name] [tool version]" && exit 1

tool=$1
version=$2

localDir="${TOOL_DIR}/${tool}/${version}"
remoteDir="${AR_URL}/${tool}/${version}"

mkdir -p $localDir
expected_md5="$(do_curl -o - -L ${remoteDir}/${tool}-${version}-jar-with-dependencies.jar.md5)"
do_curl -o ${localDir}/${tool}.jar -L ${remoteDir}/${tool}-${version}-jar-with-dependencies.jar
actual_md5="$(md5sum ${localDir}/${tool}.jar | awk '{print $1}')"
[[ $actual_md5 == "" ]] && echo "No local md5" && exit 1
if [[ $actual_md5 != $expected_md5 ]]; then
    echo "Checksum not as expected for ${tool} version ${version}" 
    rm ${localDir}/${tool}.jar
    exit 1
else
    echo "Fetched ${tool} version ${version} (md5=${actual_md5}) to ${localDir}/${tool}.jar"
fi

