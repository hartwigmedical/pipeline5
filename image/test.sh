#!/usr/bin/env bash

PV5_JAR="/Users/matthijsvanniekerk/hartwigmedical/pipeline5/cluster/target/cluster-local-SNAPSHOT.jar"
VERSION_CMD="java -cp ${PV5_JAR} com.hartwig.pipeline.tools.VersionUtils"

declare -a tool_versions
while read tool tool_version; do
   tool_versions[$tool]="$tool_version"
done <<< "$(${VERSION_CMD} tools)"

for tool in "${!tool_versions[@]}"; do
    tool_version="${tool_versions[$tool]}"
    echo "sudo /tmp/fetch_tool_from_registry.sh $tool $tool_version"
done