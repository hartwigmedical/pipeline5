#!/usr/bin/env bash

set -e

[[ $# -ne 1 ]] && echo "Provide the project hosting the private resources repository" && exit 1
mkdir /tmp/resources
gcloud source repos clone common-resources-private /tmp/resources --project=${1}
rm -r /tmp/resources/.git
cd /tmp/resources
find . -type f | while read f; do
    dirname $(echo "$f" | sed 's#^\./##')
done | sort -u | while read d; do
    [[ $d != "." ]] && rm -r /opt/resources/$d
done
tar -cf - * | tar -C /opt/resources -xv
