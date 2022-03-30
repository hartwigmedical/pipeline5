#!/usr/bin/env bash

set -e

[[ $# -ne 2 ]] && echo "USAGE: $(basename $0) [version] [private resources repo project]" && exit 1
mkdir /tmp/resources
gcloud source repos clone common-resources-private /tmp/resources --project=${2}
cd /tmp/resources
git tag "$1"
git push origin "$1"
rm -r .git/
find . -type f | while read f; do
    dirname $(echo "$f" | sed 's#^\./##')
done | sort -u | while read d; do
    [[ $d != "." ]] && rm -rf /opt/resources/$d
done
tar -cf - * | tar -C /opt/resources -xv
