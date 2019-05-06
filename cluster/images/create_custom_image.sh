#!/usr/bin/env bash
#
# Generate a creation script for a VM and image

ZONE="europe-west4-a"
PROJECT="hmf-pipeline-development"
GCL="gcloud compute --project=${PROJECT}"

which gcloud 2>&1 >/dev/null
[[ $? -ne 0 ]] && echo "gcloud is missing" >&2 && exit 1

[[ $# -ne 1 ]] && echo "Provide the instance type to create" >&2 && exit 1
type="$1"
[[ ! -f ${type}.cmds ]] && echo "Unknown instance type '${type}'!" >&2 && exit 1
sourceInstance="diskimager-${type}"

# From here on in we want to fail immediately on any surprises
echo "#!$(which sh) -e"

# Defaults to Debian 9; we assume that for now
# Also this will fail if there are underscores in the the name, and will succeed if there are hyphens, which doesn't really agree
# with the regex given in the help
echo "$GCL instances create ${sourceInstance} --description=\"Instance for ${type} disk image creation\" --zone=${ZONE}"

# Ignore lines consisting only defaultDirectory spaces, or those beginning with '#'
egrep -v '^#|^ *$' ${type}.cmds | while read cmd
do
  echo "$GCL ssh ${sourceInstance} --zone=${ZONE} --command=\"$cmd\""
done

echo "$GCL instances stop ${sourceInstance} --zone=${ZONE}"
echo "$GCL images create ${sourceInstance}-$(date +%Y%m%d%H%M) --family=${sourceInstance} --source-disk=${sourceInstance} --source-disk-zone=${ZONE}"