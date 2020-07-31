#!/usr/bin/env bash
#
# Generate a creation script for a VM and image

LOCATION="europe-west4"
ZONE="${LOCATION}-a"
DEV_PROJECT="hmf-pipeline-development" 
PROJECT="${DEV_PROJECT}"
VERSION=5-13
TYPE="pipeline5"

if [ -n "$1" ]
then
  PROJECT=$1
fi

GCL="gcloud beta compute --project=${PROJECT}"

which gcloud 2>&1 >/dev/null
[[ $? -ne 0 ]] && echo "gcloud is missing" >&2 && exit 1

cmds="$(dirname "$0")/${TYPE}.cmds"
[[ ! -f ${cmds} ]] && echo "Cannot find commands file '${TYPE}'!" >&2 && exit 1
sourceInstance="${TYPE}-${VERSION}"

# From here on in we want to fail immediately on any surprises
echo "#!$(which sh) -e"

# Defaults to Debian 9; we assume that for now
network="--network=${TYPE} --subnet=${TYPE}"
[[ "${PROJECT}" = "${DEV_PROJECT}" ]] && network=""
echo "$GCL instances create ${sourceInstance} --description=\"Instance for ${TYPE} disk image creation\" --zone=${ZONE} ${network} \
  --boot-disk-size 200 --boot-disk-type pd-ssd --machine-type n1-highcpu-4 --image-project=debian-cloud --image-family=debian-9"
echo "sleep 10"
# Ignore lines consisting only of spaces, or those beginning with '#'
tunnel="--tunnel-through-iap"
[[ "${PROJECT}" = "${DEV_PROJECT}" ]] && tunnel=""
egrep -v '^#|^ *$' ${cmds} | while read cmd
do
  echo "$GCL ssh ${sourceInstance} --zone=${ZONE} --command=\"$cmd\" ${tunnel}"
done

echo "$GCL instances stop ${sourceInstance} --zone=${ZONE}"
echo "$GCL images create ${sourceInstance}-$(date +%Y%m%d%H%M) --family=${sourceInstance} --source-disk=${sourceInstance} --source-disk-zone=${ZONE} --storage-location=${LOCATION}"
echo "$GCL instances -q delete ${sourceInstance} --zone=${ZONE}"
