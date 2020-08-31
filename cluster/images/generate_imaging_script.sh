#!/usr/bin/env bash
#
# Generate a creation script for a VM and image

LOCATION="europe-west4"
ZONE="${LOCATION}-a"
PROJECT="hmf-pipeline-development"
VERSION="$(date "+%y%m%d%H%M")"
GCL="gcloud beta compute --project=${PROJECT}"
TYPE="$1"
IMG_PROJECT="$2"
IMG_FAMILY="$3"

which gcloud 2>&1 >/dev/null
[[ $? -ne 0 ]] && echo "gcloud is missing" >&2 && exit 1

[[ $# -ne 3 ]] && echo "Provide: [type, source image project, source image family]" && exit 1
cmds="$(dirname "$0")/${TYPE}.cmds"
[[ ! -f ${cmds} ]] && echo "Cannot find commands file '${cmds}'!" >&2 && exit 1
sourceInstance="${TYPE}-${VERSION}"

echo "#!$(which sh) -e"
echo "$GCL instances create ${sourceInstance} --description=\"Instance for ${TYPE} disk image creation\" --zone=${ZONE} \
  --boot-disk-size 200 --boot-disk-type pd-ssd --machine-type n1-highcpu-4 --image-project=${IMG_PROJECT} --image-family=${IMG_FAMILY}"
echo "sleep 10"

egrep -v '^#|^ *$' ${cmds} | while read cmd
do
  echo "$GCL ssh ${sourceInstance} --zone=${ZONE} --command=\"$cmd\"" 
done

echo "$GCL instances stop ${sourceInstance} --zone=${ZONE}"
echo "$GCL images create ${sourceInstance} --family=${TYPE} --source-disk=${sourceInstance} --source-disk-zone=${ZONE} --storage-location=${LOCATION}"
echo "$GCL instances -q delete ${sourceInstance} --zone=${ZONE}"
