#!/usr/bin/env bash
#
# Generate a creation script for a VM and image

LOCATION="europe-west4"
ZONE="${LOCATION}-a"
PROJECT="hmf-pipeline-development" 
TYPE="pipeline5"
VERSION=5-16

if [ -n "$1" ]; then
    FLAVOUR="$1"
fi

GCL="gcloud beta compute --project=${PROJECT}"

which gcloud 2>&1 >/dev/null
[[ $? -ne 0 ]] && echo "gcloud is missing" >&2 && exit 1

cmds="$(dirname "$0")/${TYPE}.cmds"
[[ ! -f ${cmds} ]] && echo "Cannot find commands file '${TYPE}'!" >&2 && exit 1
sourceInstance="${TYPE}-${VERSION}${FLAVOUR+"-$FLAVOUR"}"

echo "#!$(which sh) -e"

echo "$GCL instances create ${sourceInstance} --description=\"Instance for ${TYPE} disk image creation\" --zone=${ZONE} \
    --boot-disk-size 200 --boot-disk-type pd-ssd --machine-type n1-highcpu-4 --image-project=debian-cloud --image-family=debian-9"
echo "sleep 10"
egrep -v '^#|^ *$' ${cmds} | while read cmd
do
    echo "$GCL ssh ${sourceInstance} --zone=${ZONE} --command=\"$cmd\""
done

if [ -n "$FLAVOUR" ]; then
    bucket="gs://common-resources-${FLAVOUR}-overrides"
    copy_overrides="sudo gsutil -m -o 'GSUtil:parallel_thread_count=1' -o 'GSUtil:sliced_object_download_max_components=4' cp -r ${bucket}/* /opt/resources/"
    echo "$GCL ssh ${sourceInstance} --zone=${ZONE} --command=\"$copy_overrides\""
fi

echo "$GCL instances stop ${sourceInstance} --zone=${ZONE}"
echo "$GCL images create ${sourceInstance}-$(date +%Y%m%d%H%M) --family=${sourceInstance} --source-disk=${sourceInstance} --source-disk-zone=${ZONE} --storage-location=${LOCATION}"
echo "$GCL instances -q delete ${sourceInstance} --zone=${ZONE}"
