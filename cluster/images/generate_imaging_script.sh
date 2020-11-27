#!/usr/bin/env bash
#
# Generate a creation script for a VM and image

LOCATION="europe-west4"
ZONE="${LOCATION}-a"
PROJECT="hmf-pipeline-development"
VERSION=5-17

TOOLS_ONLY=false
while getopts ':tf:' flag; do
    case "${flag}" in
        t) TOOLS_ONLY=true ;;
        f) FLAVOUR=${OPTARG} ;;
        *) ;;
    esac
done
sourceInstance="pipeline5-${VERSION}${FLAVOUR+"-$FLAVOUR"}"
image_project="debian-cloud"
image_family="debian-9"
base_image_cmds="$(dirname "$0")/base.cmds"
tools_image_cmds="$(dirname "$0")/tools.cmds"
all_cmds=$(echo $base_image_cmds $tools_image_cmds)
if [ "$TOOLS_ONLY" = true ]; then
  image_project="hmf-pipeline-development"
  image_family=${sourceInstance}
  all_cmds=$tools_image_cmds
fi

GCL="gcloud beta compute --project=${PROJECT}"

which gcloud 2>&1 >/dev/null
[[ $? -ne 0 ]] && echo "gcloud is missing" >&2 && exit 1


echo "#!$(which sh) -e"

echo "$GCL instances create ${sourceInstance} --description=\"Instance for pipeline5 disk image creation\" --zone=${ZONE} \
    --boot-disk-size 200 --boot-disk-type pd-ssd --machine-type n1-highcpu-4 --image-project=${image_project} --image-family=${image_family}"
echo "sleep 10"
cat $all_cmds | egrep -v  '^#|^ *$' | while read cmd
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
