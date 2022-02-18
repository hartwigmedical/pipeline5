#!/usr/bin/env bash

LOCATION="europe-west4"
ZONE="${LOCATION}-a"
IMAGE_PROJECT="hmf-pipeline-development"

which jq >/dev/null || (echo Please install jq && exit 1)
which gcloud >/dev/null || (echo Please install gcloud && exit 1)

while getopts ':s:d:' flag; do
    case "${flag}" in
        s) source_image=${OPTARG} ;;
        d) pilot_dir=${OPTARG} ;;
        *) ;;
    esac
done

if [ -z $source_image -o -z $pilot_dir ]; then
    echo
    echo "USAGE: $0 -s [source image in project $IMAGE_PROJECT] -d [local directory to sync to image as pilot tools]"
    echo
    echo "Build an image for pilot-testing new tools, based upon one of our public images but also including the contents of the given"
    echo "local directory so that pilot versions of Hartwig tools can be tested against the pipeline before being released."
    echo
    echo "All arguments must be provided!"
    exit 1
fi

[[ ! -d $pilot_dir ]] && echo "Given location '${pilot_dir}' is not a directory!" && exit 1

json="$(gcloud compute images describe $source_image --project=$IMAGE_PROJECT --format=json)"
[[ $? -ne 0 ]] && echo "Unable to find image $source_image in $IMAGE_PROJECT" && exit 1

dest_image="${source_image}-$(date +%Y%m%d%H%M)-pilot"
gcloud compute images describe $dest_image --project=$IMAGE_PROJECT >/dev/null 2>&1
[[ $? -eq 0 ]] && echo "$dest_image exists in project $IMAGE_PROJECT!" && exit 1

image_family="$(echo $json | jq -r '.family')"
imager_vm="${image_family}-$(whoami)-pilot-imager"

cat << EOM
Ready to create VM [${imager_vm}] in project [${IMAGE_PROJECT}].
The VM will be used to create pilot testing image [${dest_image}] 
in family [${image_family}]. You must have sufficient permissions in
[${IMAGE_PROJECT}].

This script exists to make it easier to test not-yet-released tools in a
pipeline5 setting during development. However when testing is complete
pilot tools should be released and a proper public image created.

Final note: make sure the tools in the local directory are named so that
your pipeline will be able to pick them up! Also you will need to change
the version of the tool under test to "pilot" in the appropriate Versions
class.

This for example will make a "pilot" version of the "tool" tool available
in a pilot image:

$ ls /tmp/my_pilot_versions/ 
tool.jar 
$ $(basename $0) -d /tmp/my_pilot_versions -s ...

EOM
read -p "Continue [y|N]? " response
[[ $response != 'y' && $response != 'Y' ]] && echo "Aborting at user request" && exit 1

gcloud compute instances create $imager_vm --description="Instance for pilot pipeline5 image creation started $(date) by $(whoami)" \
    --zone=${ZONE} --boot-disk-size 200 --boot-disk-type pd-ssd --machine-type n1-highcpu-2 \
    --image-project=$IMAGE_PROJECT --image=$source_image --scopes=default --project=$IMAGE_PROJECT \
    --network-interface=no-address || exit 1

ssh="gcloud compute ssh $imager_vm --zone=$ZONE --project=$IMAGE_PROJECT --tunnel-through-iap"
echo "Polling for active instance"
while true; do
    sleep 1
    $ssh --command="exit 0"
    [[ $? -eq 0 ]] && break
done
echo "Instance running, continuing with imaging"

set -e
for f in $(find ${pilot_dir} -maxdepth 1 -type f); do
    toolName="$(echo $(basename $f) | sed 's/\..*//')"
    $ssh --command="sudo mkdir -p /opt/tools/${toolName}/pilot && sudo chmod a+w /opt/tools/${toolName}/pilot"
    gcloud compute scp ${f} ${imager_vm}:/opt/tools/${toolName}/pilot/$(basename $f) --zone=$ZONE --project=$IMAGE_PROJECT --tunnel-through-iap 
done

gcloud compute instances stop $imager_vm --zone=${ZONE} --project=$IMAGE_PROJECT
gcloud compute images create $dest_image --family=$image_family --source-disk=$imager_vm --source-disk-zone=$ZONE \
    --storage-location=$LOCATION --project=$IMAGE_PROJECT
gcloud compute instances -q delete $imager_vm --zone=$ZONE --project=$IMAGE_PROJECT
