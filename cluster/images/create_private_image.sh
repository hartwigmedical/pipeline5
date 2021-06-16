#!/usr/bin/env bash
#
# Build an image containing our private resources, suitable for internal runs.

LOCATION="europe-west4"
ZONE="${LOCATION}-a"
IMAGE_SOURCE_PROJECT="hmf-pipeline-development"
SOURCE_REPO_PROJECT="hmf-pipeline-prod-e45b00f2"
DEST_PROJECT="hmf-pipeline-prod-e45b00f2"
DEST_SERVICE_ACCOUNT="649805142670-compute@developer.gserviceaccount.com"

which jq >/dev/null || (echo Please install jq && exit 1)
which gcloud >/dev/null || (echo Please install gcloud && exit 1)
[[ $# -ne 1 ]] && echo "Provide the source image" && exit 1
source_image="$1"

json="$(gcloud compute images describe $source_image --project=$IMAGE_SOURCE_PROJECT --format=json)"
[[ $? -ne 0 ]] && echo "Unable to find image $source_image in $IMAGE_SOURCE_PROJECT" && exit 1

dest_image="$(echo $source_image | sed 's/-[0-9]*$//')-private"
gcloud compute images describe $dest_image --project=$DEST_PROJECT >/dev/null 2>&1
[[ $? -eq 0 ]] && echo "$dest_image exists in project $DEST_PROJECT!" && exit 1

image_family="$(echo $json | jq -r '.family')"
imager_vm="${image_family}-imager"

echo Ready to create VM ${imager_vm} in project ${DEST_PROJECT}
echo The VM will be used to create private image ${source_image} in family ${image_family}
echo You must have sufficient permissions in ${DEST_PROJECT}
echo
echo 'If you ARE NOT currently doing a PRODUCTION upgrade maybe you should not be running this script!'
echo
read -p "Continue [y|N]? " response
[[ $response != 'y' && $response != 'Y' ]] && echo "Aborting at user request" && exit 1

gcloud compute instances create $imager_vm --description="Instance for private pipeline5 disk image creation" \
    --zone=${ZONE} --boot-disk-size 200 --boot-disk-type pd-ssd --machine-type n1-highcpu-2 \
    --image-project=$IMAGE_SOURCE_PROJECT --image=$source_image --scopes=default,cloud-source-repos-ro \
    --project=$DEST_PROJECT --service-account=$DEST_SERVICE_ACCOUNT || exit 1

ssh="gcloud compute ssh $imager_vm --zone=$ZONE --project=$DEST_PROJECT --tunnel-through-iap"
echo "Polling for active instance"
while true; do
    sleep 1
    $ssh --command="exit 0"
    [[ $? -eq 0 ]] && break
done
echo "Instance running, continuing with imaging"

set -e
$ssh --command="sudo mkdir /tmp/resources"
$ssh --command="sudo gcloud source repos clone common-resources-private /tmp/resources --project=$SOURCE_REPO_PROJECT"
$ssh --command="sudo rm -r /tmp/resources/.git"
$ssh --command="cd /tmp/resources; sudo tar -C /tmp/resources -cf - * | sudo tar -C /opt/resources -xv"

gcloud compute instances stop $imager_vm --zone=${ZONE} --project=$DEST_PROJECT
gcloud compute images create $dest_image --family=$image_family --source-disk=$imager_vm --source-disk-zone=$ZONE \
    --storage-location=$LOCATION --project=$DEST_PROJECT
gcloud compute instances -q delete $imager_vm --zone=$ZONE --project=$DEST_PROJECT
