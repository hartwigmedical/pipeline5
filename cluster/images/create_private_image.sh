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

image_family="$(echo $json | jq -r '.family')"
imager_vm="${image_family}-imager"

echo Ready to create VM ${image_family}_imager in project ${DEST_PROJECT}
echo The VM will be used to create image ${source_image} in family ${image_family}
echo You must have sufficient permissions in ${DEST_PROJECT}
echo
read -p "Continue [Y|n]? " response
[[ $response != 'y' && $response != 'Y' && $response != '' ]] && echo "Aborting at user request" && exit 1

set -e

gcloud compute instances create $imager_vm --description="Instance for private pipeline5 disk image creation" \
    --zone=${ZONE} --boot-disk-size 200 --boot-disk-type pd-ssd --machine-type n1-highcpu-2 \
    --image-project=$IMAGE_SOURCE_PROJECT --image=$source_image --scopes=default,cloud-source-repos-ro \
    --project=$DEST_PROJECT --service-account=$DEST_SERVICE_ACCOUNT

sleep 20
ssh="gcloud compute ssh $imager_vm --zone=$ZONE --project=$DEST_PROJECT --tunnel-through-iap"
$ssh --command="sudo mkdir /tmp/resources"
$ssh --command="sudo gcloud source repos clone common-resources-private /tmp/resources --project=$SOURCE_REPO_PROJECT"
$ssh --command="sudo rm -r /tmp/resources/.git"
$ssh --command="cd /tmp/resources; sudo tar -C /tmp/resources -cf - * | sudo tar -C /opt/resources -xv"

gcloud compute instances stop $imager_vm --zone=${ZONE} --project=$DEST_PROJECT
gcloud compute images create $source_image --family=$image_family --source-disk=$imager_vm --source-disk-zone=$ZONE \
    --storage-location=$LOCATION --project=$DEST_PROJECT
gcloud compute instances -q delete $imager_vm --zone=$ZONE --project=$DEST_PROJECT
