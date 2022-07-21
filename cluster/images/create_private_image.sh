#!/usr/bin/env bash
#
# Build an image containing our private resources, suitable for internal runs.

LOCATION="europe-west4"
ZONE="${LOCATION}-a"
IMAGE_SOURCE_PROJECT="hmf-pipeline-development"
SOURCE_REPO_PROJECT="hmf-pipeline-prod-e45b00f2"
DEST_PROJECT="hmf-pipeline-prod-e45b00f2"
DEST_SERVICE_ACCOUNT="649805142670-compute@developer.gserviceaccount.com"

print_usage() {
    cat <<EOM
USAGE: $0 [source image] [optional arguments]

[source image] is the name of an image that already exists in ${IMAGE_SOURCE_PROJECT}

Optional arguments:

  --resources-commit [sha1]     SHA1 in private resources repository to checkout instead of HEAD.
                                No tagging will be done in the private resources repository.
  --use-sha1-for-suffix         If using a particular commit instead of HEAD, make the final component of the name the
                                SHA1 of the checked-out commit rather than the current date.
  --result-code-url [GCS url]   GCS (eg "gs://bucket/path") URL to write the exit code to (bucket must be writable).
                                This script will interact with the bucket rather than with the imager instance over SSH.
  --non-interactive             Script is being run non-interactively so skip warnings and prompts when possible.
EOM
}

set -o pipefail

[[ $# -eq 0 ]] && print_usage && exit 1
source_image="$1"
shift

args=$(getopt -o "" --longoptions resources-commit:,use-sha1-for-suffix,result-code-url:,non-interactive -- "$@")
[[ $? != 0 ]] && print_usage && exit 1
eval set -- "$args"

use_sha1_for_suffix=""
while true; do
    case "$1" in
        --resources-commit) resources_commit=$2; shift 2 ;;
        --use-sha1-for-suffix) use_sha1_for_suffix=true; shift 1 ;;
        --result-code-url) result_code_url=$2; shift 2 ;;
        --non-interactive) non_interactive=true; shift 1 ;;
        --) shift; break ;;
    esac
done

suffix="$(date +%Y%m%d%H%M)"
[[ $# -gt 0 ]] && echo "Unexpected arguments: $@" && print_usage && exit 1
if [[ -n $use_sha1_for_suffix ]]; then
    if [[ -z $resources_commit ]]; then
        echo "Must specify the resources commit if requesting a SHA1 be used in the name"
        print_usage
        exit 1
    else
        suffix="$resources_commit"
    fi
fi

for dep in jq gcloud gsutil; do
    which ${dep} >/dev/null || (echo Please install ${dep} && exit 1)
done

json="$(gcloud compute images describe $source_image --project=$IMAGE_SOURCE_PROJECT --format=json)"
[[ $? -ne 0 ]] && echo "Unable to find image $source_image in $IMAGE_SOURCE_PROJECT" && exit 1

dest_image="${source_image}-${suffix}-private"
gcloud compute images describe $dest_image --project=$DEST_PROJECT >/dev/null 2>&1
[[ $? -eq 0 ]] && echo "$dest_image exists in project $DEST_PROJECT!" && exit 1

image_family="$(echo $json | jq -r '.family')${resources_commit:+"-unofficial"}"
imager_vm="${image_family}-imager"

if [[ -z $non_interactive ]]; then
cat <<EOM
Ready to create VM [${imager_vm}] in project [${DEST_PROJECT}]
The VM will be used to create private image [${dest_image}] in family [${image_family}]
You must have sufficient permissions in [${DEST_PROJECT}]

If you ARE NOT currently doing a PRODUCTION upgrade maybe you should not be running this script!

EOM
    read -p "Continue [y|N]? " response
else
    response=y
fi
[[ $response != 'y' && $response != 'Y' ]] && echo "Aborting at user request" && exit 0

if [[ -n $resources_commit ]]; then
    additional_args="--checkout-commit $resources_commit"
else
    additional_args="--tag-as-version $dest_image"
fi    

if [[ -n $result_code_url ]]; then
    gsutil -q stat $result_code_url && echo "Result code URL $result_code_url already exists!" && exit 1
    more_args="--metadata-from-file=startup-script=$(dirname $0)/private_resource_checkout.sh"
    more_args="$more_args --metadata=result-code-url=$result_code_url"
    more_args="${more_args},project=$SOURCE_REPO_PROJECT"
    more_args="${more_args},$(echo $additional_args | sed -e 's/^--//' -e 's/ /=/g')"
fi

gcloud compute instances create $imager_vm --description="Instance for private pipeline5 disk image creation" \
    --zone=${ZONE} --boot-disk-size 200 --boot-disk-type pd-ssd --machine-type n1-highcpu-2 \
    --image-project=$IMAGE_SOURCE_PROJECT --image=$source_image --scopes=default,cloud-platform \
    --project=$DEST_PROJECT --service-account=$DEST_SERVICE_ACCOUNT --network-interface=no-address $more_args || exit 1

if [[ -n $result_code_url ]]; then
    echo "Waiting for the presence of ${result_code_url}."
    echo "If this does not return after a reasonable time check /var/log/daemon.log on the $imager_vm instance."
    while true; do
        sleep 10
        if gsutil -q stat $result_code_url; then
            exit_code=$(gsutil cat $result_code_url)
            if [[ $exit_code != 0 ]]; then
                echo "Failure while checking out the private resources"
                exit $exit_code
            else
                echo "Private resources checkout succeeded"
                break
            fi
        fi
    done
else
    ssh="gcloud compute ssh $imager_vm --zone=$ZONE --project=$DEST_PROJECT --tunnel-through-iap"
    echo "Polling for active instance"
    while true; do
        sleep 1
        $ssh --command="exit 0"
        [[ $? -eq 0 ]] && break
    done
    echo "Instance running, continuing with imaging"
    
    set -e
    gcloud compute scp $(dirname $0)/private_resource_checkout.sh ${imager_vm}:/tmp/ --zone=$ZONE --project=$DEST_PROJECT --tunnel-through-iap 
    sleep 60
    
    $ssh --command="sudo /tmp/private_resource_checkout.sh --project $SOURCE_REPO_PROJECT $additional_args"
fi

gcloud compute instances stop $imager_vm --zone=${ZONE} --project=$DEST_PROJECT
gcloud compute images create $dest_image --family=$image_family --source-disk=$imager_vm --source-disk-zone=$ZONE \
    --storage-location=$LOCATION --project=$DEST_PROJECT
gcloud compute instances -q delete $imager_vm --zone=$ZONE --project=$DEST_PROJECT
