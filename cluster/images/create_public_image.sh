#!/usr/bin/env bash

LOCATION="europe-west4"
ZONE="${LOCATION}-a"
PROJECT="hmf-pipeline-development"
VERSION=5-26

TOOLS_ONLY=false
while getopts ':tf:' flag; do
    case "${flag}" in
        t) TOOLS_ONLY=true ;;
        f) FLAVOUR=${OPTARG} ;;
        *) ;;
    esac
done
sourceInstance="pipeline5-${VERSION}${FLAVOUR+"-$FLAVOUR"}"
imageName="${sourceInstance}-$(date +%Y%m%d%H%M)"
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

which gcloud 2>&1 >/dev/null
[[ $? -ne 0 ]] && echo "gcloud is missing" >&2 && exit 1
set -e
GCL="gcloud beta compute --project=${PROJECT}"
generated_script=$(mktemp -t image_script_generated_XXXXX.sh)

(
echo "#!/usr/bin/env bash"
echo
echo "set -e"
echo $GCL instances create $sourceInstance --description=\"Instance for pipeline5 disk image creation\" --zone=${ZONE} \
    --boot-disk-size 200 --boot-disk-type pd-ssd --machine-type n1-highcpu-4 --image-project=${image_project} \
    --image-family=${image_family} --scopes=default,cloud-source-repos-ro
echo sleep 10
echo "$GCL scp $(dirname $0)/mk_python_venv ${sourceInstance}:/tmp/ --zone=${ZONE}"
echo "$GCL scp $(dirname $0)/jranke.asc ${sourceInstance}:/tmp/ --zone=${ZONE}"
cat $all_cmds | egrep -v  '^#|^ *$' | while read cmd
do
    echo "$GCL ssh $sourceInstance --zone=${ZONE} --command=\"$cmd\""
done

if [ -n "$FLAVOUR" ]; then
    bucket="gs://common-resources-${FLAVOUR}-overrides"
    copy_overrides="sudo gsutil -m -o 'GSUtil:parallel_thread_count=1' -o 'GSUtil:sliced_object_download_max_components=4' cp -r ${bucket}/* /opt/resources/"
    echo "$GCL ssh ${sourceInstance} --zone=${ZONE} --command=\"$copy_overrides\""
fi

echo "$GCL ssh $sourceInstance --zone=${ZONE} --command=\"cd /opt/resources && sudo git tag ${imageName} && git push origin ${imageName}\""
echo "$GCL ssh $sourceInstance --zone=${ZONE} --command=\"sudo rm -r /opt/resources/.git\""

echo "$GCL instances stop ${sourceInstance} --zone=${ZONE}"
echo "$GCL images create ${imageName} --family=${sourceInstance} --source-disk=${sourceInstance} --source-disk-zone=${ZONE} --storage-location=${LOCATION}"
echo "$GCL instances -q delete ${sourceInstance} --zone=${ZONE}"
) > $generated_script
chmod +x $generated_script
$generated_script
rm $generated_script
