#!/usr/bin/env bash

set -o pipefail

LOCATION="europe-west4"
ZONE="${LOCATION}-a"
PROJECT="hmf-pipeline-development"
PV5_JAR="$(dirname "$0")/../target/cluster-local-SNAPSHOT.jar"

tools_only=false

print_usage() {
    cat <<EOM
USAGE: $0 [--tools-only] [--flavour flavour] [--checkout-target target]

Optional arguments:
  --tools-only                 Only copy down tools, don't bother with resources
  --flavour [flavour]          ...
  --checkout-target [target]   Checkout [target] instead of the HEAD of "master" and do not create any new tags.
                               May be anything accepted by "git checkout".
EOM
}

"$(dirname "$0")/check_deps.sh" || exit 1
args=$(getopt -o "" --longoptions tools-only,flavour:,checkout-target: -- "$@")
[[ $? != 0 ]] && print_usage && exit 1
eval set -- "$args"

while true; do
    case "$1" in
        --tools-only) tools_only=true ; shift ;;
        --flavour) flavour="$2"; shift 2 ;;
        --checkout-target) checkout_target="$2"; shift 2 ;;
        --) shift; break ;;
    esac
done

set -e
echo "Rebuilding pipeline JAR to ensure correct version"
mvn -f "$(dirname "$0")/../../pom.xml" clean package -DskipTests
version="$(java -cp ${PV5_JAR} com.hartwig.pipeline.tools.VersionUtils)"
set +e
[[ "$version" =~ ^5\-[0-9]+$ ]] || (echo "Got junk version: ${version}" && exit 1)

echo "Building public image for pipeline version ${version}"
image_family="nedcuppa-pipeline5-${version}${flavour+"-$flavour"}${checkout_target:+"-unofficial"}"
source_instance="${image_family}-$(whoami)"
image_name="${image_family}-$(date +%Y%m%d%H%M)"
source_project="hmf-pipeline-development"
source_family="hmf-debian-9"
base_image_cmds="$(dirname "$0")/base.cmds"
tools_image_cmds="$(dirname "$0")/tools.cmds"
all_cmds=$(echo $base_image_cmds $tools_image_cmds)
if [ "$tools_only" = true ]; then
    source_project="hmf-pipeline-development"
    source_family=${image_family}
    all_cmds=$tools_image_cmds
fi

which gcloud 2>&1 >/dev/null
[[ $? -ne 0 ]] && echo "gcloud is missing" >&2 && exit 1
set -e
GCL="gcloud beta compute --project=${PROJECT}"
SSH="$GCL ssh $source_instance --zone=${ZONE}"
generated_script=$(mktemp -t image_script_generated_XXXXX.sh)

(
echo "#!/usr/bin/env bash"
echo
echo "set -e"
echo $GCL instances create $source_instance --description=\"Pipeline5 disk imager started $(date) by $(whoami)\" --zone=${ZONE} \
    --boot-disk-size 200 --boot-disk-type pd-ssd --machine-type n1-highcpu-4 --image-project=${source_project} \
    --image-family=${source_family} --scopes=default,cloud-source-repos-ro
echo sleep 10
echo "$GCL scp $(dirname $0)/mk_python_venv ${source_instance}:/tmp/ --zone=${ZONE}"
echo "$GCL scp $(dirname $0)/jranke.asc ${source_instance}:/tmp/ --zone=${ZONE}"
cat $all_cmds | egrep -v  '^#|^ *$' | while read cmd
do
    echo "$SSH --command=\"$cmd\""
done

if [ -n "$flavour" ]; then
    bucket="gs://common-resources-${flavour}-overrides"
    copy_overrides="sudo gsutil -m -o 'GSUtil:parallel_thread_count=1' -o 'GSUtil:sliced_object_download_max_components=4' cp -r ${bucket}/* /opt/resources/"
    echo "$SSH --command=\"$copy_overrides\""
fi

if [ -n "${checkout_target}" ]; then
    echo "$SSH --command=\"cd /opt/resources && sudo git checkout ${checkout_target}\""
else
    echo "$SSH --command=\"cd /opt/resources && sudo git tag ${image_name} && git push origin ${image_name}\""
fi

echo "$SSH --command=\"sudo rm -r /opt/resources/.git\""
echo "$GCL instances stop ${source_instance} --zone=${ZONE}"
echo "$GCL images create ${image_name} --family=${image_family} --source-disk=${source_instance} --source-disk-zone=${ZONE} --storage-location=${LOCATION}"
echo "$GCL instances -q delete ${source_instance} --zone=${ZONE}"
) > $generated_script
chmod +x $generated_script
$generated_script
rm $generated_script
