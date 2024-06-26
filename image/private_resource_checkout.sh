#!/usr/bin/env bash

dmidecode -s system-product-name | grep -q "Google Compute Engine"
IN_GCP=$?

print_usage() {
    cat <<EOM
USAGE: $0 [--project project] [--tag-as-version new-version || --checkout-tag existing-tag || --checkout-branch branch-name] [--result-code-url gs-url]

Required:
  --project [project]              Project in GCP containing the source repository

Specify ONLY ONE of:
  --tag-as-version [new-version]   Create tag [new-version] in source repo after checking out HEAD
  --checkout-commit [commit-sha]   Checkout [commit-sha] rather than HEAD and create no new tags
  --checkout-branch [branch-name]  Checkout HEAD of [branch-name] rather than [master] and create no new tags

Optional:
  --result-code-url [gs-url]        GCS (eg "gs://bucket/path") URL to write exit code to (bucket must be writable).
EOM
}

gsurl_exit_handler() {
    gsutil cp <(echo $?) $result_code_url
}

local_exit_handler() {
    exit_code=$?
    [[ $? -ne 0 ]] && echo "ERROR: Non-zero exit code from previous command: $exit_code"
    exit $exit_code
}

metadata() {
    curl -f -s http://metadata.google.internal/computeMetadata/v1/instance/attributes/$1 -H "Metadata-Flavor: Google" 2>/dev/null
}

set -o pipefail

if [[ $IN_GCP && $# -eq 0 ]]; then
    echo "Running inside GCP, fetching arguments from metadata"
    project="$(metadata project)"
    new_version="$(metadata tag-as-version)"
    checkout_target="$(metadata checkout-commit)"
    result_code_url="$(metadata result-code-url)"
else
    args=$(getopt -o "" --longoptions project:,tag-as-version:,checkout-commit:,checkout-branch:,result-code-url: -- "$@")
    [[ $? != 0 ]] && print_usage && exit 1
    eval set -- "$args"

    while true; do
        case "$1" in
            --project) project=$2 ; shift 2 ;;
            --tag-as-version) new_version=$2 ; shift 2 ;;
            --checkout-commit) checkout_commit=$2 ; shift 2 ;;
            --checkout-branch) checkout_branch=$2 ; shift 2 ;;
            --result-code-url) result_code_url=$2 ; shift 2 ;;
            --) shift; break ;;
        esac
    done
fi

if [[ -n "$checkout_commit" || -n "$checkout_branch" ]]; then
    if [[ -n "$checkout_commit" && -n "$checkout_branch" ]]; then
        print_usage
        exit 1
    fi
    checkout_target="${checkout_commit:-$checkout_branch}"
fi

if [[ -z "$project" || ( -z "$new_version" && -z "$checkout_target" ) || ( -n "$new_version" && -n "$checkout_target" ) ]]; then
    print_usage
    exit 1
fi

if [[ -n $result_code_url ]]; then
    trap gsurl_exit_handler EXIT
else
    trap local_exit_handler EXIT
fi

mkdir /tmp/resources
gcloud source repos clone common-resources-private /tmp/resources --project=$project
cd /tmp/resources
if [[ -n $new_version ]]; then
    echo "Adding tag '$new_version'"
    git tag "$new_version"
    git push origin "$new_version"
else
    echo "Checking out '$checkout_target'"
    git checkout $checkout_target
fi

rm -r .git/
tar -cf - * | tar -C /opt/resources -xv
