#!/usr/bin/env bash

set -e

print_usage() {
    cat <<EOM
USAGE: $0 [--project project] [--tag-as-version new-version || --checkout-tag existing-tag]
  --project [project]              Project in GCP containing the source repository
Specify ONLY ONE of:
  --tag-as-version [new-version]   Create tag [new-version] in source repo after checking out HEAD
  --checkout-tag [existing-tag]    Checkout tag [existing-tag] rather than HEAD and create no new tags
EOM
}

args=$(getopt -o "" --longoptions project:,tag-as-version:,checkout-tag: -- "$@")
[[ $? != 0 ]] && print_usage && exit 1
eval set -- "$args"

while true; do
    case "$1" in
        --project) project=$2 ; shift 2 ;;
        --tag-as-version) new_version=$2 ; shift 2 ;;
        --checkout-tag) checkout_tag=$2 ; shift 2 ;;
        --) shift; break ;;
    esac
done

if [[ -z "$project" || ( -z "$new_version" && -z "$checkout_tag" ) || ( -n "$new_version" && -n "$checkout_tag" ) ]]; then
    print_usage
    exit 1
fi

mkdir /tmp/resources
gcloud source repos clone common-resources-private /tmp/resources --project=$project
cd /tmp/resources
if [[ -n $new_version ]]; then
    "Adding tag '$new_version'"
    git tag "$new_version"
    git push origin "$new_version"
else
   "Checking out existing tag '$checkout_tag'"
    git checkout tags/$checkout_tag
fi

rm -r .git/
find . -type f | while read f; do
    dirname $(echo "$f" | sed 's#^\./##')
done | sort -u | while read d; do
    [[ $d != "." ]] && rm -rf /opt/resources/$d
done
tar -cf - * | tar -C /opt/resources -xv
