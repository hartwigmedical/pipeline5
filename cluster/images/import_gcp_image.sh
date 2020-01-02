#!/usr/bin/env bash

[[ $# -ne 2 ]] && echo "Provide the pipeline version (eg 5-x) and image name (yyyymmddhhmm) of an existing image" && exit 1
full_name="pipeline5-${1}-${2}"

gcloud compute images create ${full_name} --source-image="${full_name}" --source-image-project="hmf-pipeline-development" \
  --storage-location=europe-west4 --family=${2}
