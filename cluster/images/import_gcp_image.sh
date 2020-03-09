#!/usr/bin/env bash

[[ $# -ne 3 ]] && echo "Provide the pipeline version (eg of the form \"5-x\"), image name (yyyymmddhhmm) and destination project" && exit 1
family="pipeline5-${1}"
full_name="${family}-${2}"

gcloud compute --project "$3" images create ${full_name} --source-image="${family}-${2}" --source-image-project="hmf-pipeline-development" \
  --storage-location="europe-west4" --family="${family}"

