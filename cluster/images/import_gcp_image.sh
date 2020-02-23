#!/usr/bin/env bash

[[ $# -ne 2 ]] && echo "Provide the pipeline version (eg of the form \"5-x\") and image name (yyyymmddhhmm) of an existing image" && exit 1
family="pipeline5-${1}"
full_name="${family}-${2}"

gcloud compute images create ${full_name} --source-image="${family}-${2}" --source-image-project="hmf-pipeline-development" \
  --storage-location="europe-west4" --family="${family}"

