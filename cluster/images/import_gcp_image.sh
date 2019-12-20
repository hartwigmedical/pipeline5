#!/usr/bin/env bash

[[ $# -ne 1 ]] && echo "Provide the name of an existing image" && exit 1
gcloud compute images create ${1} --source-uri="gs://hmf-pipeline5-image-release/${1}.tar.gz"
