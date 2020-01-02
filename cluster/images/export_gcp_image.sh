#!/usr/bin/env bash

[[ $# -ne 1 ]] && echo "Provide the name of an existing image" && exit 1
gcloud compute images export --destination-uri gs://hmf-pipeline5-image-release/${1}.tar.gz --image ${1} --project hmf-pipeline-development

