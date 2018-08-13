#!/bin/sh

gcloud auth activate-service-account --key-file keys/gcloud-read-credentials.json
gcloud config set project hmf-pipeline-development

mkdir /reference_genome
gsutil rsync gs://reference_genome/ /reference_genome/
mkdir /known_indels
gsutil rsync gs://known_indels/ /known_indels/