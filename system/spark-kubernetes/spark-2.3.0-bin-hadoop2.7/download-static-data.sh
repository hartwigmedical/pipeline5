#!/bin/sh

gcloud auth activate-service-account --key-file keys/gcloud-read-credentials.json
gcloud config set project hmf-pipeline-development

mkdir /reference_genome
gsutil rsync gs://reference-genome/ /reference_genome/
mkdir /known_indels
gsutil rsync gs://known-indels/ /known_indels/