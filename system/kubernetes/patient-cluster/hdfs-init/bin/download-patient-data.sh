#!/bin/sh

PATIENT=$1

gcloud auth activate-service-account --key-file /root/servicekey/key.json
gcloud config set project hmf-pipeline-development

mkdir /patients
gsutil cp -r gs://patients-pipeline5/${PATIENT} /patients