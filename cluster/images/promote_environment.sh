#!/usr/bin/env bash

PROD_PROJECT="hmf-pipeline-prod-e45b00f2"
DEV_PROJECT="hmf-pipeline-development"

PROD_TOOLS_BUCKET="common-tools-prod"
PROD_RESOURCES_BUCKET="common-resources-prod"

DEV_TOOLS_BUCKET="common-tools"
DEV_RESOURCES_BUCKET="common-resources"

gsutil -m rm -r gs://$PROD_TOOLS_BUCKET/**
gsutil -m cp -r gs://$DEV_TOOLS_BUCKET/* gs://$PROD_TOOLS_BUCKET

gsutil -m rm -rf gs://$PROD_RESOURCES_BUCKET/**
gsutil -m cp -r gs://$DEV_RESOURCES_BUCKET/* gs://$PROD_RESOURCES_BUCKET

./create_custom_image.sh $PROD_PROJECT $PROD_TOOLS_BUCKET
