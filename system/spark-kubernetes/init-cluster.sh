#!/usr/bin/env bash

print_usage(){
    echo "Usage: init-cluster -p patient_name"
    echo "  -p patient_name     Patient name matching what's persisted in Google Storage (Mandatory)"
    exit 1
}

while getopts ':p:' flag; do
  case "${flag}" in
    p) PATIENT=$OPTARG ;;
    *) print_usage
       exit 1 ;;
  esac
done

if [ -z $PATIENT ]
then
    print_usage
fi

CLUSTER_NAME=patient-cluster-${PATIENT}

# create a cluster
gcloud beta container --project "hmf-pipeline-development" clusters create "${CLUSTER_NAME}" --zone "us-central1-a" \
    --username "admin" --cluster-version "1.9.7-gke.5" --machine-type "n1-standard-4" --image-type "COS" --disk-type "pd-standard" \
    --disk-size "100" --scopes "https://www.googleapis.com/auth/compute","https://www.googleapis.com/auth/devstorage.read_only",\
    "https://www.googleapis.com/auth/logging.write","https://www.googleapis.com/auth/monitoring","https://www.googleapis.com/auth/servicecontrol",\
    "https://www.googleapis.com/auth/service.management.readonly","https://www.googleapis.com/auth/trace.append" \
     --num-nodes "3" --enable-cloud-logging --enable-cloud-monitoring --network "default" --subnetwork "default" \
     --addons HorizontalPodAutoscaling,HttpLoadBalancing,KubernetesDashboard --no-enable-autoupgrade --enable-autorepair

# connect with kubctl
gcloud container clusters get-credentials ${CLUSTER_NAME} --zone us-central1-a --project hmf-pipeline-development

# create a service account for spark in gcp
# create a secret with key
# delete key file

kubectl create -f hdfs/namenode.yaml
kubectl create -f hdfs/datanodel.yaml

# add spark service account to k8
kubectl create serviceaccount spark
# add cluster edit role to spark service account
kubectl create clusterrolebinding spark-role --clusterrole=edit --serviceaccount=default:spark --namespace=default

./k8-submit.sh -c



