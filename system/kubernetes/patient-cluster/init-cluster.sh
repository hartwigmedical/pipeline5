#!/usr/bin/env bash

create_gcloud_cluster_name(){
    local patient=$1
    local patient_lowercase=${patient,,}
    echo "patient-cluster-${patient_lowercase}-1"
}

create_cluster_and_connect(){
    local cluster_name=$1
    local zone=$2
    gcloud container --project "hmf-pipeline-development" clusters create ${cluster_name} --zone $zone --username "admin" --cluster-version "1.9.7-gke.5" --machine-type "n1-standard-32" --image-type "COS" --disk-type "pd-standard" --disk-size "300" --scopes "https://www.googleapis.com/auth/compute","https://www.googleapis.com/auth/devstorage.read_only","https://www.googleapis.com/auth/logging.write","https://www.googleapis.com/auth/monitoring","https://www.googleapis.com/auth/servicecontrol","https://www.googleapis.com/auth/service.management.readonly","https://www.googleapis.com/auth/trace.append" --num-nodes "5" --enable-cloud-logging --enable-cloud-monitoring --network "default" --subnetwork "default" --addons HorizontalPodAutoscaling,HttpLoadBalancing,KubernetesDashboard --no-enable-autoupgrade --enable-autorepair
    gcloud container clusters get-credentials ${cluster_name} --zone $zone --project hmf-pipeline-development
}

create_keys_and_secrets(){
    local api_out=$(gcloud iam service-accounts keys create ./key.json --iam-account reader@hmf-pipeline-development.iam.gserviceaccount.com 2>&1)
    local api_out_tokens=($api_out)
    local raw_key=${api_out_tokens[2]}
    local key=${raw_key:1:${#raw_key}-2}
    kubectl create secret generic reader-key --from-file=key.json=./key.json >/dev/null
    rm ./key.json
    echo $key
}

find_first_node() {
    nodes="$(kubectl get nodes)"
    nodes_tokens=($nodes)
    echo ${nodes_tokens[5]}
}

create_hdfs_and_populate_with_patient(){
    local patient=$1
    sed -e "s/PATIENT/${patient}/g" namenode.yaml.template > namenode.yaml
    kubectl create -f namenode.yaml
    wait_for_pod "hdfs-namenode-0"
    kubectl create -f datanode.yaml
    wait_for_pod "hdfs-datanode"
    echo "Uploading patient data into HDFS...."
    kubectl exec hdfs-namenode-0 -- gunzip /patients/*
    kubectl exec hdfs-namenode-0 -- hadoop fs -mkdir /patients
    kubectl exec hdfs-namenode-0 -- hadoop fs -put /patients/${patient} /patients/
    echo "Upload complete."
}

create_spark_service_account(){
    kubectl create serviceaccount spark
    kubectl create clusterrolebinding spark-role --clusterrole=edit --serviceaccount=default:spark --namespace=default
}

wait_for_pod(){
    local pod_name=$1
    local namenode_status="None"

    echo "Waiting for pod ${pod_name} to start..."
    while [ $namenode_status != "Running" ]
    do
    local pods="$(kubectl get pods)"
    local pods_tokens=($pods)
    local name_index=-1
    for i in "${!pods_tokens[@]}"; do
       if [[ "${pods_tokens[$i]}" = "${pod_name}"* ]]; then
           name_index="${i}";
       fi
    done

    if [ -z $name_index ]
    then
        echo "Pod ${pod_name} was not found in kubectl get pods. Are you certain you've created it?"
        exit 1
    fi

    namenode_status=${pods_tokens[$name_index+2]}
    sleep 1s
    done

    echo "Pod ${pod_name} has started."
}

extract_results_and_put_in_gs(){
    local patient=$1
    kubectl exec hdfs-namenode-0 -- hadoop fs -getmerge /results/${patient}R_indel_realigned.bam/* ${patient}R.bam
    kubectl exec hdfs-namenode-0 -- hadoop fs -getmerge /results/${patient}T_indel_realigned.bam/* ${patient}T.bam
    kubectl exec hdfs-namenode-0 -- hadoop fs -getmerge /results/${patient}_indel_realigned.bam/* ${patient}.bam

    kubectl exec hdfs-namenode-0 -- gcloud auth activate-service-account --key-file /root/servicekey/key.json
    kubectl exec hdfs-namenode-0 -- gcloud config set project hmf-pipeline-development
    kubectl exec hdfs-namenode-0 -- gsutil -m cp -r ${patient}R.bam gs://results-pipeline5/$patient/${patient}R.bam
    kubectl exec hdfs-namenode-0 -- gsutil -m cp -r ${patient}T.bam gs://results-pipeline5/$patient/${patient}T.bam
    kubectl exec hdfs-namenode-0 -- gsutil -m cp -r ${patient}.bam gs://results-pipeline5/$patient/${patient}.bam
    gsutil -m cp gs://results-pipeline5/$patient/${patient}R.bam ./
    gsutil -m cp gs://results-pipeline5/$patient/${patient}T.bam ./
    gsutil -m cp gs://results-pipeline5/$patient/${patient}.bam ./
}


tidy_up(){
    local patient=$1
    local key_id=$2
    local cluster_name=$3
    local zone=$4
    gsutil rm -r gs://patients-pipeline5/$patient
    gcloud --quiet iam service-accounts keys delete $key_id --iam-account reader@hmf-pipeline-development.iam.gserviceaccount.com
    gcloud --quiet container clusters delete $cluster_name --zone $zone
}

print_usage(){
    echo "Usage: init-cluster -p patient_name [-v version] [-z zone] [-n num_nodes] [-c cpus] [-d disk_size]"
    echo "  -p patient_name     Patient name matching what's persisted in Google Storage (Mandatory)"
    echo "  -v version          Version of pipeline5 spark/kubernetes docker image"
    echo "  -z zone             GCloud zone to use for cluster and storage"
    echo "  -d development_mode No cleanup will occur after patient run completes"
    exit 1
}

VERSION="latest"
ZONE="europe-west4-a"
while getopts ':p:v:z:d' flag; do
  case "${flag}" in
    p) PATIENT=$OPTARG ;;
    v) VERSION=$OPTARG ;;
    z) ZONE=$OPTARG ;;
    d) DEV_MODE="true" ;;
    *) print_usage
       exit 1 ;;
  esac
done

if [ -z $PATIENT ]
then
    print_usage
fi

# gsutil -m rsync -d -r $PATIENT gs://patients-pipeline5/$PATIENT
CLUSTER_NAME=$(create_gcloud_cluster_name $PATIENT)
create_cluster_and_connect $CLUSTER_NAME $ZONE
KEY_ID=$(create_keys_and_secrets)
create_hdfs_and_populate_with_patient $PATIENT
create_spark_service_account
. k8-submit.sh -v $VERSION
extract_results_and_put_in_gs $PATIENT
gsutil -m cp gs://results-pipeline5/$PATIENT* ./

if [ -z $DEV_MODE ]
then
   tidy_up $PATIENT $KEY_ID $CLUSTER_NAME $ZONE
fi

