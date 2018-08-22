#!/usr/bin/env bash

print_usage(){
    echo "Usage: k8-submit -v version"
     echo "  -v version          Version of docker container (Mandatory)"
    exit 1
}

clean(){
    local dirty=$1
    local url_length=${#dirty}-11
    local clean=${dirty:7:$url_length}
    echo $clean
}

get_cluster_url(){
    local cluster_info="$(kubectl cluster-info)"
    local cluster_info_tokens=( $cluster_info )
    local clean_url=$(clean ${cluster_info_tokens[5]})
    echo $clean_url
}

while getopts ':c:v:' flag; do
  case "${flag}" in
    v) VERSION=$OPTARG ;;
     *) print_usage
       exit 1 ;;
  esac
done

if [ -z $VERSION ]
then
    print_usage
fi
CLUSTER_URL=$(get_cluster_url)
echo "Connecting to cluster at [${CLUSTER_URL}]"
../spark-2.3.0-bin-hadoop2.7/bin/spark-submit --master k8s://$CLUSTER_URL \
    --deploy-mode cluster \
    --name pipeline5 \
    --conf spark.kubernetes.driver.secrets.reader-key=/root/servicekey/ \
    --conf spark.kubernetes.executor.secrets.reader-key=/root/servicekey/ \
    --class com.hartwig.pipeline.runtime.PipelineRuntime \
    --conf spark.driver.memory=8G \
    --conf spark.executor.instances=4 \
    --conf spark.executor.cores=12 \
    --conf spark.executor.memory=100G \
    --conf spark.locality.wait="30s" \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.kubernetes.container.image=hartwigmedicalfoundation/pipeline5-spark-kubernetes:${VERSION} \
    local:///usr/share/pipeline5/system.jar