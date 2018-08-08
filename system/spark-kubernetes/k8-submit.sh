#!/usr/bin/env bash

print_usage(){
    echo "Usage: k8-submit -c cluster_url  -v version"
    echo "  -c cluster_url      URL to kubernetes cluster IP:PORT"
    echo "  -v version          Version of docker container (Mandatory)"
    exit 1
}

while getopts ':c:v:' flag; do
  case "${flag}" in
    v) VERSION=$OPTARG ;;
    c) CLUSTER=$OPTARG ;;
    *) print_usage
       exit 1 ;;
  esac
done

if [ -z $VERSION ] || [ -z $CLUSTER ]
then
    print_usage
fi

spark-2.3.0-bin-hadoop2.7/bin/spark-submit \
    --master k8s://https://${CLUSTER} \
    --deploy-mode cluster \
    --name pipeline5 \
    --class com.hartwig.pipeline.runtime.PipelineRuntime \
    --conf spark.executor.instances=1 \
    --conf spark.kubernetes.container.image=hartwigmedicalfoundation/pipeline5-spark-kubernetes:${VERSION} \
    local:///usr/share/pipeline5/system.jar