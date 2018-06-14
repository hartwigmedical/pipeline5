#!/usr/bin/env bash

if [[ $EUID -ne 0 ]]; then
   echo "To run the pipeline2 docker container you must be root. Try [sudo run_pipeline2_docker.sh]"
   exit 1
fi

WORKING_DIRECTORY=`pwd`

print_usage(){
    echo "Usage: run_pipeline_docker -p patientdir -r referencedir -v version [-c confdir]"
    echo "  -p patientdir       Directory containing patient data for docker to mount"
    echo "  -r referencedir     Directory containing reference genome data for docker to mount"
    echo "  -v version          Version of Pipeline2 docker container"
    echo "  -c confdir          Optional directory containing confiruration (pipeline.yaml). If not specified default in Docker image is used"
    exit 1
}

while getopts ':p:r:v:c:' flag; do
  case "${flag}" in
    p) PATIENT_DIR=$OPTARG ;;
    r) REFERENCE_DIR=$OPTARG ;;
    c) CONF_DIR=$OPTARG;;
    v) VERSION=$OPTARG ;;
    *) print_usage
       exit 1 ;;
  esac
done

if [ -z $PATIENT_DIR ] || [ -z $REFERENCE_DIR ] || [ -z $VERSION ]
then
    print_usage
fi

volumes="-v ${PATIENT_DIR}:/patients -v ${REFERENCE_DIR}:/reference/ -v ${WORKING_DIRECTORY}/results:/results"

if ! [ -z $CONF_DIR ]
then
    volumes="-v ${CONF_DIR}:/conf/ ${volumes}"
fi

docker run $volumes hartwigmedicalfoundation/pipeline2:${VERSION}