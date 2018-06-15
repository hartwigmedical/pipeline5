#!/usr/bin/env bash

WORKING_DIRECTORY=`pwd`
PATIENT_DIR="/data/repos/testdata/cancerPanel/"
REFERENCE_DIR="/data/refgenomes/Homo_sapiens.GRCh37.GATK.illumina/"

print_usage(){
    echo "Usage: run_pipeline_docker -v version [-p patientdir] [-r referencedir] [-c confdir]"
    echo "  -v version          Version of Pipeline2 docker container (Mandatory)"
    echo "  -p patientdir       Directory containing patient data for docker to mount (Optional default = ${PATIENT_DIR})"
    echo "  -r referencedir     Directory containing reference genome data for docker to mount (Optional default = ${REFERENCE_DIR})"
    echo "  -c confdir          Directory containing confiruration (pipeline.yaml) (Optional default = pipeline.yaml of docker image)"
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

if [[ $EUID -ne 0 ]]; then
   echo "Docker must be run as root. Please enter password for sudo..."
fi
sudo docker run $volumes hartwigmedicalfoundation/pipeline2:${VERSION}