#!/usr/bin/env bash

set -e
set -x

CONFIG_YAML="/tmp/anaconda.yaml"

[[ ! -f $CONFIG_YAML ]] && echo "No configuration for Anaconda present, skipping env rebuild" && exit 0

echo "Found $CONFIG_YAML so rebuilding Anaconda environment"
cd /root/anaconda3
eval `/root/anaconda3/bin/conda shell.bash hook`
source /root/anaconda3/bin/activate
conda activate /root/anaconda3/envs/bioconductor-r42
conda env update --file $CONFIG_YAML --prune

tarball="$(date +%Y%m%d%H%M).tar.gz"
echo "Creating $tarball from rebuilt Anaconda environment"
tar czf $tarball anaconda3/
echo "Tarball created, uploading to tools bucket"
gsutil cp $tarball gs://common-tools/anaconda/$(cat /tmp/pipeline.version)/

