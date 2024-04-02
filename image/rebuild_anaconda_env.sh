#!/usr/bin/env bash

set -e
set -x

CONFIG_YAML="/tmp/anaconda.yaml"

[[ ! -f $CONFIG_YAML ]] && echo "No configuration for Anaconda present, skipping env rebuild" && exit 0

echo "Found $CONFIG_YAML so rebuilding Anaconda environment"
cd /root
eval `/root/anaconda3/bin/conda shell.bash hook`
source /root/anaconda3/bin/activate
conda activate /root/anaconda3/envs/bioconductor-r42
mamba env update --file $CONFIG_YAML --prune
conda env export > /root/anaconda3/anaconda.yaml
echo "Environment has been written to 'anaconda.yaml' in the artifact"
echo "It may be worth updating the same-named file in the imager directory with this as the environment was updated"

echo "Creating $tarball from rebuilt Anaconda environment"
remote="gs://common-tools/anaconda/$(cat /tmp/pipeline.version)/$(date +%Y%m%d%H%M).tar.gz"
tar -I pigz -cf - anaconda3/ | gsutil cp - $remote
echo "Archive copied to $remote"
