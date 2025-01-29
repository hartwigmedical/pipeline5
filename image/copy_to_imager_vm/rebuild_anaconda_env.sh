#!/usr/bin/env bash

set -e

CONFIG_YAML="/tmp/anaconda.yaml"

[[ ! -f $CONFIG_YAML ]] && echo "No configuration for Anaconda present, skipping env rebuild" && exit 0

echo "Found $CONFIG_YAML so rebuilding Anaconda environment"
cd /opt/tools
eval `/opt/tools/anaconda3/bin/conda shell.bash hook`
source /opt/tools/anaconda3/bin/activate
conda activate /opt/tools/anaconda3/envs/bioconductor-r42
conda env update --file $CONFIG_YAML
