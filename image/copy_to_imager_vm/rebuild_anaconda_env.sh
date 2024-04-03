#!/usr/bin/env bash

set -e

CONFIG_YAML="/tmp/anaconda.yaml"

[[ ! -f $CONFIG_YAML ]] && echo "No configuration for Anaconda present, skipping env rebuild" && exit 0

echo "Found $CONFIG_YAML so rebuilding Anaconda environment"
cd /root
eval `/root/anaconda3/bin/conda shell.bash hook`
source /root/anaconda3/bin/activate
conda activate /root/anaconda3/envs/bioconductor-r42
mamba env update --file $CONFIG_YAML 
