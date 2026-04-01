#!/usr/bin/env bash

# create a VM from the base image and perform the following

set -e

wget https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-x86_64.sh

# install into /opt/tools/miniforge3
bash Miniforge3-Linux-x86_64.sh -b -p /opt/tools/miniforge3
mamba create -n hmftools -c conda-forge --strict-channel-priority r-base r-ggplot2 r-tidyr

mamba activate hmftools

# cuppa dependencies
mamba install -c conda-forge pandas=2.0.* numpy>=1.0,<2.0 scikit-learn=1.3.0

# cobalt and purple
mamba install -c bioconda circos bioconductor-copynumber

# tar and upload to common-tools bucket
pushd /opt/tools
tar -czvf miniforge3.tar.gz miniforge3
popd
gcloud storage cp /opt/tools/miniforge3.tar.gz gs://common-tools/miniforge3/
