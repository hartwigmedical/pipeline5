#!/usr/bin/env bash

echo "etc profile.d thingo"
eval $(/root/anaconda3/bin/conda shell.bash hook)
source /root/anaconda3/bin/activate
conda activate /root/anaconda3/envs/bioconductor-r42

