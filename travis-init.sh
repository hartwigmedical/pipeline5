#!/usr/bin/env bash

apt-get update && apt-get install -y build-essential gcc-multilib apt-utils zlib1g-dev wget git
git clone https://github.com/lh3/bwa.git
cd bwa
git checkout 0.7.17
make