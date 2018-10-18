#!/usr/bin/env bash

install_bwa(){
    apt-get update && apt-get install -y build-essential gcc-multilib apt-utils zlib1g-dev wget git
    git clone https://github.com/lh3/bwa.git
    cd bwa
    git checkout 0.7.17
    make
    cp -p bwa /usr/local/bin
}

install_sambamba() {
    wget https://github.com/biod/sambamba/releases/download/v0.6.8/sambamba-0.6.8-linux-static.gz
    gunzip sambamba-0.6.8-linux-static.gz
    chmod 777 sambamba-0.6.8-linux-static
    mv sambamba-0.6.8-linux-static /usr/local/bin/sambamba
}
install_bwa
install_sambamba