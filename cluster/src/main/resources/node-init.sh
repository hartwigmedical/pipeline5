#!/usr/bin/env bash

setup_environment() {
    apt-get update && apt-get install -y build-essential gcc-multilib apt-utils zlib1g-dev wget git
}

install_bwa() {
    git clone https://github.com/lh3/bwa.git
    cd bwa
    git checkout 0.7.17
    make
    cp -p bwa /usr/local/bin
}

install_picard() {
    git clone https://github.com/broadinstitute/picard.git
    cd picard
    git checkout 2.18.27
    ./gradlew shadowJar
    cp build/libs/picard.jar /usr/local/bin/picard.jar
}

install_sambamba() {
    wget https://github.com/biod/sambamba/releases/download/v0.6.8/sambamba-0.6.8-linux-static.gz
    gunzip sambamba-0.6.8-linux-static.gz
    chmod 777 sambamba-0.6.8-linux-static
    mv sambamba-0.6.8-linux-static /usr/local/bin/sambamba
}

setup_environment
install_bwa
install_sambamba