#!/usr/bin/env bash

# Piggy-back on the dataproc node init to keep bwa in synch
chmod 777 ./patient-cluster/src/main/resources/node-init.sh
./patient-cluster/src/main/resources/node-init.sh