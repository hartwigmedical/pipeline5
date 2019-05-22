#!/usr/bin/env bash

set -e
[[ $# -ne 5 ]] && echo "Arguments: path to the JAR; config file; ??? of reference genome; VCF; output file" && exit 1
java -Xmx20G -jar ${1} -c ${2} ${3} -v ${4} -hgvs -lof -no-downstream -ud 1000 -no-intergenic -noShiftHgvs > ${5}

