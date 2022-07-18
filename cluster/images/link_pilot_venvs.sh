#!/usr/bin/env bash

set -e

find /opt/tools -type d -name '*_venv' | while read d; do
    dirname $d
done | sort -u | while read tool; do
    latest="$(find ${tool} -type d -name '*_venv' -exec basename {} \; | sed "s/_venv//" | sort -n | tail -n1)"
    ln -s "${tool}/${latest}_venv" "${tool}/pilot_venv"
    echo "Symlinked ${tool}/${latest}_venv as the virtual environment for $(basename ${tool})"
    ln -s "${tool}/${latest}" "${tool}/pilot"
    echo "Symlinked ${tool}/${latest} as the $(basename ${tool}) version"
done

