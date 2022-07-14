#!/usr/bin/env bash
#
# Makes symlinks to the latest Python virtualenv for each tool to support "pilot" versions
# 
# Known limitation: assumes two-level versioning for tools and will get confused if a given tool mixes two- and three-level (or
# more) versions. For instance it will think "1.6" is newer than "1.6.1". Fixing this results in a more-complicated script and
# honestly I don't think it will happen that often, nor do I think that the virtualenvs for a patch release would be much
# different than those for the corresponding minor release.

set -e

find /opt/tools -type d -name '*_venv' | while read d; do 
    dirname $d; 
done | sort -u | while read tool; do 
    target="$(find ${tool} -type d -name '*_venv' | sort | tail -n1)"
    ln -s "$target" "${tool}/pilot_venv"
    echo "Symlinked ${target} as the virtual environment for ${tool}"
done
