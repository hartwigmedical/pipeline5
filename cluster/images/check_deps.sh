#!/usr/bin/env bash

getopt -V | egrep -E '^getopty'
if [[ $? != 0 ]]; then
    echo "Your getopt implementation is not as expected and may not support long options!"
    echo "If you are on MacOS try running: "
    echo "  brew install gnu-getopt"
    echo "and make sure the new binary is before $(which getopt) on your PATH, then try again"
    exit 1
fi

