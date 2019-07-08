#!/usr/bin/env bash

function print_usage() {
  echo "Usage: $(basename $0) gs://src_bucket[/src_path...] gs://dest_bucket"
  echo "  Both source and dest_bucket must exist"
  echo "  A destination path will be created and assigned based on the src name"
}

function die() {
  echo "Fatal error: $@"
  exit 1
}

[[ $# -lt 2 ]] && print_usage && exit 1
gsutil -q ls ${1} >/dev/null 2>&1 || die "Source '${1}' does not exist" 
gsutil -q ls ${2} >/dev/null 2>&1 || die "Destination bucket '${2}' does not exist"

dest_dir="${2}/backup_$(date +%Y%m%d-%H%M)_$(echo ${1} | sed 's#gs://##' | tr '/' '_')"
echo "Backing up ${1} to ${dest_dir}"
gsutil -m cp -r ${1}/* ${dest_dir}/
