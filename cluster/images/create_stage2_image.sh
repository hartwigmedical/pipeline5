#!/usr/bin/env bash

set -e
/usr/bin/env bash "$(dirname "$0")/create_image.sh" pipeline5-stage2 hmf-pipeline-development pipeline5-stage1
