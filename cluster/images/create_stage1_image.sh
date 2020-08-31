#!/usr/bin/env bash

set -e
/usr/bin/env bash "$(dirname "$0")/create_image.sh" pipeline5-stage1 debian-cloud debian-9
