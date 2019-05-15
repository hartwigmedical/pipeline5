#!/usr/bin/env bash

set -x

common_tools_bucket=$(/usr/share/google/get_metadata_value attributes/common_tools_bucket)
bwa_version=$(/usr/share/google/get_metadata_value attributes/bwa_version)
sambamba_version=$(/usr/share/google/get_metadata_value attributes/sambamba_version)

install_bwa() {
    gsutil cp gs://$common_tools_bucket/bwa/$bwa_version/bwa /usr/local/bin/
    chmod a+x /usr/local/bin/bwa
}

install_sambamba() {
   gsutil cp gs://$common_tools_bucket/sambamba/$sambamba_version/sambamba /usr/local/bin/
   chmod a+x /usr/local/bin/sambamba
}

install_bwa
install_sambamba