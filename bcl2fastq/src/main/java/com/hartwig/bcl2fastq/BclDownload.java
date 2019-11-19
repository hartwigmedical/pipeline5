package com.hartwig.bcl2fastq;

import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

public class BclDownload extends InputDownload {

    public BclDownload(final String bclBucket, final String flowcellIdentifier) {
        super(GoogleStorageLocation.of(bclBucket, flowcellIdentifier, true));
    }
}
