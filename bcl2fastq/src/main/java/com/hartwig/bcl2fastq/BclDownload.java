package com.hartwig.bcl2fastq;

import com.google.cloud.storage.Bucket;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

public class BclDownload extends InputDownload {

    public BclDownload(final Bucket bclBucket, final String flowcellIdentifier) {
        super(GoogleStorageLocation.of(bclBucket.getName(), InputPath.resolve(bclBucket, flowcellIdentifier), true));
    }
}
