package com.hartwig.pipeline.transfer.google;

import static java.lang.String.format;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.storage.CloudCopy;

public class GoogleArchiver {
    private final Arguments arguments;
    private final CloudCopy cloudCopy;

    public GoogleArchiver(Arguments arguments, CloudCopy cloudCopy) {
        this.arguments = arguments;
        this.cloudCopy = cloudCopy;
    }

    public void transfer(SomaticRunMetadata metadata) {
        String source = format("gs://%s/%s", arguments.patientReportBucket(), metadata.runName());
        cloudCopy.copy(source, format("gs://%s", arguments.archiveBucket()));
    }
}
