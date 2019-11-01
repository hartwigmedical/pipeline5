package com.hartwig.pipeline.transfer.google;

import static java.lang.String.format;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.storage.GSUtil;

public class GoogleArchiver {
    private final Arguments arguments;

    public GoogleArchiver(Arguments arguments) {
        this.arguments = arguments;
    }

    public void transfer(SomaticRunMetadata metadata) {
        try {
            GSUtil.configure(true, 1);
            GSUtil.auth(arguments.cloudSdkPath(), arguments.archivePrivateKeyPath());
            GSUtil.cp(arguments.cloudSdkPath(),
                    format("gs://%s/%s", arguments.patientReportBucket(), metadata.runName()),
                    format("gs://%s/%s", arguments.archiveBucket(), metadata.runName()),
                    arguments.archiveProject(),
                    true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
