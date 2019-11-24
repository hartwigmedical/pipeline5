package com.hartwig.pipeline.transfer.google;

import static java.lang.String.format;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.storage.GSUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GoogleArchiver {

    private final static Logger LOGGER = LoggerFactory.getLogger(GoogleArchiver.class);
    private final Arguments arguments;

    public GoogleArchiver(Arguments arguments) {
        this.arguments = arguments;
    }

    public void transfer(SomaticRunMetadata metadata) {
        LOGGER.info("Starting transfer from [{}] to GCP bucket [{}]", arguments.patientReportBucket(), arguments.archiveBucket());
        try {
            GSUtil.configure(false, 12);
            GSUtil.auth(arguments.cloudSdkPath(), arguments.archivePrivateKeyPath());
            String source = format("gs://%s/%s", arguments.patientReportBucket(), metadata.runName());
            String destination = format("gs://%s/%s", arguments.archiveBucket(), metadata.runName());
            GSUtil.rsync(arguments.cloudSdkPath(), source, destination, arguments.archiveProject(), true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        LOGGER.info("Transfer complete.");
    }
}