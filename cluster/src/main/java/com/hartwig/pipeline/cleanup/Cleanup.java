package com.hartwig.pipeline.cleanup;

import java.io.IOException;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.RunTag;
import com.hartwig.pipeline.alignment.Run;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.storage.GSUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Cleanup {

    private final static Logger LOGGER = LoggerFactory.getLogger(Cleanup.class);
    private final Arguments arguments;

    Cleanup(final Arguments arguments) {
        this.arguments = arguments;
    }

    public void run(SomaticRunMetadata metadata) {
        if (!arguments.cleanup()) {
            return;
        }
        try {
            LOGGER.info("Cleaning up runtime storage on complete somatic pipeline run");
            if (arguments.privateKeyPath().isPresent()) {
                GSUtil.auth(arguments.cloudSdkPath(), arguments.privateKeyPath().get());
            }
            metadata.maybeTumor().ifPresent(tumor -> deleteBucket(Run.from(metadata, arguments).id()));
            cleanupSample(metadata.reference());
            metadata.maybeTumor().ifPresent(this::cleanupSample);
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void cleanupSample(final SingleSampleRunMetadata metadata) {
        Run run = Run.from(metadata, arguments);
        deleteBucket(run.id());
        deleteStagingDirectory(metadata);
    }

    private void deleteStagingDirectory(final SingleSampleRunMetadata metadata) {
        GSUtil.rm(arguments.cloudSdkPath(), arguments.outputBucket() + "/" + RunTag.apply(arguments, metadata.barcode()));
    }

    private void deleteBucket(final String runId) {
        GSUtil.rm(arguments.cloudSdkPath(), runId);
    }
}