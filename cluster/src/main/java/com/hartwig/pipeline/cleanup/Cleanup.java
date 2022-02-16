package com.hartwig.pipeline.cleanup;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
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
    private final Storage storage;

    Cleanup(final Arguments arguments, final Storage storage) {
        this.arguments = arguments;
        this.storage = storage;
    }

    public void run(SomaticRunMetadata metadata) {
        if (!arguments.cleanup()) {
            return;
        }

        LOGGER.info("Cleaning up runtime storage on complete somatic pipeline run");
        metadata.maybeTumor().ifPresent(tumor -> deleteBucket(Run.from(metadata, arguments).id()));
        metadata.maybeReference().ifPresent(this::cleanupSample);
        metadata.maybeTumor().ifPresent(this::cleanupSample);
    }

    private void cleanupSample(final SingleSampleRunMetadata metadata) {
        Run run = Run.from(metadata, arguments);
        deleteBucket(run.id());
        deleteStagingDirectory(metadata);
    }

    private void deleteStagingDirectory(final SingleSampleRunMetadata metadata) {
        if (storage.get(BlobId.of(arguments.outputBucket(), RunTag.apply(arguments, metadata.barcode()))) != null) {
            GSUtil.rm(arguments.cloudSdkPath(), arguments.outputBucket() + "/" + RunTag.apply(arguments, metadata.barcode()));
        }
    }

    private void deleteBucket(final String runId) {
        if (storage.get(runId) != null) {
            GSUtil.rm(arguments.cloudSdkPath(), runId);
        }
    }
}