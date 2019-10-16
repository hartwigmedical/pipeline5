package com.hartwig.pipeline.cleanup;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.RunTag;
import com.hartwig.pipeline.alignment.Run;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Cleanup {

    private final static Logger LOGGER = LoggerFactory.getLogger(Cleanup.class);
    private final Storage storage;
    private final Arguments arguments;

    Cleanup(final Storage storage, final Arguments arguments) {
        this.storage = storage;
        this.arguments = arguments;
    }

    public void run(SomaticRunMetadata metadata) {
        if (!arguments.cleanup()) {
            return;
        }
        LOGGER.info("Cleaning up all transient resources on complete somatic pipeline run (runtime buckets and dataproc jobs)");

        metadata.maybeTumor().ifPresent(tumor -> deleteBucket(Run.from(metadata, arguments).id()));
        cleanupSample(metadata.reference());
        metadata.maybeTumor().ifPresent(this::cleanupSample);
    }

    private void cleanupSample(final SingleSampleRunMetadata metadata) {
        Run run = Run.from(metadata, arguments);
        deleteBucket(run.id());
        deleteStagingDirectory(metadata);
    }

    private void deleteStagingDirectory(final SingleSampleRunMetadata metadata) {
        Bucket stagingBucket = storage.get(arguments.patientReportBucket());
        for (Blob blob : stagingBucket.list(Storage.BlobListOption.prefix(RunTag.apply(arguments, metadata.sampleId()))).iterateAll()) {
            blob.delete();
        }
    }

    private void deleteBucket(final String runId) {
        Bucket bucket = storage.get(runId);
        if (bucket != null) {
            LOGGER.debug("Cleaning up runtime bucket [{}]", runId);
            for (Blob blob : bucket.list().iterateAll()) {
                blob.delete();
            }
            bucket.delete();
        }
    }
}
