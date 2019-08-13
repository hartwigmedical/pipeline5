package com.hartwig.pipeline.report;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.common.collect.Iterables;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.RunTag;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FullSomaticResults {

    private static final Logger LOGGER = LoggerFactory.getLogger(FullSomaticResults.class);

    private final Storage storage;
    private final Arguments arguments;

    public FullSomaticResults(final Storage storage, final Arguments arguments) {
        this.storage = storage;
        this.arguments = arguments;
    }

    public void compose(SomaticRunMetadata metadata) {
        Bucket bucket = storage.get(arguments.patientReportBucket());
        copySingleSampleRun(metadata, bucket, directory(metadata.reference()));
        copySingleSampleRun(metadata, bucket, directory(metadata.tumor()));
    }

    public String directory(final SingleSampleRunMetadata metadata) {
        return RunTag.apply(arguments, metadata.sampleId());
    }

    private void copySingleSampleRun(final SomaticRunMetadata metadata, final Bucket bucket, final String directory) {
        Iterable<Blob> blobs = bucket.list(Storage.BlobListOption.prefix(directory)).iterateAll();
        LOGGER.info("Copying single sample output from [gs://{}]. Found [{}] files", directory, Iterables.size(blobs));
        for (Blob blob : blobs) {
            String pathSplit = blob.getName().substring(blob.getName().indexOf("/") + 1, blob.getName().length());
            storage.copy(Storage.CopyRequest.of(arguments.patientReportBucket(),
                    blob.getName(),
                    BlobId.of(arguments.patientReportBucket(), metadata.runName() + "/" + pathSplit))).getResult();
            blob.delete();
        }
    }
}
