package com.hartwig.pipeline.report;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageClass;
import com.hartwig.pipeline.Arguments;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipelineResultsProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(PipelineResultsProvider.class);
    private final Storage storage;
    private final Arguments arguments;
    private final String version;

    private PipelineResultsProvider(final Storage storage, final Arguments arguments, final String version) {
        this.storage = storage;
        this.arguments = arguments;
        this.version = version;
    }

    public static PipelineResultsProvider from(final Storage storage, final Arguments arguments, final String version) {
        return new PipelineResultsProvider(storage, arguments, version);
    }

    public PipelineResults get() {
        Bucket reportBucket = storage.get(arguments.outputBucket());
        if (reportBucket == null) {
            if (!arguments.publishEventsOnly()) {
                BucketInfo.Builder builder = BucketInfo.newBuilder(arguments.outputBucket())
                        .setStorageClass(StorageClass.REGIONAL)
                        .setLocation(arguments.region());
                arguments.cmek().ifPresent(builder::setDefaultKmsKeyName);
                storage.create(builder.build());
            } else {
                LOGGER.warn("Output bucket [{}] does not exist and pipeline invoked in publish-only mode", arguments.outputBucket());
            }
        }
        return new PipelineResults(version, storage, reportBucket, arguments.publishEventsOnly());
    }
}
