package com.hartwig.pipeline.report;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageClass;
import com.hartwig.pipeline.Arguments;

public class PipelineResultsProvider {

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
        Bucket reportBucket = storage.get(arguments.patientReportBucket());
        if (reportBucket == null) {
            BucketInfo.Builder builder = BucketInfo.newBuilder(arguments.patientReportBucket())
                    .setStorageClass(StorageClass.REGIONAL)
                    .setLocation(arguments.region());
            if (arguments.cmek().trim().isEmpty()) {
                throw new IllegalArgumentException("CMEK key must be used");
            }
            builder.setDefaultKmsKeyName(arguments.cmek());
            storage.create(builder.build());
        }
        return new PipelineResults(version, storage, reportBucket, arguments);
    }
}
