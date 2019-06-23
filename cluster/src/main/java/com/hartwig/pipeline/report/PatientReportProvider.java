package com.hartwig.pipeline.report;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageClass;
import com.hartwig.pipeline.Arguments;

public class PatientReportProvider {

    private final Storage storage;
    private final Arguments arguments;

    private PatientReportProvider(final Storage storage, final Arguments arguments) {
        this.storage = storage;
        this.arguments = arguments;
    }

    public static PatientReportProvider from(final Storage storage, final Arguments arguments) {
        return new PatientReportProvider(storage, arguments);
    }

    public PipelineResults get() {
        Bucket reportBucket = storage.get(arguments.patientReportBucket());
        if (reportBucket == null) {
            reportBucket = storage.create(BucketInfo.newBuilder(arguments.patientReportBucket())
                    .setStorageClass(StorageClass.REGIONAL)
                    .setLocation(arguments.region())
                    .build());
        }
        return new PipelineResults(storage, reportBucket);
    }
}
