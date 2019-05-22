package com.hartwig.pipeline.report;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;

public interface ReportComponent {

    void addToReport(final Storage storage, final Bucket reportBucket, final String setName);
}
