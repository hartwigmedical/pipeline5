package com.hartwig.pipeline.report;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.io.RuntimeBucket;

public class SingleFileComponent implements ReportComponent {

    private final RuntimeBucket runtimeBucket;
    private final String namespace;
    private final String sampleName;
    private final String fileName;
    private final ResultsDirectory resultsDirectory;

    public SingleFileComponent(final RuntimeBucket runtimeBucket, final String namespace, final String sampleName, final String fileName,
            final ResultsDirectory resultsDirectory) {
        this.runtimeBucket = runtimeBucket;
        this.namespace = namespace;
        this.sampleName = sampleName;
        this.fileName = fileName;
        this.resultsDirectory = resultsDirectory;
    }

    @Override
    public void addToReport(final Storage storage, final Bucket reportBucket, final String setName) {
        runtimeBucket.copyOutOf(resultsDirectory.path(fileName),
                reportBucket.getName(),
                String.format("%s/%s/%s/%s", setName, sampleName, namespace, fileName));
    }
}
