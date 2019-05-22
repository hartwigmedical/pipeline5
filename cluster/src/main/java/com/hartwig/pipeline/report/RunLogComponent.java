package com.hartwig.pipeline.report;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.io.RuntimeBucket;

public class RunLogComponent implements ReportComponent {

    private final RuntimeBucket runtimeBucket;
    private final String namespace;
    private final String sampleName;
    private final ResultsDirectory resultsDirectory;

    public RunLogComponent(final RuntimeBucket runtimeBucket, final String namespace, final String sampleName,
            final ResultsDirectory resultsDirectory) {
        this.runtimeBucket = runtimeBucket;
        this.namespace = namespace;
        this.sampleName = sampleName;
        this.resultsDirectory = resultsDirectory;
    }

    @Override
    public void addToReport(final Storage storage, final Bucket reportBucket, final String setName) {
        runtimeBucket.copyOutOf(resultsDirectory.path("run.log"),
                reportBucket.getName(),
                String.format("%s/%s/%s/%s", setName, sampleName, namespace, "run.log"));
    }
}
