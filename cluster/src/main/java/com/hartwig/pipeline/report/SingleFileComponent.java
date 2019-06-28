package com.hartwig.pipeline.report;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.io.RuntimeBucket;

public class SingleFileComponent implements ReportComponent {

    private final RuntimeBucket runtimeBucket;
    private final String namespace;
    private final Folder folder;
    private final String sourceFileName;
    private final String targetFileName;
    private final ResultsDirectory resultsDirectory;

    public SingleFileComponent(final RuntimeBucket runtimeBucket, final String namespace, final Folder folder,
            final String sourceFileName, final String targetFileName, final ResultsDirectory resultsDirectory) {
        this.runtimeBucket = runtimeBucket;
        this.namespace = namespace;
        this.folder = folder;
        this.sourceFileName = sourceFileName;
        this.targetFileName = targetFileName;
        this.resultsDirectory = resultsDirectory;
    }

    @Override
    public void addToReport(final Storage storage, final Bucket reportBucket, final String setName) {
        runtimeBucket.copyOutOf(resultsDirectory.path(sourceFileName),
                reportBucket.getName(),
                String.format("%s/%s%s/%s", setName, folder.name(), namespace, targetFileName));
    }
}
