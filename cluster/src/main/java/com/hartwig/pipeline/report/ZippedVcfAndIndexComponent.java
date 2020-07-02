package com.hartwig.pipeline.report;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.storage.RuntimeBucket;

public class ZippedVcfAndIndexComponent implements ReportComponent {

    private final RuntimeBucket runtimeBucket;
    private final String namespace;
    private final Folder folder;
    private final String sourceVcfFileName;
    private final String targetFileName;
    private final ResultsDirectory resultsDirectory;

    public ZippedVcfAndIndexComponent(final RuntimeBucket runtimeBucket, final String namespace, final Folder folder,
            final String filename, final ResultsDirectory resultsDirectory) {
        this(runtimeBucket, namespace, folder, filename, filename, resultsDirectory);
    }

    public ZippedVcfAndIndexComponent(final RuntimeBucket runtimeBucket, final String namespace, final Folder folder,
            final String sourceFileName, final String targetFileName, final ResultsDirectory resultsDirectory) {
        this.runtimeBucket = runtimeBucket;
        this.namespace = namespace;
        this.folder = folder;
        this.sourceVcfFileName = sourceFileName;
        this.targetFileName = targetFileName;
        this.resultsDirectory = resultsDirectory;
    }

    @Override
    public void addToReport(final Storage storage, final Bucket reportBucket, final String setName) {
        runtimeBucket.copyOutOf(resultsDirectory.path(sourceVcfFileName),
                reportBucket.getName(),
                String.format("%s/%s%s/%s", setName, folder.name(), namespace, targetFileName));
        runtimeBucket.copyOutOf(resultsDirectory.path(sourceVcfFileName + ".tbi"),
                reportBucket.getName(),
                String.format("%s/%s%s/%s", setName, folder.name(), namespace, targetFileName + ".tbi"));
    }
}
