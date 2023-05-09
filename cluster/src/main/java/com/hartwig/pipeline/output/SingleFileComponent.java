package com.hartwig.pipeline.output;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.storage.RuntimeBucket;

public class SingleFileComponent implements OutputComponent {

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
    public void addToOutput(final Storage storage, final Bucket outputBucket, final String setName) {
        runtimeBucket.copyOutOf(resultsDirectory.path(sourceFileName),
                outputBucket.getName(),
                String.format("%s/%s%s/%s", setName, folder.name(), namespace, targetFileName));
    }
}
